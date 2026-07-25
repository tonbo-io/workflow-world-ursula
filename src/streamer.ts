import { createHash, randomUUID } from 'node:crypto';
import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';

const DEFAULT_BUCKET = 'workflow';
const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;
// Leave enough headroom for gateways whose response-header timeout is 30s.
// New Ursula gateways extend their timeout for long-poll requests, but this
// default also keeps the adapter reliable with older releases and other
// reverse proxies.
const DEFAULT_LONG_POLL_TIMEOUT_MS = 25_000;
const RECORD_CONTENT_TYPE = 'application/json';
type HeaderSource = ConstructorParameters<typeof Headers>[0];

interface ChunkRecord {
  v: 1;
  data: string;
}

interface RecordEnvelope {
  record: number;
  value: unknown;
}

interface StreamHead {
  nextRecord: number;
  closed: boolean;
}

interface Producer {
  id: string;
  epoch: number;
  nextSequence: number;
  pending: Promise<void>;
}

export interface UrsulaStreamerConfig {
  /** Ursula gateway origin, for example `https://ursula.example.com`. */
  baseUrl: string;
  /** Durable Streams bucket used for Workflow-owned streams. */
  bucket?: string;
  /** Optional bearer token passed to the Ursula gateway. */
  token?: string;
  /** Additional headers applied to every Ursula request. */
  headers?: HeaderSource;
  /** Long-poll duration used by live Workflow stream readers. */
  longPollTimeoutMs?: number;
  /**
   * Time Workflow waits to coalesce adjacent chunks before calling
   * `writeMulti`. Set to `0` for immediate flushes.
   */
  streamFlushIntervalMs?: number;
  /** Fetch implementation override for tests or custom transports. */
  fetch?: typeof globalThis.fetch;
}

export class UrsulaStreamError extends Error {
  readonly status: number;
  readonly operation: string;

  constructor(operation: string, response: Response, body: string) {
    super(
      `Ursula ${operation} failed: HTTP ${response.status}${body ? `: ${body}` : ''}`
    );
    this.name = 'UrsulaStreamError';
    this.status = response.status;
    this.operation = operation;
  }
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value);
}

function parseNonNegativeInteger(
  headers: Headers,
  name: string,
  fallback = 0
): number {
  const raw = headers.get(name);
  if (raw === null) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid ${name} header from Ursula: ${raw}`);
  }
  return value;
}

function encodeCursor(index: number): string {
  return Buffer.from(JSON.stringify({ index })).toString('base64url');
}

function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const decoded = JSON.parse(
      Buffer.from(cursor, 'base64url').toString('utf8')
    ) as { index?: unknown };
    if (
      typeof decoded.index === 'number' &&
      Number.isSafeInteger(decoded.index) &&
      decoded.index >= 0
    ) {
      return decoded.index;
    }
  } catch {
    // Fall through to the contract error below.
  }
  throw new Error('Invalid Ursula Workflow stream cursor');
}

function normalizeLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_PAGE_SIZE;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_SIZE) {
    throw new Error(
      `Workflow stream chunk limit must be between 1 and ${MAX_PAGE_SIZE}`
    );
  }
  return limit;
}

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function encodeChunk(chunk: string | Uint8Array): ChunkRecord {
  return {
    v: 1,
    data: Buffer.from(toBytes(chunk)).toString('base64'),
  };
}

function isChunkRecord(value: unknown): value is ChunkRecord {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Partial<ChunkRecord>;
  return record.v === 1 && typeof record.data === 'string';
}

function decodeChunk(value: unknown): Uint8Array {
  if (!isChunkRecord(value)) {
    throw new Error('Invalid Workflow chunk record returned by Ursula');
  }
  return Uint8Array.from(Buffer.from(value.data, 'base64'));
}

function parseEnvelopeLines(body: string): RecordEnvelope[] {
  if (!body.trim()) return [];
  return body
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => {
      const parsed = JSON.parse(line) as Partial<RecordEnvelope>;
      if (
        !Number.isSafeInteger(parsed.record) ||
        (parsed.record as number) < 0 ||
        !Object.hasOwn(parsed, 'value')
      ) {
        throw new Error('Invalid record envelope returned by Ursula');
      }
      return parsed as RecordEnvelope;
    });
}

function enqueueChunkRecords(
  controller: ReadableStreamDefaultController<Uint8Array>,
  records: RecordEnvelope[],
  cursor: number
): number {
  let nextCursor = cursor;
  for (const record of records) {
    controller.enqueue(decodeChunk(record.value));
    nextCursor = record.record + 1;
  }
  return nextCursor;
}

/**
 * Maps Workflow's chunk-oriented Streamer contract onto Ursula JSON record
 * coordinates. Every Workflow chunk is one Ursula JSON record. The record
 * ordinal is therefore the Workflow chunk index without a secondary index.
 */
export function createStreamer(config: UrsulaStreamerConfig): Streamer {
  const fetchImpl = config.fetch ?? globalThis.fetch;
  const bucket = config.bucket ?? DEFAULT_BUCKET;
  const baseUrl = config.baseUrl.replace(/\/+$/, '');
  const longPollTimeoutMs =
    config.longPollTimeoutMs ?? DEFAULT_LONG_POLL_TIMEOUT_MS;
  const producerByStream = new Map<string, Producer>();
  const registeredStreams = new Set<string>();
  const registeringStreams = new Map<string, Promise<void>>();
  const ensuredStreams = new Set<string>();
  const ensuringStreams = new Map<string, Promise<void>>();

  if (!baseUrl) throw new Error('Ursula baseUrl is required');
  if (!bucket) throw new Error('Ursula bucket is required');
  if (!Number.isSafeInteger(longPollTimeoutMs) || longPollTimeoutMs < 1) {
    throw new Error('Ursula longPollTimeoutMs must be a positive integer');
  }

  function headers(extra?: HeaderSource): Headers {
    const result = new Headers(config.headers);
    if (config.token) result.set('authorization', `Bearer ${config.token}`);
    if (extra) {
      new Headers(extra).forEach((value, key) => {
        result.set(key, value);
      });
    }
    return result;
  }

  function streamId(runId: string, name: string): string {
    const nameHash = createHash('sha256')
      .update(name)
      .digest('base64url')
      .slice(0, 32);
    return `${runId}-${nameHash}`;
  }

  function streamUrl(runId: string, name: string): URL {
    return new URL(
      `${baseUrl}/${encodePathSegment(bucket)}/${encodePathSegment(streamId(runId, name))}`
    );
  }

  function registryUrl(runId: string): URL {
    return streamUrl(runId, '__workflow_streams');
  }

  async function expectSuccess(
    operation: string,
    response: Response
  ): Promise<Response> {
    if (response.ok) return response;
    throw new UrsulaStreamError(operation, response, await response.text());
  }

  async function ensureStream(url: URL): Promise<void> {
    const key = url.toString();
    if (ensuredStreams.has(key)) return;
    const pending = ensuringStreams.get(key);
    if (pending) return pending;
    const ensuring = (async () => {
      await expectSuccess(
        'create stream',
        await fetchImpl(url, {
          method: 'PUT',
          headers: headers({ 'content-type': RECORD_CONTENT_TYPE }),
        })
      );
      ensuredStreams.add(key);
    })().finally(() => {
      ensuringStreams.delete(key);
    });
    ensuringStreams.set(key, ensuring);
    return ensuring;
  }

  async function registerStream(runId: string, name: string): Promise<void> {
    if (name === '__workflow_streams') return;
    const cacheKey = `${runId}\0${name}`;
    if (registeredStreams.has(cacheKey)) return;
    const pending = registeringStreams.get(cacheKey);
    if (pending) return pending;
    const registering = (async () => {
      const url = registryUrl(runId);
      await ensureStream(url);
      await expectSuccess(
        'register stream',
        await fetchImpl(url, {
          method: 'POST',
          headers: headers({ 'content-type': RECORD_CONTENT_TYPE }),
          body: JSON.stringify({ name }),
        })
      );
      registeredStreams.add(cacheKey);
    })().finally(() => {
      registeringStreams.delete(cacheKey);
    });
    registeringStreams.set(cacheKey, registering);
    return registering;
  }

  function producerFor(key: string): Producer {
    let producer = producerByStream.get(key);
    if (!producer) {
      producer = {
        id: `workflow-${randomUUID()}`,
        epoch: 0,
        nextSequence: 0,
        pending: Promise.resolve(),
      };
      producerByStream.set(key, producer);
    }
    return producer;
  }

  async function serializeStream<T>(
    url: URL,
    operation: (producer: Producer) => Promise<T>
  ): Promise<T> {
    const producer = producerFor(url.toString());
    const previous = producer.pending;
    let release: () => void = () => {};
    producer.pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation(producer);
    } finally {
      release();
    }
  }

  async function append(
    runId: string,
    name: string,
    records: ChunkRecord[]
  ): Promise<void> {
    if (records.length === 0) return;
    const url = streamUrl(runId, name);
    await serializeStream(url, async (producer) => {
      await Promise.all([ensureStream(url), registerStream(runId, name)]);
      const sequence = producer.nextSequence;
      await expectSuccess(
        'append stream',
        await fetchImpl(url, {
          method: 'POST',
          headers: headers({
            'content-type': RECORD_CONTENT_TYPE,
            'producer-id': producer.id,
            'producer-epoch': String(producer.epoch),
            'producer-seq': String(sequence),
          }),
          body: JSON.stringify(records.length === 1 ? records[0] : records),
        })
      );
      producer.nextSequence += 1;
    });
  }

  async function head(runId: string, name: string): Promise<StreamHead> {
    const response = await fetchImpl(streamUrl(runId, name), {
      method: 'HEAD',
      headers: headers(),
    });
    await expectSuccess('head stream', response);
    return {
      nextRecord: parseNonNegativeInteger(
        response.headers,
        'stream-record-next'
      ),
      closed: response.headers.get('stream-closed') === 'true',
    };
  }

  async function readRecords(args: {
    runId: string;
    name: string;
    start: number;
    limit: number;
    live?: boolean;
    signal?: AbortSignal;
  }): Promise<{
    records: RecordEnvelope[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    const url = streamUrl(args.runId, args.name);
    url.searchParams.set('record', String(args.start));
    url.searchParams.set('max_records', String(args.limit));
    url.searchParams.set('record_view', 'envelope');
    if (args.live) {
      url.searchParams.set('live', 'long-poll');
      url.searchParams.set('timeout_ms', String(longPollTimeoutMs));
    }

    const response = await fetchImpl(url, {
      headers: headers(),
      signal: args.signal,
    });
    if (response.status !== 204) {
      await expectSuccess('read stream', response);
    }
    const body = response.status === 204 ? '' : await response.text();
    return {
      records: parseEnvelopeLines(body),
      nextRecord: parseNonNegativeInteger(
        response.headers,
        'stream-record-next',
        args.start
      ),
      closed: response.headers.get('stream-closed') === 'true',
      upToDate: response.headers.get('stream-up-to-date') === 'true',
    };
  }

  return {
    ...(config.streamFlushIntervalMs !== undefined && {
      streamFlushIntervalMs: config.streamFlushIntervalMs,
    }),
    streams: {
      async write(runId, name, chunk) {
        await append(await runId, name, [encodeChunk(chunk)]);
      },

      async writeMulti(runId, name, chunks) {
        if (chunks.length === 0) return;
        await append(await runId, name, chunks.map(encodeChunk));
      },

      async close(runId, name) {
        const resolvedRunId = await runId;
        const url = streamUrl(resolvedRunId, name);
        await serializeStream(url, async () => {
          await ensureStream(url);
          await registerStream(resolvedRunId, name);
          const response = await fetchImpl(url, {
            method: 'POST',
            headers: headers({ 'stream-closed': 'true' }),
          });
          if (
            response.status === 409 &&
            response.headers.get('stream-closed') === 'true'
          ) {
            return;
          }
          await expectSuccess('close stream', response);
        });
      },

      async list(runId) {
        const url = registryUrl(runId);
        await ensureStream(url);

        const names = new Set<string>();
        let nextRecord = 0;
        while (true) {
          const response = await readRecords({
            runId,
            name: '__workflow_streams',
            start: nextRecord,
            limit: MAX_PAGE_SIZE,
          });
          for (const envelope of response.records) {
            const value = envelope.value as { name?: unknown };
            if (typeof value.name === 'string') names.add(value.name);
          }
          if (response.records.length < MAX_PAGE_SIZE) break;
          if (response.nextRecord === nextRecord)
            throw new Error(
              'Ursula stream registry pagination made no progress'
            );
          nextRecord = response.nextRecord;
        }
        return [...names];
      },

      async getChunks(
        runId: string,
        name: string,
        options?: GetChunksOptions
      ): Promise<StreamChunksResponse> {
        const limit = normalizeLimit(options?.limit);
        const start = decodeCursor(options?.cursor);
        const result = await readRecords({
          runId,
          name,
          start,
          limit: Math.min(limit + 1, MAX_PAGE_SIZE),
        });
        const page = result.records.slice(0, limit);
        const nextIndex = start + page.length;
        let hasMore =
          result.records.length > limit || result.nextRecord > nextIndex;
        if (!hasMore && page.length === limit) {
          hasMore = (await head(runId, name)).nextRecord > nextIndex;
        }
        return {
          data: page.map((record) => ({
            index: record.record,
            data: decodeChunk(record.value),
          })),
          cursor: hasMore ? encodeCursor(nextIndex) : null,
          hasMore,
          done: result.closed && !hasMore && result.upToDate,
        };
      },

      async getInfo(runId, name): Promise<StreamInfoResponse> {
        let info: StreamHead;
        try {
          info = await head(runId, name);
        } catch (error) {
          if (error instanceof UrsulaStreamError && error.status === 404) {
            return { tailIndex: -1, done: false };
          }
          throw error;
        }
        return {
          tailIndex: info.nextRecord - 1,
          done: info.closed,
        };
      },

      async get(runId, name, startIndex = 0) {
        // Workflow can open a readable output stream before the first
        // invocation writes a chunk. Ursula reads require the stream to
        // exist. Register it at the same time so a later first write only pays
        // for the append in its latency-sensitive path.
        await Promise.all([
          ensureStream(streamUrl(runId, name)),
          registerStream(runId, name),
        ]);
        let cursor =
          startIndex < 0
            ? Math.max(0, (await head(runId, name)).nextRecord + startIndex)
            : startIndex;
        const abortController = new AbortController();
        let cancelled = false;

        return new ReadableStream<Uint8Array>({
          start(controller) {
            const poll = async (): Promise<boolean> => {
              const result = await readRecords({
                runId,
                name,
                start: cursor,
                limit: MAX_PAGE_SIZE,
                live: true,
                signal: abortController.signal,
              });
              cursor = enqueueChunkRecords(controller, result.records, cursor);
              if (result.closed && result.upToDate) {
                controller.close();
                return false;
              }
              // A long-poll timeout may report the current stream tail even
              // though it returned no record bodies. Advancing to that header
              // would skip the records on the next poll. Only delivered
              // envelopes are allowed to move a live reader's cursor.
              return true;
            };

            const pump = async () => {
              try {
                while (!cancelled && (await poll())) {}
              } catch (error) {
                if (!cancelled) controller.error(error);
              }
            };
            void pump();
          },
          cancel() {
            cancelled = true;
            abortController.abort();
          },
        });
      },
    },
  };
}
