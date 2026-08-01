import { createHash } from 'node:crypto';
import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';

const DEFAULT_BUCKET = 'workflow';
const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;
const DEFAULT_STREAM_FLUSH_INTERVAL_MS = 10;
// Leave enough headroom for gateways whose response-header timeout is 30s.
// New Ursula gateways extend their timeout for long-poll requests, but this
// default also keeps the adapter reliable with older releases and other
// reverse proxies.
const DEFAULT_LONG_POLL_TIMEOUT_MS = 25_000;
const LONG_POLL_CLIENT_HEADROOM_MS = 5_000;
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
   * `writeMulti`. Defaults to `10`; set to `0` for immediate flushes.
   */
  streamFlushIntervalMs?: number;
  /** Fetch implementation override for tests or custom transports. */
  fetch?: typeof globalThis.fetch;
  /**
   * Co-locates streams owned by one run and atomically commits a stream's
   * first chunks together with its discovery registration.
   *
   * Requires Ursula's `path-affinity-v1` and
   * `group-append-transaction-v1` extensions.
   */
  experimentalGroupTransactions?: boolean;
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
  const groupTransactions = config.experimentalGroupTransactions === true;
  const producerByStream = new Map<string, Producer>();
  const registeredStreams = new Set<string>();
  const registryOperations = new Map<string, Promise<void>>();
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

  function nameHash(name: string): string {
    return createHash('sha256')
      .update(name)
      .digest('base64url')
      .slice(0, 32);
  }

  function streamId(runId: string, name: string): string {
    if (groupTransactions) {
      return name === '__workflow_streams'
        ? 'stream-registry'
        : `stream-${nameHash(name)}`;
    }
    return `${runId}-${nameHash(name)}`;
  }

  function streamUrl(runId: string, name: string): URL {
    const affinity = groupTransactions
      ? `/${encodePathSegment(runId)}`
      : '';
    return new URL(
      `${baseUrl}/${encodePathSegment(bucket)}${affinity}/${encodePathSegment(streamId(runId, name))}`
    );
  }

  function transactionUrl(runId: string): URL {
    return new URL(
      `${baseUrl}/${encodePathSegment(bucket)}/${encodePathSegment(runId)}/$transaction`
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

  async function streamExistsAfterConflict(
    url: URL,
    response: Response
  ): Promise<boolean> {
    if (response.status !== 409) return false;
    const existing = await fetchImpl(url, {
      method: 'HEAD',
      headers: headers(),
    });
    return existing.ok;
  }

  async function ensureStream(url: URL): Promise<void> {
    const key = url.toString();
    if (ensuredStreams.has(key)) return;
    const pending = ensuringStreams.get(key);
    if (pending) return pending;
    const ensuring = (async () => {
      const created = await fetchImpl(url, {
        method: 'PUT',
        headers: headers({ 'content-type': RECORD_CONTENT_TYPE }),
      });
      if (!created.ok && !(await streamExistsAfterConflict(url, created))) {
        await expectSuccess('create stream', created);
      }
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
    const url = registryUrl(runId);
    const registryKey = url.toString();
    const previous = registryOperations.get(registryKey);
    const producerHeaders = headers({
      'content-type': RECORD_CONTENT_TYPE,
      'producer-id': `workflow-registry-${createHash('sha256')
        .update(`${bucket}\0${runId}\0${name}`)
        .digest('base64url')}`,
      'producer-epoch': '0',
      'producer-seq': '0',
    });
    const performRegistration = async () => {
      if (registeredStreams.has(cacheKey)) return;
      if (!ensuredStreams.has(registryKey)) {
        const created = await fetchImpl(url, {
          method: 'PUT',
          headers: producerHeaders,
          body: JSON.stringify({ name }),
        });
        if (created.status === 201) {
          ensuredStreams.add(registryKey);
          registeredStreams.add(cacheKey);
          return;
        }
        if (
          created.status === 200 ||
          (await streamExistsAfterConflict(url, created))
        ) {
          ensuredStreams.add(registryKey);
        } else {
          await expectSuccess('create stream registry', created);
        }
      }
      await expectSuccess(
        'register stream',
        await fetchImpl(url, {
          method: 'POST',
          headers: producerHeaders,
          body: JSON.stringify({ name }),
        })
      );
      registeredStreams.add(cacheKey);
    };
    // Start an uncontended registration immediately. Only subsequent names
    // for the same run need to queue behind the existing registry producer.
    const registering = previous
      ? previous.catch(() => undefined).then(performRegistration)
      : performRegistration();
    const tracked = registering.then(() => {
      if (registryOperations.get(registryKey) === tracked) {
        registryOperations.delete(registryKey);
      }
    });
    registryOperations.set(registryKey, tracked);
    return tracked;
  }

  function registrationProducerId(runId: string, name: string): string {
    return `workflow-registry-${createHash('sha256')
      .update(`${bucket}\0${runId}\0${name}`)
      .digest('base64url')}`;
  }

  async function appendFirstChunksAndRegister(
    runId: string,
    name: string,
    body: string,
    producer: Producer
  ): Promise<void> {
    const dataUrl = streamUrl(runId, name);
    const registry = registryUrl(runId);
    await Promise.all([ensureStream(dataUrl), ensureStream(registry)]);
    const response = await expectSuccess(
      'append stream and register it',
      await fetchImpl(transactionUrl(runId), {
        method: 'POST',
        headers: headers({ 'content-type': RECORD_CONTENT_TYPE }),
        body: JSON.stringify({
          operations: [
            {
              stream: streamId(runId, name),
              content_type: RECORD_CONTENT_TYPE,
              payload_base64: Buffer.from(body).toString('base64'),
              producer: {
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                producer_seq: producer.nextSequence,
              },
            },
            {
              stream: streamId(runId, '__workflow_streams'),
              content_type: RECORD_CONTENT_TYPE,
              payload_base64: Buffer.from(JSON.stringify({ name })).toString(
                'base64'
              ),
              producer: {
                producer_id: registrationProducerId(runId, name),
                producer_epoch: 0,
                producer_seq: 0,
              },
            },
          ],
        }),
      })
    );
    const extensions = response.headers.get('stream-extensions') ?? '';
    if (!extensions.split(',').map((value) => value.trim()).includes(
      'group-append-transaction-v1'
    )) {
      throw new Error(
        'Ursula did not advertise group-append-transaction-v1 after committing the transaction'
      );
    }
    producer.nextSequence += 1;
    registeredStreams.add(`${runId}\0${name}`);
  }

  function producerFor(key: string): Producer {
    let producer = producerByStream.get(key);
    if (!producer) {
      producer = {
        // A Workflow step may be retried in another invocation after its
        // first attempt wrote and closed the stream. Reusing the producer
        // identity and sequence lets Ursula resolve those writes through its
        // dedup table before evaluating the closed-stream check.
        id: `workflow-${createHash('sha256')
          .update(`${bucket}\0${key}`)
          .digest('base64url')}`,
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
      const cacheKey = `${runId}\0${name}`;
      const body = JSON.stringify(records.length === 1 ? records[0] : records);
      if (groupTransactions && !registeredStreams.has(cacheKey)) {
        await appendFirstChunksAndRegister(runId, name, body, producer);
        return;
      }
      // Discovery metadata and the data append target different streams (and
      // therefore potentially different Raft groups). Registration is a
      // rebuildable projection, so keep it off the latency-sensitive write
      // path. Discovery operations and close() join the retained promise.
      const registration = registerStream(runId, name);
      void registration.catch(() => undefined);
      // Let an uncontended registry operation issue its request before the
      // data request so existing request-order assertions and traces remain
      // intuitive; neither response is awaited here.
      await Promise.resolve();
      const sequence = producer.nextSequence;
      const appendHeaders = headers({
        'content-type': RECORD_CONTENT_TYPE,
        'producer-id': producer.id,
        'producer-epoch': String(producer.epoch),
        'producer-seq': String(sequence),
      });
      const key = url.toString();
      if (!ensuredStreams.has(key)) {
        const created = await fetchImpl(url, {
          method: 'PUT',
          headers: appendHeaders,
          body,
        });
        if (created.status === 201) {
          ensuredStreams.add(key);
          producer.nextSequence += 1;
          return;
        }
        if (
          created.status === 200 ||
          (await streamExistsAfterConflict(url, created))
        ) {
          ensuredStreams.add(key);
        } else {
          await expectSuccess('create stream with first chunks', created);
        }
      }
      await expectSuccess(
        'append stream',
        await fetchImpl(url, {
          method: 'POST',
          headers: appendHeaders,
          body,
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

    const requestController = args.live ? new AbortController() : undefined;
    let requestTimedOut = false;
    const abortRequest = () => requestController?.abort(args.signal?.reason);
    args.signal?.addEventListener('abort', abortRequest, { once: true });
    const requestDeadline = args.live
      ? setTimeout(() => {
          requestTimedOut = true;
          requestController?.abort();
        }, longPollTimeoutMs + LONG_POLL_CLIENT_HEADROOM_MS)
      : undefined;
    let response: Response;
    let body: string;
    try {
      response = await fetchImpl(url, {
        headers: headers(),
        signal: requestController?.signal ?? args.signal,
      });
      if (response.status !== 204) {
        await expectSuccess('read stream', response);
      }
      body = response.status === 204 ? '' : await response.text();
    } catch (error) {
      if (requestTimedOut && !args.signal?.aborted) {
        return {
          records: [],
          nextRecord: args.start,
          closed: false,
          upToDate: false,
        };
      }
      throw error;
    } finally {
      if (requestDeadline !== undefined) clearTimeout(requestDeadline);
      args.signal?.removeEventListener('abort', abortRequest);
    }
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
    streamFlushIntervalMs:
      config.streamFlushIntervalMs ?? DEFAULT_STREAM_FLUSH_INTERVAL_MS,
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
        await serializeStream(url, async (producer) => {
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
            producer.nextSequence = 0;
            return;
          }
          await expectSuccess('close stream', response);
          // Workflow may replay the whole step in the same process after its
          // state commit loses a CAS race. Restart the producer sequence so
          // writes replayed before this close resolve through Ursula's dedup
          // table instead of being treated as new writes to a closed stream.
          producer.nextSequence = 0;
        });
      },

      async list(runId) {
        const url = registryUrl(runId);
        // A live reader starts its registry write in the background so its
        // long-poll can attach as soon as the data stream exists. Preserve
        // read-your-writes for discovery by joining that work here.
        await registryOperations.get(url.toString());
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
        const streamReady = ensureStream(streamUrl(runId, name));
        const registration = registerStream(runId, name);
        // Registration is discovery metadata, not a prerequisite for reading
        // the stream. Observe failures to avoid an unhandled rejection; a
        // writer or streams.list() still joins the retained failed operation
        // and surfaces/retries it.
        void registration.catch(() => undefined);
        await streamReady;
        let cursor =
          startIndex < 0
            ? Math.max(0, (await head(runId, name)).nextRecord + startIndex)
            : startIndex;
        const abortController = new AbortController();
        let cancelled = false;

        return new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              // Keep an empty long-poll timeout inside this consumer-owned
              // promise. Workflow runtimes track reader.read()/pull promises;
              // a detached background pump can lose that invocation context
              // and leave the consumer pending forever.
              while (!cancelled) {
                const result = await readRecords({
                  runId,
                  name,
                  start: cursor,
                  limit: MAX_PAGE_SIZE,
                  live: true,
                  signal: abortController.signal,
                });
                cursor = enqueueChunkRecords(
                  controller,
                  result.records,
                  cursor
                );
                if (result.closed && result.upToDate) {
                  controller.close();
                  return;
                }
                if (result.records.length > 0) return;
              }
            } catch (error) {
              if (!cancelled) controller.error(error);
            }
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
