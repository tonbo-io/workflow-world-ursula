import { createHash } from 'node:crypto';

const JSON_CONTENT_TYPE = 'application/json';
const DEFAULT_PAGE_SIZE = 1000;
const SNAPSHOT_VISIBILITY_RETRY_DELAYS_MS = [10, 25, 50] as const;
type HeaderSource = ConstructorParameters<typeof Headers>[0];

export interface UrsulaClientConfig {
  baseUrl: string;
  bucket?: string;
  token?: string;
  headers?: HeaderSource;
  fetch?: typeof globalThis.fetch;
}

export interface UrsulaRecord<T> {
  record: number;
  value: T;
}

export interface UrsulaHead {
  nextRecord: number;
  closed: boolean;
}

export interface UrsulaReadResult<T> extends UrsulaHead {
  records: UrsulaRecord<T>[];
  upToDate: boolean;
}

export interface UrsulaAppendOptions {
  /**
   * Stable identifier for one logical mutation. Retrying the same operation,
   * including after an adapter restart, is deduplicated by Ursula.
   */
  operationId: string;
  /** Current JSON-record tail observed by the reducer. */
  expectedRecord?: number;
}

export class UrsulaRequestError extends Error {
  readonly status: number;
  readonly operation: string;
  readonly nextRecord?: number;

  constructor(operation: string, response: Response, body: string) {
    super(
      `Ursula ${operation} failed: HTTP ${response.status}${body ? `: ${body}` : ''}`
    );
    this.name = 'UrsulaRequestError';
    this.status = response.status;
    this.operation = operation;
    this.nextRecord = optionalNonNegativeInteger(
      response.headers,
      'stream-record-next'
    );
  }
}

function nonNegativeInteger(
  headers: Headers,
  name: string,
  fallback: number
): number {
  const value = optionalNonNegativeInteger(headers, name);
  return value ?? fallback;
}

function optionalNonNegativeInteger(
  headers: Headers,
  name: string
): number | undefined {
  const raw = headers.get(name);
  if (raw === null) return undefined;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid ${name} header from Ursula: ${raw}`);
  }
  return value;
}

function producerId(operationId: string): string {
  return `workflow-op-${createHash('sha256')
    .update(operationId)
    .digest('base64url')}`;
}

function jsonReplacer(
  this: Record<string, unknown>,
  key: string,
  value: unknown
): unknown {
  // Buffer.toJSON runs before a JSON replacer, so inspect the original
  // property as well as the already-transformed value. Treating an arbitrary
  // `{type:"Buffer",data:[...]}` object as binary would corrupt user JSON.
  const original = this[key];
  if (original instanceof Uint8Array) {
    return {
      __type: 'Uint8Array',
      data: Buffer.from(original).toString('base64'),
    };
  }
  return value;
}

function jsonReviver(_key: string, value: unknown): unknown {
  if (
    typeof value === 'object' &&
    value !== null &&
    (value as { __type?: unknown }).__type === 'Uint8Array' &&
    typeof (value as { data?: unknown }).data === 'string'
  ) {
    return new Uint8Array(
      Buffer.from((value as { data: string }).data, 'base64')
    );
  }
  return value;
}

export function stringifyUrsulaJson(value: unknown): string {
  return JSON.stringify(value, jsonReplacer);
}

export function parseUrsulaJson<T>(value: string): T {
  return JSON.parse(value, jsonReviver) as T;
}

function parseRecords<T>(body: string): UrsulaRecord<T>[] {
  if (!body.trim()) return [];
  return body
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const parsed = parseUrsulaJson<Partial<UrsulaRecord<T>>>(line);
      if (
        !Number.isSafeInteger(parsed.record) ||
        (parsed.record as number) < 0 ||
        !Object.hasOwn(parsed, 'value')
      ) {
        throw new Error('Invalid record envelope returned by Ursula');
      }
      return parsed as UrsulaRecord<T>;
    });
}

/**
 * Low-level record client shared by Ursula-backed World subsystems.
 *
 * It intentionally uses one deterministic producer identity per logical
 * operation. Producer state therefore does not need to survive in adapter
 * memory: an ambiguous HTTP result can be retried after process restart and
 * Ursula returns the original committed record range before evaluating the
 * now-stale record-tail precondition.
 */
export class UrsulaClient {
  readonly bucket: string;
  readonly baseUrl: string;
  private readonly ensuredStreams = new Set<string>();
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly token?: string;
  private readonly defaultHeaders?: HeaderSource;

  constructor(config: UrsulaClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, '');
    this.bucket = config.bucket ?? 'workflow';
    this.fetchImpl = config.fetch ?? globalThis.fetch;
    this.token = config.token;
    this.defaultHeaders = config.headers;
    if (!this.baseUrl) throw new Error('Ursula baseUrl is required');
    if (!this.bucket) throw new Error('Ursula bucket is required');
  }

  streamUrl(stream: string): URL {
    return new URL(
      `${this.baseUrl}/${encodeURIComponent(this.bucket)}/${encodeURIComponent(stream)}`
    );
  }

  private headers(extra?: HeaderSource): Headers {
    const headers = new Headers(this.defaultHeaders);
    if (this.token) headers.set('authorization', `Bearer ${this.token}`);
    new Headers(extra).forEach((value, key) => {
      headers.set(key, value);
    });
    return headers;
  }

  private async success(
    operation: string,
    response: Response
  ): Promise<Response> {
    if (response.ok) return response;
    throw new UrsulaRequestError(operation, response, await response.text());
  }

  async ensureJsonStream(stream: string): Promise<void> {
    if (this.ensuredStreams.has(stream)) return;
    await this.success(
      'create stream',
      await this.fetchImpl(this.streamUrl(stream), {
        method: 'PUT',
        headers: this.headers({ 'content-type': JSON_CONTENT_TYPE }),
      })
    );
    this.ensuredStreams.add(stream);
  }

  async head(stream: string): Promise<UrsulaHead> {
    const response = await this.success(
      'head stream',
      await this.fetchImpl(this.streamUrl(stream), {
        method: 'HEAD',
        headers: this.headers(),
      })
    );
    return {
      nextRecord: nonNegativeInteger(response.headers, 'stream-record-next', 0),
      closed: response.headers.get('stream-closed') === 'true',
    };
  }

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
    const records = Array.isArray(values) ? values : [values];
    if (records.length === 0) {
      throw new Error('Ursula append requires at least one record');
    }
    await this.ensureJsonStream(stream);
    const headers = this.headers({
      'content-type': JSON_CONTENT_TYPE,
      'producer-id': producerId(options.operationId),
      'producer-epoch': '0',
      'producer-seq': '0',
    });
    if (options.expectedRecord !== undefined) {
      headers.set('stream-record-match', String(options.expectedRecord));
    }
    const response = await this.success(
      'append records',
      await this.fetchImpl(this.streamUrl(stream), {
        method: 'POST',
        headers,
        body: stringifyUrsulaJson(records.length === 1 ? records[0] : records),
      })
    );
    return {
      startRecord: nonNegativeInteger(
        response.headers,
        'stream-record-start',
        options.expectedRecord ?? 0
      ),
      nextRecord: nonNegativeInteger(
        response.headers,
        'stream-record-next',
        (options.expectedRecord ?? 0) + records.length
      ),
    };
  }

  async read<T>(
    stream: string,
    start = 0,
    limit = DEFAULT_PAGE_SIZE
  ): Promise<UrsulaReadResult<T>> {
    const url = this.streamUrl(stream);
    url.searchParams.set('record', String(start));
    url.searchParams.set('max_records', String(limit));
    url.searchParams.set('record_view', 'envelope');
    const response = await this.fetchImpl(url, { headers: this.headers() });
    if (response.status !== 204) await this.success('read records', response);
    return {
      records:
        response.status === 204 ? [] : parseRecords<T>(await response.text()),
      nextRecord: nonNegativeInteger(
        response.headers,
        'stream-record-next',
        start
      ),
      closed: response.headers.get('stream-closed') === 'true',
      upToDate: response.headers.get('stream-up-to-date') === 'true',
    };
  }

  async readTail<T>(stream: string, count = 1): Promise<UrsulaReadResult<T>> {
    if (!Number.isSafeInteger(count) || count < 1) {
      throw new Error('Ursula tail record count must be a positive integer');
    }
    const url = this.streamUrl(stream);
    url.searchParams.set('tail_records', String(count));
    url.searchParams.set('record_view', 'envelope');
    const response = await this.fetchImpl(url, { headers: this.headers() });
    if (response.status !== 204)
      await this.success('read tail records', response);
    const records =
      response.status === 204 ? [] : parseRecords<T>(await response.text());
    return {
      records,
      nextRecord: nonNegativeInteger(
        response.headers,
        'stream-record-next',
        (records.at(-1)?.record ?? -1) + 1
      ),
      closed: response.headers.get('stream-closed') === 'true',
      upToDate: response.headers.get('stream-up-to-date') === 'true',
    };
  }

  async waitForRecords<T>(
    stream: string,
    start: number,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<UrsulaReadResult<T>> {
    const url = this.streamUrl(stream);
    url.searchParams.set('record', String(start));
    url.searchParams.set('max_records', String(DEFAULT_PAGE_SIZE));
    url.searchParams.set('record_view', 'envelope');
    url.searchParams.set('live', 'long-poll');
    url.searchParams.set('timeout_ms', String(timeoutMs));
    const response = await this.fetchImpl(url, {
      headers: this.headers(),
      signal,
    });
    if (response.status !== 204)
      await this.success('wait for records', response);
    return {
      records:
        response.status === 204 ? [] : parseRecords<T>(await response.text()),
      nextRecord: nonNegativeInteger(
        response.headers,
        'stream-record-next',
        start
      ),
      closed: response.headers.get('stream-closed') === 'true',
      upToDate: response.headers.get('stream-up-to-date') === 'true',
    };
  }

  async publishSnapshotAtRecord(
    stream: string,
    record: number,
    snapshot: unknown
  ): Promise<void> {
    const url = this.streamUrl(stream);
    url.pathname += '/snapshot';
    url.searchParams.set('record', String(record));
    for (
      let attempt = 0;
      attempt <= SNAPSHOT_VISIBILITY_RETRY_DELAYS_MS.length;
      attempt += 1
    ) {
      try {
        await this.success(
          'publish snapshot',
          await this.fetchImpl(url, {
            method: 'PUT',
            headers: this.headers({ 'content-type': JSON_CONTENT_TYPE }),
            body: stringifyUrsulaJson(snapshot),
          })
        );
        return;
      } catch (error) {
        const delayMs = SNAPSHOT_VISIBILITY_RETRY_DELAYS_MS[attempt];
        if (
          !(error instanceof UrsulaRequestError) ||
          error.status !== 400 ||
          !error.message.includes('InvalidRecordBoundaries') ||
          delayMs === undefined
        ) {
          throw error;
        }
        await new Promise<void>((resolve) => {
          setTimeout(resolve, delayMs);
        });
      }
    }
  }

  async advanceRetentionAtRecord(
    stream: string,
    record: number
  ): Promise<void> {
    const url = this.streamUrl(stream);
    url.pathname += '/retention';
    url.searchParams.set('record', String(record));
    await this.success(
      'advance retention',
      await this.fetchImpl(url, {
        method: 'PUT',
        headers: this.headers(),
      })
    );
  }

  async readAll<T>(stream: string, start = 0): Promise<UrsulaRecord<T>[]> {
    const records: UrsulaRecord<T>[] = [];
    let cursor = start;
    while (true) {
      const page = await this.read<T>(stream, cursor);
      records.push(...page.records);
      if (page.records.length < DEFAULT_PAGE_SIZE) return records;
      if (page.nextRecord <= cursor) {
        throw new Error('Ursula record pagination made no progress');
      }
      cursor = page.nextRecord;
    }
  }
}
