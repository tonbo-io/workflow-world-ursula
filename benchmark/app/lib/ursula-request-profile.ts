type FetchInput = Parameters<typeof fetch>[0];
type FetchInit = Parameters<typeof fetch>[1];

const PROFILED_FETCH = Symbol.for('workflow.benchmark.ursulaProfiledFetch');
const REQUEST_PROFILE = Symbol.for('workflow.benchmark.ursulaRequestProfile');
const MAX_SAMPLES = 200_000;

interface ProfiledGlobal {
  [PROFILED_FETCH]?: boolean;
  [REQUEST_PROFILE]?: UrsulaRequestProfile;
}

interface Measurement {
  count: number;
  max: number;
  min: number;
  samples: number[];
  sum: number;
}

interface RequestMeasurement {
  durationMs: Measurement;
  requestBytes: Measurement;
  statuses: Map<string, number>;
}

interface Distribution {
  avg: number;
  count: number;
  max: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  sum: number;
}

export interface UrsulaRequestProfileSnapshot {
  requests: Record<
    string,
    {
      durationMs: Distribution;
      requestBytes: Distribution;
      statuses: Record<string, number>;
    }
  >;
}

function newMeasurement(): Measurement {
  return {
    count: 0,
    max: Number.NEGATIVE_INFINITY,
    min: Number.POSITIVE_INFINITY,
    samples: [],
    sum: 0,
  };
}

function record(measurement: Measurement, value: number): void {
  measurement.count += 1;
  measurement.sum += value;
  measurement.min = Math.min(measurement.min, value);
  measurement.max = Math.max(measurement.max, value);
  if (measurement.samples.length < MAX_SAMPLES) {
    measurement.samples.push(value);
  }
}

function summarize(measurement: Measurement): Distribution {
  if (measurement.count === 0) {
    return { avg: 0, count: 0, max: 0, min: 0, p50: 0, p95: 0, p99: 0, sum: 0 };
  }
  const sorted = [...measurement.samples].sort((left, right) => left - right);
  const percentile = (fraction: number): number => {
    const index = Math.min(
      sorted.length - 1,
      Math.max(0, Math.ceil(sorted.length * fraction) - 1)
    );
    return sorted[index] ?? 0;
  };
  return {
    avg: measurement.sum / measurement.count,
    count: measurement.count,
    max: measurement.max,
    min: measurement.min,
    p50: percentile(0.5),
    p95: percentile(0.95),
    p99: percentile(0.99),
    sum: measurement.sum,
  };
}

function requestUrl(input: FetchInput): URL {
  if (input instanceof URL) return new URL(input);
  if (typeof input === 'string') return new URL(input);
  return new URL(input.url);
}

function requestMethod(input: FetchInput, init: FetchInit): string {
  return (init?.method ?? (input instanceof Request ? input.method : 'GET'))
    .toUpperCase();
}

function requestHeaders(input: FetchInput, init: FetchInit): Headers {
  const headers = new Headers(input instanceof Request ? input.headers : undefined);
  new Headers(init?.headers).forEach((value, key) => headers.set(key, value));
  return headers;
}

function requestBodyBytes(init: FetchInit): number | undefined {
  const body = init?.body;
  if (body === undefined || body === null) return 0;
  if (typeof body === 'string') return Buffer.byteLength(body);
  if (body instanceof URLSearchParams) return Buffer.byteLength(body.toString());
  if (body instanceof ArrayBuffer) return body.byteLength;
  if (ArrayBuffer.isView(body)) return body.byteLength;
  if (body instanceof Blob) return body.size;
  return undefined;
}

const QUEUE_TRANSITION_TYPES = new Set([
  'acked',
  'enqueued',
  'lease_extended',
  'leased',
  'retry_scheduled',
]);

function queueTransitionSuffix(init: FetchInit): string | undefined {
  if (typeof init?.body !== 'string') return undefined;
  try {
    const value = JSON.parse(init.body) as unknown;
    const records = Array.isArray(value) ? value : [value];
    const types = records.map((record) =>
      typeof record === 'object' &&
      record !== null &&
      typeof (record as { type?: unknown }).type === 'string'
        ? (record as { type: string }).type
        : undefined
    );
    if (
      types.length === 0 ||
      types.some(
        (type) => type === undefined || !QUEUE_TRANSITION_TYPES.has(type)
      )
    ) {
      return undefined;
    }
    return types.join('_');
  } catch {
    return undefined;
  }
}

function streamKind(stream: string): string {
  if (stream === '__workflow_streams') return 'stream_registry';
  if (stream === 'registry-run-shards' || stream.startsWith('registry-runs-')) {
    return 'run_registry';
  }
  if (stream === 'registry-queues') return 'queue_registry';
  if (stream.startsWith('queue-checkpoint-')) return 'queue_checkpoint';
  if (stream.startsWith('queue-')) return 'queue_journal';
  if (stream.startsWith('run-checkpoint-')) return 'run_checkpoint';
  if (stream.startsWith('run-')) return 'run_journal';
  if (stream.startsWith('hook-')) return 'hook_claim';
  return 'chunk_stream';
}

function operation(
  url: URL,
  method: string,
  headers: Headers,
  bodyBytes: number | undefined
): string {
  const segments = url.pathname.split('/').filter(Boolean);
  const subresource = segments[2];
  if (subresource === 'append-batch') return 'append_batch';
  if (subresource === 'bootstrap') return 'bootstrap';
  if (subresource === 'snapshot') return method === 'DELETE' ? 'snapshot_delete' : 'snapshot';
  if (method === 'HEAD') return 'head';
  if (method === 'GET') {
    return url.searchParams.has('live') ? 'live_read' : 'read';
  }
  if (method === 'PUT') return bodyBytes === 0 ? 'create' : 'create_append';
  if (method === 'POST') {
    if (headers.get('stream-closed')?.toLowerCase() === 'true') {
      return bodyBytes === 0 ? 'close' : 'append_close';
    }
    return 'append';
  }
  if (method === 'DELETE') return 'delete';
  if (method === 'PATCH') return 'update';
  return method.toLowerCase();
}

export interface UrsulaRequestClassification {
  bodyBytes?: number;
  method: string;
  operation: string;
  streamKind: string;
}

export function classifyUrsulaRequest(
  input: FetchInput,
  init?: FetchInit
): UrsulaRequestClassification | undefined {
  const baseUrl = process.env.WORKFLOW_URSULA_URL;
  if (!baseUrl) return undefined;
  const url = requestUrl(input);
  if (url.origin !== new URL(baseUrl).origin) return undefined;
  const segments = url.pathname.split('/').filter(Boolean);
  if (segments.length < 2 || segments[0]?.startsWith('__ursula')) {
    return undefined;
  }
  const method = requestMethod(input, init);
  const headers = requestHeaders(input, init);
  const bodyBytes = requestBodyBytes(init);
  const kind = streamKind(decodeURIComponent(segments[1] ?? ''));
  const baseOperation = operation(url, method, headers, bodyBytes);
  return {
    bodyBytes,
    method,
    operation:
      kind === 'queue_journal' &&
      (baseOperation === 'append' || baseOperation === 'create_append')
        ? `${baseOperation}_${queueTransitionSuffix(init) ?? 'unknown'}`
        : baseOperation,
    streamKind: kind,
  };
}

export function createProfiledUrsulaFetch(
  fetchImpl: typeof globalThis.fetch,
  profile = new UrsulaRequestProfile()
): typeof globalThis.fetch {
  return async (input, init) => {
    const classification = classifyUrsulaRequest(input, init);
    if (!classification) return fetchImpl(input, init);
    const startedAt = performance.now();
    try {
      const response = await fetchImpl(input, init);
      profile.record(
        classification,
        performance.now() - startedAt,
        String(response.status)
      );
      return response;
    } catch (error) {
      profile.record(classification, performance.now() - startedAt, 'error');
      throw error;
    }
  };
}

export class UrsulaRequestProfile {
  private requests = new Map<string, RequestMeasurement>();

  record(
    classification: UrsulaRequestClassification,
    durationMs: number,
    status: string
  ): void {
    const key = `${classification.streamKind}.${classification.operation}`;
    let measurement = this.requests.get(key);
    if (!measurement) {
      measurement = {
        durationMs: newMeasurement(),
        requestBytes: newMeasurement(),
        statuses: new Map(),
      };
      this.requests.set(key, measurement);
    }
    record(measurement.durationMs, durationMs);
    if (classification.bodyBytes !== undefined) {
      record(measurement.requestBytes, classification.bodyBytes);
    }
    measurement.statuses.set(
      status,
      (measurement.statuses.get(status) ?? 0) + 1
    );
  }

  reset(): void {
    this.requests.clear();
  }

  snapshot(): UrsulaRequestProfileSnapshot {
    return {
      requests: Object.fromEntries(
        [...this.requests]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([key, measurement]) => [
            key,
            {
              durationMs: summarize(measurement.durationMs),
              requestBytes: summarize(measurement.requestBytes),
              statuses: Object.fromEntries(
                [...measurement.statuses].sort(([left], [right]) =>
                  left.localeCompare(right)
                )
              ),
            },
          ])
      ),
    };
  }
}

export function installUrsulaRequestProfile(): void {
  const state = globalThis as typeof globalThis & ProfiledGlobal;
  if (state[PROFILED_FETCH]) return;
  const profile = new UrsulaRequestProfile();
  globalThis.fetch = createProfiledUrsulaFetch(globalThis.fetch, profile);
  state[REQUEST_PROFILE] = profile;
  state[PROFILED_FETCH] = true;
}

export function getUrsulaRequestProfile(): UrsulaRequestProfile | undefined {
  return (globalThis as typeof globalThis & ProfiledGlobal)[REQUEST_PROFILE];
}
