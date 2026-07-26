/**
 * Storage-primitive benchmark for separating Ursula core costs from World
 * adapter amplification.
 *
 * This runner executes inside the EKS benchmark worker pool. It intentionally
 * bypasses Workflow and the World adapters, comparing equivalent durable
 * primitives:
 *
 * - one request/query to create a logical stream and persist its first payload;
 * - one request/query per append to an existing stream;
 * - persisted write-to-readable latency after a subscription is established;
 * - ordered retained replay of the same payload bytes.
 *
 * PostgreSQL live delivery uses INSERT + pg_notify in one statement. The
 * notification is only a wake-up hint: the listener reads the committed row
 * before the sample completes, matching Ursula long-poll's durable read.
 */

import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { Client, type Notification, Pool } from 'pg';
import { test } from 'vitest';
import {
  findStreamForGroup,
  raftGroupForStream,
} from './ursula-placement.js';
import {
  type BackendMetricsSnapshot,
  captureBackendMetrics,
  deriveBackendMetrics,
  diffBackendMetrics,
} from './backend-metrics.js';

type BackendKind = 'ursula' | 'postgres';

interface Distribution {
  count: number;
  min: number;
  avg: number;
  p50: number;
  p75: number;
  p90: number;
  p95: number;
  p99: number;
  max: number;
}

interface PhaseResult {
  latencyMs: Distribution;
  operations: number;
  elapsedMs: number;
  operationsPerSecond: number;
  payloadBytes: number;
  transportOperations: number;
}

interface RawResult {
  schemaVersion: 1;
  backend: BackendKind;
  backendLabel: string;
  startedAt: string;
  completedAt: string;
  config: {
    sequentialAppends: number;
    independentOperations: number;
    concurrency: number[];
    liveSamples: number;
    replayRecords: number;
    replayPayloadBytes: number;
    coldReplayBytes: number;
    ursulaGroupCount?: number;
    ursulaTargetGroup?: number;
  };
  phases: {
    createAndAppend: Record<string, PhaseResult>;
    concurrentAppend: Record<string, PhaseResult>;
    sequentialAppend: PhaseResult;
    livePersistToRead: PhaseResult;
    retainedReplay: PhaseResult;
    coldCandidateReplay?: PhaseResult;
  };
  transport: {
    requestsOrQueries: number;
    uploadedBytes: number;
    downloadedBytes: number;
  };
  backendMetrics?: {
    before: BackendMetricsSnapshot;
    after: BackendMetricsSnapshot;
    delta: Record<string, number>;
    derived: Record<string, number>;
  };
}

interface RawBackend {
  readonly kind: BackendKind;
  readonly label: string;
  initialize(): Promise<void>;
  create(stream: string): Promise<void>;
  createAndAppend(stream: string, payload: Uint8Array): Promise<void>;
  append(stream: string, payload: Uint8Array): Promise<void>;
  measureLive(stream: string, payload: Uint8Array): Promise<number>;
  replay(stream: string, expectedBytes: number): Promise<Uint8Array>;
  transport(): RawResult['transport'];
  close(): Promise<void>;
}

function envInt(name: string, fallback: number, min = 1): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < min) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return value;
}

function optionalEnvInt(name: string, min: number): number | undefined {
  const raw = process.env[name];
  if (!raw) return undefined;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < min) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return value;
}

function percentile(sorted: readonly number[], fraction: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * fraction) - 1)
  );
  return sorted[index] ?? 0;
}

function distribution(values: readonly number[]): Distribution {
  const sorted = [...values].sort((left, right) => left - right);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    min: sorted[0] ?? 0,
    avg: sorted.length === 0 ? 0 : total / sorted.length,
    p50: percentile(sorted, 0.5),
    p75: percentile(sorted, 0.75),
    p90: percentile(sorted, 0.9),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted.at(-1) ?? 0,
  };
}

function elapsedMs(start: bigint): number {
  return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function phase(
  samples: readonly number[],
  operations: number,
  elapsed: number,
  payloadBytes: number,
  transportOperations: number
): PhaseResult {
  return {
    latencyMs: distribution(samples),
    operations,
    elapsedMs: elapsed,
    operationsPerSecond: elapsed <= 0 ? 0 : (operations * 1000) / elapsed,
    payloadBytes,
    transportOperations,
  };
}

async function runBounded(
  count: number,
  concurrency: number,
  operation: (index: number) => Promise<void>
): Promise<number[]> {
  const samples = new Array<number>(count);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(count, concurrency) },
    async () => {
      for (;;) {
        const index = next;
        next += 1;
        if (index >= count) return;
        const start = process.hrtime.bigint();
        await operation(index);
        samples[index] = elapsedMs(start);
      }
    }
  );
  await Promise.all(workers);
  return samples;
}

function repeatedPayload(bytes: number): Uint8Array {
  const payload = new Uint8Array(bytes);
  for (let index = 0; index < payload.length; index += 1) {
    payload[index] = 32 + (index % 95);
  }
  return payload;
}

class StreamNameFactory {
  readonly groupCount?: number;
  readonly targetGroup?: number;
  private nonce = 0;

  constructor(
    private readonly backend: BackendKind,
    private readonly prefix: string
  ) {
    this.groupCount = optionalEnvInt('RAW_URSULA_GROUP_COUNT', 1);
    this.targetGroup = optionalEnvInt('RAW_URSULA_TARGET_GROUP', 0);
    if (this.groupCount === undefined && this.targetGroup !== undefined) {
      throw new Error(
        'RAW_URSULA_GROUP_COUNT is required with RAW_URSULA_TARGET_GROUP'
      );
    }
    if (
      this.groupCount !== undefined &&
      this.targetGroup !== undefined &&
      this.targetGroup >= this.groupCount
    ) {
      throw new Error(
        `RAW_URSULA_TARGET_GROUP ${this.targetGroup} must be below RAW_URSULA_GROUP_COUNT ${this.groupCount}`
      );
    }
  }

  name(label: string, index = 0): string {
    const base = `${this.prefix}-${label}-${index}`;
    if (
      this.backend !== 'ursula' ||
      this.groupCount === undefined ||
      this.targetGroup === undefined
    ) {
      return base;
    }
    const bucket = process.env.RAW_URSULA_BUCKET ?? 'workflow-raw-benchmark';
    const placed = findStreamForGroup(
      bucket,
      base,
      this.groupCount,
      this.targetGroup,
      this.nonce
    );
    this.nonce = placed.nextNonce;
    if (
      raftGroupForStream(bucket, placed.stream, this.groupCount) !==
      this.targetGroup
    ) {
      throw new Error('Generated Ursula stream has the wrong placement');
    }
    return placed.stream;
  }
}

class UrsulaRawBackend implements RawBackend {
  readonly kind = 'ursula' as const;
  readonly label: string;
  private readonly baseUrl: string;
  private readonly bucket: string;
  private requests = 0;
  private uploadedBytes = 0;
  private downloadedBytes = 0;

  constructor() {
    const baseUrl = process.env.RAW_URSULA_URL;
    if (!baseUrl) throw new Error('RAW_URSULA_URL is required');
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.bucket = process.env.RAW_URSULA_BUCKET ?? 'workflow-raw-benchmark';
    this.label = process.env.RAW_BENCH_LABEL ?? 'ursula-eks';
  }

  async initialize(): Promise<void> {}

  private url(stream: string): URL {
    return new URL(
      `${this.baseUrl}/${encodeURIComponent(this.bucket)}/${encodeURIComponent(stream)}`
    );
  }

  private async request(
    url: URL,
    init?: RequestInit,
    expected: readonly number[] = [200, 201, 204]
  ): Promise<Response> {
    this.requests += 1;
    if (init?.body instanceof Uint8Array) {
      this.uploadedBytes += init.body.byteLength;
    }
    const response = await fetch(url, init);
    if (!expected.includes(response.status)) {
      throw new Error(
        `Ursula ${init?.method ?? 'GET'} ${url} failed: HTTP ${response.status} ${(
          await response.text()
        ).slice(0, 300)}`
      );
    }
    return response;
  }

  async create(stream: string): Promise<void> {
    await this.request(this.url(stream), {
      method: 'PUT',
      headers: { 'content-type': 'application/octet-stream' },
    });
  }

  async createAndAppend(
    stream: string,
    payload: Uint8Array
  ): Promise<void> {
    await this.request(this.url(stream), {
      method: 'PUT',
      headers: { 'content-type': 'application/octet-stream' },
      body: Buffer.from(payload),
    });
  }

  async append(stream: string, payload: Uint8Array): Promise<void> {
    await this.request(this.url(stream), {
      method: 'POST',
      headers: { 'content-type': 'application/octet-stream' },
      body: Buffer.from(payload),
    });
  }

  async measureLive(stream: string, payload: Uint8Array): Promise<number> {
    await this.create(stream);
    const liveUrl = this.url(stream);
    liveUrl.searchParams.set('offset', '0');
    liveUrl.searchParams.set('max_bytes', String(payload.byteLength));
    liveUrl.searchParams.set('live', 'long-poll');
    liveUrl.searchParams.set('timeout_ms', '10000');
    const waiter = this.request(liveUrl);
    // Establish the server-side waiter before starting the measured write.
    await new Promise((resolve) => setTimeout(resolve, 5));
    const start = process.hrtime.bigint();
    await this.append(stream, payload);
    const response = await waiter;
    const body = new Uint8Array(await response.arrayBuffer());
    this.downloadedBytes += body.byteLength;
    if (body.byteLength !== payload.byteLength) {
      throw new Error(
        `Ursula live read returned ${body.byteLength} bytes, expected ${payload.byteLength}`
      );
    }
    return elapsedMs(start);
  }

  async replay(stream: string, expectedBytes: number): Promise<Uint8Array> {
    const chunks: Uint8Array[] = [];
    let offset = 0;
    while (offset < expectedBytes) {
      const url = this.url(stream);
      url.searchParams.set('offset', String(offset));
      url.searchParams.set(
        'max_bytes',
        String(Math.min(32 * 1024 * 1024, expectedBytes - offset))
      );
      const response = await this.request(url);
      const chunk = new Uint8Array(await response.arrayBuffer());
      this.downloadedBytes += chunk.byteLength;
      if (chunk.byteLength === 0) {
        throw new Error(
          `Ursula replay made no progress at ${offset}/${expectedBytes}`
        );
      }
      chunks.push(chunk);
      offset += chunk.byteLength;
    }
    const result = new Uint8Array(offset);
    let cursor = 0;
    for (const chunk of chunks) {
      result.set(chunk, cursor);
      cursor += chunk.byteLength;
    }
    return result;
  }

  transport(): RawResult['transport'] {
    return {
      requestsOrQueries: this.requests,
      uploadedBytes: this.uploadedBytes,
      downloadedBytes: this.downloadedBytes,
    };
  }

  async close(): Promise<void> {}
}

class PostgresRawBackend implements RawBackend {
  readonly kind = 'postgres' as const;
  readonly label: string;
  private readonly connectionString: string;
  private readonly runId: string;
  private readonly pool: Pool;
  private queries = 0;
  private uploadedBytes = 0;
  private downloadedBytes = 0;

  constructor() {
    const connectionString =
      process.env.RAW_POSTGRES_URL ??
      process.env.WORKFLOW_POSTGRES_URL ??
      process.env.DATABASE_URL;
    if (!connectionString) throw new Error('RAW_POSTGRES_URL is required');
    this.connectionString = connectionString;
    this.pool = new Pool({
      connectionString,
      max: envInt('RAW_POSTGRES_POOL_SIZE', 256),
    });
    this.runId = randomUUID();
    this.label = process.env.RAW_BENCH_LABEL ?? 'postgres-rds-multi-az';
  }

  async initialize(): Promise<void> {
    await this.query(`
      CREATE TABLE IF NOT EXISTS workflow_raw_benchmark_events (
        run_id text NOT NULL,
        stream_id text NOT NULL,
        seq bigint GENERATED ALWAYS AS IDENTITY,
        payload bytea NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (run_id, stream_id, seq)
      )
    `);
    await this.query(`
      CREATE INDEX IF NOT EXISTS workflow_raw_benchmark_events_replay
      ON workflow_raw_benchmark_events (run_id, stream_id, seq)
    `);
  }

  private async query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: unknown[]
  ) {
    this.queries += 1;
    return this.pool.query<T>(text, values);
  }

  async create(_stream: string): Promise<void> {}

  async createAndAppend(
    stream: string,
    payload: Uint8Array
  ): Promise<void> {
    await this.append(stream, payload);
  }

  async append(stream: string, payload: Uint8Array): Promise<void> {
    this.uploadedBytes += payload.byteLength;
    await this.query(
      `
        INSERT INTO workflow_raw_benchmark_events (run_id, stream_id, payload)
        VALUES ($1, $2, $3)
      `,
      [this.runId, stream, Buffer.from(payload)]
    );
  }

  async measureLive(stream: string, payload: Uint8Array): Promise<number> {
    const listener = new Client({ connectionString: this.connectionString });
    await listener.connect();
    const channel = `raw_bench_${randomUUID().replaceAll('-', '')}`;
    await listener.query(`LISTEN ${channel}`);
    this.queries += 1;
    const notification = new Promise<Notification>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`PostgreSQL notification timed out: ${channel}`)),
        10_000
      );
      timer.unref?.();
      listener.once('notification', (message) => {
        clearTimeout(timer);
        resolve(message);
      });
    });
    const start = process.hrtime.bigint();
    this.uploadedBytes += payload.byteLength;
    const inserted = await this.query<{ seq: string }>(
      `
        WITH inserted AS (
          INSERT INTO workflow_raw_benchmark_events (run_id, stream_id, payload)
          VALUES ($1, $2, $3)
          RETURNING seq
        )
        SELECT seq::text, pg_notify($4, seq::text)
        FROM inserted
      `,
      [this.runId, stream, Buffer.from(payload), channel]
    );
    const message = await notification;
    const seq = message.payload ?? inserted.rows[0]?.seq;
    if (!seq) throw new Error('PostgreSQL live append returned no sequence');
    const row = await this.query<{ payload: Buffer }>(
      `
        SELECT payload
        FROM workflow_raw_benchmark_events
        WHERE run_id = $1 AND stream_id = $2 AND seq = $3
      `,
      [this.runId, stream, seq]
    );
    await listener.end();
    const bytes = row.rows[0]?.payload;
    if (!bytes || bytes.byteLength !== payload.byteLength) {
      throw new Error('PostgreSQL live read returned the wrong payload');
    }
    this.downloadedBytes += bytes.byteLength;
    return elapsedMs(start);
  }

  async replay(stream: string, expectedBytes: number): Promise<Uint8Array> {
    const rows = await this.query<{ payload: Buffer }>(
      `
        SELECT payload
        FROM workflow_raw_benchmark_events
        WHERE run_id = $1 AND stream_id = $2
        ORDER BY seq
      `,
      [this.runId, stream]
    );
    const bytes = Buffer.concat(rows.rows.map((row) => row.payload));
    this.downloadedBytes += bytes.byteLength;
    if (bytes.byteLength !== expectedBytes) {
      throw new Error(
        `PostgreSQL replay returned ${bytes.byteLength} bytes, expected ${expectedBytes}`
      );
    }
    return bytes;
  }

  transport(): RawResult['transport'] {
    return {
      requestsOrQueries: this.queries,
      uploadedBytes: this.uploadedBytes,
      downloadedBytes: this.downloadedBytes,
    };
  }

  async close(): Promise<void> {
    await this.query(
      'DELETE FROM workflow_raw_benchmark_events WHERE run_id = $1',
      [this.runId]
    );
    await this.pool.end();
  }
}

function backendFromEnvironment(): RawBackend {
  switch (process.env.RAW_BENCH_BACKEND) {
    case 'ursula':
      return new UrsulaRawBackend();
    case 'postgres':
      return new PostgresRawBackend();
    default:
      throw new Error('RAW_BENCH_BACKEND must be ursula or postgres');
  }
}

test(
  'raw durable storage primitives',
  async () => {
    const backend = backendFromEnvironment();
    const startedAt = new Date().toISOString();
    const prefix = `raw-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const streamNames = new StreamNameFactory(backend.kind, prefix);
    const sequentialAppends = envInt('RAW_SEQUENTIAL_APPENDS', 200);
    const independentOperations = envInt('RAW_INDEPENDENT_OPERATIONS', 256);
    const concurrency = (process.env.RAW_CONCURRENCY ?? '1,32,128')
      .split(',')
      .map((raw) => Number.parseInt(raw.trim(), 10));
    if (
      concurrency.length === 0 ||
      concurrency.some((value) => !Number.isSafeInteger(value) || value < 1)
    ) {
      throw new Error(`Invalid RAW_CONCURRENCY: ${process.env.RAW_CONCURRENCY}`);
    }
    const liveSamples = envInt('RAW_LIVE_SAMPLES', 50);
    const replayRecords = envInt('RAW_REPLAY_RECORDS', 1000);
    const replayPayloadBytes = envInt('RAW_REPLAY_PAYLOAD_BYTES', 1024);
    const coldReplayBytes = envInt(
      'RAW_COLD_REPLAY_BYTES',
      10 * 1024 * 1024,
      0
    );
    const payload = repeatedPayload(envInt('RAW_APPEND_PAYLOAD_BYTES', 256));
    const createAndAppend: Record<string, PhaseResult> = {};
    const concurrentAppend: Record<string, PhaseResult> = {};
    let metricsBefore: BackendMetricsSnapshot | undefined;
    let metricsAfter: BackendMetricsSnapshot | undefined;

    await backend.initialize();
    try {
      metricsBefore = await captureBackendMetrics();

      for (const level of concurrency) {
        const transportBefore = backend.transport().requestsOrQueries;
        const phaseStart = process.hrtime.bigint();
        const samples = await runBounded(
          independentOperations,
          level,
          async (index) => {
            await backend.createAndAppend(
              streamNames.name(`independent-${level}`, index),
              payload
            );
          }
        );
        const elapsed = elapsedMs(phaseStart);
        createAndAppend[String(level)] = phase(
          samples,
          independentOperations,
          elapsed,
          independentOperations * payload.byteLength,
          backend.transport().requestsOrQueries - transportBefore
        );
      }

      for (const level of concurrency) {
        const streams = Array.from(
          { length: independentOperations },
          (_, index) => streamNames.name(`warm-append-${level}`, index)
        );
        await runBounded(independentOperations, level, async (index) => {
          await backend.create(streams[index] ?? '');
        });
        const transportBefore = backend.transport().requestsOrQueries;
        const phaseStart = process.hrtime.bigint();
        const samples = await runBounded(
          independentOperations,
          level,
          async (index) => {
            await backend.append(streams[index] ?? '', payload);
          }
        );
        const elapsed = elapsedMs(phaseStart);
        concurrentAppend[String(level)] = phase(
          samples,
          independentOperations,
          elapsed,
          independentOperations * payload.byteLength,
          backend.transport().requestsOrQueries - transportBefore
        );
      }

      const sequentialStream = streamNames.name('sequential');
      await backend.create(sequentialStream);
      const sequentialTransportBefore =
        backend.transport().requestsOrQueries;
      const sequentialStart = process.hrtime.bigint();
      const sequentialSamples = await runBounded(
        sequentialAppends,
        1,
        async () => backend.append(sequentialStream, payload)
      );
      const sequentialElapsed = elapsedMs(sequentialStart);
      const sequentialTransportOperations =
        backend.transport().requestsOrQueries - sequentialTransportBefore;

      const liveTransportBefore = backend.transport().requestsOrQueries;
      const liveStart = process.hrtime.bigint();
      const liveValues: number[] = [];
      for (let index = 0; index < liveSamples; index += 1) {
        liveValues.push(
          await backend.measureLive(streamNames.name('live', index), payload)
        );
      }
      const liveElapsed = elapsedMs(liveStart);
      const liveTransportOperations =
        backend.transport().requestsOrQueries - liveTransportBefore;

      const replayStream = streamNames.name('replay');
      const replayPayload = repeatedPayload(replayPayloadBytes);
      await backend.create(replayStream);
      for (let index = 0; index < replayRecords; index += 1) {
        await backend.append(replayStream, replayPayload);
      }
      const replayExpectedBytes = replayRecords * replayPayload.byteLength;
      const replayTransportBefore = backend.transport().requestsOrQueries;
      const replayStart = process.hrtime.bigint();
      await backend.replay(replayStream, replayExpectedBytes);
      const replayElapsed = elapsedMs(replayStart);
      const replayTransportOperations =
        backend.transport().requestsOrQueries - replayTransportBefore;

      let coldCandidateReplay: PhaseResult | undefined;
      if (backend.kind === 'ursula' && coldReplayBytes > 0) {
        const coldStream = streamNames.name('cold');
        const coldChunk = repeatedPayload(
          Math.min(256 * 1024, coldReplayBytes)
        );
        await backend.create(coldStream);
        let written = 0;
        while (written < coldReplayBytes) {
          const remaining = coldReplayBytes - written;
          const chunk =
            remaining >= coldChunk.byteLength
              ? coldChunk
              : coldChunk.subarray(0, remaining);
          await backend.append(coldStream, chunk);
          written += chunk.byteLength;
        }
        await new Promise((resolve) =>
          setTimeout(
            resolve,
            envInt('RAW_COLD_SETTLE_MS', 15_000, 0)
          )
        );
        const transportBefore = backend.transport().requestsOrQueries;
        const coldStart = process.hrtime.bigint();
        await backend.replay(coldStream, coldReplayBytes);
        const coldElapsed = elapsedMs(coldStart);
        coldCandidateReplay = phase(
          [coldElapsed],
          1,
          coldElapsed,
          coldReplayBytes,
          backend.transport().requestsOrQueries - transportBefore
        );
      }

      metricsAfter = await captureBackendMetrics();
      const delta = diffBackendMetrics(metricsBefore, metricsAfter);
      const result: RawResult = {
        schemaVersion: 1,
        backend: backend.kind,
        backendLabel: backend.label,
        startedAt,
        completedAt: new Date().toISOString(),
        config: {
          sequentialAppends,
          independentOperations,
          concurrency,
          liveSamples,
          replayRecords,
          replayPayloadBytes,
          coldReplayBytes,
          ...(streamNames.groupCount === undefined
            ? {}
            : {
                ursulaGroupCount: streamNames.groupCount,
                ursulaTargetGroup: streamNames.targetGroup,
              }),
        },
        phases: {
          createAndAppend,
          concurrentAppend,
          sequentialAppend: phase(
            sequentialSamples,
            sequentialAppends,
            sequentialElapsed,
            sequentialAppends * payload.byteLength,
            sequentialTransportOperations
          ),
          livePersistToRead: phase(
            liveValues,
            liveSamples,
            liveElapsed,
            liveSamples * payload.byteLength,
            liveTransportOperations
          ),
          retainedReplay: phase(
            [replayElapsed],
            replayRecords,
            replayElapsed,
            replayExpectedBytes,
            replayTransportOperations
          ),
          ...(coldCandidateReplay ? { coldCandidateReplay } : {}),
        },
        transport: backend.transport(),
        ...(metricsBefore && metricsAfter && delta
          ? {
              backendMetrics: {
                before: metricsBefore,
                after: metricsAfter,
                delta,
                derived: deriveBackendMetrics(delta) ?? {},
              },
            }
          : {}),
      };
      const outputFile =
        process.env.RAW_BENCH_OUTPUT_FILE ??
        `/results/raw-${backend.kind}.json`;
      fs.mkdirSync(path.dirname(outputFile), { recursive: true });
      fs.writeFileSync(outputFile, `${JSON.stringify(result, null, 2)}\n`);
      console.log(`RAW_BENCH_RESULT=${JSON.stringify(result)}`);
    } finally {
      await backend.close();
    }
  },
  envInt('RAW_BENCH_TIMEOUT_MS', 30 * 60 * 1000)
);
