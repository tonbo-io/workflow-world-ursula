import type { Event, Step, WorkflowRun } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import {
  parseUrsulaJson,
  stringifyUrsulaJson,
  type UrsulaAppendOptions,
  UrsulaClient,
  UrsulaRequestError,
  type UrsulaRecord,
} from './client.js';
import { RunJournal } from './run-journal.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

class MemoryClient {
  checkpointGate?: Promise<void>;
  failNextReadAt?: number;
  failedReads = 0;
  readonly readAllStarts: Array<{ stream: string; start: number }> = [];
  readonly tailReads: string[] = [];
  readonly retainedRecords: Array<{ stream: string; record: number }> = [];
  private readonly streams = new Map<string, unknown[]>();

  clearRunStreams(): void {
    for (const stream of this.streams.keys()) {
      if (
        stream.startsWith('run-') &&
        !stream.startsWith('run-checkpoint-')
      ) {
        this.streams.set(stream, []);
      }
    }
  }

  runRecordValues(): unknown[] {
    return [...this.streams.entries()]
      .filter(
        ([stream]) =>
          stream.startsWith('run-') &&
          !stream.startsWith('run-checkpoint-')
      )
      .flatMap(([, values]) => values);
  }

  roundTripRunRecordsThroughJson(): void {
    for (const [stream, values] of this.streams) {
      if (
        stream.startsWith('run-') &&
        !stream.startsWith('run-checkpoint-')
      ) {
        this.streams.set(
          stream,
          values.map((value) =>
            parseUrsulaJson(stringifyUrsulaJson(value))
          )
        );
      }
    }
  }

  async ensureJsonStream(stream: string): Promise<void> {
    if (!this.streams.has(stream)) this.streams.set(stream, []);
  }

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
    if (stream.startsWith('run-checkpoint-') && this.checkpointGate) {
      await this.checkpointGate;
    }
    const current = this.streams.get(stream) ?? [];
    if (
      options.expectedRecord !== undefined &&
      options.expectedRecord !== current.length
    ) {
      throw new Error('test CAS mismatch');
    }
    const records = Array.isArray(values) ? values : [values];
    const startRecord = current.length;
    current.push(...records);
    this.streams.set(stream, current);
    return { startRecord, nextRecord: current.length };
  }

  async readAll<T>(stream: string, start = 0): Promise<UrsulaRecord<T>[]> {
    this.readAllStarts.push({ stream, start });
    return (this.streams.get(stream) ?? [])
      .slice(start)
      .map((value, index) => ({
        record: start + index,
        value: value as T,
      }));
  }

  async readTail<T>(
    stream: string,
    count = 1
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    this.tailReads.push(stream);
    const values = this.streams.get(stream) ?? [];
    const start = Math.max(0, values.length - count);
    return {
      records: values.slice(start).map((value, index) => ({
        record: start + index,
        value: value as T,
      })),
      nextRecord: values.length,
      closed: false,
      upToDate: true,
    };
  }

  async read<T>(
    stream: string,
    start = 0,
    limit = 1000
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    const values = this.streams.get(stream) ?? [];
    if (this.failNextReadAt === start) {
      this.failNextReadAt = undefined;
      this.failedReads += 1;
      throw new UrsulaRequestError(
        'read records',
        response(`record ${start} is beyond record tail ${values.length - 1}`, {
          status: 400,
        }),
        `InvalidRecordBoundaries: record ${start} is beyond local tail`
      );
    }
    if (start > values.length) {
      throw new UrsulaRequestError(
        'read records',
        response('InvalidRecordBoundaries', { status: 400 }),
        'InvalidRecordBoundaries'
      );
    }
    const records = values
      .slice(start, start + limit)
      .map((value, index) => ({
        record: start + index,
        value: value as T,
      }));
    return {
      records,
      nextRecord: start + records.length,
      closed: false,
      upToDate: true,
    };
  }

  async publishSnapshotAtRecord(
    _stream: string,
    _record: number,
    _snapshot: unknown
  ): Promise<void> {}

  async advanceRetentionAtRecord(
    stream: string,
    record: number
  ): Promise<void> {
    this.retainedRecords.push({ stream, record });
  }
}

describe('RunJournal', () => {
  it('starts a new mutation and reuses its materialization without cold reads', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);

    const initial = await journal.loadForMutation('wrun_hot', {
      assumeEmpty: true,
      createIfMissing: true,
    });
    await journal.append(initial, {
      operationId: 'hot-1',
      events: [],
    });
    const next = await journal.loadForMutation('wrun_hot');

    expect(next.nextRecord).toBe(1);
    expect(client.tailReads).toEqual([]);
    expect(client.readAllStarts).toEqual([]);
  });

  it('serves events through a known commit without an empty tail read', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const state = await journal.loadForMutation('wrun_events_hot', {
      assumeEmpty: true,
      createIfMissing: true,
    });
    const createdAt = new Date('2026-07-25T00:00:00.000Z');
    const event = {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_hot',
        workflowName: 'workflow//test//hot',
        input: Uint8Array.from([1]),
      },
      runId: 'wrun_events_hot',
      eventId: 'evnt_01K00000000000000000000001',
      createdAt,
      specVersion: 5,
    } satisfies Event;
    await journal.append(state, {
      operationId: 'hot-events-1',
      events: [event],
    });
    client.failNextReadAt = 1;

    await expect(journal.events('wrun_events_hot', 1)).resolves.toEqual([
      event,
    ]);
    expect(client.failedReads).toBe(0);
  });

  it('slices a hot event page without copying the preceding history', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const state = await journal.loadForMutation('wrun_events_page', {
      assumeEmpty: true,
      createIfMissing: true,
    });
    const events = [1, 2, 3].map(
      (index) =>
        ({
          eventType: 'run_cancelled',
          eventData: {},
          runId: 'wrun_events_page',
          eventId: `evnt_01K0000000000000000000000${index}`,
          createdAt: new Date('2026-07-25T00:00:00.000Z'),
          specVersion: 5,
        }) satisfies Event
    );
    await journal.append(state, {
      operationId: 'hot-events-page-1',
      events,
    });
    client.failNextReadAt = 1;

    await expect(
      journal.eventPage('wrun_events_page', 1, 1, 1)
    ).resolves.toEqual({
      events: [events[1]],
      total: 3,
    });
    expect(client.failedReads).toBe(0);
  });

  it('commits an event and its resulting run state in one guarded record', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response('missing', { status: 404 }))
      .mockResolvedValueOnce(response('', { status: 200 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: {
            'stream-record-start': '0',
            'stream-record-next': '1',
          },
        })
      )
      .mockResolvedValueOnce(
        response(null, {
          status: 204,
          headers: {
            'stream-record-next': '1',
            'stream-up-to-date': 'true',
          },
        })
      );
    const journal = new RunJournal(
      new UrsulaClient({ baseUrl: 'https://ursula.test', fetch })
    );
    const state = await journal.load('wrun_1');
    const createdAt = new Date('2026-07-24T00:00:00.000Z');
    const event = {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_1',
        workflowName: 'workflow//test//main',
        input: Uint8Array.from([1, 2]),
      },
      runId: 'wrun_1',
      eventId: 'evnt_01K00000000000000000000000',
      createdAt,
      specVersion: 5,
    } satisfies Event;
    const run = {
      runId: 'wrun_1',
      status: 'pending',
      deploymentId: 'dpl_1',
      workflowName: 'workflow//test//main',
      input: Uint8Array.from([1, 2]),
      createdAt,
      updatedAt: createdAt,
      specVersion: 5,
      attributes: {},
    } satisfies WorkflowRun;

    await journal.append(state, {
      operationId: 'create-wrun_1',
      events: [event],
      run,
    });

    expect(await journal.events('wrun_1')).toEqual([event]);
    expect(state.run).toEqual(run);
    expect(state.nextRecord).toBe(1);
    const append = fetch.mock.calls[2];
    const headers = new Headers(append?.[1]?.headers);
    expect(append?.[1]?.method).toBe('PUT');
    expect(headers.has('stream-record-match')).toBe(false);
    const body = JSON.parse(append?.[1]?.body as string);
    expect(body.events).toHaveLength(1);
    expect(body.run.status).toBe('pending');
  });

  it('compacts an owned completed step and lets an unconditional reader rebuild it', async () => {
    const client = new MemoryClient();
    const runId = 'wrun_compact_step';
    const stepId = 'step_compact';
    const createdAt = new Date('2026-07-28T00:00:00.000Z');
    const input = Uint8Array.from([1, 2, 3]);
    const output = Uint8Array.from([4, 5, 6]);
    const events = [
      {
        eventType: 'step_created',
        correlationId: stepId,
        eventData: {
          input,
          stepName: 'step//test//compact',
        },
        runId,
        eventId: 'evnt_compact_created',
        createdAt,
        specVersion: 5,
      },
      {
        eventType: 'step_started',
        correlationId: stepId,
        eventData: {
          ownerMessageId: 'msg_compact',
          stepName: 'step//test//compact',
          workflowName: 'workflow//test//compact',
        },
        runId,
        eventId: 'evnt_compact_started',
        createdAt,
        specVersion: 5,
      },
      {
        eventType: 'step_completed',
        correlationId: stepId,
        eventData: {
          result: output,
          stepName: 'step//test//compact',
          workflowName: 'workflow//test//compact',
          eventCount: 3,
          optimizations: ['turbo'],
          stepCount: 1,
          stso: 7,
        },
        runId,
        eventId: 'evnt_compact_completed',
        createdAt,
        specVersion: 5,
      },
    ] satisfies Event[];
    const step = {
      runId,
      stepId,
      stepName: 'step//test//compact',
      status: 'completed',
      input,
      output,
      attempt: 1,
      startedAt: createdAt,
      completedAt: createdAt,
      createdAt,
      updatedAt: createdAt,
      specVersion: 5,
    } satisfies Step;
    const commit = {
      operationId: 'run-step-transaction:compact',
      events,
      steps: [{ id: stepId, value: step }],
      externalStateUpdatedAt: createdAt.getTime(),
    };
    const writer = new RunJournal(client as unknown as UrsulaClient, {
      compactCompletedStepCommits: true,
    });
    const state = await writer.loadForMutation(runId, {
      assumeEmpty: true,
      createIfMissing: true,
    });
    await writer.append(state, commit);

    const stored = client.runRecordValues()[0];
    expect(stored).toMatchObject({ v: 2 });
    expect(Array.isArray((stored as { c: unknown }).c)).toBe(true);
    expect(JSON.stringify(stored).length).toBeLessThan(
      JSON.stringify({ ...commit, version: 1, runId, previousRecord: 0 })
        .length * 0.5
    );

    client.roundTripRunRecordsThroughJson();
    const reader = new RunJournal(client as unknown as UrsulaClient);
    await expect(reader.load(runId)).resolves.toMatchObject({
      nextRecord: 1,
    });
    await expect(reader.events(runId)).resolves.toEqual(events);
    await expect(reader.load(runId)).resolves.toMatchObject({
      steps: new Map([[stepId, step]]),
    });
  });

  it('resumes cold-start materialization from the latest checkpoint', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const state = await journal.load('wrun_checkpoint');
    for (let index = 0; index < 128; index += 1) {
      await journal.append(state, {
        operationId: `commit-${index}`,
        events: [],
      });
    }
    await journal.flushCheckpoints();

    client.readAllStarts.length = 0;
    const restarted = new RunJournal(client as unknown as UrsulaClient);
    const restored = await restarted.load('wrun_checkpoint');

    expect(restored.nextRecord).toBe(128);
    expect(
      client.tailReads.some((stream) => stream.startsWith('run-checkpoint-'))
    ).toBe(true);
    expect(
      client.retainedRecords.some(
        ({ stream, record }) =>
          stream.startsWith('run-checkpoint-') && record === 0
      )
    ).toBe(true);
    expect(
      client.readAllStarts.some(
        ({ stream, start }) =>
          stream.startsWith('run-') &&
          !stream.startsWith('run-checkpoint-') &&
          start === 128
      )
    ).toBe(true);
    expect(
      client.readAllStarts.some(
        ({ stream, start }) =>
          stream.startsWith('run-') &&
          !stream.startsWith('run-checkpoint-') &&
          start === 0
      )
    ).toBe(false);
  });

  it('does not block the source mutation on checkpoint persistence', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const state = await journal.load('wrun_async_checkpoint');
    for (let index = 0; index < 127; index += 1) {
      await journal.append(state, {
        operationId: `async-commit-${index}`,
        events: [],
      });
    }
    let releaseCheckpoint: () => void = () => {};
    client.checkpointGate = new Promise<void>((resolve) => {
      releaseCheckpoint = resolve;
    });

    await expect(
      Promise.race([
        journal
          .append(state, {
            operationId: 'async-commit-127',
            events: [],
          })
          .then(() => 'committed'),
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('blocked'), 50);
        }),
      ])
    ).resolves.toBe('committed');

    releaseCheckpoint();
    await journal.flushCheckpoints();
  });

  it('retries an incremental read while a follower applies an acknowledged commit', async () => {
    const client = new MemoryClient();
    const first = new RunJournal(client as unknown as UrsulaClient);
    const firstState = await first.loadForMutation('wrun_follower_lag', {
      assumeEmpty: true,
      createIfMissing: true,
    });
    await first.append(firstState, {
      operationId: 'lag-commit-0',
      events: [],
    });

    const writer = new RunJournal(client as unknown as UrsulaClient);
    const writerState = await writer.load('wrun_follower_lag');
    await writer.append(writerState, {
      operationId: 'lag-commit-1',
      events: [],
    });
    client.failNextReadAt = 1;

    await expect(first.load('wrun_follower_lag')).resolves.toMatchObject({
      nextRecord: 2,
    });
    expect(client.failedReads).toBe(1);
  });

  it('retries an event-cache read while a follower applies an acknowledged commit', async () => {
    const client = new MemoryClient();
    const first = new RunJournal(client as unknown as UrsulaClient);
    const firstState = await first.loadForMutation('wrun_event_follower_lag', {
      assumeEmpty: true,
      createIfMissing: true,
    });
    await first.append(firstState, {
      operationId: 'event-lag-commit-0',
      events: [],
    });
    await first.events('wrun_event_follower_lag');

    const writer = new RunJournal(client as unknown as UrsulaClient);
    const writerState = await writer.load('wrun_event_follower_lag');
    await writer.append(writerState, {
      operationId: 'event-lag-commit-1',
      events: [],
    });
    client.failNextReadAt = 1;

    await expect(first.events('wrun_event_follower_lag')).resolves.toEqual([]);
    expect(client.failedReads).toBe(1);
  });

  it('rebuilds a stale hot cache when its cursor is beyond the stream tail', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const state = await journal.load('wrun_stale', {
      createIfMissing: true,
    });
    await journal.append(state, {
      operationId: 'stale-commit',
      events: [],
    });
    expect(state.nextRecord).toBe(1);

    client.clearRunStreams();
    const recovered = await journal.load('wrun_stale');

    expect(recovered.nextRecord).toBe(0);
  });

  it('bounds materialization cache and lets query scans bypass it', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    for (let index = 0; index < 257; index += 1) {
      await journal.load(`wrun_lru_${index}`, { createIfMissing: true });
    }

    client.readAllStarts.length = 0;
    await journal.load('wrun_lru_0');
    expect(
      client.readAllStarts.some(
        ({ stream }) =>
          stream.startsWith('run-') && !stream.startsWith('run-checkpoint-')
      )
    ).toBe(true);

    journal.evict('wrun_scan');
    await journal.load('wrun_scan', {
      createIfMissing: true,
      cache: false,
    });
    client.readAllStarts.length = 0;
    await journal.load('wrun_scan');
    expect(
      client.readAllStarts.some(
        ({ stream }) =>
          stream.startsWith('run-') && !stream.startsWith('run-checkpoint-')
      )
    ).toBe(true);
  });

  it('keeps a hot mutation cache warm across a cache-neutral scan append', async () => {
    const client = new MemoryClient();
    const journal = new RunJournal(client as unknown as UrsulaClient);
    const runId = 'wrun_scan_hot';
    const now = new Date('2026-07-25T00:00:00.000Z');
    const hot = await journal.load(runId, { createIfMissing: true });
    await journal.append(hot, {
      operationId: 'hot-run-start',
      events: [],
      run: {
        runId,
        deploymentId: 'dpl_scan_test',
        workflowName: 'scan-test',
        status: 'running',
        createdAt: now,
        updatedAt: now,
        startedAt: now,
        executionContext: {},
        attributes: {},
      } satisfies WorkflowRun,
    });

    const scanned = await journal.load(runId, { cache: false });
    await journal.append(
      scanned,
      {
        operationId: 'scan-cleanup',
        events: [],
      },
      { cache: false }
    );

    client.readAllStarts.length = 0;
    await journal.load(runId);
    expect(
      client.readAllStarts.some(
        ({ stream }) =>
          stream.startsWith('run-') && !stream.startsWith('run-checkpoint-')
      )
    ).toBe(false);
  });
});
