import type { Event, WorkflowRun } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import {
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

  async ensureJsonStream(stream: string): Promise<void> {
    if (!this.streams.has(stream)) this.streams.set(stream, []);
  }

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
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
    const append = fetch.mock.calls[3];
    const headers = new Headers(append?.[1]?.headers);
    expect(headers.get('stream-record-match')).toBe('0');
    const body = JSON.parse(append?.[1]?.body as string);
    expect(body.events).toHaveLength(1);
    expect(body.run.status).toBe('pending');
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
