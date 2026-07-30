import type { QueuePayload, ValidQueueName } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import {
  type UrsulaAppendOptions,
  type UrsulaClient,
  type UrsulaRecord,
  UrsulaRequestError,
} from './client.js';
import { QueueJournal, queuePartition } from './queue-journal.js';

class MemoryClient {
  readonly appendedBatchSizes: number[] = [];
  readonly readAllStarts: Array<{ stream: string; start: number }> = [];
  readonly retainedRecords: number[] = [];
  beforeNextSourceReadAll?: () => Promise<void>;
  goneReads = 0;
  loseNextAppendResponse = false;
  yieldBeforeAppend = false;
  reads = 0;
  private readonly firstRecords = new Map<string, number>();
  private readonly streams = new Map<string, unknown[]>();

  async ensureJsonStream(stream: string): Promise<void> {
    if (!this.streams.has(stream)) this.streams.set(stream, []);
  }

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
    if (this.yieldBeforeAppend) {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    const current = this.streams.get(stream) ?? [];
    if (
      options.expectedRecord !== undefined &&
      options.expectedRecord !== current.length
    ) {
      throw new UrsulaRequestError(
        'append records',
        new Response('record tail mismatch', {
          status: 412,
          headers: { 'stream-record-next': String(current.length) },
        }),
        'record tail mismatch'
      );
    }
    const records = Array.isArray(values) ? values : [values];
    this.appendedBatchSizes.push(records.length);
    const startRecord = current.length;
    current.push(...records);
    this.streams.set(stream, current);
    if (this.loseNextAppendResponse) {
      this.loseNextAppendResponse = false;
      throw new TypeError('simulated lost append response');
    }
    return { startRecord, nextRecord: current.length };
  }

  async readAll<T>(stream: string, start = 0): Promise<UrsulaRecord<T>[]> {
    this.reads += 1;
    this.readAllStarts.push({ stream, start });
    if (
      this.beforeNextSourceReadAll &&
      stream.startsWith('queue-') &&
      !stream.startsWith('queue-checkpoint-')
    ) {
      const beforeRead = this.beforeNextSourceReadAll;
      this.beforeNextSourceReadAll = undefined;
      await beforeRead();
    }
    this.assertRetained(stream, start);
    return (this.streams.get(stream) ?? [])
      .slice(start)
      .map((value, index) => ({
        record: start + index,
        value: value as T,
      }));
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
    this.reads += 1;
    this.assertRetained(stream, start);
    const records = (this.streams.get(stream) ?? [])
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

  async readTail<T>(
    stream: string,
    count = 1
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    this.reads += 1;
    const values = this.streams.get(stream) ?? [];
    const start = Math.max(
      this.firstRecords.get(stream) ?? 0,
      values.length - count
    );
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

  async publishSnapshotAtRecord(
    _stream: string,
    _record: number,
    _snapshot: unknown
  ): Promise<void> {}

  async advanceRetentionAtRecord(
    stream: string,
    record: number
  ): Promise<void> {
    this.retainedRecords.push(record);
    this.firstRecords.set(stream, record);
  }

  private assertRetained(stream: string, start: number): void {
    if (start >= (this.firstRecords.get(stream) ?? 0)) return;
    this.goneReads += 1;
    throw new UrsulaRequestError(
      'read records',
      new Response('retained', { status: 410 }),
      'retained'
    );
  }
}

const queueName = '__wkf_workflow_test' as ValidQueueName;
const payload = {
  __healthCheck: true,
  correlationId: 'health-1',
} satisfies QueuePayload;

describe('QueueJournal', () => {
  it('keeps one execution lane on one stable queue partition', () => {
    const first = queuePartition(
      { runId: 'run-one', stepId: 'step-one', attempt: 1 },
      64
    );
    const retry = queuePartition(
      { runId: 'run-one', stepId: 'step-one', attempt: 2 },
      64
    );
    const workflowLane = queuePartition({ runId: 'run-one' }, 64);

    expect(retry).toBe(first);
    expect(first).toBeGreaterThanOrEqual(0);
    expect(first).toBeLessThan(64);
    expect(workflowLane).toBeGreaterThanOrEqual(0);
    expect(workflowLane).toBeLessThan(64);
  });

  it('uses the cached record tail after initial queue discovery', async () => {
    const memory = new MemoryClient();
    const journal = new QueueJournal(memory as unknown as UrsulaClient);

    await journal.enqueue(queueName, payload);
    memory.reads = 0;
    await journal.enqueue(queueName, {
      ...payload,
      correlationId: 'health-2',
    });
    const lease = await journal.claim(queueName, new Date(), 1000);
    if (!lease) throw new Error('expected hot-path lease');
    await journal.ack(queueName, lease);

    expect(memory.reads).toBe(0);
  });

  it('preserves message identity across retry, lease expiry and adapter restart', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    let journal = new QueueJournal(client);
    const messageId = await journal.enqueue(queueName, payload, {
      idempotencyKey: 'start-run-1',
    });
    await expect(
      journal.enqueue(queueName, payload, {
        idempotencyKey: 'start-run-1',
      })
    ).resolves.toBe(messageId);

    const base = Date.now() + 1000;
    const first = await journal.claim(queueName, new Date(base), 100);
    expect(first?.message.messageId).toBe(messageId);
    expect(first?.message.attempt).toBe(1);

    journal = new QueueJournal(client);
    expect(await journal.claim(queueName, new Date(base + 50), 100)).toBeNull();
    const redelivery = await journal.claim(
      queueName,
      new Date(base + 101),
      100
    );
    expect(redelivery?.message.messageId).toBe(messageId);
    expect(redelivery?.message.attempt).toBe(2);
    expect(redelivery && (await journal.ack(queueName, redelivery))).toBe(true);
    expect(
      await journal.claim(queueName, new Date(base + 1000), 100)
    ).toBeNull();
  });

  it('does not expose a delayed or retry-scheduled message early', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const journal = new QueueJournal(client);
    await journal.enqueue(queueName, payload, { delaySeconds: 10 });
    const before = new Date(Date.now() + 9000);
    expect(await journal.claim(queueName, before, 100)).toBeNull();
    const lease = await journal.claim(
      queueName,
      new Date(Date.now() + 11_000),
      100
    );
    expect(lease).not.toBeNull();
    if (!lease) throw new Error('expected queue lease');
    await journal.retry(queueName, lease, new Date(Date.now() + 20_000));
    expect(
      await journal.claim(queueName, new Date(Date.now() + 19_000), 100)
    ).toBeNull();
  });

  it('serializes one execution lane while allowing different runs', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const firstJournal = new QueueJournal(client);
    const secondJournal = new QueueJournal(client);
    await firstJournal.enqueue(queueName, {
      runId: 'run-one',
    });
    await firstJournal.enqueue(queueName, {
      runId: 'run-one',
    });
    await firstJournal.enqueue(queueName, {
      runId: 'run-two',
    });
    const now = new Date(Date.now() + 100);
    const first = await firstJournal.claim(queueName, now, 10_000);
    expect(first).not.toBeNull();
    expect(first?.message.message).toMatchObject({ runId: 'run-one' });
    const otherRun = await secondJournal.claim(queueName, now, 10_000);
    expect(otherRun?.message.message).toMatchObject({ runId: 'run-two' });
    if (!first) throw new Error('expected first lease');
    await firstJournal.ack(queueName, first);
    const sameRun = await secondJournal.claim(queueName, now, 10_000);
    expect(sameRun?.message.message).toMatchObject({ runId: 'run-one' });
  });

  it('allows parallel step lanes within the same run', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const firstJournal = new QueueJournal(client);
    const secondJournal = new QueueJournal(client);
    await firstJournal.enqueue(queueName, {
      runId: 'run-parallel',
      stepId: 'step-reader',
    });
    await firstJournal.enqueue(queueName, {
      runId: 'run-parallel',
      stepId: 'step-writer',
    });
    const now = new Date(Date.now() + 100);
    const reader = await firstJournal.claim(queueName, now, 10_000);
    const writer = await secondJournal.claim(queueName, now, 10_000);

    expect(reader?.message.message).toMatchObject({
      runId: 'run-parallel',
      stepId: 'step-reader',
    });
    expect(writer?.message.message).toMatchObject({
      runId: 'run-parallel',
      stepId: 'step-writer',
    });
  });

  it('acks and leases the next message for the same execution lane in one append', async () => {
    const memory = new MemoryClient();
    const journal = new QueueJournal(memory as unknown as UrsulaClient);
    await journal.enqueue(queueName, { runId: 'run-one', stepId: 'step-one' });
    const secondMessageId = await journal.enqueue(queueName, {
      runId: 'run-one',
      stepId: 'step-one',
    });
    await journal.enqueue(queueName, { runId: 'run-two', stepId: 'step-two' });
    const now = new Date(Date.now() + 100);
    const first = await journal.claim(queueName, now, 10_000);
    if (!first) throw new Error('expected first lease');

    const next = await journal.ackAndClaimNext(
      queueName,
      first,
      now,
      10_000
    );

    expect(next?.message.messageId).toBe(secondMessageId);
    expect(memory.appendedBatchSizes.at(-1)).toBe(2);
    const otherRun = await journal.claim(queueName, now, 10_000);
    expect(otherRun?.message.message).toMatchObject({ runId: 'run-two' });
  });

  it('refreshes and leases remotely enqueued work before acking', async () => {
    const memory = new MemoryClient();
    const dispatcher = new QueueJournal(
      memory as unknown as UrsulaClient
    );
    const producer = new QueueJournal(memory as unknown as UrsulaClient);
    await dispatcher.enqueue(queueName, {
      runId: 'run-remote-successor',
      step: 1,
    });
    const now = new Date(Date.now() + 100);
    const first = await dispatcher.claim(queueName, now, 10_000);
    if (!first) throw new Error('expected first lease');
    const secondMessageId = await producer.enqueue(queueName, {
      runId: 'run-remote-candidate',
      step: 2,
    });

    const next = await dispatcher.ackAndClaimNext(
      queueName,
      first,
      now,
      10_000
    );

    expect(next?.message.messageId).toBe(secondMessageId);
    expect(memory.appendedBatchSizes.at(-1)).toBe(2);
  });

  it('recovers the successor lease after an ack-and-claim response is lost', async () => {
    const memory = new MemoryClient();
    const journal = new QueueJournal(memory as unknown as UrsulaClient);
    await journal.enqueue(queueName, { runId: 'run-lost', step: 1 });
    const secondMessageId = await journal.enqueue(queueName, {
      runId: 'run-lost',
      step: 2,
    });
    const now = new Date(Date.now() + 100);
    const first = await journal.claim(queueName, now, 10_000);
    if (!first) throw new Error('expected first lease');
    memory.loseNextAppendResponse = true;

    const next = await journal.ackAndClaimNext(
      queueName,
      first,
      now,
      10_000
    );

    expect(next?.message.messageId).toBe(secondMessageId);
    expect(next?.message.attempt).toBe(1);
    expect(await journal.claim(queueName, now, 10_000)).toBeNull();
  });

  it('checkpoints active state, drops acked messages, and resumes from the tail', async () => {
    const memory = new MemoryClient();
    let journal = new QueueJournal(memory as unknown as UrsulaClient);
    const lagging = new QueueJournal(memory as unknown as UrsulaClient);
    const now = new Date(Date.now() + 60_000);
    await expect(lagging.claim(queueName, now, 1_000)).resolves.toBeNull();
    let firstMessageId: string | undefined;
    for (let index = 0; index < 86; index += 1) {
      const messageId = await journal.enqueue(queueName, payload, {
        idempotencyKey: `checkpoint-${index}`,
      });
      firstMessageId ??= messageId;
      const lease = await journal.claim(queueName, now, 1_000);
      if (!lease) throw new Error('expected checkpoint test lease');
      await journal.ack(queueName, lease);
    }
    await journal.flushCheckpoints();
    expect(memory.retainedRecords).toContain(256);

    memory.readAllStarts.length = 0;
    journal = new QueueJournal(memory as unknown as UrsulaClient);
    await expect(journal.claim(queueName, now, 1_000)).resolves.toBeNull();
    await expect(
      journal.enqueue(queueName, payload, {
        idempotencyKey: 'checkpoint-0',
      })
    ).resolves.toBe(firstMessageId);
    await expect(lagging.claim(queueName, now, 1_000)).resolves.toBeNull();
    expect(memory.goneReads).toBeGreaterThan(0);
    expect(
      memory.readAllStarts.some(
        ({ stream, start }) =>
          stream.startsWith('queue-') &&
          !stream.startsWith('queue-checkpoint-') &&
          start === 256
      )
    ).toBe(true);
  });

  it('expires durable idempotency entries after the retry window', async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date('2026-07-25T00:00:00.000Z'));
      const client = new MemoryClient() as unknown as UrsulaClient;
      const journal = new QueueJournal(client);
      const first = await journal.enqueue(queueName, payload, {
        idempotencyKey: 'reusable-key',
      });
      await expect(
        journal.enqueue(queueName, payload, {
          idempotencyKey: 'reusable-key',
        })
      ).resolves.toBe(first);

      vi.setSystemTime(new Date('2026-07-26T00:00:00.001Z'));
      const afterWindow = await journal.enqueue(queueName, payload, {
        idempotencyKey: 'reusable-key',
      });
      expect(afterWindow).not.toBe(first);
    } finally {
      vi.useRealTimers();
    }
  });

  it('serializes concurrent local transitions before queue CAS', async () => {
    const memory = new MemoryClient();
    memory.yieldBeforeAppend = true;
    const journal = new QueueJournal(memory as unknown as UrsulaClient);

    const messageIds = await Promise.all(
      Array.from({ length: 100 }, (_, index) =>
        journal.enqueue(
          queueName,
          { runId: `run-concurrent-${index}`, step: index },
          { idempotencyKey: `concurrent-${index}` }
        )
      )
    );

    expect(new Set(messageIds)).toHaveLength(100);
    expect(memory.appendedBatchSizes).toHaveLength(100);
  });

  it('refreshes only the missing suffix after cross-instance queue contention', async () => {
    const memory = new MemoryClient();
    const first = new QueueJournal(memory as unknown as UrsulaClient);
    const second = new QueueJournal(memory as unknown as UrsulaClient);
    await first.enqueue(queueName, { runId: 'warm-first', step: 0 });
    await second.enqueue(queueName, { runId: 'warm-second', step: 0 });
    memory.readAllStarts.length = 0;
    memory.yieldBeforeAppend = true;

    const messageIds = await Promise.all([
      ...Array.from({ length: 20 }, (_, index) =>
        first.enqueue(
          queueName,
          { runId: `first-${index}`, step: index },
          { idempotencyKey: `first-${index}` }
        )
      ),
      ...Array.from({ length: 20 }, (_, index) =>
        second.enqueue(
          queueName,
          { runId: `second-${index}`, step: index },
          { idempotencyKey: `second-${index}` }
        )
      ),
    ]);

    expect(new Set(messageIds)).toHaveLength(40);
    expect(memory.readAllStarts).toEqual([]);
  });

  it('rejects a stale local transition after an earlier queued mutation commits', async () => {
    const memory = new MemoryClient();
    const journal = new QueueJournal(memory as unknown as UrsulaClient);
    await journal.enqueue(queueName, { runId: 'same-run', step: 0 });
    const lease = await journal.claim(queueName, new Date(), 10_000);
    if (!lease) throw new Error('expected queue lease');
    memory.yieldBeforeAppend = true;

    const [acked, extended] = await Promise.all([
      journal.ack(queueName, lease),
      journal.extend(queueName, lease, new Date(Date.now() + 20_000)),
    ]);

    expect(acked).toBe(true);
    expect(extended).toBe(false);
    await journal.enqueue(queueName, { runId: 'next-run', step: 1 });
    await expect(
      journal.claim(queueName, new Date(), 10_000)
    ).resolves.not.toBeNull();
  });

  it('retries a cold rebuild when retention advances after checkpoint read', async () => {
    const memory = new MemoryClient();
    const writer = new QueueJournal(memory as unknown as UrsulaClient);
    const now = new Date(Date.now() + 60_000);
    for (let index = 0; index < 86; index += 1) {
      await writer.enqueue(queueName, payload, {
        idempotencyKey: `race-initial-${index}`,
      });
      const lease = await writer.claim(queueName, now, 1_000);
      if (!lease) throw new Error('expected initial queue lease');
      await writer.ack(queueName, lease);
    }
    await writer.flushCheckpoints();

    memory.beforeNextSourceReadAll = async () => {
      for (let index = 0; index < 85; index += 1) {
        await writer.enqueue(queueName, payload, {
          idempotencyKey: `race-advance-${index}`,
        });
        const lease = await writer.claim(queueName, now, 1_000);
        if (!lease) throw new Error('expected advancing queue lease');
        await writer.ack(queueName, lease);
      }
      await writer.flushCheckpoints();
    };

    const coldReader = new QueueJournal(memory as unknown as UrsulaClient);
    await expect(coldReader.claim(queueName, now, 1_000)).resolves.toBeNull();
    expect(memory.goneReads).toBe(1);
    expect(memory.readAllStarts.slice(-2).map(({ start }) => start)).toEqual([
      256, 512,
    ]);
  });
});
