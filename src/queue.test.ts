import type { QueuePayload, ValidQueueName } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import type {
  UrsulaAppendOptions,
  UrsulaClient,
  UrsulaRecord,
} from './client.js';
import type { RunExecutionCoordinator } from './execution.js';
import { createQueue } from './queue.js';

class MemoryClient {
  readonly waitedStreams: string[] = [];
  readonly waitTimeouts: number[] = [];
  private readonly streams = new Map<string, unknown[]>();

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

  async readAll<T>(stream: string): Promise<UrsulaRecord<T>[]> {
    return (this.streams.get(stream) ?? []).map((value, record) => ({
      record,
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
    const records = (await this.readAll<T>(stream)).slice(start, start + limit);
    return {
      records,
      nextRecord: start + records.length,
      closed: false,
      upToDate: true,
    };
  }

  async waitForRecords<T>(
    stream: string,
    start: number,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    this.waitedStreams.push(stream);
    this.waitTimeouts.push(timeoutMs);
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(resolve, 5);
      signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timeout);
          resolve();
        },
        { once: true }
      );
    });
    return this.read<T>(stream, start);
  }
}

describe('Ursula queue runtime', () => {
  it('runs workflow deliveries inside the claimed run execution lease', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const run = vi.fn(
      async (
        _delivery: unknown,
        task: () => Promise<unknown>
      ): Promise<unknown> => task()
    );
    const executions = {
      run,
    } as unknown as RunExecutionCoordinator;
    const queueName =
      '__wkf_workflow_atomic_delivery' as ValidQueueName;
    const queue = createQueue(
      client,
      {
        pollIntervalMs: 5,
        leaseDurationMs: 1_000,
      },
      executions
    );
    queue.createQueueHandler('__wkf_workflow_', async () => undefined);
    await queue.queue(queueName, {
      runId: 'wrun_atomic_delivery',
    });
    await queue.start();
    await vi.waitFor(() => expect(run).toHaveBeenCalledOnce());
    await queue.close();

    expect(run.mock.calls[0]?.[0]).toMatchObject({
      runId: 'wrun_atomic_delivery',
      lane: 'run',
      ownerMessageId: expect.stringMatching(/^msg_/),
      attempt: 1,
    });
  });

  it('recovers an enqueued message in a fresh dispatcher', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName = '__wkf_workflow_recovery' as ValidQueueName;
    const payload = {
      __healthCheck: true,
      correlationId: 'recover-1',
    } satisfies QueuePayload;
    const producer = createQueue(client, { pollIntervalMs: 5 });
    const { messageId } = await producer.queue(queueName, payload, {
      idempotencyKey: 'recover-1',
    });
    await producer.close();

    const handler = vi.fn().mockResolvedValue(undefined);
    const recovered = createQueue(client, {
      pollIntervalMs: 5,
      leaseDurationMs: 50,
    });
    recovered.createQueueHandler('__wkf_workflow_', handler);
    await recovered.start();
    await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());
    await recovered.close();

    expect(handler.mock.calls[0]?.[1]).toMatchObject({
      attempt: 1,
      queueName,
      messageId,
    });
  });

  it('persists timeout redelivery with the same message ID', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName = '__wkf_step_retry' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 5,
      leaseDurationMs: 50,
    });
    const attempts: Array<{ attempt: number; messageId: string }> = [];
    queue.createQueueHandler('__wkf_step_', async (_message, meta) => {
      attempts.push(meta);
      return meta.attempt === 1 ? { timeoutSeconds: 0 } : undefined;
    });
    await queue.queue(queueName, {
      __healthCheck: true,
      correlationId: 'retry-1',
    });
    await queue.start();
    await vi.waitFor(() => expect(attempts).toHaveLength(2));
    await queue.close();

    expect(attempts.map(({ attempt }) => attempt)).toEqual([1, 2]);
    expect(attempts[0]?.messageId).toBe(attempts[1]?.messageId);
  });

  it('wakes at a persisted retry deadline without waiting for the poll fallback', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName = '__wkf_step_deadline' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const attempts: number[] = [];
    queue.createQueueHandler('__wkf_step_', async (_message, meta) => {
      attempts.push(meta.attempt);
      return meta.attempt === 1 ? { timeoutSeconds: 0.05 } : undefined;
    });
    await queue.queue(queueName, {
      __healthCheck: true,
      correlationId: 'deadline-1',
    });
    await queue.start();
    await vi.waitFor(() => expect(attempts).toEqual([1, 2]), {
      timeout: 500,
    });
    await queue.close();
  });

  it('delivers different runs from one workflow queue concurrently', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName = '__wkf_workflow_concurrent' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 5,
      leaseDurationMs: 1_000,
      concurrency: 2,
    });
    const releases: Array<() => void> = [];
    const started: string[] = [];
    queue.createQueueHandler('__wkf_workflow_', async (message) => {
      const runId = (message as { runId: string }).runId;
      started.push(runId);
      await new Promise<void>((resolve) => releases.push(resolve));
    });
    await queue.queue(queueName, { runId: 'run-one' });
    await queue.queue(queueName, { runId: 'run-two' });

    await queue.start();
    await vi.waitFor(() =>
      expect(new Set(started)).toEqual(new Set(['run-one', 'run-two']))
    );
    for (const release of releases) release();
    await queue.close();
  });

  it('wakes immediately when this process enqueues work', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName = '__wkf_workflow_immediate' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const handler = vi.fn().mockResolvedValue(undefined);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    await queue.queue(queueName, { runId: 'run-immediate' });
    await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce(), {
      timeout: 500,
    });
    await queue.close();
  });

  it('wakes a separate dispatcher instance through Ursula long polling', async () => {
    const memory = new MemoryClient();
    const client = memory as unknown as UrsulaClient;
    const worker = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const producer = createQueue(client, { pollIntervalMs: 10_000 });
    const handler = vi.fn().mockResolvedValue(undefined);
    worker.createQueueHandler('__wkf_workflow_', handler);
    await worker.start();

    await producer.queue('__wkf_workflow_remote' as ValidQueueName, {
      runId: 'run-remote',
    });

    await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce(), {
      timeout: 500,
    });
    await Promise.all([producer.close(), worker.close()]);

    expect(memory.waitedStreams).toContain('registry-queues');
    expect(memory.waitTimeouts).toContain(25_000);
    expect(
      new Set(
        memory.waitedStreams.filter(
          (stream) => stream.startsWith('queue-') && stream !== 'registry-queues'
        )
      ).size
    ).toBe(1);
  });

  it('does not watch or spin on queues this process cannot deliver', async () => {
    const memory = new MemoryClient();
    const queue = createQueue(memory as unknown as UrsulaClient, {
      pollIntervalMs: 10_000,
    });
    await queue.start();
    await queue.queue('__wkf_workflow_producer_only' as ValidQueueName, {
      runId: 'run-producer-only',
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    await queue.close();

    expect(
      memory.waitedStreams.filter((stream) => stream.startsWith('queue-'))
    ).toEqual([]);
  });

  it('lets request-serving replicas enqueue without starting a dispatcher', async () => {
    const memory = new MemoryClient();
    const client = memory as unknown as UrsulaClient;
    const queueName =
      '__wkf_workflow_dedicated_dispatcher' as ValidQueueName;
    const requestReplica = createQueue(client, {
      dispatcherEnabled: false,
      pollIntervalMs: 10_000,
    });
    const localHandler = vi.fn().mockResolvedValue(undefined);
    requestReplica.createQueueHandler('__wkf_workflow_', localHandler);
    await requestReplica.start();
    await requestReplica.queue(queueName, {
      runId: 'run-dedicated-dispatcher',
    });
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(localHandler).not.toHaveBeenCalled();
    expect(memory.waitedStreams).toEqual([]);

    const dispatcher = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const remoteHandler = vi.fn().mockResolvedValue(undefined);
    dispatcher.createQueueHandler('__wkf_workflow_', remoteHandler);
    await dispatcher.start();
    await vi.waitFor(() => expect(remoteHandler).toHaveBeenCalledOnce(), {
      timeout: 500,
    });
    await Promise.all([requestReplica.close(), dispatcher.close()]);
  });

  it('rotates the first queue considered when capacity is saturated', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const firstQueue = '__wkf_workflow_fair_first' as ValidQueueName;
    const secondQueue = '__wkf_workflow_fair_second' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 10_000,
      concurrency: 1,
    });
    const delivered: string[] = [];
    queue.createQueueHandler('__wkf_workflow_', async (message) => {
      delivered.push((message as { runId: string }).runId);
    });
    await queue.queue(firstQueue, { runId: 'first-1' });
    await queue.queue(firstQueue, { runId: 'first-2' });
    await queue.queue(secondQueue, { runId: 'second-1' });

    await queue.start();
    await vi.waitFor(() => expect(delivered).toHaveLength(3));
    await queue.close();

    expect(delivered.slice(0, 2)).toContain('second-1');
    expect(
      delivered.slice(0, 2).filter((runId) => runId.startsWith('first-'))
    ).toHaveLength(1);
  });
});
