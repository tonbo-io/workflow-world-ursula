import type { QueuePayload, ValidQueueName } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import type {
  UrsulaAppendOptions,
  UrsulaClient,
  UrsulaRecord,
} from './client.js';
import type { RunExecutionCoordinator } from './execution.js';
import { createQueue } from './queue.js';
import { QueueJournal, queuePartition } from './queue-journal.js';

class MemoryClient {
  readonly waitedStreams: string[] = [];
  readonly waitTimeouts: number[] = [];
  readonly affinities: string[] = [];
  private readonly streams = new Map<string, unknown[]>();

  withAffinity(affinity: string): this {
    this.affinities.push(affinity);
    return this;
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

  async watchRecords<T>(
    stream: string,
    start: number,
    onRecords: (records: UrsulaRecord<T>[]) => void | Promise<void>,
    signal?: AbortSignal
  ): Promise<void> {
    this.waitedStreams.push(stream);
    let cursor = start;
    while (!signal?.aborted) {
      const page = await this.read<T>(stream, cursor);
      if (page.records.length > 0) {
        await onRecords(page.records);
        cursor = page.nextRecord;
      }
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
    }
  }
}

describe('Ursula queue runtime', () => {
  it('stores and watches a run queue through the run affinity key', async () => {
    const memory = new MemoryClient();
    const queueName = '__wkf_workflow_run_local' as ValidQueueName;
    const queue = createQueue(memory as unknown as UrsulaClient, {
      runLocalQueues: true,
      pollIntervalMs: 5,
    });
    const delivered = vi.fn().mockResolvedValue(undefined);
    queue.createQueueHandler('__wkf_workflow_', delivered);

    await queue.queue(queueName, { runId: 'wrun_local_1' });
    await queue.start();
    await vi.waitFor(() => expect(delivered).toHaveBeenCalledOnce());
    await queue.close();

    expect(memory.affinities).toContain('wrun_local_1');
    expect(memory.waitedStreams).toContainEqual(
      expect.stringMatching(/^queue-/)
    );
  });

  it('rejects an invalid static dispatcher assignment', () => {
    const client = new MemoryClient() as unknown as UrsulaClient;

    expect(() =>
      createQueue(client, {
        partitionShardCount: 2,
        partitionShardIndex: 2,
      })
    ).toThrow(/partitionShardIndex/);
    expect(() =>
      createQueue(client, {
        partitionShardCount: 2,
        partitionShardReplicas: 3,
      })
    ).toThrow(/partitionShardReplicas/);
  });

  it('runs workflow deliveries inside their execution context', async () => {
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

  it('rejects an expired fenced HTTP delivery before invoking its handler', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const run = vi.fn();
    const executions = {
      allowsOwnedLazyStarts: () => true,
      run,
    } as unknown as RunExecutionCoordinator;
    const queueName = '__wkf_workflow_stale_http' as ValidQueueName;
    const journal = new QueueJournal(client);
    await journal.enqueue(queueName, { runId: 'wrun_stale_http' });
    const lease = await journal.claim(queueName, new Date(), -1);
    if (!lease) throw new Error('expected expired queue lease');
    const handler = createQueue(client, {}, executions).createQueueHandler(
      '__wkf_workflow_',
      async () => undefined
    );

    const response = await handler(
      new Request('http://workflow.test/flow', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': queueName,
          'x-vqs-message-id': lease.message.messageId,
          'x-vqs-message-attempt': String(lease.message.attempt),
          'x-ursula-run-id': 'wrun_stale_http',
          'x-ursula-execution-lane': 'run',
          'x-ursula-execution-partition': String(lease.partition),
          'x-ursula-execution-token': lease.leaseId,
          'x-ursula-execution-generation': String(lease.generation),
          'x-ursula-execution-expires-at':
            lease.message.leaseExpiresAt?.toISOString() ?? '',
        },
        body: JSON.stringify(lease.message.message),
      })
    );

    expect(response.status).toBe(409);
    expect(run).not.toHaveBeenCalled();
  });

  it('defers full-delivery fencing to the atomic terminal commit', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const run = vi.fn(
      async (_delivery: unknown, task: () => Promise<unknown>) => task()
    );
    const executions = {
      allowsOwnedLazyStarts: () => false,
      allowsDeliveryTransactions: () => true,
      finishDelivery: async () => false,
      run,
    } as unknown as RunExecutionCoordinator;
    const queueName = '__wkf_workflow_delivery_fence' as ValidQueueName;
    const handler = createQueue(client, {}, executions).createQueueHandler(
      '__wkf_workflow_',
      async () => undefined
    );

    const response = await handler(
      new Request('http://workflow.test/flow', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': queueName,
          'x-vqs-message-id': 'msg_delivery_fence',
          'x-vqs-message-attempt': '1',
          'x-ursula-run-id': 'wrun_delivery_fence',
          'x-ursula-execution-lane': 'run',
          'x-ursula-execution-partition': '0',
          'x-ursula-execution-token': 'lease_delivery_fence',
          'x-ursula-execution-generation': '1',
          'x-ursula-execution-expires-at': new Date(
            Date.now() + 60_000
          ).toISOString(),
        },
        body: JSON.stringify({ runId: 'wrun_delivery_fence' }),
      })
    );

    expect(response.status).toBe(200);
    expect(run).toHaveBeenCalledOnce();
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
    const queueName = '__wkf_workflow_retry' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 5,
      leaseDurationMs: 50,
    });
    const attempts: Array<{ attempt: number; messageId: string }> = [];
    queue.createQueueHandler('__wkf_workflow_', async (_message, meta) => {
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
    const queueName = '__wkf_workflow_deadline' as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const attempts: number[] = [];
    queue.createQueueHandler('__wkf_workflow_', async (_message, meta) => {
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

  it('lets only the owning static dispatcher shard claim a partition', async () => {
    const client = new MemoryClient() as unknown as UrsulaClient;
    const queueName =
      '__wkf_workflow_static_dispatcher_shard' as ValidQueueName;
    const runId = 'run-static-dispatcher-shard';
    const partition = queuePartition({ runId }, 8);
    const handlers = [vi.fn(), vi.fn()];
    const dispatchers = handlers.map((handler, partitionShardIndex) => {
      const queue = createQueue(client, {
        partitionCount: 8,
        partitionShardCount: 2,
        partitionShardIndex,
        pollIntervalMs: 5,
        leaseDurationMs: 1_000,
      });
      queue.createQueueHandler('__wkf_workflow_', handler);
      return queue;
    });
    const producer = createQueue(client, {
      dispatcherEnabled: false,
      partitionCount: 8,
    });
    await producer.queue(queueName, { runId });

    await Promise.all(dispatchers.map((dispatcher) => dispatcher.start()));
    await vi.waitFor(() => {
      expect(handlers[partition % 2]).toHaveBeenCalledOnce();
    });
    await Promise.all([
      producer.close(),
      ...dispatchers.map((dispatcher) => dispatcher.close()),
    ]);

    expect(handlers[(partition + 1) % 2]).not.toHaveBeenCalled();
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

  it('routes a namespaced queue topic to its handler', async () => {
    // eve derives a per-agent queue namespace and exports it as
    // WORKFLOW_QUEUE_NAMESPACE, so every topic it enqueues carries one.
    const client = new MemoryClient() as unknown as UrsulaClient;
    const prefix = '__eve6167656e74_wkf_workflow_';
    const queueName = `${prefix}namespaced` as ValidQueueName;
    const queue = createQueue(client, {
      pollIntervalMs: 10_000,
      leaseDurationMs: 1_000,
    });
    const handler = vi.fn().mockResolvedValue(undefined);
    queue.createQueueHandler(prefix, handler);
    await queue.start();

    await queue.queue(queueName, { runId: 'run-namespaced' });
    await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce(), {
      timeout: 500,
    });
    await queue.close();

    expect(handler.mock.calls[0]?.[1]).toMatchObject({ queueName });
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
