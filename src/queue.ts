import { setTimeout as delay } from 'node:timers/promises';
import { createWorkflowUrl } from '@workflow/utils';
import type { Queue, QueuePrefix, ValidQueueName } from '@workflow/world';
import {
  MessageId as MessageIdSchema,
  parseQueueName,
  QueuePrefix as QueuePrefixSchema,
  ValidQueueName as ValidQueueNameSchema,
} from '@workflow/world';
import {
  parseUrsulaJson,
  stringifyUrsulaJson,
  type UrsulaClient,
} from './client.js';
import {
  QueueJournal,
  queuePartition,
  type QueueLease,
} from './queue-journal.js';
import { QueueRegistry } from './queue-registry.js';

const DEFAULT_POLL_INTERVAL_MS = 250;
const DEFAULT_LEASE_DURATION_MS = 60_000;
const DEFAULT_RETRY_DELAY_MS = 5_000;
const DEFAULT_CONCURRENCY = 64;
const DEFAULT_SHUTDOWN_GRACE_MS = 30_000;
const DEFAULT_CROSS_INSTANCE_WAKE_TIMEOUT_MS = 25_000;
const DEFAULT_PARTITION_COUNT = 64;

type QueueHandler = Parameters<Queue['createQueueHandler']>[1];

export interface UrsulaQueueConfig {
  deploymentId?: string;
  /**
   * HTTP origin that serves Workflow's `/.well-known/workflow/v1/*` routes.
   * Defaults to WORKFLOW_URSULA_QUEUE_DELIVERY_URL, then localhost:$PORT.
   */
  deliveryBaseUrl?: string;
  pollIntervalMs?: number;
  leaseDurationMs?: number;
  retryDelayMs?: number;
  concurrency?: number;
  shutdownGraceMs?: number;
  /** Long-poll duration for cross-instance registry and queue wakeups. */
  longPollTimeoutMs?: number;
  /**
   * Fixed physical journals per logical Workflow queue. Messages in the same
   * execution lane always hash to the same partition.
   */
  partitionCount?: number;
}

export interface UrsulaQueue extends Queue {
  start(): Promise<void>;
  close(): Promise<void>;
}

function positiveInteger(
  value: number | undefined,
  fallback: number,
  name: string
): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return result;
}

/**
 * Durable queue runtime whose complete broker state lives in Ursula.
 *
 * Handler functions are process-local execution endpoints. Any adapter
 * instance with a matching handler can claim an expired or pending message;
 * the message, attempt and lease identity are recovered from Ursula.
 */
export function createQueue(
  client: UrsulaClient,
  config: UrsulaQueueConfig = {}
): UrsulaQueue {
  const pollIntervalMs = positiveInteger(
    config.pollIntervalMs,
    DEFAULT_POLL_INTERVAL_MS,
    'Ursula queue pollIntervalMs'
  );
  const leaseDurationMs = positiveInteger(
    config.leaseDurationMs,
    DEFAULT_LEASE_DURATION_MS,
    'Ursula queue leaseDurationMs'
  );
  const retryDelayMs = positiveInteger(
    config.retryDelayMs,
    DEFAULT_RETRY_DELAY_MS,
    'Ursula queue retryDelayMs'
  );
  const concurrency = positiveInteger(
    config.concurrency,
    DEFAULT_CONCURRENCY,
    'Ursula queue concurrency'
  );
  const shutdownGraceMs = positiveInteger(
    config.shutdownGraceMs,
    DEFAULT_SHUTDOWN_GRACE_MS,
    'Ursula queue shutdownGraceMs'
  );
  const longPollTimeoutMs = positiveInteger(
    config.longPollTimeoutMs,
    DEFAULT_CROSS_INSTANCE_WAKE_TIMEOUT_MS,
    'Ursula queue longPollTimeoutMs'
  );
  const partitionCount = positiveInteger(
    config.partitionCount,
    DEFAULT_PARTITION_COUNT,
    'Ursula queue partitionCount'
  );
  const journals = Array.from(
    { length: partitionCount },
    (_, partition) => new QueueJournal(client, partition)
  );
  const registry = new QueueRegistry(client);
  const handlers = new Map<QueuePrefix, QueueHandler>();
  const inFlight = new Set<Promise<void>>();
  const shutdown = new AbortController();
  const wakeWaiters = new Set<() => void>();
  const queueWatchers = new Map<string, Promise<void>>();
  const readyPartitions = new Set<string>();
  let wakeVersion = 0;
  let queueCursor = 0;
  let loop: Promise<void> | undefined;
  let registryWatcher: Promise<void> | undefined;

  function wake(): void {
    wakeVersion += 1;
    for (const resolve of wakeWaiters) resolve();
    wakeWaiters.clear();
  }

  function waitForLocalWake(
    observedVersion: number,
    signal: AbortSignal
  ): Promise<void> {
    if (wakeVersion !== observedVersion || signal.aborted) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const finish = () => {
        wakeWaiters.delete(finish);
        signal.removeEventListener('abort', finish);
        resolve();
      };
      wakeWaiters.add(finish);
      signal.addEventListener('abort', finish, { once: true });
    });
  }

  function handlerFor(queueName: ValidQueueName): QueueHandler | undefined {
    return handlers.get(parseQueueName(queueName).prefix);
  }

  function deliveryBaseUrl(): string | undefined {
    if (config.deliveryBaseUrl) return config.deliveryBaseUrl;
    if (process.env.WORKFLOW_URSULA_QUEUE_DELIVERY_URL) {
      return process.env.WORKFLOW_URSULA_QUEUE_DELIVERY_URL;
    }
    return process.env.PORT
      ? `http://localhost:${process.env.PORT}`
      : undefined;
  }

  function canDeliver(queueName: ValidQueueName): boolean {
    return Boolean(deliveryBaseUrl() || handlerFor(queueName));
  }

  async function waitAfterWatcherError(): Promise<void> {
    try {
      await delay(pollIntervalMs, undefined, { signal: shutdown.signal });
    } catch {
      // Shutdown aborts watcher backoff.
    }
  }

  function journalForLease(lease: QueueLease): QueueJournal {
    const journal = journals[lease.partition];
    if (!journal) {
      throw new Error(`Invalid Ursula queue partition ${lease.partition}`);
    }
    return journal;
  }

  function partitionKey(
    queueName: ValidQueueName,
    partition: number
  ): string {
    return `${queueName}\u0000${partition}`;
  }

  function markReady(queueName: ValidQueueName, partition: number): void {
    readyPartitions.add(partitionKey(queueName, partition));
  }

  function ensureQueueWatcher(
    queueName: ValidQueueName,
    partition: number
  ): void {
    const key = partitionKey(queueName, partition);
    if (queueWatchers.has(key)) return;
    const journal = journals[partition];
    if (!journal) return;
    // A newly discovered partition may already contain work written before
    // this process observed its registry record.
    markReady(queueName, partition);
    // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: a durable watcher owns its long-poll, retry, wake, and shutdown lifecycle.
    const watcher = (async () => {
      while (!shutdown.signal.aborted) {
        try {
          const changed = await journal.waitForChange(
            queueName,
            longPollTimeoutMs,
            shutdown.signal
          );
          if (changed) {
            markReady(queueName, partition);
            wake();
          }
        } catch (error) {
          if (shutdown.signal.aborted) return;
          console.error('Ursula queue wake watcher failed', {
            queueName,
            error: error instanceof Error ? error.message : String(error),
          });
          await waitAfterWatcherError();
        }
      }
    })();
    queueWatchers.set(key, watcher);
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: registry watching shares the same retry and shutdown lifecycle as queue watchers.
  async function watchRegistry(): Promise<void> {
    while (!shutdown.signal.aborted) {
      try {
        const changed = await registry.waitForChange(
          longPollTimeoutMs,
          shutdown.signal
        );
        if (changed) wake();
      } catch (error) {
        if (shutdown.signal.aborted) return;
        console.error('Ursula queue registry watcher failed', {
          error: error instanceof Error ? error.message : String(error),
        });
        await waitAfterWatcherError();
      }
    }
  }

  async function invokeHttp(
    queueName: ValidQueueName,
    lease: QueueLease
  ): Promise<{ timeoutSeconds?: number } | undefined> {
    const baseUrl = deliveryBaseUrl();
    if (!baseUrl) {
      throw new Error(
        'Ursula queue delivery requires deliveryBaseUrl, WORKFLOW_URSULA_QUEUE_DELIVERY_URL, or PORT'
      );
    }
    const kind = parseQueueName(queueName).kind;
    const response = await fetch(
      createWorkflowUrl(baseUrl, {
        type: kind === 'workflow' ? 'flow' : 'step',
      }),
      {
        method: 'POST',
        headers: {
          ...lease.message.headers,
          'content-type': 'application/json',
          'x-vqs-queue-name': queueName,
          'x-vqs-message-id': lease.message.messageId,
          'x-vqs-message-attempt': String(lease.message.attempt),
        },
        body: stringifyUrsulaJson(lease.message.message),
      }
    );
    const text = await response.text();
    if (!response.ok) {
      throw new Error(
        `Workflow queue delivery failed: HTTP ${response.status}${text ? `: ${text}` : ''}`
      );
    }
    if (!text) return;
    const result = JSON.parse(text) as { timeoutSeconds?: unknown };
    return typeof result.timeoutSeconds === 'number'
      ? { timeoutSeconds: result.timeoutSeconds }
      : undefined;
  }

  async function invoke(
    queueName: ValidQueueName,
    lease: QueueLease,
    handler: QueueHandler | undefined
  ): Promise<{ timeoutSeconds?: number } | undefined> {
    if (!handler) return invokeHttp(queueName, lease);
    return (
      (await handler(lease.message.message, {
        attempt: lease.message.attempt,
        queueName,
        messageId: lease.message.messageId,
        requestId: lease.message.headers?.['x-vercel-id'],
      })) ?? undefined
    );
  }

  async function heartbeat(
    queueName: ValidQueueName,
    lease: QueueLease,
    signal: AbortSignal
  ): Promise<void> {
    const interval = Math.max(1, Math.floor(leaseDurationMs / 2));
    while (!signal.aborted && !shutdown.signal.aborted) {
      try {
        await delay(interval, undefined, { signal });
      } catch {
        return;
      }
      if (signal.aborted || shutdown.signal.aborted) return;
      const extended = await journalForLease(lease).extend(
        queueName,
        lease,
        new Date(Date.now() + leaseDurationMs)
      );
      if (!extended) return;
    }
  }

  async function deliver(
    queueName: ValidQueueName,
    lease: QueueLease,
    handler: QueueHandler | undefined
  ): Promise<QueueLease | null> {
    const heartbeatController = new AbortController();
    const heartbeatTask = heartbeat(
      queueName,
      lease,
      heartbeatController.signal
    );
    try {
      const result = await invoke(queueName, lease, handler);
      if (typeof result?.timeoutSeconds === 'number') {
        const timeoutMs = Math.max(0, result.timeoutSeconds) * 1000;
        await journalForLease(lease).retry(
          queueName,
          lease,
          new Date(Date.now() + timeoutMs)
        );
        return null;
      } else {
        return journalForLease(lease).ackAndClaimNext(
          queueName,
          lease,
          new Date(),
          leaseDurationMs
        );
      }
    } catch (error) {
      console.error('Ursula queue delivery failed; scheduling redelivery', {
        queueName,
        messageId: lease.message.messageId,
        attempt: lease.message.attempt,
        error: error instanceof Error ? error.message : String(error),
      });
      await journalForLease(lease).retry(
        queueName,
        lease,
        new Date(Date.now() + retryDelayMs)
      );
      return null;
    } finally {
      heartbeatController.abort();
      await heartbeatTask;
    }
  }

  async function deliverChain(
    queueName: ValidQueueName,
    firstLease: QueueLease,
    handler: QueueHandler | undefined
  ): Promise<void> {
    let lease: QueueLease | null = firstLease;
    while (lease && !shutdown.signal.aborted) {
      lease = await deliver(queueName, lease, handler);
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: queue discovery, capacity and per-run ownership checks belong in one dispatcher pass.
  async function pump(): Promise<ValidQueueName[]> {
    const queueNames = registry.current().filter(canDeliver);
    const now = new Date();
    for (const queueName of queueNames) {
      for (const partition of registry.partitions(queueName)) {
        ensureQueueWatcher(queueName, partition);
        const journal = journals[partition];
        const deadline = journal?.nextLocalDeadline(queueName, now);
        if (deadline && deadline <= now) markReady(queueName, partition);
      }
    }
    if (inFlight.size >= concurrency || queueNames.length === 0) {
      return queueNames;
    }
    const queueStart = queueCursor % queueNames.length;
    const orderedQueues = [
      ...queueNames.slice(queueStart),
      ...queueNames.slice(0, queueStart),
    ];
    // Visit every partition of the selected logical queue, then rotate the
    // logical starting point after a successful claim. This preserves topic
    // fairness even when two topics hash their ready work to distant shards.
    const work = orderedQueues.flatMap((queueName) =>
      registry
        .partitions(queueName)
        .filter((partition) =>
          readyPartitions.has(partitionKey(queueName, partition))
        )
        .map((partition) => journals[partition])
        .filter((journal): journal is QueueJournal => Boolean(journal))
        .map((journal) => ({ queueName, journal }))
    );
    for (const item of work) {
      if (!item) continue;
      const { queueName, journal } = item;
      if (inFlight.size >= concurrency) return queueNames;
      // Production delivery always crosses the Workflow HTTP route. Besides
      // matching hosted execution, this preserves per-invocation middleware,
      // tracing and accounting. Direct handlers are only a fallback for
      // embedded/test runtimes that have no delivery origin.
      const handler = deliveryBaseUrl() ? undefined : handlerFor(queueName);
      if (!handler && !deliveryBaseUrl()) continue;
      while (inFlight.size < concurrency) {
        const lease = await journal.claim(
          queueName,
          new Date(),
          leaseDurationMs
        );
        if (!lease) {
          readyPartitions.delete(partitionKey(queueName, journal.partition));
          break;
        }
        const logicalIndex = queueNames.indexOf(queueName);
        queueCursor = (logicalIndex + 1) % queueNames.length;
        const task = deliverChain(queueName, lease, handler)
          .catch(() => {
            // Delivery already persisted a retry when possible. A lost lease
            // is safely recovered by expiry on another dispatcher.
          })
          .finally(() => {
            inFlight.delete(task);
            markReady(queueName, lease.partition);
            wake();
          });
        inFlight.add(task);
      }
    }
    return queueNames;
  }

  async function waitForWork(
    queueNames: ValidQueueName[],
    observedVersion: number
  ): Promise<void> {
    const now = new Date();
    const nextDeadline = queueNames.reduce<Date | undefined>(
      (queueEarliest, queueName) =>
        registry
          .partitions(queueName)
          .map((partition) => journals[partition])
          .filter((journal): journal is QueueJournal => Boolean(journal))
          .reduce<Date | undefined>((earliest, journal) => {
            const deadline = journal.nextLocalDeadline(queueName, now);
            if (!deadline) return earliest;
            return !earliest || deadline < earliest ? deadline : earliest;
          }, queueEarliest),
      undefined
    );
    const deadlineWait = nextDeadline
      ? delay(Math.max(0, nextDeadline.getTime() - now.getTime()), undefined, {
          signal: shutdown.signal,
        }).catch(() => undefined)
      : new Promise<void>(() => undefined);
    await Promise.race([
      waitForLocalWake(observedVersion, shutdown.signal),
      deadlineWait,
    ]);
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: wake-driven dispatch and transient-error fallback share one shutdown-aware loop.
  async function run(): Promise<void> {
    while (!shutdown.signal.aborted) {
      const observedVersion = wakeVersion;
      try {
        const queueNames = await pump();
        if (wakeVersion !== observedVersion) continue;
        await waitForWork(queueNames, observedVersion);
      } catch (error) {
        if (shutdown.signal.aborted) return;
        // A transient Ursula or network error must not permanently stop queue
        // delivery. Leases and acknowledgements remain authoritative, so the
        // next pass can safely retry discovery or claiming.
        console.error('Ursula queue dispatcher pass failed', error);
        try {
          await delay(pollIntervalMs, undefined, {
            signal: shutdown.signal,
          });
        } catch {
          return;
        }
      }
    }
  }

  const queue: Queue['queue'] = async (queueName, message, options) => {
    ValidQueueNameSchema.parse(queueName);
    const partition = queuePartition(message, partitionCount);
    await registry.register(queueName, partition);
    const journal = journals[partition];
    if (!journal) {
      throw new Error(`Invalid Ursula queue partition ${partition}`);
    }
    const messageId = await journal.enqueue(queueName, message, options);
    markReady(queueName, partition);
    wake();
    return { messageId };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (
    queueNamePrefix,
    handler
  ) => {
    const prefix = QueuePrefixSchema.parse(queueNamePrefix);
    handlers.set(prefix, handler);
    wake();
    return async (request) => {
      const queueName = ValidQueueNameSchema.safeParse(
        request.headers.get('x-vqs-queue-name')
      );
      const messageId = MessageIdSchema.safeParse(
        request.headers.get('x-vqs-message-id')
      );
      const attempt = Number(request.headers.get('x-vqs-message-attempt'));
      if (
        !queueName.success ||
        !messageId.success ||
        !Number.isSafeInteger(attempt) ||
        attempt < 1 ||
        !queueName.data.startsWith(prefix)
      ) {
        return Response.json(
          { error: 'Missing or invalid Ursula queue headers' },
          { status: 400 }
        );
      }
      try {
        const result = await handler(
          parseUrsulaJson<unknown>(await request.text()),
          {
            attempt,
            queueName: queueName.data,
            messageId: messageId.data,
            requestId: request.headers.get('x-vercel-id') ?? undefined,
          }
        );
        return Response.json(result ?? { ok: true });
      } catch (error) {
        return Response.json(String(error), { status: 500 });
      }
    };
  };

  return {
    queue,
    createQueueHandler,
    async getDeploymentId() {
      return (
        config.deploymentId ??
        process.env.WORKFLOW_URSULA_DEPLOYMENT_ID ??
        'dpl_ursula'
      );
    },
    async start() {
      if (!registryWatcher) registryWatcher = watchRegistry();
      if (!loop) loop = run();
    },
    async close() {
      if (shutdown.signal.aborted) return;
      shutdown.abort();
      wake();
      await loop;
      await Promise.allSettled([
        ...(registryWatcher ? [registryWatcher] : []),
        ...queueWatchers.values(),
      ]);
      const grace = new AbortController();
      try {
        await Promise.race([
          Promise.allSettled(inFlight),
          delay(shutdownGraceMs, undefined, { signal: grace.signal }).catch(
            () => undefined
          ),
        ]);
      } finally {
        grace.abort();
      }
    },
  };
}
