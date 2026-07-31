import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
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
  RunExecutionCoordinator,
  type DeliveryExecution,
} from './execution.js';
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
const DEFAULT_PARTITION_COUNT = 8;

type QueueHandler = Parameters<Queue['createQueueHandler']>[1];

export interface UrsulaQueueConfig {
  /** Store every run's queue in the same Ursula affinity group as its journal. */
  runLocalQueues?: boolean;
  deploymentId?: string;
  /**
   * Whether this process claims and delivers queue messages.
   *
   * Keep enabled for the zero-configuration single-process topology. Larger
   * deployments can disable dispatch on request-serving replicas and run a
   * smaller redundant dispatcher pool; all replicas can still enqueue.
   */
  dispatcherEnabled?: boolean;
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
  /**
   * Optional static dispatcher sharding. Every replica in one dispatcher
   * pool must use the same shard count and a distinct index.
   */
  partitionShardCount?: number;
  partitionShardIndex?: number;
  partitionShardReplicas?: number;
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
  config: UrsulaQueueConfig = {},
  executions?: RunExecutionCoordinator
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
  const partitionShardCount = positiveInteger(
    config.partitionShardCount,
    1,
    'Ursula queue partitionShardCount'
  );
  const partitionShardIndex = config.partitionShardIndex ?? 0;
  const partitionShardReplicas = positiveInteger(
    config.partitionShardReplicas,
    1,
    'Ursula queue partitionShardReplicas'
  );
  const runLocalQueues = config.runLocalQueues === true;
  if (
    !Number.isSafeInteger(partitionShardIndex) ||
    partitionShardIndex < 0 ||
    partitionShardIndex >= partitionShardCount
  ) {
    throw new Error(
      'Ursula queue partitionShardIndex must be within partitionShardCount'
    );
  }
  if (partitionShardReplicas > partitionShardCount) {
    throw new Error(
      'Ursula queue partitionShardReplicas cannot exceed partitionShardCount'
    );
  }
  const journals = Array.from(
    { length: partitionCount },
    (_, partition) => new QueueJournal(client, partition)
  );
  const runJournals = new Map<string, QueueJournal>();
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

  function ownsPartition(partition: number): boolean {
    const firstOwner = partition % partitionShardCount;
    return (
      (partitionShardIndex - firstOwner + partitionShardCount) %
        partitionShardCount <
      partitionShardReplicas
    );
  }

  function affinityForMessage(message: Parameters<Queue['queue']>[1]): string {
    if ('__healthCheck' in message) {
      if (message.runId) return message.runId;
      return `health-${createHash('sha256')
        .update(message.correlationId)
        .digest('base64url')
        .slice(0, 32)}`;
    }
    return message.runId;
  }

  function ownsRun(runId: string): boolean {
    const owner =
      createHash('sha256').update(runId).digest().readUInt32BE(0) %
      partitionShardCount;
    return (
      (partitionShardIndex - owner + partitionShardCount) %
        partitionShardCount <
      partitionShardReplicas
    );
  }

  function targetKey(
    queueName: ValidQueueName,
    partition: number,
    runId?: string
  ): string {
    return `${queueName}\u0000${runId ?? ''}\u0000${partition}`;
  }

  function journalForTarget(
    queueName: ValidQueueName,
    partition: number,
    runId?: string
  ): QueueJournal | undefined {
    if (!runId) return journals[partition];
    const key = targetKey(queueName, partition, runId);
    let journal = runJournals.get(key);
    if (!journal) {
      journal = new QueueJournal(client, partition, runId);
      runJournals.set(key, journal);
    }
    return journal;
  }

  async function waitAfterWatcherError(): Promise<void> {
    try {
      await delay(pollIntervalMs, undefined, { signal: shutdown.signal });
    } catch {
      // Shutdown aborts watcher backoff.
    }
  }

  function journalForLease(lease: QueueLease): QueueJournal {
    const journal = journalForTarget(
      lease.message.queueName,
      lease.partition,
      lease.affinity
    );
    if (!journal) {
      throw new Error(`Invalid Ursula queue partition ${lease.partition}`);
    }
    return journal;
  }

  function deliveryExecution(
    queueName: ValidQueueName,
    lease: QueueLease
  ): DeliveryExecution | undefined {
    const message = lease.message.message;
    // A health probe may carry a `runId`, but it never owns an execution lane.
    if ('__healthCheck' in message) return;
    const expiresAt = lease.message.leaseExpiresAt;
    if (!expiresAt) return;
    return {
      runId: message.runId,
      lane: message.stepId ? `step:${message.stepId}` : 'run',
      queueName,
      queuePartition: lease.partition,
      token: lease.leaseId,
      generation: lease.generation,
      ownerMessageId: lease.message.messageId,
      attempt: lease.message.attempt,
      expiresAt,
    };
  }

  function partitionKey(
    queueName: ValidQueueName,
    partition: number,
    runId?: string
  ): string {
    return targetKey(queueName, partition, runId);
  }

  function markReady(
    queueName: ValidQueueName,
    partition: number,
    runId?: string
  ): void {
    readyPartitions.add(partitionKey(queueName, partition, runId));
  }

  function ensureQueueWatcher(
    queueName: ValidQueueName,
    partition: number,
    runId?: string
  ): void {
    const key = partitionKey(queueName, partition, runId);
    if (queueWatchers.has(key)) return;
    const journal = journalForTarget(queueName, partition, runId);
    if (!journal) return;
    // A newly discovered partition may already contain work written before
    // this process observed its registry record.
    markReady(queueName, partition, runId);
    // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: a durable watcher owns its long-poll, retry, wake, and shutdown lifecycle.
    const watcher = (async () => {
      while (!shutdown.signal.aborted) {
        try {
          if (runId) {
            await journal.watchChanges(
              queueName,
              () => {
                markReady(queueName, partition, runId);
                wake();
              },
              shutdown.signal
            );
            if (!shutdown.signal.aborted) await waitAfterWatcherError();
          } else {
            const changed = await journal.waitForChange(
              queueName,
              longPollTimeoutMs,
              shutdown.signal
            );
            if (changed) {
              markReady(queueName, partition);
              wake();
            }
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
    const execution = deliveryExecution(queueName, lease);
    // Queued steps share the workflow topic and the combined flow handler.
    const response = await fetch(
      createWorkflowUrl(baseUrl, { type: 'flow' }),
      {
        method: 'POST',
        headers: {
          ...lease.message.headers,
          'content-type': 'application/json',
          'x-vqs-queue-name': queueName,
          'x-vqs-message-id': lease.message.messageId,
          'x-vqs-message-attempt': String(lease.message.attempt),
          ...(execution
            ? {
                'x-ursula-run-id': execution.runId,
                'x-ursula-execution-lane': execution.lane,
                'x-ursula-execution-partition': String(
                  execution.queuePartition
                ),
                'x-ursula-execution-token': execution.token,
                'x-ursula-execution-generation': String(
                  execution.generation
                ),
                'x-ursula-execution-expires-at':
                  execution.expiresAt.toISOString(),
              }
            : {}),
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
    const call = async () =>
      (await handler(lease.message.message, {
        attempt: lease.message.attempt,
        queueName,
        messageId: lease.message.messageId,
        requestId: lease.message.headers?.['x-vercel-id'],
      })) ?? undefined;
    return executions
      ? executions.run(deliveryExecution(queueName, lease), call)
      : call();
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

  interface DispatchTarget {
    queueName: ValidQueueName;
    partition: number;
    runId?: string;
    journal: QueueJournal;
  }

  function dispatchTargets(): DispatchTarget[] {
    if (runLocalQueues) {
      return registry
        .targets()
        .filter(({ queueName, runId }) => canDeliver(queueName) && Boolean(runId))
        .filter(({ runId }) => runId !== undefined && ownsRun(runId))
        .flatMap(({ queueName, partition, runId }) => {
          const journal = journalForTarget(queueName, partition, runId);
          return journal ? [{ queueName, partition, runId, journal }] : [];
        });
    }
    return registry.current().filter(canDeliver).flatMap((queueName) =>
      registry
        .partitions(queueName)
        .filter(ownsPartition)
        .flatMap((partition) => {
          const journal = journalForTarget(queueName, partition);
          return journal ? [{ queueName, partition, journal }] : [];
        })
    );
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: queue discovery, capacity and per-run ownership checks belong in one dispatcher pass.
  async function pump(): Promise<DispatchTarget[]> {
    const targets = dispatchTargets();
    const now = new Date();
    for (const { queueName, partition, runId, journal } of targets) {
      ensureQueueWatcher(queueName, partition, runId);
      const deadline = journal.nextLocalDeadline(queueName, now);
      if (deadline && deadline <= now) markReady(queueName, partition, runId);
    }
    if (inFlight.size >= concurrency || targets.length === 0) {
      return targets;
    }
    const queueStart = queueCursor % targets.length;
    const work = [
      ...targets.slice(queueStart),
      ...targets.slice(0, queueStart),
    ].filter(({ queueName, partition, runId }) =>
      readyPartitions.has(partitionKey(queueName, partition, runId))
    );
    for (const item of work) {
      const { queueName, partition, runId, journal } = item;
      if (inFlight.size >= concurrency) return targets;
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
          readyPartitions.delete(partitionKey(queueName, partition, runId));
          break;
        }
        const logicalIndex = targets.indexOf(item);
        let nextIndex = (logicalIndex + 1) % targets.length;
        if (!runLocalQueues) {
          while (
            nextIndex !== logicalIndex &&
            targets[nextIndex]?.queueName === queueName
          ) {
            nextIndex = (nextIndex + 1) % targets.length;
          }
        }
        queueCursor = nextIndex;
        const task = deliverChain(queueName, lease, handler)
          .catch(() => {
            // Delivery already persisted a retry when possible. A lost lease
            // is safely recovered by expiry on another dispatcher.
          })
          .finally(() => {
            inFlight.delete(task);
            markReady(queueName, lease.partition, lease.affinity);
            wake();
          });
        inFlight.add(task);
      }
    }
    return targets;
  }

  async function waitForWork(
    targets: DispatchTarget[],
    observedVersion: number
  ): Promise<void> {
    const now = new Date();
    const nextDeadline = targets.reduce<Date | undefined>(
      (earliest, { queueName, journal }) => {
        const deadline = journal.nextLocalDeadline(queueName, now);
        if (!deadline) return earliest;
        return !earliest || deadline < earliest ? deadline : earliest;
      },
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
        const targets = await pump();
        if (wakeVersion !== observedVersion) continue;
        await waitForWork(targets, observedVersion);
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
    const runId = runLocalQueues ? affinityForMessage(message) : undefined;
    const partition = runLocalQueues ? 0 : queuePartition(message, partitionCount);
    const journal = journalForTarget(queueName, partition, runId);
    if (!journal) {
      throw new Error(`Invalid Ursula queue partition ${partition}`);
    }
    // A run-local queue and its global discovery record live in independent
    // Raft groups. Persist them concurrently so the queue hot path pays the
    // slower quorum rather than the sum of both quorums. queue() still waits
    // for both, preserving crash-safe discovery before it acknowledges work.
    const [messageId] = await Promise.all([
      journal.enqueue(queueName, message, options),
      runId
        ? registry.registerRun(queueName, runId)
        : registry.register(queueName, partition),
    ]);
    markReady(queueName, partition, runId);
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
        const message = parseUrsulaJson<unknown>(await request.text());
        const call = async () =>
          handler(message, {
            attempt,
            queueName: queueName.data,
            messageId: messageId.data,
            requestId: request.headers.get('x-vercel-id') ?? undefined,
          });
        const runId = request.headers.get('x-ursula-run-id');
        const lane = request.headers.get('x-ursula-execution-lane');
        const token = request.headers.get('x-ursula-execution-token');
        const partitionRaw = request.headers.get(
          'x-ursula-execution-partition'
        );
        const generationRaw = request.headers.get(
          'x-ursula-execution-generation'
        );
        const partition = partitionRaw === null ? NaN : Number(partitionRaw);
        const generation = generationRaw === null ? NaN : Number(generationRaw);
        const expiresAtRaw = request.headers.get(
          'x-ursula-execution-expires-at'
        );
        const expiresAt = expiresAtRaw ? new Date(expiresAtRaw) : undefined;
        const execution =
          runId &&
          lane &&
          token &&
          Number.isSafeInteger(partition) &&
          partition >= 0 &&
          Number.isSafeInteger(generation) &&
          generation >= 0 &&
          expiresAt &&
          !Number.isNaN(expiresAt.getTime())
            ? {
                runId,
                lane,
                queueName: queueName.data,
                queuePartition: partition,
                token,
                generation,
                ownerMessageId: messageId.data,
                attempt,
                expiresAt,
              }
            : undefined;
        if (execution && executions?.allowsOwnedLazyStarts()) {
          const journal = journalForTarget(
            queueName.data,
            execution.queuePartition,
            runLocalQueues ? execution.runId : undefined
          );
          const ownsLease =
            journal &&
            (await journal.ownsLease(queueName.data, {
              messageId: messageId.data,
              leaseId: execution.token,
              generation: execution.generation,
            }));
          if (!ownsLease) {
            return Response.json(
              { error: 'Ursula queue delivery lease is no longer active' },
              { status: 409 }
            );
          }
        }
        const result = executions
          ? await executions.run(execution, call)
          : await call();
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
      if (config.dispatcherEnabled === false) return;
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
