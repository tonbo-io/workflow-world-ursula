import type { World } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { UrsulaClient } from './client.js';
import { RunExecutionCoordinator } from './execution.js';
import { createQueue, type UrsulaQueueConfig } from './queue.js';
import { RunJournal } from './run-journal.js';
import { createStorage } from './storage.js';
import { createStreamer, type UrsulaStreamerConfig } from './streamer.js';

export interface UrsulaWorldConfig
  extends UrsulaStreamerConfig,
    UrsulaQueueConfig {
  /**
   * Experimental optimization for Workflow's optimistic owned-lazy step path.
   * The runtime must guarantee one active handler for the owning queue message.
   */
  experimentalOwnedStepTransactions?: boolean;
  /**
   * Writes successful owned-step transactions in the compact journal format.
   *
   * Reader support is unconditional. Enable writing only after every process
   * in a rolling deployment runs a version that understands the format.
   */
  experimentalCompactCompletedStepCommits?: boolean;
  /** Executes run-journal transitions in an Ursula-hosted WebAssembly reducer. */
  experimentalServerReducerModuleId?: string;
}

function positiveInteger(
  value: string | undefined,
  name: string
): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeInteger(
  value: string | undefined,
  name: string
): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function boolean(
  value: string | undefined,
  fallback: boolean,
  name: string
): boolean {
  if (value === undefined || value === '') return fallback;
  if (value === '1' || value === 'true') return true;
  if (value === '0' || value === 'false') return false;
  throw new Error(`${name} must be 1, 0, true, or false`);
}

function environmentConfig(): UrsulaWorldConfig {
  const baseUrl = process.env.WORKFLOW_URSULA_URL;
  if (!baseUrl) {
    throw new Error(
      'WORKFLOW_URSULA_URL is required when @tonbo-io/world-ursula is loaded directly'
    );
  }
  return {
    baseUrl,
    bucket: process.env.WORKFLOW_URSULA_BUCKET,
    token: process.env.WORKFLOW_URSULA_TOKEN,
    deploymentId: process.env.WORKFLOW_URSULA_DEPLOYMENT_ID,
    dispatcherEnabled: boolean(
      process.env.WORKFLOW_URSULA_QUEUE_DISPATCHER_ENABLED,
      true,
      'WORKFLOW_URSULA_QUEUE_DISPATCHER_ENABLED'
    ),
    deliveryBaseUrl: process.env.WORKFLOW_URSULA_QUEUE_DELIVERY_URL,
    longPollTimeoutMs: positiveInteger(
      process.env.WORKFLOW_URSULA_LONG_POLL_TIMEOUT_MS,
      'WORKFLOW_URSULA_LONG_POLL_TIMEOUT_MS'
    ),
    streamFlushIntervalMs: nonNegativeInteger(
      process.env.WORKFLOW_URSULA_STREAM_FLUSH_INTERVAL_MS,
      'WORKFLOW_URSULA_STREAM_FLUSH_INTERVAL_MS'
    ),
    pollIntervalMs: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_POLL_INTERVAL_MS,
      'WORKFLOW_URSULA_QUEUE_POLL_INTERVAL_MS'
    ),
    leaseDurationMs: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_LEASE_DURATION_MS,
      'WORKFLOW_URSULA_QUEUE_LEASE_DURATION_MS'
    ),
    retryDelayMs: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_RETRY_DELAY_MS,
      'WORKFLOW_URSULA_QUEUE_RETRY_DELAY_MS'
    ),
    concurrency: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_CONCURRENCY,
      'WORKFLOW_URSULA_QUEUE_CONCURRENCY'
    ),
    partitionCount: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_PARTITIONS,
      'WORKFLOW_URSULA_QUEUE_PARTITIONS'
    ),
    partitionShardCount: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_COUNT,
      'WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_COUNT'
    ),
    partitionShardIndex: nonNegativeInteger(
      process.env.WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_INDEX,
      'WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_INDEX'
    ),
    partitionShardReplicas: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_REPLICAS,
      'WORKFLOW_URSULA_QUEUE_PARTITION_SHARD_REPLICAS'
    ),
    shutdownGraceMs: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS,
      'WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS'
    ),
    experimentalOwnedStepTransactions:
      process.env.WORKFLOW_URSULA_EXPERIMENTAL_OWNED_STEP_TRANSACTIONS === '1',
    experimentalCompactCompletedStepCommits:
      process.env
        .WORKFLOW_URSULA_EXPERIMENTAL_COMPACT_COMPLETED_STEP_COMMITS === '1',
    experimentalServerReducerModuleId:
      process.env.WORKFLOW_URSULA_EXPERIMENTAL_SERVER_REDUCER_MODULE_ID,
  };
}

/**
 * Replaces only a World's chunk streams with Ursula.
 *
 * Kept as an incremental migration seam for existing custom Worlds. The
 * package's `createWorld()` no longer uses a delegate: its Storage, Streamer
 * and Queue are all Ursula-backed.
 */
export function withUrsulaStreams(
  world: World,
  config: UrsulaStreamerConfig
): World {
  return {
    ...world,
    ...createStreamer(config),
  };
}

/**
 * Creates a complete Ursula-backed Workflow World.
 *
 * Ursula is the only durable source of truth. In-memory dispatcher state and
 * query caches are disposable and rebuilt from Ursula journals.
 */
export function createWorld(
  config: UrsulaWorldConfig = environmentConfig()
): World {
  const client = new UrsulaClient(config);
  const journal = new RunJournal(client, {
    compactCompletedStepCommits:
      config.experimentalCompactCompletedStepCommits,
  });
  const executions = new RunExecutionCoordinator({
    allowOwnedLazyStarts: config.experimentalOwnedStepTransactions,
  });
  const { storage } = createStorage(client, {
    journal,
    executions,
    serverReducerModuleId: config.experimentalServerReducerModuleId,
  });
  const queue = createQueue(client, config, executions);
  return {
    specVersion: SPEC_VERSION_CURRENT,
    capabilities: {
      preconditionGuard: true,
      maxConcurrency: true,
    },
    processExitTriggersQueueRedelivery: false,
    ...storage,
    ...createStreamer(config),
    queue: queue.queue,
    createQueueHandler: queue.createQueueHandler,
    getDeploymentId: queue.getDeploymentId,
    start: queue.start,
    close: queue.close,
  };
}
