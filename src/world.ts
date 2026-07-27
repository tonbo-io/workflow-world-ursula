import type { World } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { UrsulaClient } from './client.js';
import { createQueue, type UrsulaQueueConfig } from './queue.js';
import { createStorage } from './storage.js';
import { createStreamer, type UrsulaStreamerConfig } from './streamer.js';

export interface UrsulaWorldConfig
  extends UrsulaStreamerConfig,
    UrsulaQueueConfig {}

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
    shutdownGraceMs: positiveInteger(
      process.env.WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS,
      'WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS'
    ),
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
  const { storage } = createStorage(client);
  const queue = createQueue(client, config);
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
