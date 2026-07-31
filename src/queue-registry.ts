import { createHash } from 'node:crypto';
import type { ValidQueueName } from '@workflow/world';
import { ValidQueueName as ValidQueueNameSchema } from '@workflow/world';
import { type UrsulaClient, UrsulaRequestError } from './client.js';

const QUEUE_REGISTRY_STREAM = 'registry-queues';
export const RUN_QUEUE_REGISTRY_SHARDS = 32;

function runQueueRegistryStream(shard: number): string {
  return `registry-run-queues-${shard.toString(16).padStart(2, '0')}`;
}

export function runQueueRegistryShard(runId: string): number {
  return (
    (createHash('sha256').update(runId).digest()[0] ?? 0) %
    RUN_QUEUE_REGISTRY_SHARDS
  );
}

interface QueueRegistration {
  version: 1 | 2 | 3;
  queueName: ValidQueueName;
  partition?: number;
  runId?: string;
}

export interface QueueTarget {
  queueName: ValidQueueName;
  partition: number;
  runId?: string;
}

export class QueueRegistry {
  private readonly queueNames = new Set<ValidQueueName>();
  private readonly partitionsByQueue = new Map<ValidQueueName, Set<number>>();
  private readonly targetsByKey = new Map<string, QueueTarget>();
  private readonly runNextRecords = new Map<number, number>();
  private nextRecord = 0;

  constructor(private readonly client: UrsulaClient) {}

  private applyRecords(
    records: {
      record: number;
      value: QueueRegistration;
    }[]
  ): void {
    for (const { record, value } of records) {
      if (record < this.nextRecord) continue;
      if (record !== this.nextRecord) {
        throw new Error(
          `Ursula queue registry is discontinuous at record ${this.nextRecord}`
        );
      }
      const queueName = ValidQueueNameSchema.parse(value.queueName);
      this.queueNames.add(queueName);
      if (
        value.version === 2 &&
        Number.isSafeInteger(value.partition) &&
        (value.partition ?? -1) >= 0
      ) {
        let partitions = this.partitionsByQueue.get(queueName);
        if (!partitions) {
          partitions = new Set();
          this.partitionsByQueue.set(queueName, partitions);
        }
        partitions.add(value.partition as number);
      }
      if (
        value.version === 3 &&
        typeof value.runId === 'string' &&
        value.runId.length > 0
      ) {
        const target = {
          queueName,
          partition: value.partition ?? 0,
          runId: value.runId,
        } satisfies QueueTarget;
        this.targetsByKey.set(this.targetKey(target), target);
      }
      this.nextRecord = record + 1;
    }
  }

  private targetKey(target: QueueTarget): string {
    return `${target.queueName}\u0000${target.runId ?? ''}\u0000${target.partition}`;
  }

  async register(queueName: ValidQueueName, partition: number): Promise<void> {
    if (this.partitionsByQueue.get(queueName)?.has(partition)) return;
    await this.client.append(
      QUEUE_REGISTRY_STREAM,
      { version: 2, queueName, partition } satisfies QueueRegistration,
      {
        operationId: `register-queue:${queueName}:partition:${partition}`,
        createIfMissing: true,
      }
    );
    this.queueNames.add(queueName);
    let partitions = this.partitionsByQueue.get(queueName);
    if (!partitions) {
      partitions = new Set();
      this.partitionsByQueue.set(queueName, partitions);
    }
    partitions.add(partition);
  }

  async registerRun(queueName: ValidQueueName, runId: string): Promise<void> {
    const target = { queueName, partition: 0, runId } satisfies QueueTarget;
    const key = this.targetKey(target);
    if (this.targetsByKey.has(key)) return;
    const shard = runQueueRegistryShard(runId);
    await this.client.append(
      runQueueRegistryStream(shard),
      { version: 3, queueName, partition: 0, runId } satisfies QueueRegistration,
      {
        operationId: `register-run-queue:${queueName}:${runId}`,
        createIfMissing: true,
      }
    );
    this.queueNames.add(queueName);
    this.targetsByKey.set(key, target);
  }

  private applyRunRecords(
    shard: number,
    records: { record: number; value: QueueRegistration }[]
  ): void {
    let nextRecord = this.runNextRecords.get(shard) ?? 0;
    for (const { record, value } of records) {
      if (record < nextRecord) continue;
      if (record !== nextRecord) {
        throw new Error(
          `Ursula run queue registry shard ${shard} is discontinuous at record ${nextRecord}`
        );
      }
      if (
        value.version === 3 &&
        typeof value.runId === 'string' &&
        value.runId.length > 0
      ) {
        const queueName = ValidQueueNameSchema.parse(value.queueName);
        const target = {
          queueName,
          partition: value.partition ?? 0,
          runId: value.runId,
        } satisfies QueueTarget;
        this.queueNames.add(queueName);
        this.targetsByKey.set(this.targetKey(target), target);
      }
      nextRecord = record + 1;
    }
    this.runNextRecords.set(shard, nextRecord);
  }

  async watchRunChanges(
    onChange: () => void,
    shards: readonly number[],
    signal?: AbortSignal
  ): Promise<void> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal?.addEventListener('abort', abort, { once: true });
    try {
      await Promise.all(
        shards.map(async (shard) => {
          const stream = runQueueRegistryStream(shard);
          await this.client.ensureJsonStream(stream);
          await this.client.watchRecords<QueueRegistration>(
            stream,
            this.runNextRecords.get(shard) ?? 0,
            (records) => {
              this.applyRunRecords(shard, records);
              if (records.length > 0) onChange();
            },
            controller.signal
          );
        })
      );
    } finally {
      controller.abort();
      signal?.removeEventListener('abort', abort);
    }
  }

  current(): ValidQueueName[] {
    return [...this.queueNames];
  }

  partitions(queueName: ValidQueueName): number[] {
    return [...(this.partitionsByQueue.get(queueName) ?? [])].sort(
      (left, right) => left - right
    );
  }

  targets(): QueueTarget[] {
    return [...this.targetsByKey.values()];
  }

  async list(): Promise<ValidQueueName[]> {
    // The dispatcher may start before the first producer has created the
    // bucket. Creating the registry stream here makes startup deterministic
    // instead of letting an initial BucketNotFound terminate the poll loop.
    await this.client.ensureJsonStream(QUEUE_REGISTRY_STREAM);
    try {
      let cursor = this.nextRecord;
      while (true) {
        const page = await this.client.read<QueueRegistration>(
          QUEUE_REGISTRY_STREAM,
          cursor
        );
        this.applyRecords(page.records);
        if (page.records.length < 1000) return [...this.queueNames];
        if (this.nextRecord <= cursor) {
          throw new Error('Ursula queue registry pagination made no progress');
        }
        cursor = this.nextRecord;
      }
    } catch (error) {
      if (error instanceof UrsulaRequestError && error.status === 404) {
        return [];
      }
      throw error;
    }
  }

  async waitForChange(
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<boolean> {
    await this.client.ensureJsonStream(QUEUE_REGISTRY_STREAM);
    const page = await this.client.waitForRecords<QueueRegistration>(
      QUEUE_REGISTRY_STREAM,
      this.nextRecord,
      timeoutMs,
      signal
    );
    this.applyRecords(page.records);
    return page.records.length > 0;
  }
}
