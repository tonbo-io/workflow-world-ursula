import type { ValidQueueName } from '@workflow/world';
import { ValidQueueName as ValidQueueNameSchema } from '@workflow/world';
import type { UrsulaClient } from './client.js';

const QUEUE_REGISTRY_STREAM = 'registry-queues';

interface QueueRegistration {
  version: 3;
  queueName: ValidQueueName;
  partition: number;
  runId: string;
}

export interface QueueTarget {
  queueName: ValidQueueName;
  partition: number;
  runId: string;
}

export class QueueRegistry {
  private readonly targetsByKey = new Map<string, QueueTarget>();
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
      if (
        value.version !== 3 ||
        !Number.isSafeInteger(value.partition) ||
        value.partition < 0 ||
        typeof value.runId !== 'string' ||
        value.runId.length === 0
      ) {
        throw new Error('Invalid Ursula run-queue registry record');
      }
      const target = {
        queueName,
        partition: value.partition,
        runId: value.runId,
      } satisfies QueueTarget;
      this.targetsByKey.set(this.targetKey(target), target);
      this.nextRecord = record + 1;
    }
  }

  private targetKey(target: QueueTarget): string {
    return `${target.queueName}\u0000${target.runId}\u0000${target.partition}`;
  }

  async registerRun(queueName: ValidQueueName, runId: string): Promise<void> {
    const target = { queueName, partition: 0, runId } satisfies QueueTarget;
    const key = this.targetKey(target);
    if (this.targetsByKey.has(key)) return;
    await this.client.append(
      QUEUE_REGISTRY_STREAM,
      { version: 3, queueName, partition: 0, runId } satisfies QueueRegistration,
      {
        operationId: `register-run-queue:${queueName}:${runId}`,
        createIfMissing: true,
      }
    );
    this.targetsByKey.set(key, target);
  }

  targets(): QueueTarget[] {
    return [...this.targetsByKey.values()];
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
