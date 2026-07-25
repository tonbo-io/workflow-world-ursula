import type { ValidQueueName } from '@workflow/world';
import { ValidQueueName as ValidQueueNameSchema } from '@workflow/world';
import { type UrsulaClient, UrsulaRequestError } from './client.js';

const QUEUE_REGISTRY_STREAM = 'registry-queues';

interface QueueRegistration {
  version: 1;
  queueName: ValidQueueName;
}

export class QueueRegistry {
  private readonly queueNames = new Set<ValidQueueName>();
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
      this.queueNames.add(ValidQueueNameSchema.parse(value.queueName));
      this.nextRecord = record + 1;
    }
  }

  async register(queueName: ValidQueueName): Promise<void> {
    if (this.queueNames.has(queueName)) return;
    await this.client.append(
      QUEUE_REGISTRY_STREAM,
      { version: 1, queueName } satisfies QueueRegistration,
      {
        operationId: `register-queue:${queueName}`,
        createIfMissing: true,
      }
    );
    this.queueNames.add(queueName);
  }

  current(): ValidQueueName[] {
    return [...this.queueNames];
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
