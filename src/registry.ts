import { createHash } from 'node:crypto';
import { type UrsulaClient, UrsulaRequestError } from './client.js';

const RUN_REGISTRY_SHARDS = 32;
const RUN_REGISTRY_DIRECTORY = 'registry-run-shards';

interface RunRegistration {
  version: 1;
  runId: string;
  createdAt: string;
}

interface ShardRegistration {
  version: 1;
  shard: number;
}

function shardFor(runId: string): number {
  const first = createHash('sha256').update(runId).digest()[0] ?? 0;
  return first % RUN_REGISTRY_SHARDS;
}

function registryStream(shard: number): string {
  return `registry-runs-${shard.toString(16).padStart(2, '0')}`;
}

export class RunRegistry {
  private directoryNextRecord = 0;
  private readonly registrations = new Map<string, RunRegistration>();
  private readonly shardNextRecord = new Map<number, number>();
  private readonly shards = new Set<number>();

  constructor(private readonly client: UrsulaClient) {}

  private async loadShard(shard: number): Promise<void> {
    const start = this.shardNextRecord.get(shard) ?? 0;
    try {
      const records = await this.client.readAll<RunRegistration>(
        registryStream(shard),
        start
      );
      let nextRecord = start;
      for (const { record, value } of records) {
        if (record < nextRecord) continue;
        if (record !== nextRecord) {
          throw new Error(
            `Ursula run registry shard ${shard} is discontinuous at record ${nextRecord}`
          );
        }
        if (
          value.version === 1 &&
          typeof value.runId === 'string' &&
          typeof value.createdAt === 'string'
        ) {
          this.registrations.set(value.runId, value);
        }
        nextRecord = record + 1;
      }
      this.shardNextRecord.set(shard, nextRecord);
    } catch (error) {
      if (error instanceof UrsulaRequestError && error.status === 404) return;
      throw error;
    }
  }

  /**
   * Registration is written before the first run commit. A crash may leave an
   * ignorable entry for an empty run stream, but never an undiscoverable run.
   */
  async register(runId: string, createdAt: Date): Promise<void> {
    if (this.registrations.has(runId)) return;
    const shard = shardFor(runId);
    if (!this.shards.has(shard)) {
      await this.client.append(
        RUN_REGISTRY_DIRECTORY,
        { version: 1, shard } satisfies ShardRegistration,
        {
          operationId: `register-run-shard:${shard}`,
          createIfMissing: true,
        }
      );
      this.shards.add(shard);
    }
    const registration = {
      version: 1,
      runId,
      createdAt: createdAt.toISOString(),
    } satisfies RunRegistration;
    await this.client.append(
      registryStream(shard),
      registration,
      { operationId: `register-run:${runId}`, createIfMissing: true }
    );
    this.registrations.set(runId, registration);
  }

  async list(): Promise<RunRegistration[]> {
    try {
      const directory = await this.client.readAll<ShardRegistration>(
        RUN_REGISTRY_DIRECTORY,
        this.directoryNextRecord
      );
      for (const { record, value } of directory) {
        if (record < this.directoryNextRecord) continue;
        if (record !== this.directoryNextRecord) {
          throw new Error(
            `Ursula run registry directory is discontinuous at record ${this.directoryNextRecord}`
          );
        }
        if (
          Number.isSafeInteger(value.shard) &&
          value.shard >= 0 &&
          value.shard < RUN_REGISTRY_SHARDS
        ) {
          this.shards.add(value.shard);
        }
        this.directoryNextRecord = record + 1;
      }
    } catch (error) {
      if (error instanceof UrsulaRequestError && error.status === 404) {
        return [];
      }
      throw error;
    }

    const shards = [...this.shards];
    for (let offset = 0; offset < shards.length; offset += 8) {
      await Promise.all(
        shards.slice(offset, offset + 8).map((shard) => this.loadShard(shard))
      );
    }
    return [...this.registrations.values()];
  }
}
