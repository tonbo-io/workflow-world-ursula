import { createHash, randomUUID } from 'node:crypto';
import type {
  MessageId,
  QueueOptions,
  QueuePayload,
  ValidQueueName,
} from '@workflow/world';
import {
  MessageId as MessageIdSchema,
  QueuePayloadSchema,
} from '@workflow/world';
import { ulid } from 'ulid';
import {
  isUrsulaRequestError,
  type UrsulaClient,
  type UrsulaRecord,
} from './client.js';

const MAX_CAS_RETRIES = 32;
const CHECKPOINT_INTERVAL_RECORDS = 256;
const IDEMPOTENCY_RETENTION_MS = 24 * 60 * 60 * 1000;

type QueueTransition =
  | {
      version: 1;
      type: 'enqueued';
      messageId: MessageId;
      queueName: ValidQueueName;
      message: QueuePayload;
      headers?: Record<string, string>;
      idempotencyKey?: string;
      availableAt: string;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'leased';
      messageId: MessageId;
      leaseId: string;
      attempt: number;
      expiresAt: string;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'lease_extended';
      messageId: MessageId;
      leaseId: string;
      expiresAt: string;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'acked';
      messageId: MessageId;
      leaseId: string;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'retry_scheduled';
      messageId: MessageId;
      leaseId: string;
      availableAt: string;
      createdAt: string;
    };

export interface QueueMessageState {
  messageId: MessageId;
  queueName: ValidQueueName;
  message: QueuePayload;
  headers?: Record<string, string>;
  idempotencyKey?: string;
  availableAt: Date;
  createdAt: Date;
  attempt: number;
  status: 'pending' | 'leased' | 'acked';
  leaseId?: string;
  leaseExpiresAt?: Date;
}

interface QueueState {
  nextRecord: number;
  messages: Map<MessageId, QueueMessageState>;
  idempotency: Map<string, IdempotencyEntry>;
}

interface IdempotencyEntry {
  messageId: MessageId;
  expiresAt: Date;
}

interface QueueCheckpoint {
  version: 1;
  queueName: ValidQueueName;
  sourceNextRecord: number;
  messages: Array<
    Omit<QueueMessageState, 'availableAt' | 'createdAt' | 'leaseExpiresAt'> & {
      availableAt: string;
      createdAt: string;
      leaseExpiresAt?: string;
    }
  >;
  idempotency: [string, MessageId, string][];
}

export interface QueueLease {
  message: QueueMessageState;
  leaseId: string;
}

function queueStream(queueName: ValidQueueName): string {
  const digest = createHash('sha256').update(queueName).digest('base64url');
  return `queue-${digest}`;
}

function checkpointStream(queueName: ValidQueueName): string {
  const digest = createHash('sha256').update(queueName).digest('base64url');
  return `queue-checkpoint-${digest}`;
}

function concurrencyKey(message: QueuePayload): string {
  if ('runId' in message) return `run:${message.runId}`;
  if ('workflowRunId' in message) return `run:${message.workflowRunId}`;
  return `health:${message.correlationId}`;
}

function pruneIdempotency(state: QueueState, now: Date): void {
  for (const [key, entry] of state.idempotency) {
    if (entry.expiresAt.getTime() <= now.getTime()) {
      state.idempotency.delete(key);
    }
  }
}

function activeLeaseDeadlines(
  state: QueueState,
  nowMs: number
): Map<string, number> {
  const deadlines = new Map<string, number>();
  for (const message of state.messages.values()) {
    const leaseExpiresAt = message.leaseExpiresAt?.getTime() ?? 0;
    if (message.status === 'leased' && leaseExpiresAt > nowMs) {
      deadlines.set(concurrencyKey(message.message), leaseExpiresAt);
    }
  }
  return deadlines;
}

function replay(
  records: { record: number; value: QueueTransition }[]
): QueueState {
  const state: QueueState = {
    nextRecord: records.length,
    messages: new Map(),
    idempotency: new Map(),
  };
  for (const { value } of records) applyTransition(state, value);
  return state;
}

function applyTransition(state: QueueState, value: QueueTransition): void {
  if (value.version !== 1) {
    throw new Error('Unsupported Ursula queue journal version');
  }
  if (value.type === 'enqueued') {
    if (state.messages.has(value.messageId)) return;
    const message: QueueMessageState = {
      messageId: MessageIdSchema.parse(value.messageId),
      queueName: value.queueName,
      message: QueuePayloadSchema.parse(value.message),
      headers: value.headers,
      idempotencyKey: value.idempotencyKey,
      availableAt: new Date(value.availableAt),
      createdAt: new Date(value.createdAt),
      attempt: 0,
      status: 'pending',
    };
    state.messages.set(message.messageId, message);
    if (message.idempotencyKey) {
      state.idempotency.set(message.idempotencyKey, {
        messageId: message.messageId,
        expiresAt: new Date(
          message.createdAt.getTime() + IDEMPOTENCY_RETENTION_MS
        ),
      });
    }
    return;
  }
  const message = state.messages.get(value.messageId);
  if (!message) {
    throw new Error(
      `Queue transition references missing message "${value.messageId}"`
    );
  }
  if (value.type === 'leased') {
    message.status = 'leased';
    message.leaseId = value.leaseId;
    message.leaseExpiresAt = new Date(value.expiresAt);
    message.attempt = value.attempt;
  } else if (
    value.type === 'lease_extended' &&
    message.status === 'leased' &&
    message.leaseId === value.leaseId
  ) {
    message.leaseExpiresAt = new Date(value.expiresAt);
  } else if (
    value.type === 'acked' &&
    message.status === 'leased' &&
    message.leaseId === value.leaseId
  ) {
    state.messages.delete(value.messageId);
  } else if (
    value.type === 'retry_scheduled' &&
    message.status === 'leased' &&
    message.leaseId === value.leaseId
  ) {
    message.status = 'pending';
    message.availableAt = new Date(value.availableAt);
    message.leaseId = undefined;
    message.leaseExpiresAt = undefined;
  }
}

export class QueueJournal {
  private readonly cache = new Map<ValidQueueName, QueueState>();
  private readonly checkpointTasks = new Map<ValidQueueName, Promise<void>>();

  constructor(private readonly client: UrsulaClient) {}

  private checkpointFromState(
    queueName: ValidQueueName,
    state: QueueState
  ): QueueCheckpoint {
    pruneIdempotency(state, new Date());
    return {
      version: 1,
      queueName,
      sourceNextRecord: state.nextRecord,
      messages: [...state.messages.values()].map((message) => {
        const { availableAt, createdAt, leaseExpiresAt, ...rest } = message;
        return {
          ...rest,
          availableAt: availableAt.toISOString(),
          createdAt: createdAt.toISOString(),
          ...(leaseExpiresAt
            ? { leaseExpiresAt: leaseExpiresAt.toISOString() }
            : {}),
        };
      }),
      idempotency: [...state.idempotency].map(
        ([key, { messageId, expiresAt }]) => [
          key,
          messageId,
          expiresAt.toISOString(),
        ]
      ),
    };
  }

  private stateFromCheckpoint(
    queueName: ValidQueueName,
    value: unknown
  ): QueueState | undefined {
    if (typeof value !== 'object' || value === null) return;
    const checkpoint = value as Partial<QueueCheckpoint>;
    if (
      checkpoint.version !== 1 ||
      checkpoint.queueName !== queueName ||
      !Number.isSafeInteger(checkpoint.sourceNextRecord) ||
      (checkpoint.sourceNextRecord as number) < 0 ||
      !Array.isArray(checkpoint.messages) ||
      !Array.isArray(checkpoint.idempotency)
    ) {
      return;
    }
    try {
      const messages = new Map<MessageId, QueueMessageState>();
      for (const value of checkpoint.messages) {
        const { availableAt, createdAt, leaseExpiresAt, ...rest } = value;
        const message: QueueMessageState = {
          ...rest,
          messageId: MessageIdSchema.parse(value.messageId),
          queueName,
          message: QueuePayloadSchema.parse(value.message),
          availableAt: new Date(availableAt),
          createdAt: new Date(createdAt),
          ...(leaseExpiresAt
            ? { leaseExpiresAt: new Date(leaseExpiresAt) }
            : {}),
        };
        messages.set(message.messageId, message);
      }
      const idempotency = new Map<string, IdempotencyEntry>();
      for (const [key, messageId, expiresAt] of checkpoint.idempotency) {
        const expiry = new Date(expiresAt);
        if (!Number.isFinite(expiry.getTime())) {
          throw new Error('Invalid queue idempotency expiry');
        }
        idempotency.set(key, {
          messageId: MessageIdSchema.parse(messageId),
          expiresAt: expiry,
        });
      }
      return {
        nextRecord: checkpoint.sourceNextRecord as number,
        messages,
        idempotency,
      };
    } catch {
      return;
    }
  }

  private async loadCheckpoint(queueName: ValidQueueName): Promise<QueueState> {
    let records: UrsulaRecord<unknown>[];
    try {
      records = (
        await this.client.readTail<unknown>(checkpointStream(queueName))
      ).records;
    } catch (error) {
      if (isUrsulaRequestError(error, 404)) {
        return replay([]);
      }
      throw error;
    }
    if (records.length === 0) return replay([]);
    const state = this.stateFromCheckpoint(queueName, records[0]?.value);
    if (state) {
      pruneIdempotency(state, new Date());
      return state;
    }
    throw new Error(`Queue "${queueName}" has an invalid latest checkpoint`);
  }

  private async writeCheckpoint(
    queueName: ValidQueueName,
    checkpoint: QueueCheckpoint
  ): Promise<void> {
    const checkpointStreamId = checkpointStream(queueName);
    const receipt = await this.client.append(checkpointStreamId, checkpoint, {
      operationId: `queue-checkpoint:${queueName}:${checkpoint.sourceNextRecord}`,
      createIfMissing: true,
    });
    await this.client.publishSnapshotAtRecord(
      checkpointStreamId,
      receipt.nextRecord,
      checkpoint
    );
    await this.client.advanceRetentionAtRecord(
      checkpointStreamId,
      receipt.startRecord
    );
    // Ursula only permits source retention after a checkpoint has been
    // published at or beyond the requested boundary. The adapter rebuilds
    // from the derived checkpoint stream, but this source snapshot is still
    // the safety proof that authorizes Ursula to discard the journal prefix.
    await this.client.publishSnapshotAtRecord(
      queueStream(queueName),
      checkpoint.sourceNextRecord,
      checkpoint
    );
    await this.client.advanceRetentionAtRecord(
      queueStream(queueName),
      checkpoint.sourceNextRecord
    );
  }

  private scheduleCheckpoint(
    queueName: ValidQueueName,
    state: QueueState
  ): void {
    if (
      state.nextRecord === 0 ||
      state.nextRecord % CHECKPOINT_INTERVAL_RECORDS !== 0
    ) {
      return;
    }
    const checkpoint = this.checkpointFromState(queueName, state);
    const previous =
      this.checkpointTasks.get(queueName) ?? Promise.resolve();
    const task = previous
      .catch(() => undefined)
      .then(() => this.writeCheckpoint(queueName, checkpoint))
      .catch((error: unknown) => {
        console.error('Ursula World queue checkpoint failed', {
          queueName,
          sourceNextRecord: checkpoint.sourceNextRecord,
          error: error instanceof Error ? error.message : String(error),
        });
      });
    this.checkpointTasks.set(queueName, task);
    void task.then(() => {
      if (this.checkpointTasks.get(queueName) === task) {
        this.checkpointTasks.delete(queueName);
      }
    });
  }

  async flushCheckpoints(): Promise<void> {
    while (this.checkpointTasks.size > 0) {
      await Promise.all(this.checkpointTasks.values());
    }
  }

  private async appendTransition(
    queueName: ValidQueueName,
    state: QueueState,
    transition: QueueTransition,
    operationId: string
  ): Promise<void> {
    const expectedRecord = state.nextRecord;
    await this.client.append(queueStream(queueName), transition, {
      operationId,
      expectedRecord,
    });
    // A concurrent long-poll waiter may have observed and applied our record
    // before the append response arrived. Never replay the same transition
    // twice into the shared cache.
    if (state.nextRecord === expectedRecord) {
      applyTransition(state, transition);
      state.nextRecord = expectedRecord + 1;
    }
    this.scheduleCheckpoint(queueName, state);
  }

  private applyRecords(
    state: QueueState,
    records: { record: number; value: QueueTransition }[]
  ): void {
    for (const { record, value } of records) {
      if (record < state.nextRecord) continue;
      if (record !== state.nextRecord) {
        throw new Error(
          `Queue journal is discontinuous at record ${state.nextRecord}`
        );
      }
      applyTransition(state, value);
      state.nextRecord = record + 1;
    }
  }

  private invalidate(queueName: ValidQueueName): void {
    this.cache.delete(queueName);
  }

  /**
   * Uses the local state as an optimistic CAS base. A remote writer can only
   * make this state stale, never unsafe: every transition append carries the
   * cached record tail and a 412 invalidates the cache before retry.
   */
  private async loadForMutation(
    queueName: ValidQueueName,
    options: { createIfMissing?: boolean } = {}
  ): Promise<QueueState> {
    const cached = this.cache.get(queueName);
    if (cached) {
      pruneIdempotency(cached, new Date());
      return cached;
    }
    if (options.createIfMissing)
      await this.client.ensureJsonStream(queueStream(queueName));
    return this.load(queueName);
  }

  async enqueue(
    queueName: ValidQueueName,
    message: QueuePayload,
    options: QueueOptions = {}
  ): Promise<MessageId> {
    QueuePayloadSchema.parse(message);
    const messageId = MessageIdSchema.parse(`msg_${ulid()}`);
    for (let attempt = 0; attempt < MAX_CAS_RETRIES; attempt += 1) {
      const state = await this.loadForMutation(queueName, {
        createIfMissing: true,
      });
      const now = new Date();
      pruneIdempotency(state, now);
      if (options.idempotencyKey) {
        const existing = state.idempotency.get(options.idempotencyKey);
        if (existing) return existing.messageId;
      }
      const availableAt = new Date(
        now.getTime() + Math.max(0, options.delaySeconds ?? 0) * 1000
      );
      const transition: QueueTransition = {
        version: 1,
        type: 'enqueued',
        messageId,
        queueName,
        message,
        headers: options.headers,
        idempotencyKey: options.idempotencyKey,
        availableAt: availableAt.toISOString(),
        createdAt: now.toISOString(),
      };
      try {
        await this.appendTransition(
          queueName,
          state,
          transition,
          `queue-enqueue:${queueName}:${messageId}`
        );
        return messageId;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.invalidate(queueName);
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Queue "${queueName}" remained contended during enqueue`);
  }

  private claimCandidate(
    state: QueueState,
    now: Date
  ): QueueMessageState | undefined {
    const activeLeaseKeys = new Set(
      [...state.messages.values()]
        .filter(
          (candidate) =>
            candidate.status === 'leased' &&
            (candidate.leaseExpiresAt?.getTime() ?? 0) > now.getTime()
        )
        .map((candidate) => concurrencyKey(candidate.message))
    );
    let message: QueueMessageState | undefined;
    for (const candidate of state.messages.values()) {
      if (candidate.status === 'acked') continue;
      if (candidate.availableAt.getTime() > now.getTime()) continue;
      if (activeLeaseKeys.has(concurrencyKey(candidate.message))) continue;
      if (
        candidate.status !== 'pending' &&
        (candidate.leaseExpiresAt?.getTime() ?? 0) > now.getTime()
      ) {
        continue;
      }
      if (
        !message ||
        candidate.availableAt.getTime() < message.availableAt.getTime() ||
        (candidate.availableAt.getTime() === message.availableAt.getTime() &&
          candidate.createdAt.getTime() < message.createdAt.getTime())
      ) {
        message = candidate;
      }
    }
    return message;
  }

  async claim(
    queueName: ValidQueueName,
    now: Date,
    leaseDurationMs: number
  ): Promise<QueueLease | null> {
    for (let retry = 0; retry < MAX_CAS_RETRIES; retry += 1) {
      let state = await this.loadForMutation(queueName);
      let message = this.claimCandidate(state, now);
      if (!message) {
        state = await this.load(queueName);
        message = this.claimCandidate(state, now);
      }
      if (!message) return null;
      const leaseId = randomUUID();
      const transition: QueueTransition = {
        version: 1,
        type: 'leased',
        messageId: message.messageId,
        leaseId,
        attempt: message.attempt + 1,
        expiresAt: new Date(now.getTime() + leaseDurationMs).toISOString(),
        createdAt: now.toISOString(),
      };
      try {
        await this.appendTransition(
          queueName,
          state,
          transition,
          `queue-lease:${message.messageId}:${leaseId}`
        );
        return {
          leaseId,
          message: {
            ...message,
            status: 'leased',
            attempt: transition.attempt,
            leaseId,
            leaseExpiresAt: new Date(transition.expiresAt),
          },
        };
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.invalidate(queueName);
          continue;
        }
        throw error;
      }
    }
    return null;
  }

  nextLocalDeadline(queueName: ValidQueueName, now: Date): Date | undefined {
    const state = this.cache.get(queueName);
    if (!state) return;
    const leaseDeadlines = activeLeaseDeadlines(state, now.getTime());
    let deadline: number | undefined;
    for (const message of state.messages.values()) {
      const availableAt = message.availableAt.getTime();
      const leaseExpiresAt =
        message.status === 'leased'
          ? (message.leaseExpiresAt?.getTime() ?? availableAt)
          : availableAt;
      const blockedUntil =
        leaseDeadlines.get(concurrencyKey(message.message)) ?? availableAt;
      const candidate = Math.max(availableAt, leaseExpiresAt, blockedUntil);
      if (candidate <= now.getTime()) return now;
      if (deadline === undefined || candidate < deadline) {
        deadline = candidate;
      }
    }
    return deadline === undefined ? undefined : new Date(deadline);
  }

  async extend(
    queueName: ValidQueueName,
    lease: QueueLease,
    expiresAt: Date
  ): Promise<boolean> {
    for (let retry = 0; retry < MAX_CAS_RETRIES; retry += 1) {
      let state = await this.loadForMutation(queueName);
      const current = state.messages.get(lease.message.messageId);
      if (current?.status !== 'leased' || current.leaseId !== lease.leaseId) {
        state = await this.load(queueName);
        const refreshed = state.messages.get(lease.message.messageId);
        if (
          refreshed?.status !== 'leased' ||
          refreshed.leaseId !== lease.leaseId
        ) {
          return false;
        }
      }
      try {
        const transition = {
          version: 1,
          type: 'lease_extended',
          messageId: lease.message.messageId,
          leaseId: lease.leaseId,
          expiresAt: expiresAt.toISOString(),
          createdAt: new Date().toISOString(),
        } satisfies QueueTransition;
        await this.appendTransition(
          queueName,
          state,
          transition,
          `queue-extend:${lease.leaseId}:${expiresAt.getTime()}`
        );
        return true;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.invalidate(queueName);
          continue;
        }
        throw error;
      }
    }
    return false;
  }

  async ack(queueName: ValidQueueName, lease: QueueLease): Promise<boolean> {
    return this.finish(queueName, lease, {
      version: 1,
      type: 'acked',
      messageId: lease.message.messageId,
      leaseId: lease.leaseId,
      createdAt: new Date().toISOString(),
    });
  }

  async retry(
    queueName: ValidQueueName,
    lease: QueueLease,
    availableAt: Date
  ): Promise<boolean> {
    return this.finish(queueName, lease, {
      version: 1,
      type: 'retry_scheduled',
      messageId: lease.message.messageId,
      leaseId: lease.leaseId,
      availableAt: availableAt.toISOString(),
      createdAt: new Date().toISOString(),
    });
  }

  private async finish(
    queueName: ValidQueueName,
    lease: QueueLease,
    transition: Extract<QueueTransition, { type: 'acked' | 'retry_scheduled' }>
  ): Promise<boolean> {
    for (let retry = 0; retry < MAX_CAS_RETRIES; retry += 1) {
      let state = await this.loadForMutation(queueName);
      let current = state.messages.get(lease.message.messageId);
      if (!current && transition.type === 'acked') {
        return true;
      }
      if (current?.status !== 'leased' || current.leaseId !== lease.leaseId) {
        state = await this.load(queueName);
        current = state.messages.get(lease.message.messageId);
        if (!current && transition.type === 'acked') return true;
        if (
          current?.status !== 'leased' ||
          current.leaseId !== lease.leaseId
        ) {
          return false;
        }
      }
      try {
        await this.appendTransition(
          queueName,
          state,
          transition,
          `queue-${transition.type}:${lease.leaseId}`
        );
        return true;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.invalidate(queueName);
          continue;
        }
        throw error;
      }
    }
    return false;
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: cached paging, 410 checkpoint recovery, and first-load 404 handling are one recovery boundary.
  private async load(queueName: ValidQueueName): Promise<QueueState> {
    const cached = this.cache.get(queueName);
    if (cached) {
      try {
        let cursor = cached.nextRecord;
        while (true) {
          const page = await this.client.read<QueueTransition>(
            queueStream(queueName),
            cursor
          );
          this.applyRecords(cached, page.records);
          if (page.records.length < 1000) {
            pruneIdempotency(cached, new Date());
            return cached;
          }
          if (cached.nextRecord <= cursor) {
            throw new Error('Ursula queue journal pagination made no progress');
          }
          cursor = cached.nextRecord;
        }
      } catch (error) {
        if (!isUrsulaRequestError(error, 410)) {
          throw error;
        }
        this.cache.delete(queueName);
      }
    }
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const state = await this.loadCheckpoint(queueName);
        this.applyRecords(
          state,
          await this.client.readAll<QueueTransition>(
            queueStream(queueName),
            state.nextRecord
          )
        );
        pruneIdempotency(state, new Date());
        this.cache.set(queueName, state);
        return state;
      } catch (error) {
        if (isUrsulaRequestError(error, 404)) {
          const state = replay([]);
          this.cache.set(queueName, state);
          return state;
        }
        if (
          isUrsulaRequestError(error, 410) &&
          attempt === 0
        ) {
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Queue "${queueName}" checkpoint recovery exhausted`);
  }

  async waitForChange(
    queueName: ValidQueueName,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<boolean> {
    // Registration and the first enqueue are separate Ursula appends. A
    // remote dispatcher can observe the registry entry in between them, so
    // make the queue stream exist before starting its long-poll.
    await this.client.ensureJsonStream(queueStream(queueName));
    const state = this.cache.get(queueName) ?? (await this.load(queueName));
    try {
      const page = await this.client.waitForRecords<QueueTransition>(
        queueStream(queueName),
        state.nextRecord,
        timeoutMs,
        signal
      );
      this.applyRecords(state, page.records);
      return page.records.length > 0;
    } catch (error) {
      if (!isUrsulaRequestError(error, 410)) {
        throw error;
      }
      this.cache.delete(queueName);
      await this.load(queueName);
      return true;
    }
  }
}
