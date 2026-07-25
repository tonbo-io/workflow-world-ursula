import { createHash } from 'node:crypto';
import type { Event, Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import {
  EventSchema,
  HookSchema,
  isTerminalWorkflowRunStatus,
  StepSchema,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import {
  isUrsulaRequestError,
  type UrsulaClient,
  type UrsulaReadResult,
} from './client.js';

export interface EntityChange<T> {
  id: string;
  value: T | null;
}

export interface RunCommit {
  version: 1;
  operationId: string;
  runId: string;
  previousRecord: number;
  events: Event[];
  run?: WorkflowRun;
  steps?: EntityChange<Step>[];
  hooks?: EntityChange<Hook>[];
  waits?: EntityChange<Wait>[];
  externalStateUpdatedAt?: number;
}

export interface RunJournalState {
  runId: string;
  nextRecord: number;
  run?: WorkflowRun;
  steps: Map<string, Step>;
  hooks: Map<string, Hook>;
  hookRetentionUntil: Map<string, Date>;
  waits: Map<string, Wait>;
  externalStateUpdatedAt?: number;
}

interface RunCheckpoint {
  version: 1;
  runId: string;
  sourceNextRecord: number;
  run?: WorkflowRun;
  steps: Step[];
  hooks: Hook[];
  hookRetentionUntil: [string, string][];
  waits: Wait[];
  externalStateUpdatedAt?: number;
}

interface EventCache {
  nextRecord: number;
  events: Event[];
}

const CHECKPOINT_INTERVAL_RECORDS = 128;
const STATE_CACHE_MAX_RUNS = 256;
const EVENT_CACHE_MAX_RUNS = 128;
const FOLLOWER_CATCH_UP_RETRIES = 6;

function isCursorBeyondLocalTail(error: unknown): boolean {
  return (
    isUrsulaRequestError(error, 400) &&
    error.message.includes('InvalidRecordBoundaries')
  );
}

async function waitForFollowerCatchUp(attempt: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, Math.min(25, 2 ** attempt));
  });
}

function streamId(runId: string): string {
  const digest = createHash('sha256').update(runId).digest('base64url');
  return `run-${digest}`;
}

function checkpointStreamId(runId: string): string {
  const digest = createHash('sha256').update(runId).digest('base64url');
  return `run-checkpoint-${digest}`;
}

function emptyState(runId: string): RunJournalState {
  return {
    runId,
    nextRecord: 0,
    steps: new Map(),
    hooks: new Map(),
    hookRetentionUntil: new Map(),
    waits: new Map(),
  };
}

function cloneState(state: RunJournalState): RunJournalState {
  // Reducer evaluation is pure, so calls may share the immutable entity maps.
  // A successful append mutates them only after Ursula accepts the guarded
  // record. Competing callers retain their own nextRecord scalar and therefore
  // still fail the stale CAS before applying their materialization.
  return { ...state };
}

function checkpointFromState(state: RunJournalState): RunCheckpoint {
  return {
    version: 1,
    runId: state.runId,
    sourceNextRecord: state.nextRecord,
    ...(state.run ? { run: state.run } : {}),
    steps: [...state.steps.values()],
    hooks: [...state.hooks.values()],
    hookRetentionUntil: [...state.hookRetentionUntil].map(([hookId, value]) => [
      hookId,
      value.toISOString(),
    ]),
    waits: [...state.waits.values()],
    ...(state.externalStateUpdatedAt !== undefined
      ? { externalStateUpdatedAt: state.externalStateUpdatedAt }
      : {}),
  };
}

function stateFromCheckpoint(
  runId: string,
  value: unknown
): RunJournalState | undefined {
  if (typeof value !== 'object' || value === null) return;
  const checkpoint = value as Partial<RunCheckpoint>;
  if (
    checkpoint.version !== 1 ||
    checkpoint.runId !== runId ||
    !Number.isSafeInteger(checkpoint.sourceNextRecord) ||
    (checkpoint.sourceNextRecord as number) < 0 ||
    !Array.isArray(checkpoint.steps) ||
    !Array.isArray(checkpoint.hooks) ||
    !Array.isArray(checkpoint.hookRetentionUntil) ||
    !Array.isArray(checkpoint.waits)
  ) {
    return;
  }
  try {
    return {
      runId,
      nextRecord: checkpoint.sourceNextRecord as number,
      ...(checkpoint.run
        ? { run: WorkflowRunSchema.parse(checkpoint.run) }
        : {}),
      steps: new Map(
        checkpoint.steps.map((step) => {
          const parsed = StepSchema.parse(step);
          return [parsed.stepId, parsed];
        })
      ),
      hooks: new Map(
        checkpoint.hooks.map((hook) => {
          const parsed = HookSchema.parse(hook);
          return [parsed.hookId, parsed];
        })
      ),
      hookRetentionUntil: new Map(
        checkpoint.hookRetentionUntil.map(([hookId, value]) => [
          hookId,
          new Date(value),
        ])
      ),
      waits: new Map(
        checkpoint.waits.map((wait) => {
          const parsed = WaitSchema.parse(wait);
          return [parsed.waitId, parsed];
        })
      ),
      ...(checkpoint.externalStateUpdatedAt !== undefined
        ? { externalStateUpdatedAt: checkpoint.externalStateUpdatedAt }
        : {}),
    };
  } catch {
    return;
  }
}

function parseCommit(value: unknown): RunCommit {
  if (typeof value !== 'object' || value === null) {
    throw new Error('Invalid Ursula World run commit');
  }
  const commit = value as Partial<RunCommit>;
  if (
    commit.version !== 1 ||
    typeof commit.operationId !== 'string' ||
    typeof commit.runId !== 'string' ||
    !Number.isSafeInteger(commit.previousRecord) ||
    !Array.isArray(commit.events)
  ) {
    throw new Error('Invalid Ursula World run commit');
  }
  return {
    version: 1,
    operationId: commit.operationId,
    runId: commit.runId,
    previousRecord: commit.previousRecord as number,
    events: commit.events.map((event) => EventSchema.parse(event)),
    ...(commit.run ? { run: WorkflowRunSchema.parse(commit.run) } : {}),
    ...(commit.steps
      ? {
          steps: commit.steps.map(({ id, value }) => ({
            id,
            value: value === null ? null : StepSchema.parse(value),
          })),
        }
      : {}),
    ...(commit.hooks
      ? {
          hooks: commit.hooks.map(({ id, value }) => ({
            id,
            value: value === null ? null : HookSchema.parse(value),
          })),
        }
      : {}),
    ...(commit.waits
      ? {
          waits: commit.waits.map(({ id, value }) => ({
            id,
            value: value === null ? null : WaitSchema.parse(value),
          })),
        }
      : {}),
    ...(commit.externalStateUpdatedAt !== undefined
      ? { externalStateUpdatedAt: commit.externalStateUpdatedAt }
      : {}),
  };
}

function applyChanges<T>(
  target: Map<string, T>,
  changes: EntityChange<T>[] | undefined
): void {
  for (const { id, value } of changes ?? []) {
    if (value === null) target.delete(id);
    else target.set(id, value);
  }
}

function applyHookRetention(
  state: RunJournalState,
  events: Event[],
  hookChanges: EntityChange<Hook>[] | undefined
): void {
  for (const event of events) {
    if (event.eventType === 'hook_created') {
      const retentionUntil = event.eventData.tokenRetentionUntil;
      if (retentionUntil) {
        state.hookRetentionUntil.set(event.correlationId, retentionUntil);
      } else {
        state.hookRetentionUntil.delete(event.correlationId);
      }
    } else if (event.eventType === 'hook_disposed') {
      state.hookRetentionUntil.delete(event.correlationId);
    }
  }
  for (const change of hookChanges ?? []) {
    if (change.value === null) state.hookRetentionUntil.delete(change.id);
  }
}

function applyCommit(
  state: RunJournalState,
  commit: RunCommit,
  record: number
): void {
  if (commit.runId !== state.runId) {
    throw new Error(
      `Run journal ${state.runId} contains commit for ${commit.runId}`
    );
  }
  if (commit.previousRecord !== record) {
    throw new Error(
      `Run journal ${state.runId} is discontinuous at record ${record}`
    );
  }
  applyHookRetention(state, commit.events, commit.hooks);
  if (commit.run) state.run = commit.run;
  applyChanges(state.steps, commit.steps);
  applyChanges(state.hooks, commit.hooks);
  applyChanges(state.waits, commit.waits);
  if (commit.externalStateUpdatedAt !== undefined) {
    state.externalStateUpdatedAt = commit.externalStateUpdatedAt;
  }
  state.nextRecord = record + 1;
}

/**
 * Authoritative per-run journal. Every record contains both events and the
 * post-event entity changes, making materialization atomic at Ursula's
 * single-stream consensus boundary.
 */
export class RunJournal {
  private readonly checkpointTasks = new Map<string, Promise<void>>();
  private readonly cache = new Map<string, RunJournalState>();
  private readonly eventCache = new Map<string, EventCache>();

  constructor(private readonly client: UrsulaClient) {}

  private rememberState(runId: string, state: RunJournalState): void {
    this.cache.delete(runId);
    if (state.run && isTerminalWorkflowRunStatus(state.run.status)) return;
    this.cache.set(runId, cloneState(state));
    while (this.cache.size > STATE_CACHE_MAX_RUNS) {
      const oldest = this.cache.keys().next().value;
      if (typeof oldest === 'string') this.cache.delete(oldest);
    }
  }

  private rememberEvents(runId: string, events: EventCache): void {
    this.eventCache.delete(runId);
    this.eventCache.set(runId, events);
    while (this.eventCache.size > EVENT_CACHE_MAX_RUNS) {
      const oldest = this.eventCache.keys().next().value;
      if (typeof oldest === 'string') this.eventCache.delete(oldest);
    }
  }

  evict(runId: string): void {
    this.cache.delete(runId);
    this.eventCache.delete(runId);
  }

  /**
   * Returns the process-local materialization without an incremental read.
   *
   * Mutations are guarded by the stream record tail, so a stale cache cannot
   * commit: the append returns 412 and the caller evicts before retrying. For
   * a create-if-missing mutation, starting optimistically at record zero is
   * safe for the same reason and avoids two cold reads for a brand-new run.
   */
  async loadForMutation(
    runId: string,
    options: { assumeEmpty?: boolean; createIfMissing?: boolean } = {}
  ): Promise<RunJournalState> {
    if (options.createIfMissing && !options.assumeEmpty) {
      await this.client.ensureJsonStream(streamId(runId));
    }
    const cached = this.cache.get(runId);
    if (cached) {
      this.cache.delete(runId);
      this.cache.set(runId, cached);
      return cloneState(cached);
    }
    if (options.assumeEmpty) {
      const state = emptyState(runId);
      this.rememberState(runId, state);
      return cloneState(state);
    }
    return this.load(runId);
  }

  private async loadCheckpoint(runId: string): Promise<RunJournalState> {
    let records: { value: unknown }[];
    try {
      records = (await this.client.readTail<unknown>(checkpointStreamId(runId)))
        .records;
    } catch (error) {
      if (isUrsulaRequestError(error, 404)) {
        return emptyState(runId);
      }
      throw error;
    }
    if (records.length === 0) return emptyState(runId);
    const state = stateFromCheckpoint(runId, records[0]?.value);
    if (state) return state;
    throw new Error(`Run "${runId}" has an invalid latest checkpoint`);
  }

  private async writeCheckpoint(
    runId: string,
    checkpoint: RunCheckpoint
  ): Promise<void> {
    const stream = checkpointStreamId(runId);
    const receipt = await this.client.append(stream, checkpoint, {
      operationId: `run-checkpoint:${runId}:${checkpoint.sourceNextRecord}`,
      createIfMissing: true,
    });
    await this.client.publishSnapshotAtRecord(
      stream,
      receipt.nextRecord,
      checkpoint
    );
    await this.client.advanceRetentionAtRecord(stream, receipt.startRecord);
  }

  private scheduleCheckpoint(state: RunJournalState): void {
    if (
      state.nextRecord === 0 ||
      state.nextRecord % CHECKPOINT_INTERVAL_RECORDS !== 0
    ) {
      return;
    }
    const runId = state.runId;
    // Capture the exact boundary before later commits mutate the shared entity
    // maps. Checkpoint persistence is derived work and must not extend the
    // latency of the already-authoritative source append.
    const checkpoint = checkpointFromState(state);
    const previous =
      this.checkpointTasks.get(runId) ?? Promise.resolve();
    const task = previous
      .catch(() => undefined)
      .then(() => this.writeCheckpoint(runId, checkpoint))
      .catch((error: unknown) => {
        console.error('Ursula World run checkpoint failed', {
          runId,
          sourceNextRecord: checkpoint.sourceNextRecord,
          error: error instanceof Error ? error.message : String(error),
        });
      });
    this.checkpointTasks.set(runId, task);
    void task.then(() => {
      if (this.checkpointTasks.get(runId) === task) {
        this.checkpointTasks.delete(runId);
      }
    });
  }

  async flushCheckpoints(): Promise<void> {
    while (this.checkpointTasks.size > 0) {
      await Promise.all(this.checkpointTasks.values());
    }
  }

  private applyRecords(
    state: RunJournalState,
    records: { record: number; value: RunCommit }[]
  ): void {
    for (const record of records) {
      if (record.record < state.nextRecord) continue;
      if (record.record !== state.nextRecord) {
        throw new Error(
          `Run journal ${state.runId} is discontinuous at record ${state.nextRecord}`
        );
      }
      applyCommit(state, parseCommit(record.value), record.record);
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: LRU hits, cold checkpoints, pagination, and create-if-missing recovery form one journal load boundary.
  async load(
    runId: string,
    options: { createIfMissing?: boolean; cache?: boolean } = {}
  ): Promise<RunJournalState> {
    const useCache = options.cache !== false;
    const stream = streamId(runId);
    if (options.createIfMissing) {
      await this.client.ensureJsonStream(stream);
    }
    const cached = useCache ? this.cache.get(runId) : undefined;
    if (cached) {
      this.cache.delete(runId);
      this.cache.set(runId, cached);
      let cursor = cached.nextRecord;
      let catchUpAttempt = 0;
      while (true) {
        let page: UrsulaReadResult<RunCommit>;
        try {
          page = await this.client.read<RunCommit>(stream, cursor);
        } catch (error) {
          if (
            isCursorBeyondLocalTail(error) &&
            catchUpAttempt < FOLLOWER_CATCH_UP_RETRIES
          ) {
            await waitForFollowerCatchUp(catchUpAttempt);
            catchUpAttempt += 1;
            continue;
          }
          if (isCursorBeyondLocalTail(error)) {
            this.cache.delete(runId);
            return this.load(runId, options);
          }
          throw error;
        }
        catchUpAttempt = 0;
        this.applyRecords(cached, page.records);
        if (page.records.length < 1000) {
          if (cached.run && isTerminalWorkflowRunStatus(cached.run.status)) {
            this.cache.delete(runId);
          }
          return cloneState(cached);
        }
        if (cached.nextRecord <= cursor) {
          throw new Error('Ursula run journal pagination made no progress');
        }
        cursor = cached.nextRecord;
      }
    }

    const state = await this.loadCheckpoint(runId);
    try {
      this.applyRecords(
        state,
        await this.client.readAll<RunCommit>(stream, state.nextRecord)
      );
    } catch (error) {
      if (
        options.createIfMissing &&
        isUrsulaRequestError(error, 404)
      ) {
        if (useCache) this.rememberState(runId, state);
        return useCache ? cloneState(state) : state;
      }
      throw error;
    }
    if (useCache) this.rememberState(runId, state);
    return useCache ? cloneState(state) : state;
  }

  async events(runId: string): Promise<Event[]> {
    let cached = this.eventCache.get(runId);
    if (!cached) {
      cached = { nextRecord: 0, events: [] };
      this.rememberEvents(runId, cached);
    } else {
      this.rememberEvents(runId, cached);
    }
    let cursor = cached.nextRecord;
    let catchUpAttempt = 0;
    while (true) {
      let page: UrsulaReadResult<RunCommit>;
      try {
        page = await this.client.read<RunCommit>(streamId(runId), cursor);
      } catch (error) {
        if (
          isCursorBeyondLocalTail(error) &&
          catchUpAttempt < FOLLOWER_CATCH_UP_RETRIES
        ) {
          await waitForFollowerCatchUp(catchUpAttempt);
          catchUpAttempt += 1;
          continue;
        }
        throw error;
      }
      catchUpAttempt = 0;
      for (const record of page.records) {
        if (record.record < cached.nextRecord) continue;
        if (record.record !== cached.nextRecord) {
          throw new Error(
            `Run event journal ${runId} is discontinuous at record ${cached.nextRecord}`
          );
        }
        cached.events.push(...parseCommit(record.value).events);
        cached.nextRecord = record.record + 1;
      }
      if (page.records.length < 1000) return [...cached.events];
      if (cached.nextRecord <= cursor) {
        throw new Error('Ursula run event pagination made no progress');
      }
      cursor = cached.nextRecord;
    }
  }

  async append(
    state: RunJournalState,
    commit: Omit<RunCommit, 'version' | 'runId' | 'previousRecord'>,
    options: { cache?: boolean } = {}
  ): Promise<RunJournalState> {
    const useCache = options.cache !== false;
    const value: RunCommit = {
      ...commit,
      version: 1,
      runId: state.runId,
      previousRecord: state.nextRecord,
    };
    await this.client.append(streamId(state.runId), value, {
      operationId: commit.operationId,
      expectedRecord: state.nextRecord,
      createIfMissing: state.nextRecord === 0,
    });
    applyCommit(state, parseCommit(value), state.nextRecord);
    if (useCache) {
      const cached = this.cache.get(state.runId);
      if (cached && cached.nextRecord === value.previousRecord) {
        applyCommit(cached, parseCommit(value), cached.nextRecord);
      } else {
        this.rememberState(state.runId, state);
      }
      if (state.run && isTerminalWorkflowRunStatus(state.run.status)) {
        this.cache.delete(state.runId);
      }
      const events = this.eventCache.get(state.runId);
      if (events && events.nextRecord === value.previousRecord) {
        events.events.push(...value.events);
        events.nextRecord = value.previousRecord + 1;
      } else if (!events && value.previousRecord === 0) {
        this.rememberEvents(state.runId, {
          nextRecord: 1,
          events: [...value.events],
        });
      } else if (events) {
        this.rememberEvents(state.runId, events);
      }
    }
    this.scheduleCheckpoint(state);
    return state;
  }
}
