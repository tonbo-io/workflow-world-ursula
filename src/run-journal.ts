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
  type UrsulaTransactionOperation,
} from './client.js';
import {
  DEFAULT_RUN_AFFINITY_LANES,
  runAffinity,
} from './affinity.js';

export interface EntityChange<T> {
  id: string;
  value: T | null;
}

export interface RunExecutionFence {
  lane: string;
  epoch: number;
  queueName: string;
  queuePartition: number;
  token: string;
  generation: number;
  ownerMessageId: string;
  attempt: number;
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
  executionFence?: RunExecutionFence;
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
  executionFences: Map<string, RunExecutionFence>;
}

export interface RunJournalOptions {
  /** Number of deterministic path-affinity lanes shared by run journals. */
  pathAffinityLanes?: number;
}

export interface PreparedRunAppend {
  operation: UrsulaTransactionOperation;
  apply(): void;
  deduplicated(): void;
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
  executionFences?: RunExecutionFence[];
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
    executionFences: new Map(),
  };
}

function cloneState(state: RunJournalState): RunJournalState {
  // Reducer evaluation is pure, so calls may share the immutable entity maps.
  // A successful append mutates them only after Ursula accepts the guarded
  // record. Competing callers retain their own nextRecord scalar and therefore
  // still fail the stale CAS before applying their materialization.
  return { ...state };
}

function cloneStateForPreview(state: RunJournalState): RunJournalState {
  return {
    ...state,
    steps: new Map(state.steps),
    hooks: new Map(state.hooks),
    hookRetentionUntil: new Map(state.hookRetentionUntil),
    waits: new Map(state.waits),
    executionFences: new Map(state.executionFences),
  };
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
    executionFences: [...state.executionFences.values()],
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
      executionFences: new Map(
        (checkpoint.executionFences ?? []).map((fence) => {
          const parsed = parseExecutionFence(fence);
          return [parsed.lane, parsed];
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

function parseExecutionFence(value: unknown): RunExecutionFence {
  if (
    typeof value !== 'object' ||
    value === null ||
    typeof (value as { lane?: unknown }).lane !== 'string' ||
    !Number.isSafeInteger((value as { epoch?: unknown }).epoch) ||
    ((value as { epoch: number }).epoch ?? 0) < 1 ||
    typeof (value as { queueName?: unknown }).queueName !== 'string' ||
    !Number.isSafeInteger(
      (value as { queuePartition?: unknown }).queuePartition
    ) ||
    ((value as { queuePartition: number }).queuePartition ?? -1) < 0 ||
    typeof (value as { token?: unknown }).token !== 'string' ||
    !Number.isSafeInteger((value as { generation?: unknown }).generation) ||
    ((value as { generation: number }).generation ?? -1) < 0 ||
    typeof (value as { ownerMessageId?: unknown }).ownerMessageId !==
      'string' ||
    !Number.isSafeInteger((value as { attempt?: unknown }).attempt) ||
    ((value as { attempt: number }).attempt ?? 0) < 1
  ) {
    throw new Error('Invalid Ursula World execution fence');
  }
  const fence = value as RunExecutionFence;
  return {
    lane: fence.lane,
    epoch: fence.epoch,
    queueName: fence.queueName,
    queuePartition: fence.queuePartition,
    token: fence.token,
    generation: fence.generation,
    ownerMessageId: fence.ownerMessageId,
    attempt: fence.attempt,
  };
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
    ...(commit.executionFence
      ? { executionFence: parseExecutionFence(commit.executionFence) }
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
  if (commit.executionFence) {
    state.executionFences.set(
      commit.executionFence.lane,
      commit.executionFence
    );
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
  private readonly affinityClients = new Map<string, UrsulaClient>();
  private readonly pathAffinityLanes: number;

  constructor(
    private readonly client: UrsulaClient,
    options: RunJournalOptions = {}
  ) {
    this.pathAffinityLanes =
      options.pathAffinityLanes ?? DEFAULT_RUN_AFFINITY_LANES;
  }

  private clientFor(runId: string): UrsulaClient {
    const affinity = runAffinity(runId, this.pathAffinityLanes);
    let client = this.affinityClients.get(affinity);
    if (!client) {
      client = this.client.withAffinity(affinity);
      this.affinityClients.set(affinity, client);
      while (this.affinityClients.size > STATE_CACHE_MAX_RUNS) {
        const oldest = this.affinityClients.keys().next().value;
        if (typeof oldest === 'string') this.affinityClients.delete(oldest);
      }
    } else {
      this.affinityClients.delete(affinity);
      this.affinityClients.set(affinity, client);
    }
    return client;
  }

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

  preview(
    state: RunJournalState,
    commit: Omit<RunCommit, 'version' | 'runId' | 'previousRecord'>
  ): RunJournalState {
    const preview = cloneStateForPreview(state);
    applyCommit(
      preview,
      {
        ...commit,
        version: 1,
        runId: state.runId,
        previousRecord: state.nextRecord,
      },
      state.nextRecord
    );
    return preview;
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
    return this.load(runId, {
      createIfMissing: options.createIfMissing,
    });
  }

  private async loadCheckpoint(runId: string): Promise<RunJournalState> {
    let records: { value: unknown }[];
    try {
      records = (await this.clientFor(runId).readTail<unknown>(checkpointStreamId(runId)))
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
    // A checkpoint this process cannot decode — a newer schema during a
    // rolling deployment, or a corrupt record — costs replay time, not the
    // run. Run source streams are never truncated (only the checkpoint
    // stream's own prefix is), so the authoritative events are still there
    // and an empty state replays them from record 0.
    console.warn(
      'Ursula run checkpoint could not be decoded; replaying the full journal',
      { runId }
    );
    return emptyState(runId);
  }

  private async writeCheckpoint(
    runId: string,
    checkpoint: RunCheckpoint
  ): Promise<void> {
    const stream = checkpointStreamId(runId);
    const client = this.clientFor(runId);
    const receipt = await client.append(stream, checkpoint, {
      operationId: `run-checkpoint:${runId}:${checkpoint.sourceNextRecord}`,
      createIfMissing: true,
    });
    await client.publishSnapshotAtRecord(
      stream,
      receipt.nextRecord,
      checkpoint
    );
    await client.advanceRetentionAtRecord(stream, receipt.startRecord);
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
    records: { record: number; value: unknown }[]
  ): void {
    for (const record of records) {
      if (record.record < state.nextRecord) continue;
      if (record.record !== state.nextRecord) {
        throw new Error(
          `Run journal ${state.runId} is discontinuous at record ${state.nextRecord}`
        );
      }
      applyCommit(
        state,
        parseCommit(record.value),
        record.record
      );
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: LRU hits, cold checkpoints, pagination, and create-if-missing recovery form one journal load boundary.
  async load(
    runId: string,
    options: { createIfMissing?: boolean; cache?: boolean } = {}
  ): Promise<RunJournalState> {
    const useCache = options.cache !== false;
    const stream = streamId(runId);
    const cached = useCache ? this.cache.get(runId) : undefined;
    if (cached) {
      this.cache.delete(runId);
      this.cache.set(runId, cached);
      let cursor = cached.nextRecord;
      let catchUpAttempt = 0;
      while (true) {
        let page: UrsulaReadResult<unknown>;
        try {
          page = await this.clientFor(runId).read<unknown>(stream, cursor);
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

    // Most Workflow runs finish well before the first checkpoint boundary.
    // Read that bounded prefix first so a cold process can materialize a short
    // run with one request instead of probing a checkpoint stream that cannot
    // exist and then reading the source journal. Long runs still resume from
    // their latest checkpoint; the bounded prefix is discarded once Ursula
    // reports that more source records remain.
    const prefix = emptyState(runId);
    for (
      let catchUpAttempt = 0;
      catchUpAttempt <= FOLLOWER_CATCH_UP_RETRIES;
      catchUpAttempt += 1
    ) {
      try {
        const page = await this.clientFor(runId).read<unknown>(
          stream,
          0,
          CHECKPOINT_INTERVAL_RECORDS
        );
        if (
          page.upToDate &&
          page.records.length < CHECKPOINT_INTERVAL_RECORDS
        ) {
          this.applyRecords(prefix, page.records);
          if (useCache) this.rememberState(runId, prefix);
          return useCache ? cloneState(prefix) : prefix;
        }
        break;
      } catch (error) {
        if (options.createIfMissing && isUrsulaRequestError(error, 404)) {
          if (useCache) this.rememberState(runId, prefix);
          return useCache ? cloneState(prefix) : prefix;
        }
        if (
          isUrsulaRequestError(error, 404) &&
          catchUpAttempt < FOLLOWER_CATCH_UP_RETRIES
        ) {
          await waitForFollowerCatchUp(catchUpAttempt);
          continue;
        }
        throw error;
      }
    }

    const state = await this.loadCheckpoint(runId);
    this.applyRecords(
      state,
      await this.clientFor(runId).readAll<unknown>(stream, state.nextRecord)
    );
    if (useCache) this.rememberState(runId, state);
    return useCache ? cloneState(state) : state;
  }

  private async loadEvents(
    runId: string,
    throughRecord?: number
  ): Promise<EventCache> {
    let cached = this.eventCache.get(runId);
    if (!cached) {
      cached = { nextRecord: 0, events: [] };
      this.rememberEvents(runId, cached);
    } else {
      this.rememberEvents(runId, cached);
    }
    // A successful mutation already installed every event through its
    // committed record in this process-local cache. Callers that only need
    // the response page through that commit do not need an empty tail GET.
    if (
      throughRecord !== undefined &&
      cached.nextRecord >= throughRecord
    ) {
      return cached;
    }
    let cursor = cached.nextRecord;
    let catchUpAttempt = 0;
    while (true) {
      let page: UrsulaReadResult<unknown>;
      try {
        page = await this.clientFor(runId).read<unknown>(streamId(runId), cursor);
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
        cached.events.push(
          ...parseCommit(record.value).events
        );
        cached.nextRecord = record.record + 1;
      }
      if (page.records.length < 1000) return cached;
      if (cached.nextRecord <= cursor) {
        throw new Error('Ursula run event pagination made no progress');
      }
      cursor = cached.nextRecord;
    }
  }

  async events(runId: string, throughRecord?: number): Promise<Event[]> {
    return [...(await this.loadEvents(runId, throughRecord)).events];
  }

  async eventPage(
    runId: string,
    start: number,
    limit: number,
    throughRecord?: number
  ): Promise<{ events: Event[]; total: number }> {
    const cached = await this.loadEvents(runId, throughRecord);
    return {
      events: cached.events.slice(start, start + limit),
      total: cached.events.length,
    };
  }

  async append(
    state: RunJournalState,
    commit: Omit<RunCommit, 'version' | 'runId' | 'previousRecord'>,
    options: { cache?: boolean } = {}
  ): Promise<RunJournalState> {
    const prepared = this.prepareAppend(state, commit, options);
    await this.clientFor(state.runId).append(
      prepared.operation.stream,
      prepared.operation.values,
      {
        operationId: prepared.operation.operationId,
        expectedRecord: prepared.operation.expectedRecord,
        createIfMissing: state.nextRecord === 0,
      }
    );
    prepared.apply();
    return state;
  }

  prepareAppend(
    state: RunJournalState,
    commit: Omit<RunCommit, 'version' | 'runId' | 'previousRecord'>,
    options: { cache?: boolean } = {}
  ): PreparedRunAppend {
    const useCache = options.cache !== false;
    const value: RunCommit = {
      ...commit,
      version: 1,
      runId: state.runId,
      previousRecord: state.nextRecord,
    };
    return {
      operation: {
        stream: streamId(state.runId),
        values: value,
        operationId: commit.operationId,
        expectedRecord: state.nextRecord,
      },
      apply: () => {
        // `value` was built from already validated World entities above.
        // Parsing it again here would run Zod over every event twice.
        applyCommit(state, value, state.nextRecord);
        if (useCache) {
          const cached = this.cache.get(state.runId);
          if (cached && cached.nextRecord === value.previousRecord) {
            applyCommit(cached, value, cached.nextRecord);
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
      },
      deduplicated: () => {
        this.cache.delete(state.runId);
        this.eventCache.delete(state.runId);
      },
    };
  }
}
