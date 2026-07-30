import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
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

export interface RunJournalOptions {
  /**
   * Writes the common owned successful-step transaction as a compact tuple.
   *
   * Readers always understand the compact representation, so a rolling
   * deployment must first ship reader support everywhere and enable this
   * option only in a second rollout.
   */
  compactCompletedStepCommits?: boolean;
}

type CompactCompletedStepFieldsV2 = [
  2,
  [string, string, string],
  string,
  Date | string,
  string,
  string,
  number,
  unknown,
  unknown,
  string,
  [
    number | null,
    number | null,
    number | null,
    number | null,
    string[] | null,
  ],
  number | null,
];

type CompactCompletedStepFieldsV3 = [
  3,
  [string, string, string],
  string,
  [Date | string, Date | string, Date | string],
  string,
  string,
  number,
  unknown,
  unknown,
  string,
  Record<string, unknown>,
  number | null,
];

interface CompactCompletedStepCommit {
  v: 2;
  c: CompactCompletedStepFieldsV3;
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

export function runStreamId(runId: string): string {
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

function cloneStateForPreview(state: RunJournalState): RunJournalState {
  return {
    ...state,
    steps: new Map(state.steps),
    hooks: new Map(state.hooks),
    hookRetentionUntil: new Map(state.hookRetentionUntil),
    waits: new Map(state.waits),
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

function objectHasOnlyKeys(
  value: Record<string, unknown>,
  allowed: ReadonlySet<string>
): boolean {
  return Object.entries(value).every(
    ([key, nested]) => nested === undefined || allowed.has(key)
  );
}

function sameInstant(left: unknown, right: unknown): boolean {
  const leftTime =
    left instanceof Date ? left.getTime() : Date.parse(String(left));
  const rightTime =
    right instanceof Date ? right.getTime() : Date.parse(String(right));
  return (
    Number.isFinite(leftTime) &&
    Number.isFinite(rightTime) &&
    leftTime === rightTime
  );
}

const EVENT_KEYS = new Set([
  'correlationId',
  'createdAt',
  'eventData',
  'eventId',
  'eventType',
  'runId',
  'specVersion',
]);
const CREATED_STEP_DATA_KEYS = new Set(['input', 'stepName']);
const STARTED_STEP_DATA_KEYS = new Set([
  'ownerMessageId',
  'stepName',
  'workflowName',
]);
const COMPLETED_STEP_KEYS = new Set([
  'attempt',
  'completedAt',
  'createdAt',
  'input',
  'output',
  'runId',
  'specVersion',
  'startedAt',
  'status',
  'stepId',
  'stepName',
  'updatedAt',
]);
const COMPACTABLE_COMMIT_KEYS = new Set([
  'events',
  'externalStateUpdatedAt',
  'operationId',
  'previousRecord',
  'runId',
  'steps',
  'version',
]);

/**
 * Losslessly recognizes the World-visible hot owned-step shape.
 *
 * The tuple omits data already implied by the stream coordinate and the
 * transaction shape: run id, record number, event field names, and the
 * materialized Step fields duplicated by the three lifecycle events. The
 * request-only operation id is already represented by Ursula producer
 * deduplication and is not retained in v2. Any schema extension or
 * non-identical World field falls back to the v1 object.
 */
function compactCompletedStepCommit(
  commit: RunCommit
): CompactCompletedStepCommit | undefined {
  if (
    commit.events.length !== 3 ||
    commit.run !== undefined ||
    commit.steps?.length !== 1 ||
    commit.hooks !== undefined ||
    commit.waits !== undefined ||
    !objectHasOnlyKeys(
      commit as unknown as Record<string, unknown>,
      COMPACTABLE_COMMIT_KEYS
    )
  ) {
    return undefined;
  }
  const [created, started, completed] = commit.events;
  const stepChange = commit.steps[0];
  if (
    created?.eventType !== 'step_created' ||
    started?.eventType !== 'step_started' ||
    completed?.eventType !== 'step_completed' ||
    !stepChange ||
    stepChange.value === null
  ) {
    return undefined;
  }
  const step = stepChange.value;
  const createdData = created.eventData as Record<string, unknown>;
  const startedData = started.eventData as Record<string, unknown>;
  const completedData = completed.eventData as Record<string, unknown>;
  if (
    !objectHasOnlyKeys(created as unknown as Record<string, unknown>, EVENT_KEYS) ||
    !objectHasOnlyKeys(started as unknown as Record<string, unknown>, EVENT_KEYS) ||
    !objectHasOnlyKeys(
      completed as unknown as Record<string, unknown>,
      EVENT_KEYS
    ) ||
    !objectHasOnlyKeys(createdData, CREATED_STEP_DATA_KEYS) ||
    !objectHasOnlyKeys(startedData, STARTED_STEP_DATA_KEYS) ||
    !objectHasOnlyKeys(
      step as unknown as Record<string, unknown>,
      COMPLETED_STEP_KEYS
    )
  ) {
    return undefined;
  }

  const stepId = created.correlationId;
  const createdAt = created.createdAt;
  const startedAt = started.createdAt;
  const completedAt = completed.createdAt;
  const stepName = createdData.stepName;
  const workflowName = startedData.workflowName;
  const ownerMessageId = startedData.ownerMessageId;
  const specVersion = created.specVersion;
  if (
    typeof stepId !== 'string' ||
    typeof stepName !== 'string' ||
    typeof workflowName !== 'string' ||
    typeof ownerMessageId !== 'string' ||
    typeof specVersion !== 'number' ||
    started.correlationId !== stepId ||
    completed.correlationId !== stepId ||
    stepChange.id !== stepId ||
    step.stepId !== stepId ||
    created.runId !== commit.runId ||
    started.runId !== commit.runId ||
    completed.runId !== commit.runId ||
    step.runId !== commit.runId ||
    started.specVersion !== specVersion ||
    completed.specVersion !== specVersion ||
    step.specVersion !== specVersion ||
    startedData.stepName !== stepName ||
    completedData.stepName !== stepName ||
    completedData.workflowName !== workflowName ||
    step.stepName !== stepName ||
    step.status !== 'completed' ||
    step.attempt !== 1 ||
    !sameInstant(step.createdAt, createdAt) ||
    !sameInstant(step.startedAt, startedAt) ||
    !sameInstant(step.completedAt, completedAt) ||
    !sameInstant(step.updatedAt, completedAt) ||
    !isDeepStrictEqual(step.input, createdData.input) ||
    !isDeepStrictEqual(step.output, completedData.result)
  ) {
    return undefined;
  }

  if (createdData.input === undefined || completedData.result === undefined) {
    return undefined;
  }
  const completedExtras = Object.fromEntries(
    Object.entries(completedData).filter(
      ([key, nested]) =>
        nested !== undefined &&
        key !== 'result' &&
        key !== 'stepName' &&
        key !== 'workflowName'
    )
  );

  return {
    v: 2,
    c: [
      3,
      [created.eventId, started.eventId, completed.eventId],
      stepId,
      [createdAt, startedAt, completedAt],
      stepName,
      workflowName,
      specVersion,
      createdData.input,
      completedData.result,
      ownerMessageId,
      completedExtras,
      commit.externalStateUpdatedAt ?? null,
    ],
  };
}

function parseCompactCompletedStepCommitV2(
  value: unknown[],
  runId: string,
  record: number
): RunCommit {
  if (
    value.length !== 12 ||
    value[0] !== 2 ||
    !Array.isArray(value[1]) ||
    value[1].length !== 3 ||
    !value[1].every((eventId) => typeof eventId === 'string') ||
    typeof value[2] !== 'string' ||
    !(typeof value[3] === 'string' || value[3] instanceof Date) ||
    typeof value[4] !== 'string' ||
    typeof value[5] !== 'string' ||
    typeof value[6] !== 'number' ||
    typeof value[9] !== 'string' ||
    !Array.isArray(value[10]) ||
    value[10].length !== 5 ||
    (value[11] !== null && typeof value[11] !== 'number')
  ) {
    throw new Error('Invalid compact Ursula World completed-step commit');
  }
  const [
    ,
    eventIds,
    stepId,
    createdAt,
    stepName,
    workflowName,
    specVersion,
    input,
    output,
    ownerMessageId,
    telemetry,
    externalStateUpdatedAt,
  ] = value as CompactCompletedStepFieldsV2;
  const [ttfs, stso, stepCount, eventCount, optimizations] = telemetry;
  if (
    [ttfs, stso, stepCount, eventCount].some(
      (nested) => nested !== null && typeof nested !== 'number'
    ) ||
    (optimizations !== null &&
      (!Array.isArray(optimizations) ||
        !optimizations.every((item) => typeof item === 'string')))
  ) {
    throw new Error('Invalid compact Ursula World completed-step telemetry');
  }
  const optional = <T>(nested: T | null): T | undefined =>
    nested === null ? undefined : nested;
  const common = { runId, createdAt, specVersion, correlationId: stepId };
  const events = [
    EventSchema.parse({
      ...common,
      eventId: eventIds[0],
      eventType: 'step_created',
      eventData: { input, stepName },
    }),
    EventSchema.parse({
      ...common,
      eventId: eventIds[1],
      eventType: 'step_started',
      eventData: { ownerMessageId, stepName, workflowName },
    }),
    EventSchema.parse({
      ...common,
      eventId: eventIds[2],
      eventType: 'step_completed',
      eventData: {
        result: output,
        stepName,
        workflowName,
        ...(optional(ttfs) === undefined ? {} : { ttfs }),
        ...(optional(stso) === undefined ? {} : { stso }),
        ...(optional(stepCount) === undefined ? {} : { stepCount }),
        ...(optional(eventCount) === undefined ? {} : { eventCount }),
        ...(optional(optimizations) === undefined ? {} : { optimizations }),
      },
    }),
  ];
  const step = StepSchema.parse({
    runId,
    stepId,
    stepName,
    status: 'completed',
    input,
    output,
    attempt: 1,
    startedAt: createdAt,
    completedAt: createdAt,
    createdAt,
    updatedAt: createdAt,
    specVersion,
  });
  return {
    version: 1,
    operationId: '',
    runId,
    previousRecord: record,
    events,
    steps: [{ id: stepId, value: step }],
    ...(externalStateUpdatedAt === null ? {} : { externalStateUpdatedAt }),
  };
}

function parseCompactCompletedStepCommitV3(
  value: unknown[],
  runId: string,
  record: number
): RunCommit {
  const eventTimes = value[3];
  const completedExtras = value[10];
  if (
    value.length !== 12 ||
    value[0] !== 3 ||
    !Array.isArray(value[1]) ||
    value[1].length !== 3 ||
    !value[1].every((eventId) => typeof eventId === 'string') ||
    typeof value[2] !== 'string' ||
    !Array.isArray(eventTimes) ||
    eventTimes.length !== 3 ||
    !eventTimes.every(
      (nested) => typeof nested === 'string' || nested instanceof Date
    ) ||
    typeof value[4] !== 'string' ||
    typeof value[5] !== 'string' ||
    typeof value[6] !== 'number' ||
    typeof value[9] !== 'string' ||
    typeof completedExtras !== 'object' ||
    completedExtras === null ||
    Array.isArray(completedExtras) ||
    (value[11] !== null && typeof value[11] !== 'number')
  ) {
    throw new Error('Invalid compact Ursula World completed-step commit');
  }
  const [
    ,
    eventIds,
    stepId,
    [createdAt, startedAt, completedAt],
    stepName,
    workflowName,
    specVersion,
    input,
    output,
    ownerMessageId,
    extras,
    externalStateUpdatedAt,
  ] = value as CompactCompletedStepFieldsV3;
  if (
    Object.hasOwn(extras, 'result') ||
    Object.hasOwn(extras, 'stepName') ||
    Object.hasOwn(extras, 'workflowName')
  ) {
    throw new Error('Invalid compact Ursula World completed-step extras');
  }
  const events = [
    EventSchema.parse({
      runId,
      eventId: eventIds[0],
      eventType: 'step_created',
      correlationId: stepId,
      createdAt,
      specVersion,
      eventData: { input, stepName },
    }),
    EventSchema.parse({
      runId,
      eventId: eventIds[1],
      eventType: 'step_started',
      correlationId: stepId,
      createdAt: startedAt,
      specVersion,
      eventData: { ownerMessageId, stepName, workflowName },
    }),
    EventSchema.parse({
      runId,
      eventId: eventIds[2],
      eventType: 'step_completed',
      correlationId: stepId,
      createdAt: completedAt,
      specVersion,
      eventData: {
        result: output,
        stepName,
        workflowName,
        ...extras,
      },
    }),
  ];
  const step = StepSchema.parse({
    runId,
    stepId,
    stepName,
    status: 'completed',
    input,
    output,
    attempt: 1,
    createdAt,
    startedAt,
    completedAt,
    updatedAt: completedAt,
    specVersion,
  });
  return {
    version: 1,
    operationId: '',
    runId,
    previousRecord: record,
    events,
    steps: [{ id: stepId, value: step }],
    ...(externalStateUpdatedAt === null ? {} : { externalStateUpdatedAt }),
  };
}

function parseCommit(
  value: unknown,
  runId?: string,
  record?: number
): RunCommit {
  if (
    typeof value === 'object' &&
    value !== null &&
    (value as { v?: unknown }).v === 2
  ) {
    if (runId === undefined || record === undefined) {
      throw new Error('Compact Ursula World commit requires stream coordinates');
    }
    const fields = (value as { c?: unknown }).c;
    if (!Array.isArray(fields)) {
      throw new Error('Invalid compact Ursula World completed-step commit');
    }
    return fields[0] === 3
      ? parseCompactCompletedStepCommitV3(fields, runId, record)
      : parseCompactCompletedStepCommitV2(fields, runId, record);
  }
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
  private readonly compactCompletedStepCommits: boolean;

  constructor(
    private readonly client: UrsulaClient,
    options: RunJournalOptions = {}
  ) {
    this.compactCompletedStepCommits =
      options.compactCompletedStepCommits ?? false;
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
        parseCommit(record.value, state.runId, record.record),
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
    const stream = runStreamId(runId);
    const cached = useCache ? this.cache.get(runId) : undefined;
    if (cached) {
      this.cache.delete(runId);
      this.cache.set(runId, cached);
      let cursor = cached.nextRecord;
      let catchUpAttempt = 0;
      while (true) {
        let page: UrsulaReadResult<unknown>;
        try {
          page = await this.client.read<unknown>(stream, cursor);
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
    try {
      const page = await this.client.read<unknown>(
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
    } catch (error) {
      if (
        options.createIfMissing &&
        isUrsulaRequestError(error, 404)
      ) {
        if (useCache) this.rememberState(runId, prefix);
        return useCache ? cloneState(prefix) : prefix;
      }
      throw error;
    }

    const state = await this.loadCheckpoint(runId);
    this.applyRecords(
      state,
      await this.client.readAll<unknown>(stream, state.nextRecord)
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
        page = await this.client.read<unknown>(runStreamId(runId), cursor);
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
          ...parseCommit(record.value, runId, record.record).events
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
    const useCache = options.cache !== false;
    const value: RunCommit = {
      ...commit,
      version: 1,
      runId: state.runId,
      previousRecord: state.nextRecord,
    };
    const storedValue = this.compactCompletedStepCommits
      ? (compactCompletedStepCommit(value) ?? value)
      : value;
    await this.client.append(runStreamId(state.runId), storedValue, {
      operationId: commit.operationId,
      expectedRecord: state.nextRecord,
      createIfMissing: state.nextRecord === 0,
    });
    // `value` was built from already validated World entities above. Parsing
    // it again here would run Zod over every event twice on the hot path.
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
    return state;
  }

  async reduce<TIntent, TResult>(
    runId: string,
    moduleId: string,
    intent: TIntent,
    createIfMissing: boolean
  ): Promise<TResult> {
    const result = await this.client.reduce<TIntent, TResult>(
      runStreamId(runId),
      moduleId,
      intent,
      { createIfMissing }
    );
    this.evict(runId);
    return result.value;
  }
}
