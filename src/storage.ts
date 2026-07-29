import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import {
  EntityConflictError,
  HookNotFoundError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  EventResult,
  Hook,
  PaginatedResponse,
  PaginationOptions,
  ResolveData,
  Step,
  Storage,
  WorkflowRun,
} from '@workflow/world';
import {
  isTerminalWorkflowRunStatus,
  stripEventDataRefs,
} from '@workflow/world';
import { ulid } from 'ulid';
import {
  isUrsulaRequestError,
  UrsulaClient,
  type UrsulaClientConfig,
} from './client.js';
import {
  RunExecutionCoordinator,
  type StagedStepStart,
} from './execution.js';
import { HookClaims } from './hook-claims.js';
import { materializeEvent } from './reducer.js';
import { RunRegistry } from './registry.js';
import {
  RunJournal,
  type EntityChange,
  type RunCommit,
  type RunExecutionLease,
  type RunJournalState,
} from './run-journal.js';

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const MAX_COMMIT_RETRIES = 16;
const QUERY_READ_CONCURRENCY = 16;
const STEP_VISIBILITY_RETRIES = 8;
const STEP_VISIBILITY_BASE_DELAY_MS = 5;

type PendingRunCommit = Omit<
  RunCommit,
  'version' | 'runId' | 'previousRecord'
>;

function mergeChanges<T>(
  first: EntityChange<T>[] | undefined,
  second: EntityChange<T>[] | undefined
): EntityChange<T>[] | undefined {
  if (!first && !second) return;
  const merged = new Map<string, T | null>();
  for (const change of first ?? []) merged.set(change.id, change.value);
  for (const change of second ?? []) merged.set(change.id, change.value);
  return [...merged].map(([id, value]) => ({ id, value }));
}

function combineStepCommits(
  operationId: string,
  started: PendingRunCommit,
  terminal: PendingRunCommit
): PendingRunCommit {
  return {
    operationId,
    events: [...started.events, ...terminal.events],
    ...(terminal.run ?? started.run
      ? { run: terminal.run ?? started.run }
      : {}),
    ...(mergeChanges(started.steps, terminal.steps)
      ? { steps: mergeChanges(started.steps, terminal.steps) }
      : {}),
    ...(mergeChanges(started.hooks, terminal.hooks)
      ? { hooks: mergeChanges(started.hooks, terminal.hooks) }
      : {}),
    ...(mergeChanges(started.waits, terminal.waits)
      ? { waits: mergeChanges(started.waits, terminal.waits) }
      : {}),
    ...(mergeChanges(started.executionLeases, terminal.executionLeases)
      ? {
          executionLeases: mergeChanges(
            started.executionLeases,
            terminal.executionLeases
          ),
        }
      : {}),
    ...(terminal.externalStateUpdatedAt !== undefined
      ? { externalStateUpdatedAt: terminal.externalStateUpdatedAt }
      : started.externalStateUpdatedAt !== undefined
        ? { externalStateUpdatedAt: started.externalStateUpdatedAt }
        : {}),
  };
}

function ownsExecutionLease(
  state: RunJournalState,
  lease: RunExecutionLease
): boolean {
  const current = state.executionLeases.get(lease.lane);
  return (
    current?.token === lease.token &&
    current.generation === lease.generation
  );
}

async function mapWithConcurrency<T, R>(
  values: readonly T[],
  concurrency: number,
  mapper: (value: T) => Promise<R>
): Promise<R[]> {
  const results: R[] = [];
  for (let offset = 0; offset < values.length; offset += concurrency) {
    results.push(
      ...(await Promise.all(
        values.slice(offset, offset + concurrency).map(mapper)
      ))
    );
  }
  return results;
}

function encodeCursor(index: number): string {
  return Buffer.from(JSON.stringify({ index })).toString('base64url');
}

function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const value = JSON.parse(
      Buffer.from(cursor, 'base64url').toString('utf8')
    ) as { index?: unknown };
    if (
      typeof value.index === 'number' &&
      Number.isSafeInteger(value.index) &&
      value.index >= 0
    ) {
      return value.index;
    }
  } catch {
    // Throw the common contract error below.
  }
  throw new WorkflowWorldError('Invalid Ursula World pagination cursor');
}

function paginate<T>(
  values: T[],
  options: PaginationOptions | undefined
): PaginatedResponse<T> {
  const { start, limit } = paginationBounds(options);
  const data = values.slice(start, start + limit);
  const next = start + data.length;
  const hasMore = next < values.length;
  return {
    data,
    // World cursors identify the position after the last returned entity,
    // including on the final non-empty page. Runtime replay keeps this cursor
    // to request only events committed by the next mutation.
    cursor: data.length > 0 ? encodeCursor(next) : null,
    hasMore,
  };
}

function paginationBounds(
  options: PaginationOptions | undefined
): { start: number; limit: number } {
  const limit = options?.limit ?? DEFAULT_LIMIT;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
    throw new WorkflowWorldError(
      `Pagination limit must be between 1 and ${MAX_LIMIT}`
    );
  }
  return { start: decodeCursor(options?.cursor), limit };
}

function withoutRunData(
  run: WorkflowRun,
  resolveData: ResolveData | undefined
) {
  const detached = structuredClone(run);
  return resolveData === 'none'
    ? { ...detached, input: undefined, output: undefined }
    : detached;
}

function withoutStepData(step: Step, resolveData: ResolveData | undefined) {
  const detached = structuredClone(step);
  return resolveData === 'none'
    ? { ...detached, input: undefined, output: undefined }
    : detached;
}

function withoutHookData(hook: Hook, resolveData: ResolveData | undefined) {
  const detached = structuredClone(hook);
  return resolveData === 'none'
    ? { ...detached, metadata: undefined }
    : detached;
}

function runOf(state: RunJournalState): WorkflowRun {
  if (!state.run) throw new WorkflowRunNotFoundError(state.runId);
  return state.run;
}

function requestDigest(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('base64url');
}

export function mutationOperationId(
  state: RunJournalState,
  request: AnyEventRequest,
  callId: string,
  requestId: string | undefined
): string {
  const requestScope = requestId ? `request:${requestId}` : `call:${callId}`;
  return `run-mutation:${state.runId}:${state.nextRecord}:${requestScope}:${requestDigest(request)}`;
}

function missingRun(error: unknown, runId: string): never {
  if (isUrsulaRequestError(error, 404)) {
    throw new WorkflowRunNotFoundError(runId);
  }
  throw error;
}

async function withEventPage(
  result: EventResult,
  runId: string,
  journal: RunJournal,
  request: AnyEventRequest,
  params: Parameters<Storage['events']['create']>[2],
  throughRecord: number
): Promise<EventResult> {
  const preload =
    request.eventType === 'run_started' && params?.skipPreload !== true;
  const delta =
    (request.eventType === 'step_completed' ||
      request.eventType === 'step_failed') &&
    typeof params?.sinceCursor === 'string';
  if (!preload && !delta) return result;

  const { start, limit } = paginationBounds({
    cursor: delta ? params?.sinceCursor : undefined,
    limit: MAX_LIMIT,
    sortOrder: 'asc',
  });
  const page = await journal.eventPage(
    runId,
    start,
    limit,
    throughRecord
  );
  const next = start + page.events.length;
  return {
    ...result,
    events: page.events.map((event) =>
      stripEventDataRefs(event, params?.resolveData ?? 'all')
    ),
    cursor: page.events.length > 0 ? encodeCursor(next) : null,
    hasMore: next < page.total,
  };
}

export interface UrsulaStorage {
  storage: Storage;
  client: UrsulaClient;
}

export function createStorage(
  config: UrsulaClientConfig | UrsulaClient,
  options: {
    journal?: RunJournal;
    executions?: RunExecutionCoordinator;
  } = {}
): UrsulaStorage {
  const client =
    config instanceof UrsulaClient ? config : new UrsulaClient(config);
  const journal = options.journal ?? new RunJournal(client);
  const executions = options.executions;
  const registry = new RunRegistry(client);
  const hookClaims = new HookClaims(client);

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: hook expiry, run-CAS retry, and cross-stream claim release are one recovery transaction.
  async function cleanupExpiredHooks(
    initial: RunJournalState,
    now = new Date(),
    options: { cache?: boolean } = {}
  ): Promise<RunJournalState> {
    let state = initial;
    for (let attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt += 1) {
      if (!state.run || !isTerminalWorkflowRunStatus(state.run.status)) {
        return state;
      }
      const expired = [...state.hooks.values()].filter(
        (hook) =>
          (state.hookRetentionUntil.get(hook.hookId)?.getTime() ?? 0) <=
          now.getTime()
      );
      if (expired.length === 0) return state;
      try {
        await journal.append(
          state,
          {
            operationId: `hook-retention-cleanup:${state.runId}:${state.nextRecord}:${expired
              .map(({ hookId }) => hookId)
              .sort()
              .join(',')}`,
            events: [],
            hooks: expired.map(({ hookId }) => ({
              id: hookId,
              value: null,
            })),
          },
          { cache: options.cache }
        );
        for (const hook of expired) {
          await hookClaims.release(
            hook.token,
            { runId: hook.runId, hookId: hook.hookId },
            `retention-expired:${state.runId}:${state.nextRecord}:${hook.hookId}`
          );
        }
        return state;
      } catch (error) {
        if (
          isUrsulaRequestError(error, 412) &&
          attempt + 1 < MAX_COMMIT_RETRIES
        ) {
          state = await journal.load(state.runId, { cache: options.cache });
          continue;
        }
        throw error;
      }
    }
    return state;
  }

  async function loadRun(
    runId: string,
    options: { cache?: boolean } = {}
  ): Promise<RunJournalState> {
    try {
      let state = await journal.load(runId, { cache: options.cache });
      if (!state.run) throw new WorkflowRunNotFoundError(runId);
      state = await cleanupExpiredHooks(state, new Date(), {
        cache: options.cache,
      });
      return state;
    } catch (error) {
      return missingRun(error, runId);
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: reconciliation audits both the owner run and token journal before releasing ownership.
  async function reconcileHookClaim(token: string): Promise<void> {
    const claim = await hookClaims.get(token);
    if (!claim) return;
    let state: RunJournalState;
    try {
      state = await loadRun(claim.runId);
    } catch (error) {
      if (error instanceof WorkflowRunNotFoundError) return;
      throw error;
    }
    if (!state.run || !isTerminalWorkflowRunStatus(state.run.status)) return;
    const hook = state.hooks.get(claim.hookId);
    if (hook) {
      const retentionUntil = state.hookRetentionUntil.get(hook.hookId);
      if (retentionUntil && retentionUntil.getTime() > Date.now()) return;
      state = await cleanupExpiredHooks(state);
    }
    if (!state.hooks.has(claim.hookId)) {
      await hookClaims.release(
        token,
        { runId: claim.runId, hookId: claim.hookId },
        `reconcile-terminal:${claim.operationId}`
      );
    }
  }

  async function allStates(): Promise<RunJournalState[]> {
    const registrations = await registry.list();
    const states = await mapWithConcurrency(
      registrations,
      QUERY_READ_CONCURRENCY,
      async ({ runId }) => {
        try {
          return await loadRun(runId, { cache: false });
        } catch (error) {
          if (error instanceof WorkflowRunNotFoundError) return null;
          throw error;
        }
      }
    );
    return states.filter((state): state is RunJournalState => state !== null);
  }

  async function stageStepStart(
    runId: string,
    request: AnyEventRequest & { eventType: 'step_started' },
    params: CreateEventParams | undefined,
    callId: string
  ): Promise<EventResult | undefined> {
    const coordinator = executions;
    const lease = coordinator?.current(runId);
    const stepId = request.correlationId;
    const ownedLazyStart =
      coordinator?.allowsOwnedLazyStarts() === true &&
      request.eventData?.input !== undefined &&
      request.eventData.ownerMessageId !== undefined;
    if (!coordinator || (!lease && !ownedLazyStart) || !stepId) return;
    const existing = coordinator.staged(runId, stepId);
    if (existing) return existing.result;
    const state = await journal.loadForMutation(runId);
    if (lease && !ownsExecutionLease(state, lease)) {
      throw new EntityConflictError(
        `Execution lease for run "${runId}" changed ownership`
      );
    }
    const operationId = mutationOperationId(
      state,
      request,
      callId,
      params?.requestId
    );
    const eventId = `evnt_${ulid()}`;
    const syntheticEventId = `evnt_${ulid()}`;
    const now = new Date();
    const materialized = materializeEvent(state, request, {
      eventId,
      syntheticEventId,
      operationId,
      now,
      params,
    });
    if (!materialized.commit) return materialized.result;
    const staged: StagedStepStart = {
      request,
      params,
      callId,
      eventId,
      syntheticEventId,
      now,
      result: materialized.result,
    };
    coordinator.stage(runId, stepId, staged);
    return staged.result;
  }

  async function commitStagedStep(
    runId: string,
    request: AnyEventRequest,
    params: CreateEventParams | undefined,
    callId: string
  ): Promise<EventResult | undefined> {
    const coordinator = executions;
    const stepId =
      'correlationId' in request ? request.correlationId : undefined;
    const lease = coordinator?.current(runId);
    const staged = stepId ? coordinator?.staged(runId, stepId) : undefined;
    if (!coordinator || !stepId || !staged) return;
    const terminalEventId = `evnt_${ulid()}`;
    const terminalSyntheticEventId = `evnt_${ulid()}`;
    const terminalNow = new Date();
    return coordinator.exclusive(runId, async () => {
      for (let attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt += 1) {
        const state = await journal.loadForMutation(runId);
        if (lease && !ownsExecutionLease(state, lease)) {
          throw new EntityConflictError(
            `Execution lease for run "${runId}" changed ownership`
          );
        }
        const startOperationId = mutationOperationId(
          state,
          staged.request,
          staged.callId,
          staged.params?.requestId
        );
        const startParams = staged.params
          ? { ...staged.params, stateUpdatedAt: undefined }
          : undefined;
        const started = materializeEvent(state, staged.request, {
          eventId: staged.eventId,
          syntheticEventId: staged.syntheticEventId,
          operationId: startOperationId,
          now: staged.now,
          params: startParams,
        });
        if (!started.commit) {
          throw new EntityConflictError(
            `Step "${stepId}" no longer has a start transition`
          );
        }
        const preview = journal.preview(state, started.commit);
        const terminalOperationId = mutationOperationId(
          preview,
          request,
          callId,
          params?.requestId
        );
        const terminal = materializeEvent(preview, request, {
          eventId: terminalEventId,
          syntheticEventId: terminalSyntheticEventId,
          operationId: terminalOperationId,
          now: terminalNow,
          params,
        });
        if (!terminal.commit) return terminal.result;
        const owner =
          lease?.token ??
          staged.request.eventData?.ownerMessageId ??
          staged.callId;
        const operationId = `run-step-transaction:${runId}:${owner}:${stepId}:${requestDigest(request)}`;
        const commit = combineStepCommits(
          operationId,
          started.commit,
          terminal.commit
        );
        if (lease?.lane.startsWith('step:')) {
          commit.executionLeases = mergeChanges(commit.executionLeases, [
            { id: lease.lane, value: null },
          ]);
        }
        try {
          await journal.append(state, commit);
          coordinator.finish(runId, stepId);
          if (lease?.lane.startsWith('step:')) {
            coordinator.releaseLane(runId, lease.lane);
          }
          return withEventPage(
            terminal.result,
            runId,
            journal,
            request,
            params,
            state.nextRecord
          );
        } catch (error) {
          if (
            isUrsulaRequestError(error, 412) &&
            attempt + 1 < MAX_COMMIT_RETRIES
          ) {
            journal.evict(runId);
            continue;
          }
          throw error;
        }
      }
      throw new WorkflowWorldError(
        `Step "${stepId}" remained contended after ${MAX_COMMIT_RETRIES} attempts`
      );
    });
  }

  const storage: Storage = {
    runs: {
      async get(id, params) {
        return withoutRunData(runOf(await loadRun(id)), params?.resolveData);
      },
      async getMany(ids, params) {
        return mapWithConcurrency(ids, QUERY_READ_CONCURRENCY, async (id) => {
          try {
            return withoutRunData(
              runOf(await loadRun(id, { cache: false })),
              params?.resolveData
            );
          } catch (error) {
            if (error instanceof WorkflowRunNotFoundError) return null;
            throw error;
          }
        });
      },
      async list(params) {
        let runs = (await allStates()).map(runOf);
        if (params?.workflowName) {
          runs = runs.filter(
            ({ workflowName }) => workflowName === params.workflowName
          );
        }
        if (params?.status) {
          runs = runs.filter(({ status }) => status === params.status);
        }
        runs.sort(
          (left, right) => left.createdAt.getTime() - right.createdAt.getTime()
        );
        if (params?.pagination?.sortOrder === 'desc') runs.reverse();
        return paginate(
          runs.map((run) => withoutRunData(run, params?.resolveData)),
          params?.pagination
        );
      },
    } as Storage['runs'],
    steps: {
      async get(runId, stepId, params) {
        for (
          let attempt = 0;
          attempt < STEP_VISIBILITY_RETRIES;
          attempt += 1
        ) {
          const step = (await loadRun(runId)).steps.get(stepId);
          if (step) return withoutStepData(step, params?.resolveData);
          if (attempt + 1 < STEP_VISIBILITY_RETRIES) {
            // A queue message can be claimed by another adapter instance
            // immediately after the authoritative run append commits. Evict a
            // follower-backed materialization and briefly retry rather than
            // turning ordinary replica catch-up into a failed delivery.
            journal.evict(runId);
            await delay(STEP_VISIBILITY_BASE_DELAY_MS * 2 ** attempt);
          }
        }
        throw new WorkflowWorldError(`Step "${stepId}" not found`);
      },
      async list(params) {
        const steps = [...(await loadRun(params.runId)).steps.values()].sort(
          (left, right) => left.createdAt.getTime() - right.createdAt.getTime()
        );
        if (params.pagination?.sortOrder === 'desc') steps.reverse();
        return paginate(
          steps.map((step) => withoutStepData(step, params.resolveData)),
          params.pagination
        );
      },
    } as Storage['steps'],
    events: {
      // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: retry, registration, and run-ID creation are one atomic-facing World operation.
      async create(runId, data, params) {
        // Distinct World calls must never collapse merely because they raced
        // from the same record tail with identical payloads. The nonce remains
        // stable across this call's CAS retries but intentionally changes for
        // a separate call; the World contract exposes no true mutation key.
        const callId = randomUUID();
        const effectiveRunId =
          data.eventType === 'run_created' && !runId ? `wrun_${ulid()}` : runId;
        if (!effectiveRunId) {
          throw new WorkflowWorldError(
            'runId is required for non-run_created events'
          );
        }
        if (data.eventType === 'step_started') {
          const staged = await stageStepStart(
            effectiveRunId,
            data,
            params,
            callId
          );
          if (staged) return staged;
        } else if (
          data.eventType === 'step_completed' ||
          data.eventType === 'step_failed' ||
          data.eventType === 'step_retrying'
        ) {
          const committed = await commitStagedStep(
            effectiveRunId,
            data,
            params,
            callId
          );
          if (committed) return committed;
        }
        const lazyRunStart =
          data.eventType === 'run_started' &&
          data.eventData?.deploymentId !== undefined &&
          data.eventData.workflowName !== undefined &&
          data.eventData.input !== undefined;
        const createsRun = data.eventType === 'run_created' || lazyRunStart;
        const initialState = createsRun
          ? Promise.all([
              registry.register(effectiveRunId, new Date()),
              journal.loadForMutation(effectiveRunId, {
                assumeEmpty: data.eventType === 'run_created',
                createIfMissing: true,
              }),
            ]).then(([, state]) => state)
          : undefined;
        let assumeEmpty = data.eventType === 'run_created';
        for (let attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt += 1) {
          const state =
            attempt === 0 && initialState
              ? await initialState
              : await journal.loadForMutation(effectiveRunId, {
                  assumeEmpty,
                  createIfMissing: createsRun,
                });
          const op = mutationOperationId(
            state,
            data,
            callId,
            params?.requestId
          );
          if (data.eventType === 'hook_created') {
            await reconcileHookClaim(data.eventData.token);
          }
          const hookReservation =
            data.eventType === 'hook_created'
              ? await hookClaims.reserve({
                  operationId: op,
                  token: data.eventData.token,
                  runId: effectiveRunId,
                  hookId: data.correlationId,
                  retentionUntil: data.eventData.tokenRetentionUntil,
                })
              : undefined;
          const disposedHook =
            data.eventType === 'hook_disposed'
              ? state.hooks.get(data.correlationId)
              : undefined;
          const materialized = materializeEvent(state, data, {
            eventId: `evnt_${ulid()}`,
            syntheticEventId: `evnt_${ulid()}`,
            operationId: op,
            now: new Date(),
            params,
            hookTokenReserved: hookReservation?.acquired,
            ...(!hookReservation?.acquired && hookReservation
              ? { hookConflictRunId: hookReservation.claim.runId }
              : {}),
          });
          if (!materialized.commit) {
            return withEventPage(
              materialized.result,
              effectiveRunId,
              journal,
              data,
              params,
              state.nextRecord
            );
          }
          const removedHooks = (materialized.commit.hooks ?? [])
            .filter((change) => change.value === null)
            .map((change) => state.hooks.get(change.id))
            .filter((hook): hook is Hook => hook !== undefined);
          try {
            await journal.append(state, materialized.commit);
            if (hookReservation?.acquired) {
              await hookClaims.commit(hookReservation.claim, state.nextRecord);
            }
            if (disposedHook) {
              await hookClaims.release(
                disposedHook.token,
                {
                  runId: disposedHook.runId,
                  hookId: disposedHook.hookId,
                },
                `dispose:${op}:${disposedHook.hookId}`
              );
            }
            for (const releasedHook of removedHooks) {
              if (releasedHook.hookId === disposedHook?.hookId) continue;
              await hookClaims.release(
                releasedHook.token,
                {
                  runId: releasedHook.runId,
                  hookId: releasedHook.hookId,
                },
                `terminal:${op}:${releasedHook.hookId}`
              );
            }
            return withEventPage(
              materialized.result,
              effectiveRunId,
              journal,
              data,
              params,
              state.nextRecord
            );
          } catch (error) {
            if (
              isUrsulaRequestError(error, 412) &&
              attempt + 1 < MAX_COMMIT_RETRIES
            ) {
              journal.evict(effectiveRunId);
              assumeEmpty = false;
              continue;
            }
            throw error;
          }
        }
        throw new WorkflowWorldError(
          `Run "${effectiveRunId}" remained contended after ${MAX_COMMIT_RETRIES} attempts`
        );
      },
      async get(runId, eventId, params) {
        await loadRun(runId);
        const event = (await journal.events(runId)).find(
          (candidate) => candidate.eventId === eventId
        );
        if (!event)
          throw new WorkflowWorldError(`Event "${eventId}" not found`);
        return stripEventDataRefs(event, params?.resolveData ?? 'all');
      },
      async list(params) {
        await loadRun(params.runId);
        const events = (await journal.events(params.runId)).map((event) =>
          stripEventDataRefs(event, params.resolveData ?? 'all')
        );
        if (params.pagination?.sortOrder === 'desc') events.reverse();
        return paginate(events, params.pagination);
      },
      async listByCorrelationId(params) {
        const events: Event[] = [];
        for (const state of await allStates()) {
          events.push(
            ...(await journal.events(state.runId)).filter(
              ({ correlationId }) => correlationId === params.correlationId
            )
          );
        }
        events.sort(
          (left, right) => left.createdAt.getTime() - right.createdAt.getTime()
        );
        if (params.pagination?.sortOrder === 'desc') events.reverse();
        return paginate(
          events.map((event) =>
            stripEventDataRefs(event, params.resolveData ?? 'all')
          ),
          params.pagination
        );
      },
    },
    hooks: {
      async get(hookId, params) {
        for (const state of await allStates()) {
          const hook = state.hooks.get(hookId);
          if (hook) return withoutHookData(hook, params?.resolveData);
        }
        throw new HookNotFoundError(hookId);
      },
      async getByToken(token, params) {
        await reconcileHookClaim(token);
        const claim = await hookClaims.get(token);
        if (!claim) throw new HookNotFoundError(token);
        const hook = (await loadRun(claim.runId)).hooks.get(claim.hookId);
        if (!hook) throw new HookNotFoundError(token);
        return withoutHookData(hook, params?.resolveData);
      },
      async list(params) {
        const hooks = (await allStates())
          .flatMap((state) => [...state.hooks.values()])
          .filter((hook) => !params.runId || hook.runId === params.runId)
          .sort(
            (left, right) =>
              left.createdAt.getTime() - right.createdAt.getTime()
          );
        if (params.pagination?.sortOrder === 'desc') hooks.reverse();
        return paginate(
          hooks.map((hook) => withoutHookData(hook, params.resolveData)),
          params.pagination
        );
      },
    },
  };

  return { storage, client };
}
