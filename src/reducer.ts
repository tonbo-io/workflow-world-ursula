import {
  EntityConflictError,
  HookNotFoundError,
  PreconditionFailedError,
  RunExpiredError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  EventResult,
  Hook,
  Step,
  Wait,
  WorkflowRun,
} from '@workflow/world';
import {
  applyAttributeChanges,
  isChildEntityCreationEvent,
  isTerminalStepStatus,
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_CURRENT,
  validateAttributeChanges,
  WorkflowRunSchema,
} from '@workflow/world';
import { decodeTime } from 'ulid';
import type { RunCommit, RunJournalState } from './run-journal.js';

export interface MaterializeOptions {
  eventId: string;
  syntheticEventId?: string;
  now: Date;
  operationId: string;
  params?: CreateEventParams;
  /** Set only after the caller has acquired the global token reservation. */
  hookTokenReserved?: boolean;
  /** Active owner returned when a hook token reservation loses. */
  hookConflictRunId?: string;
}

export interface MaterializeResult {
  commit?: Omit<RunCommit, 'version' | 'runId' | 'previousRecord'>;
  result: EventResult;
}

function requireRun(state: RunJournalState): WorkflowRun {
  if (!state.run) throw new WorkflowRunNotFoundError(state.runId);
  return state.run;
}

function requireStep(state: RunJournalState, stepId: string): Step {
  const step = state.steps.get(stepId);
  if (!step) throw new WorkflowWorldError(`Step "${stepId}" not found`);
  return step;
}

function requireHook(state: RunJournalState, hookId: string): Hook {
  const hook = state.hooks.get(hookId);
  if (!hook) throw new HookNotFoundError(hookId);
  return hook;
}

function eventFrom(
  state: RunJournalState,
  request: AnyEventRequest,
  options: MaterializeOptions,
  eventData: unknown = 'eventData' in request ? request.eventData : undefined
): Event {
  return {
    ...request,
    ...(eventData === undefined ? { eventData: undefined } : { eventData }),
    runId: state.runId,
    eventId: options.eventId,
    createdAt: options.now,
    ...(options.params?.occurredAt
      ? { occurredAt: options.params.occurredAt }
      : {}),
    specVersion: request.specVersion ?? SPEC_VERSION_CURRENT,
  } as Event;
}

function terminalRunGuard(run: WorkflowRun, request: AnyEventRequest): void {
  if (!isTerminalWorkflowRunStatus(run.status)) return;
  if (request.eventType === 'run_cancelled' && run.status === 'cancelled') {
    return;
  }
  if (request.eventType === 'run_started') {
    throw new RunExpiredError(
      `Workflow run "${run.runId}" is already in terminal state "${run.status}"`
    );
  }
  if (
    request.eventType.startsWith('run_') ||
    request.eventType === 'attr_set' ||
    isChildEntityCreationEvent(request)
  ) {
    throw new EntityConflictError(
      `Cannot apply "${request.eventType}" to run in terminal state "${run.status}"`
    );
  }
}

function makeCommit(
  options: MaterializeOptions,
  events: Event[],
  changes: Omit<
    RunCommit,
    | 'version'
    | 'operationId'
    | 'runId'
    | 'previousRecord'
    | 'events'
    | 'externalStateUpdatedAt'
  >,
  externalStateUpdatedAt?: number
): Omit<RunCommit, 'version' | 'runId' | 'previousRecord'> {
  return {
    operationId: options.operationId,
    events,
    ...changes,
    ...(externalStateUpdatedAt !== undefined ? { externalStateUpdatedAt } : {}),
  };
}

/**
 * Pure Workflow event reducer. No state is mutated until its returned commit
 * has been accepted by Ursula.
 */
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: the exhaustive event union is intentionally centralized so one Ursula record contains the complete transition.
export function materializeEvent(
  state: RunJournalState,
  request: AnyEventRequest,
  options: MaterializeOptions
): MaterializeResult {
  const specVersion = request.specVersion ?? SPEC_VERSION_CURRENT;
  const now = options.now;
  const correlationId =
    'correlationId' in request ? request.correlationId : undefined;

  if (
    options.params?.stateUpdatedAt !== undefined &&
    state.externalStateUpdatedAt !== undefined &&
    options.params.stateUpdatedAt < state.externalStateUpdatedAt
  ) {
    throw new PreconditionFailedError(
      `Run "${state.runId}" changed after the replay snapshot`
    );
  }

  if (request.eventType === 'run_created') {
    if (state.run) {
      throw new EntityConflictError(
        `Workflow run "${state.runId}" already exists`
      );
    }
    validateAttributeChanges(
      Object.entries(request.eventData.attributes ?? {}).map(
        ([key, value]) => ({
          key,
          value,
        })
      ),
      {
        allowReservedAttributes:
          request.eventData.allowReservedAttributes === true,
      }
    );
    const run: WorkflowRun = {
      runId: state.runId,
      deploymentId: request.eventData.deploymentId,
      workflowName: request.eventData.workflowName,
      input: request.eventData.input,
      executionContext: request.eventData.executionContext,
      attributes: request.eventData.attributes ?? {},
      status: 'pending',
      specVersion,
      createdAt: now,
      updatedAt: now,
    };
    const event = eventFrom(state, request, options);
    return {
      commit: makeCommit(options, [event], { run }),
      result: { event, run, maxEvents: 25_000 },
    };
  }

  if (
    request.eventType === 'run_started' &&
    !state.run &&
    request.eventData?.deploymentId &&
    request.eventData.workflowName &&
    request.eventData.input !== undefined
  ) {
    if (!options.syntheticEventId) {
      throw new WorkflowWorldError(
        'Lazy run start requires a synthetic run_created event ID'
      );
    }
    validateAttributeChanges(
      Object.entries(request.eventData.attributes ?? {}).map(
        ([key, value]) => ({ key, value })
      ),
      {
        allowReservedAttributes:
          request.eventData.allowReservedAttributes === true,
      }
    );
    const run = WorkflowRunSchema.parse({
      runId: state.runId,
      deploymentId: request.eventData.deploymentId,
      workflowName: request.eventData.workflowName,
      input: request.eventData.input,
      executionContext: request.eventData.executionContext,
      attributes: request.eventData.attributes ?? {},
      status: 'running',
      specVersion,
      createdAt: now,
      updatedAt: now,
      startedAt: now,
    });
    const createdEvent: Event = {
      eventType: 'run_created',
      eventData: {
        deploymentId: run.deploymentId,
        workflowName: run.workflowName,
        input: run.input,
        executionContext: run.executionContext,
        attributes: run.attributes,
        allowReservedAttributes: request.eventData.allowReservedAttributes,
      },
      runId: state.runId,
      eventId: options.syntheticEventId,
      createdAt: now,
      specVersion,
    };
    const startedEvent = eventFrom(state, request, options);
    return {
      commit: makeCommit(options, [createdEvent, startedEvent], { run }),
      result: { event: startedEvent, run, maxEvents: 25_000 },
    };
  }

  let run = requireRun(state);
  terminalRunGuard(run, request);

  if (request.eventType === 'run_started' && run.status === 'running') {
    return { result: { run } };
  }
  if (request.eventType === 'run_cancelled' && run.status === 'cancelled') {
    const event = eventFrom(state, request, options);
    return {
      commit: makeCommit(options, [event], {}),
      result: { event, run },
    };
  }

  let event = eventFrom(state, request, options);
  const changes: Omit<
    RunCommit,
    | 'version'
    | 'operationId'
    | 'runId'
    | 'previousRecord'
    | 'events'
    | 'externalStateUpdatedAt'
  > = {};
  const result: EventResult = { event };

  switch (request.eventType) {
    case 'run_started': {
      run = WorkflowRunSchema.parse({
        ...run,
        status: 'running',
        output: undefined,
        error: undefined,
        errorCode: undefined,
        completedAt: undefined,
        startedAt: now,
        updatedAt: now,
      });
      changes.run = run;
      result.run = run;
      break;
    }
    case 'run_completed': {
      run = WorkflowRunSchema.parse({
        ...run,
        status: 'completed',
        output: request.eventData.output,
        error: undefined,
        errorCode: undefined,
        completedAt: now,
        updatedAt: now,
      });
      changes.run = run;
      changes.hooks = [...state.hooks.keys()]
        .filter(
          (hookId) =>
            (state.hookRetentionUntil.get(hookId)?.getTime() ?? 0) <=
            now.getTime()
        )
        .map((id) => ({ id, value: null }));
      changes.waits = [...state.waits.keys()].map((id) => ({
        id,
        value: null,
      }));
      result.run = run;
      break;
    }
    case 'run_failed': {
      run = WorkflowRunSchema.parse({
        ...run,
        status: 'failed',
        output: undefined,
        error: request.eventData.error,
        errorCode: request.eventData.errorCode,
        completedAt: now,
        updatedAt: now,
      });
      changes.run = run;
      changes.hooks = [...state.hooks.keys()]
        .filter(
          (hookId) =>
            (state.hookRetentionUntil.get(hookId)?.getTime() ?? 0) <=
            now.getTime()
        )
        .map((id) => ({ id, value: null }));
      changes.waits = [...state.waits.keys()].map((id) => ({
        id,
        value: null,
      }));
      result.run = run;
      break;
    }
    case 'run_cancelled': {
      run = WorkflowRunSchema.parse({
        ...run,
        status: 'cancelled',
        output: undefined,
        error: undefined,
        errorCode: undefined,
        completedAt: now,
        updatedAt: now,
      });
      changes.run = run;
      changes.hooks = [...state.hooks.keys()]
        .filter(
          (hookId) =>
            (state.hookRetentionUntil.get(hookId)?.getTime() ?? 0) <=
            now.getTime()
        )
        .map((id) => ({ id, value: null }));
      changes.waits = [...state.waits.keys()].map((id) => ({
        id,
        value: null,
      }));
      result.run = run;
      break;
    }
    case 'attr_set': {
      validateAttributeChanges(request.eventData.changes, {
        existingKeys: Object.keys(run.attributes),
        allowReservedAttributes:
          request.eventData.allowReservedAttributes === true,
      });
      run = {
        ...run,
        attributes: applyAttributeChanges(
          run.attributes,
          request.eventData.changes
        ),
        updatedAt: now,
      };
      changes.run = run;
      result.run = run;
      break;
    }
    case 'step_created': {
      if (!correlationId) throw new WorkflowWorldError('stepId is required');
      if (state.steps.has(correlationId)) {
        throw new EntityConflictError(
          `Step "${correlationId}" already created`
        );
      }
      const step: Step = {
        runId: state.runId,
        stepId: correlationId,
        stepName: request.eventData.stepName,
        input: request.eventData.input,
        status: 'pending',
        attempt: 0,
        specVersion,
        createdAt: now,
        updatedAt: now,
      };
      changes.steps = [{ id: correlationId, value: step }];
      result.step = step;
      break;
    }
    case 'step_started': {
      if (!correlationId) throw new WorkflowWorldError('stepId is required');
      const lazy =
        request.eventData?.stepName !== undefined &&
        request.eventData.input !== undefined;
      let step = state.steps.get(correlationId);
      const events: Event[] = [];
      if (lazy) {
        if (step) {
          throw new EntityConflictError(
            `Step "${correlationId}" already created`
          );
        }
        step = {
          runId: state.runId,
          stepId: correlationId,
          stepName: request.eventData?.stepName as string,
          input: request.eventData?.input,
          status: 'pending',
          attempt: 0,
          specVersion,
          createdAt: now,
          updatedAt: now,
        };
        if (!options.syntheticEventId) {
          throw new WorkflowWorldError(
            'Lazy step start requires a synthetic step_created event ID'
          );
        }
        events.push({
          eventType: 'step_created',
          correlationId,
          eventData: {
            stepName: step.stepName,
            input: step.input,
          },
          runId: state.runId,
          eventId: options.syntheticEventId,
          createdAt: now,
          specVersion,
        });
        result.stepCreated = true;
      } else {
        step = requireStep(state, correlationId);
      }
      if (isTerminalStepStatus(step.status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${step.status}"`
        );
      }
      if (step.retryAfter && step.retryAfter.getTime() > now.getTime()) {
        throw new TooEarlyError(
          `Cannot start step "${correlationId}": retryAfter timestamp has not been reached yet`,
          {
            retryAfter: Math.ceil(
              (step.retryAfter.getTime() - now.getTime()) / 1000
            ),
          }
        );
      }
      step = {
        ...step,
        status: 'running',
        attempt: step.attempt + 1,
        startedAt: step.startedAt ?? now,
        updatedAt: now,
        retryAfter: undefined,
      };
      if (request.eventData?.input !== undefined) {
        const { input: _input, ...storedData } = request.eventData;
        event = eventFrom(state, request, options, storedData);
        result.event = event;
      }
      events.push(event);
      changes.steps = [{ id: correlationId, value: step }];
      result.step = step;
      return {
        commit: makeCommit(options, events, changes),
        result,
      };
    }
    case 'step_completed':
    case 'step_failed':
    case 'step_retrying': {
      if (!correlationId) throw new WorkflowWorldError('stepId is required');
      let step = requireStep(state, correlationId);
      if (isTerminalStepStatus(step.status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${step.status}"`
        );
      }
      if (request.eventType === 'step_completed') {
        step = {
          ...step,
          status: 'completed',
          output: request.eventData.result,
          completedAt: now,
          updatedAt: now,
        };
      } else if (request.eventType === 'step_failed') {
        step = {
          ...step,
          status: 'failed',
          error: request.eventData.error,
          completedAt: now,
          updatedAt: now,
        };
      } else {
        step = {
          ...step,
          status: 'pending',
          error: request.eventData.error,
          retryAfter: request.eventData.retryAfter,
          updatedAt: now,
        };
      }
      changes.steps = [{ id: correlationId, value: step }];
      result.step = step;
      break;
    }
    case 'hook_created': {
      if (!correlationId) throw new WorkflowWorldError('hookId is required');
      if (options.hookConflictRunId) {
        const conflict: Event = {
          eventType: 'hook_conflict',
          correlationId,
          eventData: {
            token: request.eventData.token,
            conflictingRunId: options.hookConflictRunId,
          },
          runId: state.runId,
          eventId: options.eventId,
          createdAt: now,
          specVersion,
        };
        return {
          commit: makeCommit(options, [conflict], {}),
          result: { event: conflict },
        };
      }
      if (!options.hookTokenReserved) {
        throw new WorkflowWorldError(
          'hook_created requires an acquired Ursula token reservation'
        );
      }
      if (state.hooks.has(correlationId)) {
        throw new EntityConflictError(
          `Hook "${correlationId}" already created`
        );
      }
      const hook: Hook = {
        runId: state.runId,
        hookId: correlationId,
        token: request.eventData.token,
        ownerId: '',
        projectId: '',
        environment: '',
        metadata: request.eventData.metadata,
        isWebhook: request.eventData.isWebhook,
        isSystem: request.eventData.isSystem,
        createdAt: now,
        specVersion,
      };
      changes.hooks = [{ id: correlationId, value: hook }];
      result.hook = hook;
      break;
    }
    case 'hook_received': {
      if (!correlationId) throw new WorkflowWorldError('hookId is required');
      requireHook(state, correlationId);
      break;
    }
    case 'hook_disposed': {
      if (!correlationId) throw new WorkflowWorldError('hookId is required');
      requireHook(state, correlationId);
      changes.hooks = [{ id: correlationId, value: null }];
      break;
    }
    case 'wait_created': {
      if (!correlationId) throw new WorkflowWorldError('waitId is required');
      if (state.waits.has(correlationId)) {
        throw new EntityConflictError(
          `Wait "${correlationId}" already created`
        );
      }
      const wait: Wait = {
        waitId: correlationId,
        runId: state.runId,
        status: 'waiting',
        resumeAt: request.eventData.resumeAt,
        createdAt: now,
        updatedAt: now,
        specVersion,
      };
      changes.waits = [{ id: correlationId, value: wait }];
      result.wait = wait;
      break;
    }
    case 'wait_completed': {
      if (!correlationId) throw new WorkflowWorldError('waitId is required');
      const current = state.waits.get(correlationId);
      if (!current)
        throw new WorkflowWorldError(`Wait "${correlationId}" not found`);
      const wait: Wait = {
        ...current,
        status: 'completed',
        completedAt: now,
        updatedAt: now,
      };
      changes.waits = [{ id: correlationId, value: wait }];
      result.wait = wait;
      break;
    }
  }

  const advancesExternalMarker =
    options.params?.stateUpdatedAt === undefined &&
    (request.eventType === 'hook_received' ||
      request.eventType === 'step_completed');
  const marker = advancesExternalMarker
    ? // stateUpdatedAt is the ULID timestamp of the newest replayed event, not
      // the server wall-clock acceptance time. Using `now` here makes an
      // up-to-date replay look stale by a few milliseconds forever.
      decodeTime(options.eventId.slice('evnt_'.length))
    : state.externalStateUpdatedAt;
  result.maxEvents = result.run ? 25_000 : undefined;
  return {
    commit: makeCommit(options, [event], changes, marker),
    result,
  };
}
