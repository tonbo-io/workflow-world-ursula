import type { AnyEventRequest, Hook, WorkflowRun } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import { materializeEvent } from './reducer.js';
import type { RunJournalState } from './run-journal.js';

function state(run?: WorkflowRun): RunJournalState {
  return {
    runId: run?.runId ?? 'wrun_1',
    nextRecord: 0,
    run,
    steps: new Map(),
    hooks: new Map(),
    hookRetentionUntil: new Map(),
    waits: new Map(),
  };
}

function options(eventId: string) {
  return {
    eventId,
    syntheticEventId: `${eventId}-synthetic`,
    operationId: `operation-${eventId}`,
    now: new Date('2026-07-24T00:00:00.000Z'),
  };
}

function pendingRun(): WorkflowRun {
  const now = new Date('2026-07-23T00:00:00.000Z');
  return {
    runId: 'wrun_1',
    status: 'pending',
    deploymentId: 'dpl_1',
    workflowName: 'workflow//test//main',
    input: Uint8Array.from([1]),
    attributes: {},
    specVersion: 5,
    createdAt: now,
    updatedAt: now,
  };
}

function runningRun(): WorkflowRun {
  const run = pendingRun();
  return {
    ...run,
    status: 'running',
    startedAt: run.createdAt,
  };
}

function hook(hookId: string): Hook {
  return {
    runId: 'wrun_1',
    hookId,
    token: `token-${hookId}`,
    ownerId: '',
    projectId: '',
    environment: '',
    createdAt: new Date('2026-07-23T00:00:00.000Z'),
    specVersion: 5,
  };
}

describe('materializeEvent', () => {
  it('materializes run creation into the same commit as the event', () => {
    const request = {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_1',
        workflowName: 'workflow//test//main',
        input: Uint8Array.from([1]),
      },
      specVersion: 5,
    } satisfies AnyEventRequest;

    const materialized = materializeEvent(state(), request, options('evnt_1'));

    expect(materialized.commit?.events).toHaveLength(1);
    expect(materialized.commit?.run?.status).toBe('pending');
    expect(materialized.result.run?.input).toEqual(Uint8Array.from([1]));
  });

  it('atomically synthesizes run_created for a resilient run start', () => {
    const request = {
      eventType: 'run_started',
      eventData: {
        deploymentId: 'dpl_1',
        workflowName: 'workflow//test//main',
        input: Uint8Array.from([1]),
        executionContext: { trace: 'context' },
        attributes: { tenant: 'test' },
      },
      specVersion: 5,
    } satisfies AnyEventRequest;

    const materialized = materializeEvent(state(), request, options('evnt_2'));

    expect(
      materialized.commit?.events.map(({ eventType }) => eventType)
    ).toEqual(['run_created', 'run_started']);
    expect(materialized.commit?.run?.status).toBe('running');
    expect(materialized.result.run?.attributes).toEqual({ tenant: 'test' });
    expect(materialized.result.run?.startedAt).toEqual(
      new Date('2026-07-24T00:00:00.000Z')
    );
  });

  it('atomically synthesizes step_created for a lazy step start', () => {
    const current = state(pendingRun());
    const request = {
      eventType: 'step_started',
      correlationId: 'step_1',
      eventData: {
        stepName: 'step//test//one',
        input: Uint8Array.from([2]),
        ownerMessageId: 'msg_1',
      },
      specVersion: 5,
    } satisfies AnyEventRequest;

    const materialized = materializeEvent(current, request, options('evnt_2'));

    expect(
      materialized.commit?.events.map(({ eventType }) => eventType)
    ).toEqual(['step_created', 'step_started']);
    expect(materialized.commit?.steps?.[0]?.value?.status).toBe('running');
    expect(materialized.result.stepCreated).toBe(true);
    expect(
      (
        materialized.commit?.events[1]?.eventData as
          | Record<string, unknown>
          | undefined
      )?.input
    ).toBeUndefined();
  });

  it('enforces the replay precondition marker', () => {
    const current = state(pendingRun());
    current.externalStateUpdatedAt = 200;
    const request = {
      eventType: 'run_started',
      specVersion: 5,
    } satisfies AnyEventRequest;

    expect(() =>
      materializeEvent(current, request, {
        ...options('evnt_3'),
        params: { stateUpdatedAt: 199 },
      })
    ).toThrow('changed after the replay snapshot');
  });

  it('removes non-retained hooks and waits when a run becomes terminal', () => {
    const current = state(runningRun());
    current.hooks.set('hook-1', hook('hook-1'));
    current.waits.set('wait-1', {
      runId: 'wrun_1',
      waitId: 'wait-1',
      createdAt: new Date('2026-07-23T00:00:00.000Z'),
    });
    const request = {
      eventType: 'run_completed',
      eventData: { output: Uint8Array.from([1]) },
      specVersion: 5,
    } satisfies AnyEventRequest;

    const materialized = materializeEvent(
      current,
      request,
      options('evnt_terminal')
    );

    expect(materialized.commit?.hooks).toEqual([{ id: 'hook-1', value: null }]);
    expect(materialized.commit?.waits).toEqual([{ id: 'wait-1', value: null }]);
  });

  it('keeps a hook until its explicit retention deadline', () => {
    const current = state(runningRun());
    current.hooks.set('hook-1', hook('hook-1'));
    current.hookRetentionUntil.set(
      'hook-1',
      new Date('2026-07-25T00:00:00.000Z')
    );
    const request = {
      eventType: 'run_completed',
      eventData: { output: Uint8Array.from([1]) },
      specVersion: 5,
    } satisfies AnyEventRequest;

    const materialized = materializeEvent(
      current,
      request,
      options('evnt_retained')
    );

    expect(materialized.commit?.hooks).toEqual([]);
  });
});
