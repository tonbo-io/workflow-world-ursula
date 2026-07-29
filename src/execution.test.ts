import { PreconditionFailedError } from '@workflow/errors';
import type { AnyEventRequest } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  parseUrsulaJson,
  stringifyUrsulaJson,
  type UrsulaAppendOptions,
  UrsulaClient,
  type UrsulaRecord,
  UrsulaRequestError,
} from './client.js';
import {
  type DeliveryExecution,
  RunExecutionCoordinator,
} from './execution.js';
import { HookClaims } from './hook-claims.js';
import { RunJournal, type RunCommit } from './run-journal.js';
import { createStorage } from './storage.js';

class MemoryClient extends UrsulaClient {
  readonly appends: Array<{ stream: string; values: unknown[] }> = [];
  private readonly streams = new Map<string, unknown[]>();
  private remainingRunAppendConflicts = 0;

  constructor() {
    super({ baseUrl: 'https://ursula.test' });
  }

  async ensureJsonStream(stream: string): Promise<void> {
    if (!this.streams.has(stream)) this.streams.set(stream, []);
  }

  failRunAppendsWithConflict(count: number): void {
    this.remainingRunAppendConflicts = count;
  }

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
    if (
      stream.startsWith('run-') &&
      !stream.startsWith('run-checkpoint-') &&
      this.remainingRunAppendConflicts > 0
    ) {
      this.remainingRunAppendConflicts -= 1;
      throw new UrsulaRequestError(
        'append records',
        new Response('record tail mismatch', { status: 412 }),
        'record tail mismatch'
      );
    }
    const current = this.streams.get(stream) ?? [];
    if (
      options.expectedRecord !== undefined &&
      options.expectedRecord !== current.length
    ) {
      throw new UrsulaRequestError(
        'append records',
        new Response('record tail mismatch', { status: 412 }),
        'record tail mismatch'
      );
    }
    const records = Array.isArray(values) ? [...values] : [values];
    const startRecord = current.length;
    // Ursula stores the encoded record, so a reload re-parses what the codec
    // wrote. Keeping the in-memory objects instead lets a value that only
    // survives by reference — a Date where the wire carries a string — pass a
    // reload it would fail against the real client.
    current.push(
      ...records.map((value) => parseUrsulaJson(stringifyUrsulaJson(value)))
    );
    this.streams.set(stream, current);
    this.appends.push({ stream, values: records });
    return { startRecord, nextRecord: current.length };
  }

  async readAll<T>(stream: string, start = 0): Promise<UrsulaRecord<T>[]> {
    return (this.streams.get(stream) ?? [])
      .slice(start)
      .map((value, index) => ({
        record: start + index,
        value: value as T,
      }));
  }

  async read<T>(
    stream: string,
    start = 0,
    limit = 1000
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    const records = (await this.readAll<T>(stream, start)).slice(0, limit);
    return {
      records,
      nextRecord: start + records.length,
      closed: false,
      upToDate: true,
    };
  }

  async readTail<T>(
    stream: string,
    count = 1
  ): Promise<{
    records: UrsulaRecord<T>[];
    nextRecord: number;
    closed: boolean;
    upToDate: boolean;
  }> {
    const values = this.streams.get(stream) ?? [];
    const start = Math.max(0, values.length - count);
    return {
      records: values.slice(start).map((value, index) => ({
        record: start + index,
        value: value as T,
      })),
      nextRecord: values.length,
      closed: false,
      upToDate: true,
    };
  }

  /** Backdates every hook reservation so orphan reconciliation can run. */
  expireHookReservations(): void {
    const past = new Date(Date.now() - 1000).toISOString();
    for (const [stream, values] of this.streams) {
      if (!stream.startsWith('hook-')) continue;
      this.streams.set(
        stream,
        values.map((value) => {
          const record = value as Record<string, unknown>;
          return record.type === 'reserved'
            ? { ...record, reservedUntil: past }
            : record;
        })
      );
    }
  }

  async publishSnapshotAtRecord(): Promise<void> {}

  async advanceRetentionAtRecord(): Promise<void> {}

  runCommits(): RunCommit[] {
    return this.appends
      .filter(
        ({ stream }) =>
          stream.startsWith('run-') &&
          !stream.startsWith('run-checkpoint-')
      )
      .flatMap(({ values }) => values)
      .filter(
        (value): value is RunCommit =>
          typeof value === 'object' &&
          value !== null &&
          'runId' in value &&
          'events' in value
      );
  }
}

function delivery(
  token: string,
  overrides: Partial<DeliveryExecution> = {}
): DeliveryExecution {
  return {
    runId: 'wrun_atomic',
    lane: 'run',
    token,
    ownerMessageId: 'msg_atomic',
    attempt: 1,
    expiresAt: new Date(Date.now() + 60_000),
    ...overrides,
  };
}

function stepCreated(stepId: string): AnyEventRequest {
  return {
    eventType: 'step_created',
    correlationId: stepId,
    eventData: {
      stepName: `step//test//${stepId}`,
      input: Uint8Array.from([1]),
    },
    specVersion: 5,
  } as AnyEventRequest;
}

/** A start that transitions an existing step rather than creating one. */
function plainStepStarted(stepId: string): AnyEventRequest {
  return {
    eventType: 'step_started',
    correlationId: stepId,
    eventData: {
      stepName: `step//test//${stepId}`,
      workflowName: 'workflow//test//atomic',
      ownerMessageId: 'msg_atomic',
    },
    specVersion: 5,
  } as AnyEventRequest;
}

function stepStarted(stepId: string): AnyEventRequest {
  return {
    eventType: 'step_started',
    correlationId: stepId,
    eventData: {
      stepName: `step//test//${stepId}`,
      workflowName: 'workflow//test//atomic',
      input: Uint8Array.from([1]),
      ownerMessageId: 'msg_atomic',
    },
    specVersion: 5,
  };
}

function stepCompleted(stepId: string): AnyEventRequest {
  return {
    eventType: 'step_completed',
    correlationId: stepId,
    eventData: {
      stepName: `step//test//${stepId}`,
      workflowName: 'workflow//test//atomic',
      result: Uint8Array.from([2]),
    },
    specVersion: 5,
  };
}

async function setup(allowOwnedLazyStarts = true) {
  const memory = new MemoryClient();
  const client = memory;
  const journal = new RunJournal(client);
  const executions = new RunExecutionCoordinator(journal, {
    allowOwnedLazyStarts,
  });
  const { storage } = createStorage(client, { journal, executions });
  await storage.events.create('wrun_atomic', {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'dpl_atomic',
      workflowName: 'workflow//test//atomic',
      input: Uint8Array.from([0]),
    },
    specVersion: 5,
  });
  await storage.events.create('wrun_atomic', {
    eventType: 'run_started',
    specVersion: 5,
  });
  return { client, executions, journal, memory, storage };
}

describe('atomic step transactions', () => {
  it('keeps the public two-append contract by default', async () => {
    const { memory, storage } = await setup(false);
    const before = memory.runCommits().length;

    await storage.events.create('wrun_atomic', stepStarted('step-default'));
    await storage.events.create('wrun_atomic', stepCompleted('step-default'));

    expect(memory.runCommits().slice(before)).toHaveLength(2);
  });

  it('does not serialize owned commits from different runs', async () => {
    const { executions } = await setup();
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = () => {};
    const firstStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    const first = executions.exclusive('run-a', async () => {
      started();
      await gate;
    });
    await firstStarted;

    await expect(
      executions.exclusive('run-b', async () => 'done')
    ).resolves.toBe('done');
    release();
    await first;
  });

  it('commits an owned lazy turbo step without a queue delivery context', async () => {
    const { memory, storage } = await setup();
    const before = memory.runCommits().length;

    const started = await storage.events.create(
      'wrun_atomic',
      stepStarted('step-turbo')
    );
    expect(started.step?.status).toBe('running');
    await storage.events.create(
      'wrun_atomic',
      stepCompleted('step-turbo')
    );

    // The creation lands on its own record so `stepCreated` is decided by the
    // record tail; only the start and its terminal event still share one.
    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(2);
    expect(commits[0]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_created',
      'step_started',
    ]);
    expect(commits[1]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_completed',
    ]);
  });

  it('commits step_started and step_completed in one run record', async () => {
    const { executions, memory, storage } = await setup();
    const before = memory.runCommits().length;

    await executions.run(delivery('lease-1'), async () => {
      // Create the step up front so its start is a plain transition: a start
      // that would create the step is never staged, since its `stepCreated`
      // answer has to be settled durably.
      await storage.events.create('wrun_atomic', stepCreated('step-1'));
      const started = await storage.events.create(
        'wrun_atomic',
        plainStepStarted('step-1')
      );
      expect(started.step?.status).toBe('running');
      await storage.events.create('wrun_atomic', stepCompleted('step-1'));
    });

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(3);
    expect(commits[0]?.events).toEqual([]);
    expect(commits[1]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_created',
    ]);
    expect(commits[2]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_started',
      'step_completed',
    ]);
    await expect(
      storage.steps.get('wrun_atomic', 'step-1')
    ).resolves.toMatchObject({ status: 'completed', attempt: 1 });
  });

  it('serializes parallel terminal commits without losing either step', async () => {
    const { executions, memory, storage } = await setup();
    const before = memory.runCommits().length;

    await executions.run(delivery('lease-parallel'), async () => {
      await Promise.all([
        (async () => {
          await storage.events.create(
            'wrun_atomic',
            stepStarted('step-a')
          );
          await storage.events.create(
            'wrun_atomic',
            stepCompleted('step-a')
          );
        })(),
        (async () => {
          await storage.events.create(
            'wrun_atomic',
            stepStarted('step-b')
          );
          await storage.events.create(
            'wrun_atomic',
            stepCompleted('step-b')
          );
        })(),
      ]);
    });

    // One lease claim, a creation per step, then each step's start and
    // terminal event sharing a record.
    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(5);
    expect(
      commits
        .slice(1)
        .flatMap(({ events }) => events)
        .filter(({ eventType }) => eventType === 'step_completed')
        .map(({ correlationId }) => correlationId)
        .sort()
    ).toEqual(['step-a', 'step-b']);
  });

  it('surfaces exhausted staged-step CAS conflicts as replayable precondition failures', async () => {
    const { memory, storage } = await setup();
    await storage.events.create(
      'wrun_atomic',
      stepStarted('step-contended')
    );
    memory.failRunAppendsWithConflict(16);

    await expect(
      storage.events.create(
        'wrun_atomic',
        stepCompleted('step-contended')
      )
    ).rejects.toSatisfy((error: unknown) =>
      PreconditionFailedError.is(error)
    );
  });

  it('surfaces exhausted ordinary event CAS conflicts as replayable precondition failures', async () => {
    const { memory, storage } = await setup(false);
    memory.failRunAppendsWithConflict(16);

    await expect(
      storage.events.create(
        'wrun_atomic',
        stepStarted('step-ordinary-contended')
      )
    ).rejects.toSatisfy((error: unknown) =>
      PreconditionFailedError.is(error)
    );
  });

  it('grants create ownership only once across discarded deliveries', async () => {
    const { client, journal, memory } = await setup();
    const first = new RunExecutionCoordinator(journal);
    const second = new RunExecutionCoordinator(journal);
    const before = memory.runCommits().length;

    // A delivery that starts a step and is then abandoned without ever
    // committing a terminal event — a crash, or a lost race.
    const claims: (boolean | undefined)[] = [];
    const start = async (coordinator: RunExecutionCoordinator, tag: string) => {
      const { storage } = createStorage(client, {
        journal,
        executions: coordinator,
      });
      await coordinator.run(delivery(tag), async () => {
        const result = await storage.events.create(
          'wrun_atomic',
          stepStarted('step-once')
        );
        claims.push(result.stepCreated);
      });
    };

    await start(first, 'lease-first');
    await expect(start(second, 'lease-second')).rejects.toThrow(
      'already created'
    );

    // `stepCreated` is the runtime's exactly-once signal for running a step
    // body inline. Answering it from an uncommitted stage handed it to every
    // delivery that materialized the same creation.
    expect(claims.filter(Boolean)).toHaveLength(1);
    expect(
      memory
        .runCommits()
        .slice(before)
        .flatMap(({ events }) => events)
        .filter(({ eventType }) => eventType === 'step_created')
    ).toHaveLength(1);
  });

  it('reclaims a hook token whose owner died before committing its run', async () => {
    const { client, memory, storage } = await setup();
    const token = 'hook-token-orphan';

    // A process reserves the token and dies before appending the run record
    // that would finalize the claim, so the run never gains the Hook.
    const orphaned = new HookClaims(client);
    await orphaned.reserve({
      operationId: 'orphan-op',
      token,
      runId: 'wrun_atomic',
      hookId: 'hook-never-committed',
    });
    memory.expireHookReservations();

    // Without reconciliation the token stays reserved forever and this
    // hook_created lands as a hook_conflict instead.
    const result = await storage.events.create('wrun_atomic', {
      eventType: 'hook_created',
      correlationId: 'hook-live',
      eventData: { token },
      specVersion: 5,
    } as AnyEventRequest);

    expect(result.event?.eventType).toBe('hook_created');
    expect(result.hook).toMatchObject({ hookId: 'hook-live', token });
  });

  it('starts a step through the ordinary path when its lease was superseded', async () => {
    const { client, journal, memory } = await setup();
    const first = new RunExecutionCoordinator(journal);
    const second = new RunExecutionCoordinator(journal);
    const { storage } = createStorage(client, { journal, executions: first });

    await first.run(
      delivery('lease-superseded', {
        expiresAt: new Date(Date.now() + 20),
      }),
      async () => {
        // Let this handler's lease lapse so a newer delivery can take the lane
        // while it still holds the lease it captured at claim time.
        await new Promise<void>((resolve) => setTimeout(resolve, 30));
        await second.run(
          delivery('lease-newer', { ownerMessageId: 'msg_newer', attempt: 2 }),
          async () => {}
        );
      // The staged-step optimization is unavailable now, but the step must
      // still start: an error here would strand it as `pending` forever.
        await expect(
          storage.events.create('wrun_atomic', stepStarted('step-superseded'))
        ).resolves.toMatchObject({ step: { status: 'running' } });
        await storage.events.create(
          'wrun_atomic',
          stepCompleted('step-superseded')
        );
      }
    );

    expect(
      memory
        .runCommits()
        .flatMap(({ events }) => events)
        .filter(
          ({ eventType, correlationId }) =>
            eventType === 'step_completed' &&
            correlationId === 'step-superseded'
        )
    ).toHaveLength(1);
  });

  it('fences a stale handler after a newer lease generation takes over', async () => {
    const { client, journal } = await setup();
    const first = new RunExecutionCoordinator(journal);
    const second = new RunExecutionCoordinator(journal);
    const { storage } = createStorage(client, {
      journal,
      executions: first,
    });
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let staged = () => {};
    const stagedGate = new Promise<void>((resolve) => {
      staged = resolve;
    });
    const stale = first.run(
      delivery('lease-old', {
        expiresAt: new Date(Date.now() + 20),
      }),
      async () => {
        // The step exists already, so its start is staged rather than
        // committed — which is what puts the terminal write behind the fence.
        await storage.events.create('wrun_atomic', stepCreated('step-stale'));
        await storage.events.create(
          'wrun_atomic',
          plainStepStarted('step-stale')
        );
        staged();
        await gate;
        await storage.events.create(
          'wrun_atomic',
          stepCompleted('step-stale')
        );
      }
    );
    await stagedGate;
    await new Promise<void>((resolve) => setTimeout(resolve, 30));
    await second.run(
      delivery('lease-new', {
        ownerMessageId: 'msg_new',
        attempt: 2,
      }),
      async () => {}
    );
    release();

    await expect(stale).rejects.toThrow('changed ownership');
  });
});
