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
  type UrsulaTransactionOperation,
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

  async appendTransaction(
    operations: readonly UrsulaTransactionOperation[]
  ): Promise<void> {
    for (const operation of operations) {
      const current = this.streams.get(operation.stream) ?? [];
      if (
        operation.expectedRecord !== undefined &&
        operation.expectedRecord !== current.length
      ) {
        throw new UrsulaRequestError(
          'append group transaction',
          new Response('record tail mismatch', { status: 412 }),
          'record tail mismatch'
        );
      }
    }
    for (const operation of operations) {
      await this.append(operation.stream, operation.values, {
        operationId: operation.operationId,
        expectedRecord: operation.expectedRecord,
      });
    }
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
    queueName: '__wkf_workflow_test',
    queuePartition: 0,
    token,
    generation: 10,
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
  const executions = new RunExecutionCoordinator({
    allowOwnedLazyStarts,
  });
  executions.setOwnedStepCommitter(async (_delivery, append) => {
    await memory.appendTransaction([append.operation]);
    append.apply();
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
  it('commits a complete delivery journal as one run record', async () => {
    const memory = new MemoryClient();
    const journal = new RunJournal(memory);
    const executions = new RunExecutionCoordinator({
      allowDeliveryTransactions: true,
    });
    executions.setDeliveryCommitter(
      async (_delivery, append, _timeoutSeconds) => {
        await memory.appendTransaction([append.operation]);
        append.apply();
      }
    );
    const { storage } = createStorage(memory, { journal, executions });
    await storage.events.create('wrun_delivery', {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_delivery',
        workflowName: 'workflow//test//delivery',
        input: Uint8Array.from([0]),
      },
      specVersion: 5,
    });
    const before = memory.runCommits().length;

    await executions.run(
      delivery('lease-delivery', { runId: 'wrun_delivery' }),
      async () => {
        await storage.events.create('wrun_delivery', {
          eventType: 'run_started',
          specVersion: 5,
        });
        await storage.events.create(
          'wrun_delivery',
          stepStarted('step-delivery')
        );
        await storage.events.create(
          'wrun_delivery',
          stepCompleted('step-delivery')
        );

        expect(memory.runCommits()).toHaveLength(before);
        await expect(
          storage.steps.get('wrun_delivery', 'step-delivery')
        ).resolves.toMatchObject({ status: 'completed' });
        await expect(
          storage.events.list({ runId: 'wrun_delivery' })
        ).resolves.toMatchObject({
          data: expect.arrayContaining([
            expect.objectContaining({ eventType: 'step_completed' }),
          ]),
        });
        await expect(
          executions.finishDelivery('wrun_delivery', undefined)
        ).resolves.toBe(true);
      }
    );

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(1);
    expect(commits[0]?.events.map(({ eventType }) => eventType)).toEqual([
      'run_started',
      'step_created',
      'step_started',
      'step_completed',
    ]);
  });

  it('serializes parallel run mutations into the same delivery record', async () => {
    const memory = new MemoryClient();
    const journal = new RunJournal(memory);
    const executions = new RunExecutionCoordinator({
      allowDeliveryTransactions: true,
    });
    executions.setDeliveryCommitter(
      async (_delivery, append, _timeoutSeconds) => {
        await memory.appendTransaction([append.operation]);
        append.apply();
      }
    );
    const { storage } = createStorage(memory, { journal, executions });
    await storage.events.create('wrun_parallel_delivery', {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_delivery',
        workflowName: 'workflow//test//atomic',
        input: Uint8Array.from([0]),
      },
      specVersion: 5,
    });
    await storage.events.create('wrun_parallel_delivery', {
      eventType: 'run_started',
      specVersion: 5,
    });
    const before = memory.runCommits().length;

    await executions.run(
      delivery('lease-parallel', { runId: 'wrun_parallel_delivery' }),
      async () => {
        await Promise.all([
          storage.events.create(
            'wrun_parallel_delivery',
            stepStarted('step-parallel-a')
          ),
          storage.events.create(
            'wrun_parallel_delivery',
            stepStarted('step-parallel-b')
          ),
        ]);
        await Promise.all([
          storage.events.create(
            'wrun_parallel_delivery',
            stepCompleted('step-parallel-a')
          ),
          storage.events.create(
            'wrun_parallel_delivery',
            stepCompleted('step-parallel-b')
          ),
        ]);
        await executions.finishDelivery(
          'wrun_parallel_delivery',
          undefined
        );
      }
    );

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(1);
    expect(
      commits[0]?.events.filter(
        ({ eventType }) => eventType === 'step_completed'
      )
    ).toHaveLength(2);
  });

  it('does not expose an abandoned delivery batch after redelivery', async () => {
    const memory = new MemoryClient();
    const journal = new RunJournal(memory);
    const first = new RunExecutionCoordinator({
      allowDeliveryTransactions: true,
    });
    const { storage: firstStorage } = createStorage(memory, {
      journal,
      executions: first,
    });
    await firstStorage.events.create('wrun_abandoned_delivery', {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_delivery',
        workflowName: 'workflow//test//delivery',
        input: Uint8Array.from([0]),
      },
      specVersion: 5,
    });
    const before = memory.runCommits().length;

    await first.run(
      delivery('lease-abandoned', { runId: 'wrun_abandoned_delivery' }),
      async () => {
        await firstStorage.events.create(
          'wrun_abandoned_delivery',
          stepStarted('step-abandoned')
        );
        await expect(
          firstStorage.steps.get(
            'wrun_abandoned_delivery',
            'step-abandoned'
          )
        ).resolves.toMatchObject({ status: 'running' });
        // Simulate a process crash by leaving the delivery without calling
        // finishDelivery(). The preview must remain process-local only.
      }
    );

    const second = new RunExecutionCoordinator({
      allowDeliveryTransactions: true,
    });
    const { storage: secondStorage } = createStorage(memory, {
      journal: new RunJournal(memory),
      executions: second,
    });
    expect(memory.runCommits()).toHaveLength(before);
    await expect(
      secondStorage.steps.get(
        'wrun_abandoned_delivery',
        'step-abandoned'
      )
    ).rejects.toThrow();
  });

  it('keeps the public contract without a queue delivery lease', async () => {
    const { memory, storage } = await setup();
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

  it('shares the active async delivery context between server bundles', async () => {
    const first = new RunExecutionCoordinator();
    const second = new RunExecutionCoordinator();

    await first.run(delivery('lease-first'), async () => {
      expect(first.current('wrun_atomic')?.token).toBe('lease-first');
      expect(second.current('wrun_atomic')?.token).toBe('lease-first');
      await second.run(delivery('lease-second'), async () => {
        expect(first.current('wrun_atomic')?.token).toBe('lease-second');
        expect(second.current('wrun_atomic')?.token).toBe('lease-second');
      });
      expect(second.current('wrun_atomic')?.token).toBe('lease-first');
    });
  });

  it('bridges an owned delivery to a separate server bundle by owner message', async () => {
    const handlerBundle = new RunExecutionCoordinator({
      allowOwnedLazyStarts: true,
    });
    const storageBundle = new RunExecutionCoordinator({
      allowOwnedLazyStarts: true,
    });
    const current = delivery('lease-bundle');

    await handlerBundle.run(current, async () => {
      expect(
        storageBundle.current(current.runId, current.ownerMessageId)
      ).toEqual(current);
      expect(storageBundle.current(current.runId)).toEqual(current);
    });
    expect(
      storageBundle.current(current.runId, current.ownerMessageId)
    ).toBeUndefined();
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

  it('does not append a delivery fence when the experiment is disabled', async () => {
    const { executions, memory, storage } = await setup(false);
    const before = memory.runCommits().length;

    await executions.run(delivery('lease-disabled'), async () => {
      await storage.events.create('wrun_atomic', stepCreated('step-disabled'));
      await storage.events.create(
        'wrun_atomic',
        plainStepStarted('step-disabled')
      );
      await storage.events.create(
        'wrun_atomic',
        stepCompleted('step-disabled')
      );
    });

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(2);
    expect(commits.every(({ executionFence }) => !executionFence)).toBe(true);
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
    expect(commits).toHaveLength(2);
    expect(commits[0]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_created',
    ]);
    expect(commits[1]?.events.map(({ eventType }) => eventType)).toEqual([
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

    // One durable delivery fence covers both steps. Each successful step then
    // commits its complete lifecycle in one record.
    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(2);
    expect(
      commits
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

  it('rejects an old handler when a newer lease wins the terminal fence', async () => {
    const { client, journal, memory } = await setup();
    const first = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const second = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const { storage: firstStorage } = createStorage(client, {
      journal,
      executions: first,
    });
    const { storage: secondStorage } = createStorage(client, {
      journal,
      executions: second,
    });
    let activeToken = 'lease-old';
    const installCommitter = (coordinator: RunExecutionCoordinator) => {
      coordinator.setOwnedStepCommitter(async (delivery, append) => {
        if (delivery.token !== activeToken) {
          throw new PreconditionFailedError('delivery lease was superseded');
        }
        await memory.appendTransaction([append.operation]);
        append.apply();
      });
    };
    installCommitter(first);
    installCommitter(second);
    const before = memory.runCommits().length;
    let releaseFirst = () => {};
    const firstMayFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstStarted = () => {};
    const firstDidStart = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });

    const oldHandler = first.run(delivery('lease-old'), async () => {
      await firstStorage.events.create(
        'wrun_atomic',
        stepStarted('step-fenced')
      );
      firstStarted();
      await firstMayFinish;
      await expect(
        firstStorage.events.create(
          'wrun_atomic',
          stepCompleted('step-fenced')
        )
      ).rejects.toSatisfy((error: unknown) =>
        PreconditionFailedError.is(error)
      );
    });
    await firstDidStart;

    await second.run(
      delivery('lease-new', { attempt: 2, generation: 11 }),
      async () => {
        activeToken = 'lease-new';
        releaseFirst();
        await oldHandler;
        await secondStorage.events.create(
          'wrun_atomic',
          stepStarted('step-fenced')
        );
        await secondStorage.events.create(
          'wrun_atomic',
          stepCompleted('step-fenced')
        );
      }
    );

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(1);
    expect(
      commits
        .flatMap(({ events }) => events)
        .filter(({ eventType }) => eventType === 'step_completed')
    ).toHaveLength(1);
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

  it('lets a newer delivery reclaim an uncommitted lazy step', async () => {
    const { client, journal, memory } = await setup();
    const first = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const second = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const before = memory.runCommits().length;

    // A delivery that starts a step and is then abandoned without ever
    // committing a terminal event — a crash, or a lost race.
    const claims: (boolean | undefined)[] = [];
    const start = async (
      coordinator: RunExecutionCoordinator,
      tag: string,
      generation: number
    ) => {
      const { storage } = createStorage(client, {
        journal,
        executions: coordinator,
      });
      await coordinator.run(delivery(tag, { generation }), async () => {
        const result = await storage.events.create(
          'wrun_atomic',
          stepStarted('step-once')
        );
        claims.push(result.stepCreated);
      });
    };

    await start(first, 'lease-first', 10);
    await start(second, 'lease-second', 11);

    // No speculative lifecycle event is visible until its terminal commit.
    // A newer at-least-once delivery must therefore rerun the abandoned body.
    expect(claims).toEqual([true, true]);
    expect(
      memory
        .runCommits()
        .slice(before)
        .flatMap(({ events }) => events)
        .filter(({ eventType }) => eventType === 'step_created')
    ).toHaveLength(0);
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

  it('keeps the staged transaction available after the queue lease expires', async () => {
    const { executions, memory, storage } = await setup();
    const before = memory.runCommits().length;

    await executions.run(
      delivery('lease-expiring', {
        expiresAt: new Date(Date.now() + 20),
      }),
      async () => {
        await storage.events.create(
          'wrun_atomic',
          stepCreated('step-expiring')
        );
        await new Promise<void>((resolve) => setTimeout(resolve, 30));
        await expect(
          storage.events.create(
            'wrun_atomic',
            plainStepStarted('step-expiring')
          )
        ).resolves.toMatchObject({ step: { status: 'running' } });
        await storage.events.create(
          'wrun_atomic',
          stepCompleted('step-expiring')
        );
      }
    );

    const commits = memory.runCommits().slice(before);
    expect(commits).toHaveLength(2);
    expect(commits[1]?.events.map(({ eventType }) => eventType)).toEqual([
      'step_started',
      'step_completed',
    ]);
  });

  it('converges when an expired delivery completes after its redelivery', async () => {
    const { client, journal } = await setup();
    const first = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const second = new RunExecutionCoordinator({ allowOwnedLazyStarts: true });
    const { storage: firstStorage } = createStorage(client, {
      journal,
      executions: first,
    });
    const { storage: secondStorage } = createStorage(client, {
      journal,
      executions: second,
    });
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let staged = () => {};
    const stagedGate = new Promise<void>((resolve) => {
      staged = resolve;
    });
    await firstStorage.events.create(
      'wrun_atomic',
      stepCreated('step-redelivered')
    );
    const stale = first.run(
      delivery('lease-old', {
        expiresAt: new Date(Date.now() + 20),
      }),
      async () => {
        await firstStorage.events.create(
          'wrun_atomic',
          plainStepStarted('step-redelivered')
        );
        staged();
        await gate;
        await firstStorage.events.create(
          'wrun_atomic',
          stepCompleted('step-redelivered')
        );
      }
    );
    await stagedGate;
    await new Promise<void>((resolve) => setTimeout(resolve, 30));
    await second.run(
      delivery('lease-new', {
        ownerMessageId: 'msg_new',
        attempt: 2,
        generation: 11,
      }),
      async () => {
        await secondStorage.events.create(
          'wrun_atomic',
          plainStepStarted('step-redelivered')
        );
        await secondStorage.events.create(
          'wrun_atomic',
          stepCompleted('step-redelivered')
        );
      }
    );
    release();

    await expect(stale).resolves.toBeUndefined();
    await expect(
      firstStorage.steps.get('wrun_atomic', 'step-redelivered')
    ).resolves.toMatchObject({ status: 'completed', attempt: 1 });
  });
});
