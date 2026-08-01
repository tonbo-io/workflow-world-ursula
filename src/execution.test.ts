import { describe, expect, it, vi } from 'vitest';
import {
  type DeliveryExecution,
  RunExecutionCoordinator,
} from './execution.js';
import type { PreparedRunAppend, RunJournalState } from './run-journal.js';

function delivery(
  token: string,
  overrides: Partial<DeliveryExecution> = {}
): DeliveryExecution {
  return {
    runId: 'wrun_delivery',
    lane: 'run',
    queueName: '__wkf_workflow_test',
    queuePartition: 0,
    token,
    generation: 1,
    ownerMessageId: 'msg_delivery',
    attempt: 1,
    expiresAt: new Date(Date.now() + 60_000),
    ...overrides,
  };
}

function state(runId: string): RunJournalState {
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

function append(): PreparedRunAppend {
  return {
    operation: {
      stream: 'run-test',
      values: { version: 1 },
      operationId: 'run-delivery:test',
      expectedRecord: 0,
    },
    apply: vi.fn(),
    deduplicated: vi.fn(),
  };
}

describe('RunExecutionCoordinator', () => {
  it('shares the active delivery across package instances', async () => {
    const first = new RunExecutionCoordinator();
    const second = new RunExecutionCoordinator();
    const current = delivery('lease-shared');

    await first.run(current, async () => {
      expect(second.current(current.runId)).toEqual(current);
      expect(
        second.current(current.runId, current.ownerMessageId)
      ).toEqual(current);
    });
    expect(second.current(current.runId)).toBeUndefined();
  });

  it('commits a staged run batch together with delivery completion', async () => {
    const coordinator = new RunExecutionCoordinator();
    const commit = vi.fn().mockResolvedValue(undefined);
    coordinator.setDeliveryCommitter(commit);
    const current = delivery('lease-complete');
    const prepared = append();

    await coordinator.run(current, async () => {
      const base = state(current.runId);
      coordinator.stageRunTransaction(current.runId, {
        baseState: base,
        state: base,
        commit: { operationId: 'batch', events: [] },
        append: prepared,
      });
      await expect(
        coordinator.finishDelivery(current.runId, 5)
      ).resolves.toBe(true);
    });

    expect(commit).toHaveBeenCalledWith(current, prepared, 5);
  });

  it('flushes a staged run batch before publishing external work', async () => {
    const coordinator = new RunExecutionCoordinator();
    const commit = vi.fn().mockResolvedValue(undefined);
    coordinator.setRunCommitter(commit);
    const current = delivery('lease-flush');
    const prepared = append();

    await coordinator.run(current, async () => {
      const base = state(current.runId);
      coordinator.stageRunTransaction(current.runId, {
        baseState: base,
        state: base,
        commit: { operationId: 'batch', events: [] },
        append: prepared,
      });
      await expect(
        coordinator.flushRunTransaction(current.runId)
      ).resolves.toBe(true);
      await expect(
        coordinator.finishDelivery(current.runId, undefined)
      ).resolves.toBe(false);
    });

    expect(commit).toHaveBeenCalledWith(current, prepared);
  });

  it('serializes mutations only within one run', async () => {
    const coordinator = new RunExecutionCoordinator();
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered = () => {};
    const firstEntered = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const first = coordinator.exclusive('run-a', async () => {
      entered();
      await gate;
    });
    await firstEntered;

    await expect(
      coordinator.exclusive('run-b', async () => 'done')
    ).resolves.toBe('done');
    release();
    await first;
  });
});
