import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AnyEventRequest,
  CreateEventParams,
  EventResult,
} from '@workflow/world';
import { isUrsulaRequestError } from './client.js';
import {
  RunJournal,
  type RunExecutionLease,
  type RunJournalState,
} from './run-journal.js';

const MAX_LEASE_CAS_RETRIES = 16;

export interface DeliveryExecution {
  runId: string;
  lane: string;
  token: string;
  ownerMessageId: string;
  attempt: number;
  expiresAt: Date;
}

export interface StagedStepStart {
  request: AnyEventRequest & { eventType: 'step_started' };
  params: CreateEventParams | undefined;
  callId: string;
  eventId: string;
  syntheticEventId: string;
  now: Date;
  result: EventResult;
}

interface DeliveryContext {
  runId: string;
  lease: RunExecutionLease;
  stagedStarts: Map<string, StagedStepStart>;
  turn: Promise<void>;
}

export class RunExecutionCoordinator {
  private readonly contexts = new AsyncLocalStorage<DeliveryContext>();
  private readonly ownedStarts = new Map<string, StagedStepStart>();
  private readonly ownedTurns = new Map<string, Promise<void>>();

  constructor(
    private readonly journal: RunJournal,
    private readonly options: { allowOwnedLazyStarts?: boolean } = {}
  ) {}

  allowsOwnedLazyStarts(): boolean {
    return this.options.allowOwnedLazyStarts === true;
  }

  private async claim(
    delivery: DeliveryExecution
  ): Promise<RunExecutionLease | undefined> {
    for (let attempt = 0; attempt < MAX_LEASE_CAS_RETRIES; attempt += 1) {
      let state: RunJournalState;
      try {
        state = await this.journal.loadForMutation(delivery.runId);
      } catch (error) {
        if (isUrsulaRequestError(error, 404)) return;
        throw error;
      }
      const current = state.executionLeases.get(delivery.lane);
      if (current?.token === delivery.token) return current;
      if (current && current.expiresAt.getTime() > Date.now()) {
        return;
      }
      const lease: RunExecutionLease = {
        lane: delivery.lane,
        token: delivery.token,
        ownerMessageId: delivery.ownerMessageId,
        attempt: delivery.attempt,
        expiresAt: delivery.expiresAt,
        generation: (current?.generation ?? 0) + 1,
      };
      try {
        await this.journal.append(state, {
          operationId: `run-execution-claim:${delivery.runId}:${delivery.lane}:${delivery.token}`,
          events: [],
          executionLeases: [{ id: delivery.lane, value: lease }],
        });
        return lease;
      } catch (error) {
        if (
          isUrsulaRequestError(error, 412) &&
          attempt + 1 < MAX_LEASE_CAS_RETRIES
        ) {
          this.journal.evict(delivery.runId);
          continue;
        }
        throw error;
      }
    }
    return;
  }

  async run<T>(
    delivery: DeliveryExecution | undefined,
    task: () => Promise<T>
  ): Promise<T> {
    if (!delivery || delivery.expiresAt.getTime() <= Date.now()) {
      return task();
    }
    const lease = await this.claim(delivery);
    if (!lease) return task();
    return this.contexts.run(
      {
        runId: delivery.runId,
        lease,
        stagedStarts: new Map(),
        turn: Promise.resolve(),
      },
      task
    );
  }

  current(runId: string): RunExecutionLease | undefined {
    const context = this.contexts.getStore();
    return context?.runId === runId ? context.lease : undefined;
  }

  staged(runId: string, stepId: string): StagedStepStart | undefined {
    const context = this.contexts.getStore();
    if (context?.runId === runId) {
      const staged = context.stagedStarts.get(stepId);
      if (staged) return staged;
    }
    return this.ownedStarts.get(`${runId}\0${stepId}`);
  }

  stage(runId: string, stepId: string, start: StagedStepStart): void {
    const context = this.contexts.getStore();
    if (context?.runId === runId) {
      context.stagedStarts.set(stepId, start);
      return;
    }
    this.ownedStarts.set(`${runId}\0${stepId}`, start);
  }

  finish(runId: string, stepId: string): void {
    const context = this.contexts.getStore();
    if (context?.runId === runId) context.stagedStarts.delete(stepId);
    this.ownedStarts.delete(`${runId}\0${stepId}`);
  }

  async exclusive<T>(runId: string, task: () => Promise<T>): Promise<T> {
    const context = this.contexts.getStore();
    const previous =
      context?.runId === runId
        ? context.turn
        : (this.ownedTurns.get(runId) ?? Promise.resolve());
    let release = () => {};
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    if (context?.runId === runId) context.turn = next;
    else this.ownedTurns.set(runId, next);
    await previous;
    try {
      return await task();
    } finally {
      release();
      if (
        context?.runId !== runId &&
        this.ownedTurns.get(runId) === next
      ) {
        this.ownedTurns.delete(runId);
      }
    }
  }
}
