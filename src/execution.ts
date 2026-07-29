import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AnyEventRequest,
  CreateEventParams,
  EventResult,
} from '@workflow/world';

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
  delivery: DeliveryExecution;
  stagedStarts: Map<string, StagedStepStart>;
  turn: Promise<void>;
}

/**
 * Coordinates owned step commits within one adapter process.
 *
 * The durable queue lease remains the delivery authority. Run mutations use
 * Ursula's record-tail CAS as their correctness boundary, so this coordinator
 * never writes a second lease into the run journal.
 */
export class RunExecutionCoordinator {
  private readonly contexts = new AsyncLocalStorage<DeliveryContext>();
  private readonly ownedStarts = new Map<string, StagedStepStart>();
  private readonly ownedTurns = new Map<string, Promise<void>>();

  constructor(
    private readonly options: { allowOwnedLazyStarts?: boolean } = {}
  ) {}

  allowsOwnedLazyStarts(): boolean {
    return this.options.allowOwnedLazyStarts === true;
  }

  async run<T>(
    delivery: DeliveryExecution | undefined,
    task: () => Promise<T>
  ): Promise<T> {
    if (!delivery || delivery.expiresAt.getTime() <= Date.now()) {
      return task();
    }
    return this.contexts.run(
      {
        runId: delivery.runId,
        delivery,
        stagedStarts: new Map(),
        turn: Promise.resolve(),
      },
      task
    );
  }

  current(runId: string): DeliveryExecution | undefined {
    const context = this.contexts.getStore();
    return context?.runId === runId ? context.delivery : undefined;
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
