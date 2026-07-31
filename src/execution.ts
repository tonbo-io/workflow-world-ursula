import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AnyEventRequest,
  CreateEventParams,
  EventResult,
} from '@workflow/world';

export interface DeliveryExecution {
  runId: string;
  lane: string;
  queueName: string;
  queuePartition: number;
  token: string;
  generation: number;
  ownerMessageId: string;
  attempt: number;
  expiresAt: Date;
}

export interface ClaimedExecutionFence extends DeliveryExecution {
  epoch: number;
}

export interface StagedStepStart {
  request: AnyEventRequest & { eventType: 'step_started' };
  params: CreateEventParams | undefined;
  callId: string;
  eventId: string;
  syntheticEventId: string;
  now: Date;
  result: EventResult;
  fence?: ClaimedExecutionFence;
}

interface DeliveryContext {
  runId: string;
  delivery: DeliveryExecution;
  fence?: ClaimedExecutionFence;
  stagedStarts: Map<string, StagedStepStart>;
  turn: Promise<void>;
}

type ExecutionFenceClaimer = (
  delivery: DeliveryExecution
) => Promise<ClaimedExecutionFence>;

/**
 * Coordinates owned step commits within one adapter process.
 *
 * The durable queue lease remains the delivery authority. The optional fence
 * claimer projects that ownership into the run journal before handler code is
 * allowed to execute.
 */
export class RunExecutionCoordinator {
  private readonly contexts = new AsyncLocalStorage<DeliveryContext>();
  private readonly ownedStarts = new Map<string, StagedStepStart>();
  private readonly ownedTurns = new Map<string, Promise<void>>();
  private fenceClaimer: ExecutionFenceClaimer | undefined;

  constructor(
    private readonly options: { allowOwnedLazyStarts?: boolean } = {}
  ) {}

  allowsOwnedLazyStarts(): boolean {
    return this.options.allowOwnedLazyStarts === true;
  }

  setFenceClaimer(claimer: ExecutionFenceClaimer): void {
    this.fenceClaimer = claimer;
  }

  async run<T>(
    delivery: DeliveryExecution | undefined,
    task: () => Promise<T>
  ): Promise<T> {
    if (!delivery || delivery.expiresAt.getTime() <= Date.now()) {
      return task();
    }
    const fence = this.allowsOwnedLazyStarts()
      ? await this.claimFence(delivery)
      : undefined;
    return this.contexts.run(
      {
        runId: delivery.runId,
        delivery,
        fence,
        stagedStarts: new Map(),
        turn: Promise.resolve(),
      },
      task
    );
  }

  private async claimFence(
    delivery: DeliveryExecution
  ): Promise<ClaimedExecutionFence> {
    if (!this.fenceClaimer) {
      throw new Error('Ursula execution fence claimer is not configured');
    }
    return this.fenceClaimer(delivery);
  }

  current(
    runId: string,
    ownerMessageId?: string
  ): DeliveryExecution | undefined {
    const context = this.contexts.getStore();
    if (
      context?.runId !== runId ||
      (ownerMessageId !== undefined &&
        context.delivery.ownerMessageId !== ownerMessageId)
    ) {
      return;
    }
    return context.delivery;
  }

  fence(
    runId: string,
    ownerMessageId?: string
  ): ClaimedExecutionFence | undefined {
    const context = this.contexts.getStore();
    if (
      context?.runId !== runId ||
      (ownerMessageId !== undefined &&
        context.delivery.ownerMessageId !== ownerMessageId)
    ) {
      return;
    }
    return context.fence;
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
