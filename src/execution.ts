import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AnyEventRequest,
  CreateEventParams,
  EventResult,
} from '@workflow/world';
import type { PreparedRunAppend } from './run-journal.js';

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

export interface StagedStepStart {
  request: AnyEventRequest & { eventType: 'step_started' };
  params: CreateEventParams | undefined;
  callId: string;
  eventId: string;
  syntheticEventId: string;
  now: Date;
  result: EventResult;
  delivery?: DeliveryExecution;
}

interface DeliveryContext {
  runId: string;
  delivery: DeliveryExecution;
  stagedStarts: Map<string, StagedStepStart>;
  turn: Promise<void>;
}

type OwnedStepCommitter = (
  delivery: DeliveryExecution,
  append: PreparedRunAppend
) => Promise<void>;

interface SharedDeliveryRegistry {
  deliveries: Map<string, DeliveryExecution>;
}

const DELIVERY_REGISTRY = Symbol.for(
  '@tonbo-io/world-ursula/delivery-registry'
);

function deliveryKey(runId: string, ownerMessageId: string): string {
  return `${runId}\0${ownerMessageId}`;
}

function sharedDeliveries(): SharedDeliveryRegistry {
  const globals = globalThis as typeof globalThis & {
    [DELIVERY_REGISTRY]?: SharedDeliveryRegistry;
  };
  globals[DELIVERY_REGISTRY] ??= { deliveries: new Map() };
  return globals[DELIVERY_REGISTRY];
}

/**
 * Coordinates owned step commits within one adapter process.
 *
 * The durable queue lease remains the delivery authority. A complete owned
 * step validates that lease and commits its run record in one group-local
 * Ursula transaction.
 */
export class RunExecutionCoordinator {
  private readonly contexts = new AsyncLocalStorage<DeliveryContext>();
  private readonly ownedStarts = new Map<string, StagedStepStart>();
  private readonly ownedTurns = new Map<string, Promise<void>>();
  private ownedStepCommitter: OwnedStepCommitter | undefined;

  constructor(
    private readonly options: { allowOwnedLazyStarts?: boolean } = {}
  ) {}

  allowsOwnedLazyStarts(): boolean {
    return this.options.allowOwnedLazyStarts === true;
  }

  setOwnedStepCommitter(committer: OwnedStepCommitter): void {
    this.ownedStepCommitter = committer;
  }

  async commitOwnedStep(
    runId: string,
    delivery: DeliveryExecution,
    append: PreparedRunAppend
  ): Promise<boolean> {
    if (!this.ownedStepCommitter || delivery.runId !== runId) return false;
    await this.ownedStepCommitter(delivery, append);
    return true;
  }

  async run<T>(
    delivery: DeliveryExecution | undefined,
    task: () => Promise<T>
  ): Promise<T> {
    if (!delivery || delivery.expiresAt.getTime() <= Date.now()) {
      return task();
    }
    const key = deliveryKey(delivery.runId, delivery.ownerMessageId);
    const registry = sharedDeliveries().deliveries;
    const previous = registry.get(key);
    if (!previous || previous.generation <= delivery.generation) {
      registry.set(key, delivery);
    }
    try {
      return await this.contexts.run(
        {
          runId: delivery.runId,
          delivery,
          stagedStarts: new Map(),
          turn: Promise.resolve(),
        },
        task
      );
    } finally {
      if (registry.get(key)?.token === delivery.token) registry.delete(key);
    }
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
      return ownerMessageId === undefined
        ? undefined
        : sharedDeliveries().deliveries.get(
            deliveryKey(runId, ownerMessageId)
          );
    }
    return context.delivery;
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
