import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  PreparedRunAppend,
  RunCommit,
  RunJournalState,
} from './run-journal.js';

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

interface DeliveryContext {
  runId: string;
  delivery: DeliveryExecution;
  turn: Promise<void>;
  runTransaction?: DeliveryRunTransaction;
}

export type PendingRunCommit = Omit<
  RunCommit,
  'version' | 'runId' | 'previousRecord'
>;

export interface DeliveryRunTransaction {
  baseState: RunJournalState;
  state: RunJournalState;
  commit: PendingRunCommit;
  append: PreparedRunAppend;
}

type RunCommitter = (
  delivery: DeliveryExecution,
  append: PreparedRunAppend
) => Promise<void>;

type DeliveryCommitter = (
  delivery: DeliveryExecution,
  append: PreparedRunAppend,
  timeoutSeconds: number | undefined
) => Promise<void>;

interface SharedDeliveryRegistry {
  deliveries: Map<string, DeliveryExecution>;
  contexts: AsyncLocalStorage<DeliveryContext>;
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
  globals[DELIVERY_REGISTRY] ??= {
    deliveries: new Map(),
    contexts: new AsyncLocalStorage<DeliveryContext>(),
  };
  return globals[DELIVERY_REGISTRY];
}

/**
 * Coordinates run mutations produced by one queue delivery.
 *
 * The durable queue lease remains the delivery authority. A complete
 * delivery validates that lease and commits its run record in one group-local
 * Ursula transaction.
 */
export class RunExecutionCoordinator {
  private readonly contexts = sharedDeliveries().contexts;
  private readonly turns = new Map<string, Promise<void>>();
  private runCommitter: RunCommitter | undefined;
  private deliveryCommitter: DeliveryCommitter | undefined;

  setRunCommitter(committer: RunCommitter): void {
    this.runCommitter = committer;
  }

  setDeliveryCommitter(committer: DeliveryCommitter): void {
    this.deliveryCommitter = committer;
  }

  stagedRunState(runId: string): RunJournalState | undefined {
    const context = this.contexts.getStore();
    return context?.runId === runId
      ? context.runTransaction?.state
      : undefined;
  }

  stagedRunCommit(runId: string): PendingRunCommit | undefined {
    const context = this.contexts.getStore();
    return context?.runId === runId
      ? context.runTransaction?.commit
      : undefined;
  }

  stagedRunBaseState(runId: string): RunJournalState | undefined {
    const context = this.contexts.getStore();
    return context?.runId === runId
      ? context.runTransaction?.baseState
      : undefined;
  }

  stageRunTransaction(
    runId: string,
    transaction: DeliveryRunTransaction
  ): void {
    const context = this.contexts.getStore();
    if (context?.runId !== runId) {
      throw new Error(`No active Ursula delivery transaction for run "${runId}"`);
    }
    context.runTransaction = transaction;
  }

  async flushRunTransaction(runId: string): Promise<boolean> {
    const context = this.contexts.getStore();
    const transaction =
      context?.runId === runId ? context.runTransaction : undefined;
    if (!context || !transaction || !this.runCommitter) return false;
    await this.runCommitter(context.delivery, transaction.append);
    context.runTransaction = undefined;
    return true;
  }

  async finishDelivery(
    runId: string,
    timeoutSeconds: number | undefined
  ): Promise<boolean> {
    const context = this.contexts.getStore();
    const transaction =
      context?.runId === runId ? context.runTransaction : undefined;
    if (!context || !transaction || !this.deliveryCommitter) return false;
    await this.deliveryCommitter(
      context.delivery,
      transaction.append,
      timeoutSeconds
    );
    context.runTransaction = undefined;
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

  async exclusive<T>(runId: string, task: () => Promise<T>): Promise<T> {
    const context = this.contexts.getStore();
    const previous =
      context?.runId === runId
        ? context.turn
        : (this.turns.get(runId) ?? Promise.resolve());
    let release = () => {};
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    if (context?.runId === runId) context.turn = next;
    else this.turns.set(runId, next);
    await previous;
    try {
      return await task();
    } finally {
      release();
      if (
        context?.runId !== runId &&
        this.turns.get(runId) === next
      ) {
        this.turns.delete(runId);
      }
    }
  }
}
