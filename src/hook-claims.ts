import { createHash } from 'node:crypto';
import {
  isUrsulaRequestError,
  type UrsulaClient,
} from './client.js';

type HookClaimTransition =
  | {
      version: 1;
      type: 'reserved';
      operationId: string;
      token: string;
      runId: string;
      hookId: string;
      retentionUntil?: string;
      /**
       * Deadline after which an uncommitted reservation may be reconciled
       * against its owner run journal. Absent on records written before this
       * field existed; readers fall back to `createdAt` plus the default.
       */
      reservedUntil?: string;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'committed';
      operationId: string;
      runRecord: number;
      createdAt: string;
    }
  | {
      version: 1;
      type: 'released';
      operationId: string;
      claimOperationId: string;
      createdAt: string;
    };

export interface ActiveHookClaim {
  operationId: string;
  token: string;
  runId: string;
  hookId: string;
  retentionUntil?: Date;
  committedRunRecord?: number;
  /** See {@link HookClaimTransition}'s `reservedUntil`. */
  reservedUntil: Date;
}

/**
 * How long a reservation stays unreconcilable.
 *
 * A reservation is appended immediately before its owner run record, so the
 * honest window is one journal append plus its CAS retries — well under a
 * second. This is deliberately far longer so the deadline only ever elapses
 * for a process that actually died, never for one that is merely slow.
 */
export const HOOK_RESERVATION_TIMEOUT_MS = 5 * 60 * 1000;

interface ClaimState {
  nextRecord: number;
  active?: ActiveHookClaim;
}

const MAX_CACHED_CLAIMS = 4096;
const MAX_RETRY_DELAY_MS = 50;

function cloneState(state: ClaimState): ClaimState {
  return {
    nextRecord: state.nextRecord,
    ...(state.active
      ? {
          active: {
            ...state.active,
            reservedUntil: new Date(state.active.reservedUntil),
            ...(state.active.retentionUntil
              ? { retentionUntil: new Date(state.active.retentionUntil) }
              : {}),
          },
        }
      : {}),
  };
}

/**
 * Whether a claim is an orphan: reserved, never committed, and past its
 * deadline. Its owner process died between reserving the token and appending
 * the run record that would have finalized the claim. Callers must still
 * confirm against the owner run journal before releasing it.
 */
export function isExpiredReservation(
  claim: ActiveHookClaim,
  now = new Date()
): boolean {
  return (
    claim.committedRunRecord === undefined &&
    claim.reservedUntil.getTime() <= now.getTime()
  );
}

async function contentionBackoff(attempt: number): Promise<void> {
  const ceiling = Math.min(MAX_RETRY_DELAY_MS, 2 ** Math.min(attempt, 6));
  const delayMs = Math.max(1, Math.floor(Math.random() * ceiling));
  await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
}

function claimStream(token: string): string {
  const digest = createHash('sha256').update(token).digest('base64url');
  return `hook-${digest}`;
}

export class HookClaims {
  private readonly cache = new Map<string, ClaimState>();

  constructor(private readonly client: UrsulaClient) {}

  async get(token: string): Promise<ActiveHookClaim | undefined> {
    return (await this.load(token)).active;
  }

  async reserve(args: {
    operationId: string;
    token: string;
    runId: string;
    hookId: string;
    retentionUntil?: Date;
  }): Promise<
    | { acquired: true; claim: ActiveHookClaim }
    | { acquired: false; claim: ActiveHookClaim }
  > {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      // Hook tokens are normally unique. Optimistically create the first
      // reservation in one request; an existing owner rejects expected tail
      // zero and the next attempt loads the authoritative claim.
      const state: ClaimState =
        attempt === 0 ? { nextRecord: 0 } : await this.load(args.token);
      if (state.active) {
        const sameOwner =
          state.active.operationId === args.operationId ||
          (state.active.runId === args.runId &&
            state.active.hookId === args.hookId);
        return { acquired: sameOwner, claim: state.active };
      }
      const now = new Date();
      const reservedUntil = new Date(
        now.getTime() + HOOK_RESERVATION_TIMEOUT_MS
      );
      const transition: HookClaimTransition = {
        version: 1,
        type: 'reserved',
        operationId: args.operationId,
        token: args.token,
        runId: args.runId,
        hookId: args.hookId,
        ...(args.retentionUntil
          ? { retentionUntil: args.retentionUntil.toISOString() }
          : {}),
        reservedUntil: reservedUntil.toISOString(),
        createdAt: now.toISOString(),
      };
      try {
        await this.client.append(claimStream(args.token), transition, {
          operationId: `hook-reserve:${args.operationId}`,
          expectedRecord: state.nextRecord,
          createIfMissing: state.nextRecord === 0,
        });
        const claim: ActiveHookClaim = {
          operationId: args.operationId,
          token: args.token,
          runId: args.runId,
          hookId: args.hookId,
          retentionUntil: args.retentionUntil,
          reservedUntil,
        };
        this.cacheState(args.token, {
          nextRecord: state.nextRecord + 1,
          active: claim,
        });
        return {
          acquired: true,
          claim,
        };
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.cache.delete(args.token);
          await contentionBackoff(attempt);
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Hook token "${args.token}" remained contended`);
  }

  async commit(claim: ActiveHookClaim, runRecord: number): Promise<void> {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const state = await this.load(claim.token);
      if (
        state.active?.operationId === claim.operationId &&
        state.active.committedRunRecord === runRecord
      ) {
        return;
      }
      if (state.active?.operationId !== claim.operationId) {
        throw new Error(`Hook claim for "${claim.token}" changed ownership`);
      }
      try {
        await this.client.append(
          claimStream(claim.token),
          {
            version: 1,
            type: 'committed',
            operationId: claim.operationId,
            runRecord,
            createdAt: new Date().toISOString(),
          } satisfies HookClaimTransition,
          {
            operationId: `hook-commit:${claim.operationId}:${runRecord}`,
            expectedRecord: state.nextRecord,
          }
        );
        this.cacheState(claim.token, {
          nextRecord: state.nextRecord + 1,
          active: { ...state.active, committedRunRecord: runRecord },
        });
        return;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.cache.delete(claim.token);
          await contentionBackoff(attempt);
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Hook claim "${claim.token}" remained contended at commit`);
  }

  async release(
    token: string,
    owner: { runId: string; hookId: string },
    operationId: string
  ): Promise<void> {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const state = await this.load(token);
      if (!state.active) return;
      if (
        state.active.runId !== owner.runId ||
        state.active.hookId !== owner.hookId
      ) {
        return;
      }
      try {
        await this.client.append(
          claimStream(token),
          {
            version: 1,
            type: 'released',
            operationId,
            claimOperationId: state.active.operationId,
            createdAt: new Date().toISOString(),
          } satisfies HookClaimTransition,
          {
            operationId: `hook-release:${operationId}`,
            expectedRecord: state.nextRecord,
          }
        );
        this.cacheState(token, { nextRecord: state.nextRecord + 1 });
        return;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          this.cache.delete(token);
          await contentionBackoff(attempt);
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Hook claim "${token}" remained contended at release`);
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: replaying the three-transition claim state machine is intentionally kept in one audit-friendly function.
  private async load(token: string): Promise<ClaimState> {
    const cached = this.cache.get(token);
    if (cached) {
      this.cache.delete(token);
      this.cache.set(token, cached);
      return cloneState(cached);
    }
    let records: { record: number; value: HookClaimTransition }[];
    try {
      records = await this.client.readAll<HookClaimTransition>(
        claimStream(token)
      );
    } catch (error) {
      if (isUrsulaRequestError(error, 404)) {
        const empty = { nextRecord: 0 };
        this.cacheState(token, empty);
        return empty;
      }
      throw error;
    }
    let active: ActiveHookClaim | undefined;
    for (const { value } of records) {
      if (value.version !== 1) {
        throw new Error('Unsupported Ursula hook claim version');
      }
      if (value.type === 'reserved') {
        active = {
          operationId: value.operationId,
          token: value.token,
          runId: value.runId,
          hookId: value.hookId,
          ...(value.retentionUntil
            ? { retentionUntil: new Date(value.retentionUntil) }
            : {}),
          // Records written before `reservedUntil` existed still become
          // reconcilable, so orphans predating this field are not stranded.
          reservedUntil: new Date(
            value.reservedUntil ??
              new Date(value.createdAt).getTime() +
                HOOK_RESERVATION_TIMEOUT_MS
          ),
        };
      } else if (
        value.type === 'committed' &&
        active?.operationId === value.operationId
      ) {
        active.committedRunRecord = value.runRecord;
      } else if (
        value.type === 'released' &&
        active?.operationId === value.claimOperationId
      ) {
        active = undefined;
      }
    }
    const state = { nextRecord: records.length, active };
    this.cacheState(token, state);
    return cloneState(state);
  }

  private cacheState(token: string, state: ClaimState): void {
    this.cache.delete(token);
    this.cache.set(token, cloneState(state));
    if (this.cache.size <= MAX_CACHED_CLAIMS) return;
    const oldest = this.cache.keys().next().value;
    if (oldest !== undefined) this.cache.delete(oldest);
  }
}
