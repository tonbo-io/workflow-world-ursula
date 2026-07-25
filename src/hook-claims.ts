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
}

interface ClaimState {
  nextRecord: number;
  active?: ActiveHookClaim;
}

function claimStream(token: string): string {
  const digest = createHash('sha256').update(token).digest('base64url');
  return `hook-${digest}`;
}

export class HookClaims {
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
        createdAt: new Date().toISOString(),
      };
      try {
        await this.client.append(claimStream(args.token), transition, {
          operationId: `hook-reserve:${args.operationId}`,
          expectedRecord: state.nextRecord,
          createIfMissing: state.nextRecord === 0,
        });
        return {
          acquired: true,
          claim: {
            operationId: args.operationId,
            token: args.token,
            runId: args.runId,
            hookId: args.hookId,
            retentionUntil: args.retentionUntil,
          },
        };
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
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
        return;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
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
        return;
      } catch (error) {
        if (isUrsulaRequestError(error, 412)) {
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Hook claim "${token}" remained contended at release`);
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: replaying the three-transition claim state machine is intentionally kept in one audit-friendly function.
  private async load(token: string): Promise<ClaimState> {
    let records: { record: number; value: HookClaimTransition }[];
    try {
      records = await this.client.readAll<HookClaimTransition>(
        claimStream(token)
      );
    } catch (error) {
      if (isUrsulaRequestError(error, 404)) {
        return { nextRecord: 0 };
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
    return { nextRecord: records.length, active };
  }
}
