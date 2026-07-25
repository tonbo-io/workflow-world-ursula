import { describe, expect, it } from 'vitest';
import {
  type UrsulaAppendOptions,
  type UrsulaClient,
  type UrsulaRecord,
  UrsulaRequestError,
} from './client.js';
import { HookClaims } from './hook-claims.js';

class MemoryClient {
  private readonly streams = new Map<string, unknown[]>();
  private failCommitOnce = true;
  private failReleaseOnce = true;

  async append<T>(
    stream: string,
    values: T | readonly T[],
    options: UrsulaAppendOptions
  ): Promise<{ startRecord: number; nextRecord: number }> {
    if (
      (options.operationId.startsWith('hook-commit:') && this.failCommitOnce) ||
      (options.operationId.startsWith('hook-release:') && this.failReleaseOnce)
    ) {
      if (options.operationId.startsWith('hook-commit:')) {
        this.failCommitOnce = false;
      } else {
        this.failReleaseOnce = false;
      }
      throw new UrsulaRequestError(
        'append records',
        new Response('conflict', { status: 412 }),
        'conflict'
      );
    }
    const current = this.streams.get(stream) ?? [];
    if (
      options.expectedRecord !== undefined &&
      options.expectedRecord !== current.length
    ) {
      throw new UrsulaRequestError(
        'append records',
        new Response('conflict', { status: 412 }),
        'conflict'
      );
    }
    const records = Array.isArray(values) ? values : [values];
    const startRecord = current.length;
    current.push(...records);
    this.streams.set(stream, current);
    return { startRecord, nextRecord: current.length };
  }

  async readAll<T>(stream: string): Promise<UrsulaRecord<T>[]> {
    return (this.streams.get(stream) ?? []).map((value, record) => ({
      record,
      value: value as T,
    }));
  }
}

describe('HookClaims', () => {
  it('retries commit and release CAS conflicts without losing ownership', async () => {
    const claims = new HookClaims(
      new MemoryClient() as unknown as UrsulaClient
    );
    const reserved = await claims.reserve({
      operationId: 'create-hook-1',
      token: 'token-1',
      runId: 'run-1',
      hookId: 'hook-1',
    });
    expect(reserved.acquired).toBe(true);

    await expect(claims.commit(reserved.claim, 7)).resolves.toBeUndefined();
    expect(await claims.get('token-1')).toMatchObject({
      committedRunRecord: 7,
    });
    await expect(
      claims.release(
        'token-1',
        { runId: 'run-1', hookId: 'hook-1' },
        'terminal-1'
      )
    ).resolves.toBeUndefined();
    expect(await claims.get('token-1')).toBeUndefined();
  });

  it('cannot release a claim owned by a different run or hook', async () => {
    const claims = new HookClaims(
      new MemoryClient() as unknown as UrsulaClient
    );
    await claims.reserve({
      operationId: 'create-hook-2',
      token: 'token-2',
      runId: 'run-2',
      hookId: 'hook-2',
    });

    await claims.release(
      'token-2',
      { runId: 'other-run', hookId: 'hook-2' },
      'stale-cleanup'
    );
    expect(await claims.get('token-2')).toMatchObject({
      runId: 'run-2',
      hookId: 'hook-2',
    });
  });
});
