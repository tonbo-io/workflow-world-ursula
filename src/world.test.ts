import type { World } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import { createWorld, withUrsulaStreams } from './world.js';

describe('withUrsulaStreams', () => {
  it('preserves storage and queue while replacing streams', async () => {
    const queue = vi.fn();
    const originalWrite = vi.fn();
    const world = {
      specVersion: 5,
      queue,
      streams: { write: originalWrite },
      runs: {},
      steps: {},
      events: {},
      hooks: {},
    } as unknown as World;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(new Response(null, { status: 201 }));

    const wrapped = withUrsulaStreams(world, {
      baseUrl: 'https://ursula.test',
      fetch,
    });

    expect(wrapped.queue).toBe(queue);
    expect(wrapped.runs).toBe(world.runs);
    expect(wrapped.specVersion).toBe(5);
    expect(wrapped.streams.write).not.toBe(originalWrite);
  });

  it('exports a complete loadable World without a delegate', () => {
    const world = createWorld({
      baseUrl: 'https://ursula.test',
      fetch: vi.fn<typeof globalThis.fetch>(),
    });

    expect(world.specVersion).toBe(5);
    expect(typeof world.events.create).toBe('function');
    expect(typeof world.streams.write).toBe('function');
    expect(typeof world.queue).toBe('function');
    expect(world.capabilities).toMatchObject({
      preconditionGuard: true,
      maxConcurrency: true,
    });
  });
});
