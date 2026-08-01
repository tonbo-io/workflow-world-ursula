import { describe, expect, it, vi } from 'vitest';
import { runAffinity } from './affinity.js';
import { createStreamer } from './streamer.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

function transactionResponse(): Response {
  return response(null, {
    status: 200,
    headers: {
      'stream-extensions': 'group-append-transaction-v1',
    },
  });
}

describe('Ursula Workflow streamer', () => {
  it('coalesces adjacent chunks for 10ms by default', () => {
    expect(
      createStreamer({ baseUrl: 'https://ursula.test' })
        .streamFlushIntervalMs
    ).toBe(10);
  });

  it('atomically writes first chunks and discovery in the run affinity lane', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(transactionResponse());
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await streamer.streams.writeMulti?.('wrun_1', 'output', [
      'hello',
      Uint8Array.from([0, 255]),
    ]);

    expect(fetch).toHaveBeenCalledTimes(3);
    const lane = runAffinity('wrun_1', 8);
    expect(fetch.mock.calls[0]?.[0]?.toString()).toContain(`/workflow/${lane}/`);
    expect(fetch.mock.calls[1]?.[0]?.toString()).toContain(`/workflow/${lane}/`);
    const transaction = fetch.mock.calls[2];
    expect(transaction?.[0]?.toString()).toBe(
      `https://ursula.test/workflow/${lane}/$transaction`
    );
    const body = JSON.parse(transaction?.[1]?.body as string) as {
      operations: Array<{ stream: string; payload_base64: string }>;
    };
    expect(body.operations).toHaveLength(2);
    expect(body.operations[0]?.stream).toMatch(/^stream-/);
    expect(
      JSON.parse(Buffer.from(body.operations[0]?.payload_base64 ?? '', 'base64').toString())
    ).toEqual([
      { v: 1, data: 'aGVsbG8=' },
      { v: 1, data: 'AP8=' },
    ]);
    expect(body.operations[1]?.stream).toBe('stream-registry');
  });

  it('uses an ordinary producer append after atomic first-write registration', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(transactionResponse())
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await streamer.streams.write('wrun_1', 'output', 'first');
    await streamer.streams.write('wrun_1', 'output', 'second');

    const append = fetch.mock.calls[3];
    expect(append?.[1]?.method).toBe('POST');
    expect(JSON.parse(append?.[1]?.body as string)).toEqual({
      v: 1,
      data: 'c2Vjb25k',
    });
    const headers = new Headers(append?.[1]?.headers);
    expect(headers.get('producer-seq')).toBe('1');
  });

  it('rejects a server that does not advertise the required transaction extension', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.write('wrun_1', 'output', 'hello')
    ).rejects.toThrow(/group-append-transaction-v1/);
  });

  it('returns an empty stream info shape for a missing stream', async () => {
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch: vi.fn().mockResolvedValue(response('not found', { status: 404 })),
    });

    await expect(
      streamer.streams.getInfo('wrun_missing', 'output')
    ).resolves.toEqual({ tailIndex: -1, done: false });
  });

  it('reads envelope records as Workflow chunks', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      response(
        `${JSON.stringify({ record: 3, value: { v: 1, data: 'aGVsbG8=' } })}\n`,
        {
          status: 200,
          headers: {
            'stream-record-next': '4',
            'stream-up-to-date': 'true',
          },
        }
      )
    );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.getChunks('wrun_1', 'output', {
        cursor: Buffer.from(JSON.stringify({ index: 3 })).toString('base64url'),
        limit: 1,
      })
    ).resolves.toMatchObject({
      data: [{ index: 3, data: Uint8Array.from(Buffer.from('hello')) }],
      hasMore: false,
    });
  });
});
