import { describe, expect, it, vi } from 'vitest';
import { createStreamer } from './streamer.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

describe('Ursula Workflow streamer', () => {
  it('coalesces adjacent chunks for 10ms by default', () => {
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
    });

    expect(streamer.streamFlushIntervalMs).toBe(10);
  });

  it('exposes the configured Workflow chunk coalescing interval', () => {
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      streamFlushIntervalMs: 0,
    });

    expect(streamer.streamFlushIntervalMs).toBe(0);
  });

  it('maps a batch to one atomic JSON-record append', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 201,
          headers: {
            'stream-record-start': '0',
            'stream-record-next': '2',
          },
        })
      );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await streamer.streams.writeMulti?.('wrun_1', 'output', [
      'hello',
      Uint8Array.from([0, 255]),
    ]);

    expect(fetch).toHaveBeenCalledTimes(2);
    const append = fetch.mock.calls[1];
    const request = append?.[1] as RequestInit;
    expect(append?.[0]?.toString()).toMatch(
      /^https:\/\/ursula\.test\/workflow\/wrun_1-[A-Za-z0-9_-]{32}$/
    );
    expect(request.method).toBe('PUT');
    expect(JSON.parse(request.body as string)).toEqual([
      { v: 1, data: 'aGVsbG8=' },
      { v: 1, data: 'AP8=' },
    ]);
    const headers = new Headers(request.headers);
    expect(headers.get('producer-id')).toMatch(/^workflow-/);
    expect(headers.get('producer-seq')).toBe('0');
  });

  it('does not serialize a data append behind stream registration', async () => {
    let finishRegistration: ((value: Response) => void) | undefined;
    const registration = new Promise<Response>((resolve) => {
      finishRegistration = resolve;
    });
    const fetch = vi.fn<typeof globalThis.fetch>((_input, init) => {
      if (
        typeof init?.body === 'string' &&
        init.body.includes('"name":"output"')
      ) {
        return registration;
      }
      return Promise.resolve(response(null, { status: 201 }));
    });
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const write = streamer.streams.write('wrun_1', 'output', 'hello');
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    expect(JSON.parse(fetch.mock.calls[1]?.[1]?.body as string)).toEqual({
      v: 1,
      data: 'aGVsbG8=',
    });

    finishRegistration?.(response(null, { status: 201 }));
    await write;
  });

  it('reuses the producer sequence when an append response is lost', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockRejectedValueOnce(new Error('connection reset'))
      .mockResolvedValueOnce(response(null, { status: 200 }))
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.write('wrun_1', 'output', 'hello')
    ).rejects.toThrow('connection reset');
    await streamer.streams.write('wrun_1', 'output', 'hello');

    const firstAppendHeaders = new Headers(fetch.mock.calls[1]?.[1]?.headers);
    const retriedAppendHeaders = new Headers(fetch.mock.calls[2]?.[1]?.headers);
    expect(fetch).toHaveBeenCalledTimes(4);
    expect(firstAppendHeaders.get('producer-id')).toBe(
      retriedAppendHeaders.get('producer-id')
    );
    expect(firstAppendHeaders.get('producer-seq')).toBe('0');
    expect(retriedAppendHeaders.get('producer-seq')).toBe('0');
  });

  it('reuses producer identity when a closed stream is retried elsewhere', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(null, { status: 201 }));
    const first = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await first.streams.write('wrun_1', 'output', 'hello');
    await first.streams.close('wrun_1', 'output');

    const retry = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });
    await retry.streams.write('wrun_1', 'output', 'hello');

    const firstAppendHeaders = new Headers(fetch.mock.calls[1]?.[1]?.headers);
    const retriedAppendHeaders = new Headers(fetch.mock.calls[4]?.[1]?.headers);
    expect(firstAppendHeaders.get('producer-id')).toBe(
      retriedAppendHeaders.get('producer-id')
    );
    expect(firstAppendHeaders.get('producer-seq')).toBe('0');
    expect(retriedAppendHeaders.get('producer-seq')).toBe('0');
  });

  it('restarts the producer sequence when a closed stream is retried locally', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await streamer.streams.write('wrun_1', 'output', 'first attempt');
    await streamer.streams.close('wrun_1', 'output');
    await streamer.streams.write('wrun_1', 'output', 'replayed attempt');

    const firstAppendHeaders = new Headers(fetch.mock.calls[3]?.[1]?.headers);
    const retriedAppendHeaders = new Headers(fetch.mock.calls[5]?.[1]?.headers);
    expect(fetch).toHaveBeenCalledTimes(6);
    expect(firstAppendHeaders.get('producer-id')).toBe(
      retriedAppendHeaders.get('producer-id')
    );
    expect(firstAppendHeaders.get('producer-seq')).toBe('0');
    expect(retriedAppendHeaders.get('producer-seq')).toBe('0');
  });

  it('waits for pending writes before closing a stream', async () => {
    let finishAppend: ((response: Response) => void) | undefined;
    const appendResponse = new Promise<Response>((resolve) => {
      finishAppend = resolve;
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockImplementationOnce(() => appendResponse)
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const write = streamer.streams.write('wrun_1', 'output', 'hello');
    await vi.waitFor(() => {
      expect(fetch).toHaveBeenCalledTimes(2);
    });
    const close = streamer.streams.close('wrun_1', 'output');
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(2);

    finishAppend?.(response(null, { status: 201 }));
    await Promise.all([write, close]);

    expect(fetch).toHaveBeenCalledTimes(3);
    const closeHeaders = new Headers(fetch.mock.calls[2]?.[1]?.headers);
    expect(closeHeaders.get('stream-closed')).toBe('true');
  });

  it('paginates by Ursula record ordinal and decodes binary chunks', async () => {
    const body = [
      JSON.stringify({
        record: 3,
        value: { v: 1, data: Buffer.from('a').toString('base64') },
      }),
      JSON.stringify({
        record: 4,
        value: { v: 1, data: Buffer.from('b').toString('base64') },
      }),
      JSON.stringify({
        record: 5,
        value: { v: 1, data: Buffer.from('c').toString('base64') },
      }),
    ].join('\n');
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      response(body, {
        status: 200,
        headers: {
          'stream-record-next': '6',
          'stream-up-to-date': 'true',
        },
      })
    );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });
    const cursor = Buffer.from(JSON.stringify({ index: 3 })).toString(
      'base64url'
    );

    const page = await streamer.streams.getChunks('wrun_1', 'output', {
      cursor,
      limit: 2,
    });

    expect(
      page.data.map((chunk) => Buffer.from(chunk.data).toString())
    ).toEqual(['a', 'b']);
    expect(page.data.map((chunk) => chunk.index)).toEqual([3, 4]);
    expect(page.hasMore).toBe(true);
    expect(page.cursor).not.toBeNull();
    expect(page.done).toBe(false);
    expect(fetch.mock.calls[0]?.[0]?.toString()).toContain(
      'record=3&max_records=3&record_view=envelope'
    );
  });

  it('treats repeated close on an already closed stream as idempotent', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 200 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 204 }))
      .mockResolvedValueOnce(
        response('already closed', {
          status: 409,
          headers: { 'stream-closed': 'true' },
        })
      );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.close('wrun_1', 'output')
    ).resolves.toBeUndefined();
    await expect(
      streamer.streams.close('wrun_1', 'output')
    ).resolves.toBeUndefined();
  });

  it('recovers a close replay when create reports closed metadata conflict', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 204 }))
      .mockResolvedValueOnce(
        response('StreamAlreadyExistsConflict', { status: 409 })
      )
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: { 'stream-closed': 'true' },
        })
      )
      .mockResolvedValueOnce(response(null, { status: 200 }))
      .mockResolvedValueOnce(response(null, { status: 200 }))
      .mockResolvedValueOnce(
        response('already closed', {
          status: 409,
          headers: { 'stream-closed': 'true' },
        })
      );
    const first = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });
    await first.streams.close('wrun_1', 'output');

    const replay = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });
    await expect(
      replay.streams.close('wrun_1', 'output')
    ).resolves.toBeUndefined();
    expect(fetch.mock.calls[4]?.[1]?.method).toBe('HEAD');
  });

  it('reports record tail and close state without reading payloads', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      response(null, {
        status: 200,
        headers: {
          'stream-record-next': '7',
          'stream-closed': 'true',
        },
      })
    );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(streamer.streams.getInfo('wrun_1', 'output')).resolves.toEqual(
      {
        tailIndex: 6,
        done: true,
      }
    );
  });

  it('reports the empty info shape for a missing stream', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response('missing', { status: 404 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.getInfo('wrun_1', 'missing')
    ).resolves.toEqual({
      tailIndex: -1,
      done: false,
    });
  });

  it('does not skip records when a long-poll timeout reports a newer tail', async () => {
    const chunk = JSON.stringify({
      record: 0,
      value: { v: 1, data: Buffer.from('arrived').toString('base64') },
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 204,
          headers: { 'stream-record-next': '1' },
        })
      )
      .mockResolvedValueOnce(
        response(chunk, {
          status: 200,
          headers: {
            'stream-record-next': '1',
            'stream-closed': 'true',
            'stream-up-to-date': 'true',
          },
        })
      );
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const stream = await streamer.streams.get('wrun_1', 'output');
    const reader = stream.getReader();

    await expect(reader.read()).resolves.toEqual({
      done: false,
      value: new TextEncoder().encode('arrived'),
    });
    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });
    expect(fetch.mock.calls[3]?.[0]?.toString()).toContain('record=0');
    expect(fetch.mock.calls[3]?.[0]?.toString()).toContain('timeout_ms=25000');
  });

  it('reconnects when a long-poll request outlives its server timeout', async () => {
    vi.useFakeTimers();
    try {
      const chunk = JSON.stringify({
        record: 0,
        value: { v: 1, data: Buffer.from('reconnected').toString('base64') },
      });
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(response(null, { status: 201 }))
        .mockResolvedValueOnce(response(null, { status: 201 }))
        .mockImplementationOnce((_input, init) => {
          return new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(
              'abort',
              () => reject(new DOMException('aborted', 'AbortError')),
              { once: true }
            );
          });
        })
        .mockResolvedValueOnce(
          response(chunk, {
            status: 200,
            headers: {
              'stream-record-next': '1',
              'stream-closed': 'true',
              'stream-up-to-date': 'true',
            },
          })
        );
      const streamer = createStreamer({
        baseUrl: 'https://ursula.test',
        fetch,
        longPollTimeoutMs: 10,
      });
      const stream = await streamer.streams.get('wrun_1', 'output');
      const reader = stream.getReader();
      const read = reader.read();

      await vi.advanceTimersByTimeAsync(5_010);

      await expect(read).resolves.toEqual({
        done: false,
        value: new TextEncoder().encode('reconnected'),
      });
      expect(fetch.mock.calls[2]?.[1]?.signal?.aborted).toBe(true);
      expect(fetch.mock.calls[3]?.[0]?.toString()).toContain('record=0');
    } finally {
      vi.useRealTimers();
    }
  });

  it('reconnects when a long-poll response body stalls', async () => {
    vi.useFakeTimers();
    try {
      const chunk = JSON.stringify({
        record: 0,
        value: { v: 1, data: Buffer.from('reconnected').toString('base64') },
      });
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(response(null, { status: 201 }))
        .mockResolvedValueOnce(response(null, { status: 201 }))
        .mockImplementationOnce((_input, init) => {
          const signal = init?.signal;
          return Promise.resolve({
            ok: true,
            status: 200,
            headers: new Headers(),
            text: () =>
              new Promise<string>((_resolve, reject) => {
                signal?.addEventListener(
                  'abort',
                  () => reject(new DOMException('aborted', 'AbortError')),
                  { once: true }
                );
              }),
          } as Response);
        })
        .mockResolvedValueOnce(
          response(chunk, {
            status: 200,
            headers: {
              'stream-record-next': '1',
              'stream-closed': 'true',
              'stream-up-to-date': 'true',
            },
          })
        );
      const streamer = createStreamer({
        baseUrl: 'https://ursula.test',
        fetch,
        longPollTimeoutMs: 10,
      });
      const stream = await streamer.streams.get('wrun_1', 'output');
      const reader = stream.getReader();
      const read = reader.read();

      await vi.advanceTimersByTimeAsync(5_010);

      await expect(read).resolves.toEqual({
        done: false,
        value: new TextEncoder().encode('reconnected'),
      });
      expect(fetch.mock.calls[2]?.[1]?.signal?.aborted).toBe(true);
      expect(fetch.mock.calls[3]?.[0]?.toString()).toContain('record=0');
    } finally {
      vi.useRealTimers();
    }
  });

  it('registers a live-read stream before its first latency-sensitive write', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>((input, init) => {
      if (input.toString().includes('live=long-poll')) {
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            'abort',
            () => reject(new DOMException('aborted', 'AbortError')),
            { once: true }
          );
        });
      }
      return Promise.resolve(response(null, { status: 201 }));
    });
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const readable = await streamer.streams.get('wrun_1', 'output');
    const reader = readable.getReader();
    await streamer.streams.write('wrun_1', 'output', 'hello');
    await reader.cancel();

    expect(fetch).toHaveBeenCalledTimes(4);
    expect(fetch.mock.calls[0]?.[1]?.method).toBe('PUT');
    expect(fetch.mock.calls[1]?.[1]?.method).toBe('PUT');
    expect(fetch.mock.calls[2]?.[0]?.toString()).toContain('live=long-poll');
    expect(fetch.mock.calls[3]?.[1]?.method).toBe('POST');
  });

  it('does not hold a data write behind registry durability', async () => {
    let finishRegistration: ((value: Response) => void) | undefined;
    const registration = new Promise<Response>((resolve) => {
      finishRegistration = resolve;
    });
    const fetch = vi.fn<typeof globalThis.fetch>((_input, init) => {
      if (
        init?.method === 'PUT' &&
        typeof init.body === 'string' &&
        init.body.includes('"name":"output"')
      ) {
        return registration;
      }
      return Promise.resolve(response(null, { status: 201 }));
    });
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.write('wrun_1', 'output', 'hello')
    ).resolves.toBeUndefined();
    finishRegistration?.(response(null, { status: 201 }));
    await streamer.streams.list('wrun_1');
  });

  it('attaches a live read without waiting for serialized registry metadata', async () => {
    let finishRegistration: ((value: Response) => void) | undefined;
    const registration = new Promise<Response>((resolve) => {
      finishRegistration = resolve;
    });
    const fetch = vi.fn<typeof globalThis.fetch>((input, init) => {
      const url = input.toString();
      if (
        init?.method === 'PUT' &&
        typeof init.body === 'string' &&
        init.body.includes('"name":"output"')
      ) {
        return registration;
      }
      if (url.includes('live=long-poll')) {
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            'abort',
            () => reject(new DOMException('aborted', 'AbortError')),
            { once: true }
          );
        });
      }
      if (url.includes('record=0')) {
        return Promise.resolve(
          response(JSON.stringify({ record: 0, value: { name: 'output' } }), {
            status: 200,
            headers: {
              'stream-record-next': '1',
              'stream-up-to-date': 'true',
            },
          })
        );
      }
      return Promise.resolve(response(null, { status: 201 }));
    });
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const readable = await streamer.streams.get('wrun_1', 'output');
    const reader = readable.getReader();
    const read = reader.read();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    expect(fetch.mock.calls[2]?.[0]?.toString()).toContain('live=long-poll');

    const list = streamer.streams.list('wrun_1');
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(3);

    finishRegistration?.(response(null, { status: 201 }));
    await expect(list).resolves.toEqual(['output']);
    await reader.cancel();
    await read.catch(() => undefined);
  });
});
