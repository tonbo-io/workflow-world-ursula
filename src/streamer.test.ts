import { describe, expect, it, vi } from 'vitest';
import { createStreamer } from './streamer.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

describe('Ursula Workflow streamer', () => {
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
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 204 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
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

    expect(fetch).toHaveBeenCalledTimes(4);
    const append = fetch.mock.calls[3];
    const request = append?.[1] as RequestInit;
    expect(append?.[0]?.toString()).toMatch(
      /^https:\/\/ursula\.test\/workflow\/wrun_1-[A-Za-z0-9_-]{32}$/
    );
    expect(request.method).toBe('POST');
    expect(JSON.parse(request.body as string)).toEqual([
      { v: 1, data: 'aGVsbG8=' },
      { v: 1, data: 'AP8=' },
    ]);
    const headers = new Headers(request.headers);
    expect(headers.get('producer-id')).toMatch(/^workflow-/);
    expect(headers.get('producer-seq')).toBe('0');
  });

  it('reuses the producer sequence when an append response is lost', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 204 }))
      .mockRejectedValueOnce(new Error('connection reset'))
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      streamer.streams.write('wrun_1', 'output', 'hello')
    ).rejects.toThrow('connection reset');
    await streamer.streams.write('wrun_1', 'output', 'hello');

    const firstAppendHeaders = new Headers(fetch.mock.calls[3]?.[1]?.headers);
    const retriedAppendHeaders = new Headers(fetch.mock.calls[4]?.[1]?.headers);
    expect(fetch).toHaveBeenCalledTimes(5);
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
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 204 }))
      .mockImplementationOnce(() => appendResponse)
      .mockResolvedValueOnce(response(null, { status: 200 }));
    const streamer = createStreamer({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const write = streamer.streams.write('wrun_1', 'output', 'hello');
    await vi.waitFor(() => {
      expect(fetch).toHaveBeenCalledTimes(4);
    });
    const close = streamer.streams.close('wrun_1', 'output');
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(4);

    finishAppend?.(response(null, { status: 200 }));
    await Promise.all([write, close]);

    expect(fetch).toHaveBeenCalledTimes(5);
    const closeHeaders = new Headers(fetch.mock.calls[4]?.[1]?.headers);
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
      .mockResolvedValueOnce(response(null, { status: 204 }))
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
    expect(fetch.mock.calls[4]?.[0]?.toString()).toContain('record=0');
    expect(fetch.mock.calls[4]?.[0]?.toString()).toContain('timeout_ms=25000');
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

    expect(fetch).toHaveBeenCalledTimes(5);
    expect(fetch.mock.calls[0]?.[1]?.method).toBe('PUT');
    expect(fetch.mock.calls[1]?.[1]?.method).toBe('PUT');
    expect(fetch.mock.calls[2]?.[1]?.method).toBe('POST');
    expect(fetch.mock.calls[3]?.[0]?.toString()).toContain('live=long-poll');
    expect(fetch.mock.calls[4]?.[1]?.method).toBe('POST');
  });
});
