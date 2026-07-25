import { describe, expect, it, vi } from 'vitest';
import { UrsulaClient, UrsulaRequestError } from './client.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

describe('UrsulaClient', () => {
  it('combines stable operation deduplication with a record-tail guard', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: {
            'stream-record-start': '7',
            'stream-record-next': '8',
          },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      client.append(
        'run-1',
        { event: 'started' },
        {
          operationId: 'event-1',
          expectedRecord: 7,
        }
      )
    ).resolves.toEqual({ startRecord: 7, nextRecord: 8 });

    const appendHeaders = new Headers(fetch.mock.calls[1]?.[1]?.headers);
    expect(appendHeaders.get('stream-record-match')).toBe('7');
    expect(appendHeaders.get('producer-id')).toMatch(/^workflow-op-/);
    expect(appendHeaders.get('producer-epoch')).toBe('0');
    expect(appendHeaders.get('producer-seq')).toBe('0');
  });

  it('surfaces the current record tail on a precondition failure', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 200 }))
      .mockResolvedValueOnce(
        response('record tail mismatch', {
          status: 412,
          headers: { 'stream-record-next': '9' },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const error = await client
      .append(
        'run-1',
        { event: 'started' },
        {
          operationId: 'event-1',
          expectedRecord: 7,
        }
      )
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(UrsulaRequestError);
    expect((error as UrsulaRequestError).status).toBe(412);
    expect((error as UrsulaRequestError).nextRecord).toBe(9);
  });

  it('round-trips binary Workflow payloads in JSON records', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      response(
        JSON.stringify({
          record: 0,
          value: {
            payload: { __type: 'Uint8Array', data: 'AP8=' },
          },
        }),
        {
          status: 200,
          headers: {
            'stream-record-next': '1',
            'stream-up-to-date': 'true',
          },
        }
      )
    );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const result = await client.read<{ payload: Uint8Array }>('run-1');

    expect(result.records[0]?.value.payload).toEqual(Uint8Array.from([0, 255]));
  });

  it('reads only the latest checkpoint record', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      response('{"record":7,"value":{"sourceNextRecord":256}}', {
        status: 200,
        headers: {
          'stream-record-next': '8',
          'stream-up-to-date': 'true',
        },
      })
    );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    const result = await client.readTail<{ sourceNextRecord: number }>(
      'queue-checkpoint-1'
    );

    expect(result.records).toEqual([
      { record: 7, value: { sourceNextRecord: 256 } },
    ]);
    const url = new URL(String(fetch.mock.calls[0]?.[0]));
    expect(url.searchParams.get('tail_records')).toBe('1');
    expect(url.searchParams.get('record_view')).toBe('envelope');
  });

  it('encodes Node Buffers as opaque binary instead of Buffer JSON', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: {
            'stream-record-start': '0',
            'stream-record-next': '1',
          },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await client.append(
      'run-1',
      { payload: Buffer.from([0, 255]) },
      { operationId: 'buffer-event' }
    );

    expect(JSON.parse(fetch.mock.calls[1]?.[1]?.body as string)).toEqual({
      payload: { __type: 'Uint8Array', data: 'AP8=' },
    });
  });
});
