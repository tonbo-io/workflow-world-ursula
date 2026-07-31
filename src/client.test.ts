import { describe, expect, it, vi } from 'vitest';
import {
  isUrsulaRequestError,
  UrsulaClient,
  UrsulaRequestError,
} from './client.js';

function response(body: BodyInit | null, init: ResponseInit = {}): Response {
  return new Response(body, init);
}

describe('UrsulaClient', () => {
  it('recognizes a request error created by another server bundle', () => {
    const error = Object.assign(new Error('precondition failed'), {
      name: 'UrsulaRequestError',
      operation: 'append records',
      status: 412,
    });

    expect(error).not.toBeInstanceOf(UrsulaRequestError);
    expect(isUrsulaRequestError(error, 412)).toBe(true);
    expect(isUrsulaRequestError(error, 404)).toBe(false);
    expect(
      isUrsulaRequestError(
        new Error(
          'Ursula append records failed: HTTP 412: record tail mismatch'
        ),
        412
      )
    ).toBe(true);
  });

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

  it('creates a missing stream with its first records in one request', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValueOnce(
      response(null, {
        status: 201,
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

    await expect(
      client.append(
        'run-1',
        { event: 'created' },
        {
          operationId: 'event-1',
          expectedRecord: 0,
          createIfMissing: true,
        }
      )
    ).resolves.toEqual({ startRecord: 0, nextRecord: 1 });

    expect(fetch).toHaveBeenCalledOnce();
    const request = fetch.mock.calls[0]?.[1];
    expect(request?.method).toBe('PUT');
    expect(new Headers(request?.headers).has('stream-record-match')).toBe(
      false
    );
    expect(request?.body).toBe('{"event":"created"}');
  });

  it('retries a leader-unknown create with the same producer operation', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response('leader election in progress', { status: 503 })
      )
      .mockResolvedValueOnce(
        response(null, {
          status: 201,
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

    await expect(
      client.append(
        'run-1',
        { event: 'created' },
        {
          operationId: 'event-1',
          expectedRecord: 0,
          createIfMissing: true,
        }
      )
    ).resolves.toEqual({ startRecord: 0, nextRecord: 1 });

    expect(fetch).toHaveBeenCalledTimes(2);
    const first = fetch.mock.calls[0]?.[1];
    const second = fetch.mock.calls[1]?.[1];
    expect(first?.method).toBe('PUT');
    expect(second?.method).toBe('PUT');
    expect(first?.body).toBe(second?.body);
    expect(
      new Headers(first?.headers).get('producer-id')
    ).toBe(new Headers(second?.headers).get('producer-id'));
  });

  it('retries a leader-unknown read', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response('leader election in progress', { status: 503 })
      )
      .mockResolvedValueOnce(
        response('{"record":0,"value":{"event":"created"}}', {
          status: 200,
          headers: {
            'stream-record-next': '1',
            'stream-up-to-date': 'true',
          },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(client.read<{ event: string }>('run-1')).resolves.toEqual({
      records: [{ record: 0, value: { event: 'created' } }],
      nextRecord: 1,
      closed: false,
      upToDate: true,
    });

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(fetch.mock.calls[0]?.[0]).toEqual(fetch.mock.calls[1]?.[0]);
  });

  it('does not re-create a stream whose existence a read already proved', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response('{"record":0,"value":{"event":"created"}}', {
          status: 200,
          headers: {
            'stream-record-next': '1',
            'stream-up-to-date': 'true',
          },
        })
      )
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: {
            'stream-record-start': '1',
            'stream-record-next': '2',
          },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await client.read<{ event: string }>('run-1');
    await expect(
      client.append(
        'run-1',
        { event: 'started' },
        {
          operationId: 'event-2',
          expectedRecord: 1,
        }
      )
    ).resolves.toEqual({ startRecord: 1, nextRecord: 2 });

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(fetch.mock.calls[0]?.[1]?.method).toBeUndefined();
    expect(fetch.mock.calls[1]?.[1]?.method).toBe('POST');
  });

  it('falls back to guarded append when create finds an existing stream', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 200 }))
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
      { event: 'created' },
      {
        operationId: 'event-1',
        expectedRecord: 0,
        createIfMissing: true,
      }
    );

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(fetch.mock.calls[0]?.[1]?.method).toBe('PUT');
    expect(fetch.mock.calls[1]?.[1]?.method).toBe('POST');
    expect(
      new Headers(fetch.mock.calls[1]?.[1]?.headers).get('stream-record-match')
    ).toBe('0');
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

  it('retries a snapshot until the preceding append is visible', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response('InvalidRecordBoundaries', { status: 400 })
      )
      .mockResolvedValueOnce(
        response('InvalidRecordBoundaries', { status: 400 })
      )
      .mockResolvedValueOnce(response(null, { status: 204 }));
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      client.publishSnapshotAtRecord('run-checkpoint-1', 1, {
        sourceNextRecord: 128,
      })
    ).resolves.toBeUndefined();

    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it('does not retry a non-transient snapshot error', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response('invalid snapshot', { status: 400 }));
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });

    await expect(
      client.publishSnapshotAtRecord('run-checkpoint-1', 99, {})
    ).rejects.toMatchObject({ status: 400 });

    expect(fetch).toHaveBeenCalledOnce();
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

  it('places affinity between the bucket and local stream id', () => {
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      bucket: 'workflow',
    }).withAffinity('run-42');

    expect(client.streamUrl('queue main').pathname).toBe(
      '/workflow/run-42/queue%20main'
    );
  });

  it('shares known streams between clients for the same affinity', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response(null, {
          status: 200,
          headers: { 'stream-record-next': '4' },
        })
      )
      .mockResolvedValueOnce(response(null, { status: 201 }));
    const root = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      bucket: 'workflow',
      fetch,
    });

    await root.withAffinity('run-1').head('run');
    await root.withAffinity('run-1').ensureJsonStream('run');
    await root.withAffinity('run-2').ensureJsonStream('run');

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(String(fetch.mock.calls[1]?.[0])).toBe(
      'https://ursula.test/workflow/run-2/run'
    );
  });

  it('encodes guarded affinity appends as one group transaction', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(response(null, { status: 201 }))
      .mockResolvedValueOnce(
        response('{"acknowledgements":[]}', {
          status: 200,
          headers: {
            'stream-extensions': 'group-append-transaction-v1',
          },
        })
      );
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      bucket: 'workflow',
      fetch,
    }).withAffinity('run-1');

    await client.appendTransaction([
      {
        stream: 'run-run-1',
        values: { event: 'completed' },
        operationId: 'run-completed',
        expectedRecord: 7,
      },
      {
        stream: 'queue-test',
        values: { type: 'lease_extended' },
        operationId: 'queue-fenced',
        expectedRecord: 11,
      },
    ]);

    expect(fetch).toHaveBeenCalledTimes(3);
    expect(String(fetch.mock.calls[2]?.[0])).toBe(
      'https://ursula.test/workflow/run-1/$transaction'
    );
    const body = JSON.parse(String(fetch.mock.calls[2]?.[1]?.body)) as {
      operations: Array<{
        stream: string;
        record_match: number;
        payload_base64: string;
      }>;
    };
    expect(
      body.operations.map(({ stream, record_match }) => [
        stream,
        record_match,
      ])
    ).toEqual([
      ['run-run-1', 7],
      ['queue-test', 11],
    ]);
    expect(
      Buffer.from(
        body.operations[0]?.payload_base64 ?? '',
        'base64'
      ).toString()
    ).toBe('{"event":"completed"}');
  });

  it('delivers record-envelope SSE events in wire order', async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          new TextEncoder().encode(
            'event: data\ndata:{"record":4,"value":{"id":1}}\n\n'
          )
        );
        controller.enqueue(
          new TextEncoder().encode(
            'event: control\ndata:{"streamNextRecord":5}\n\n'
          )
        );
        controller.close();
      },
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(body, { status: 200 }));
    const client = new UrsulaClient({
      baseUrl: 'https://ursula.test',
      fetch,
    });
    const records: unknown[] = [];

    await client.watchRecords('queue', 4, (batch) => records.push(...batch));

    expect(records).toEqual([{ record: 4, value: { id: 1 } }]);
    const url = new URL(String(fetch.mock.calls[0]?.[0]));
    expect(url.searchParams.get('live')).toBe('sse');
    expect(url.searchParams.get('record')).toBe('4');
  });
});
