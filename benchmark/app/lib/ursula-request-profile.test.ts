import { afterEach, describe, expect, it } from 'vitest';
import {
  classifyUrsulaRequest,
  UrsulaRequestProfile,
} from './ursula-request-profile';

const previousUrl = process.env.WORKFLOW_URSULA_URL;

afterEach(() => {
  if (previousUrl === undefined) {
    delete process.env.WORKFLOW_URSULA_URL;
  } else {
    process.env.WORKFLOW_URSULA_URL = previousUrl;
  }
});

describe('UrsulaRequestProfile', () => {
  it('aggregates bounded operation keys without retaining stream ids', () => {
    const profile = new UrsulaRequestProfile();
    profile.record(
      {
        bodyBytes: 10,
        method: 'POST',
        operation: 'append',
        streamKind: 'run_journal',
      },
      4,
      '200'
    );
    profile.record(
      {
        bodyBytes: 20,
        method: 'POST',
        operation: 'append',
        streamKind: 'run_journal',
      },
      8,
      '200'
    );

    expect(profile.snapshot().requests['run_journal.append']).toMatchObject({
      durationMs: { avg: 6, count: 2, sum: 12 },
      requestBytes: { avg: 15, count: 2, sum: 30 },
      statuses: { '200': 2 },
    });
  });
});

describe('classifyUrsulaRequest', () => {
  it('classifies journal and queue requests without retaining stream ids', () => {
    process.env.WORKFLOW_URSULA_URL = 'http://ursula:4437';

    expect(
      classifyUrsulaRequest('http://ursula:4437/workflow/run-abc', {
        method: 'POST',
        body: '{"v":1}',
      })
    ).toMatchObject({
      bodyBytes: 7,
      method: 'POST',
      operation: 'append',
      streamKind: 'run_journal',
    });
    expect(
      classifyUrsulaRequest(
        'http://ursula:4437/workflow/queue-checkpoint-abc-p000?record=-1'
      )
    ).toMatchObject({
      method: 'GET',
      operation: 'read',
      streamKind: 'queue_checkpoint',
    });
    expect(
      classifyUrsulaRequest('http://ursula:4437/workflow/queue-abc-p000', {
        method: 'POST',
        body: JSON.stringify([
          { version: 1, type: 'acked' },
          { version: 1, type: 'leased' },
        ]),
      })
    ).toMatchObject({
      operation: 'append_acked_leased',
      streamKind: 'queue_journal',
    });
  });

  it('distinguishes live reads, close-only writes, and stream registries', () => {
    process.env.WORKFLOW_URSULA_URL = 'http://ursula:4437';

    expect(
      classifyUrsulaRequest(
        'http://ursula:4437/workflow/__workflow_streams?live=long-poll'
      )
    ).toMatchObject({
      operation: 'live_read',
      streamKind: 'stream_registry',
    });
    expect(
      classifyUrsulaRequest('http://ursula:4437/workflow/wrun-stream', {
        method: 'POST',
        headers: { 'stream-closed': 'true' },
      })
    ).toMatchObject({
      bodyBytes: 0,
      operation: 'close',
      streamKind: 'chunk_stream',
    });
  });

  it('ignores non-Ursula and admin requests', () => {
    process.env.WORKFLOW_URSULA_URL = 'http://ursula:4437';

    expect(
      classifyUrsulaRequest('http://workflow-app:3000/api/health')
    ).toBeUndefined();
    expect(
      classifyUrsulaRequest('http://ursula:4437/__ursula/metrics')
    ).toBeUndefined();
  });
});
