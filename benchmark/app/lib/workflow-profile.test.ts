import { trace } from '@opentelemetry/api';
import type { Profiler } from 'node:inspector';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-node';
import { describe, expect, it } from 'vitest';
import {
  WorkflowProfile,
  installWorkflowProfileTracing,
  summarizeCpuProfile,
} from './workflow-profile';

function span(
  name: string,
  duration: [number, number],
  attributes: Record<string, string | number> = {}
): ReadableSpan {
  return {
    attributes,
    duration,
    name,
  } as unknown as ReadableSpan;
}

describe('WorkflowProfile', () => {
  it('aggregates span duration, numeric attributes, and bounded categories', async () => {
    const profile = new WorkflowProfile();
    profile.record(
      span('step.execute', [0, 1_000_000], {
        'step.stso_ms': 7,
        'workflow.run.id': 'run-1',
        'step.status': 'completed',
      })
    );
    profile.record(
      span('step.execute', [0, 3_000_000], {
        'step.stso_ms': 11,
        'workflow.run.id': 'run-2',
        'step.status': 'completed',
      })
    );

    const snapshot = await profile.snapshot();
    expect(snapshot.spans['step.execute']?.durationMs).toMatchObject({
      avg: 2,
      count: 2,
      p50: 1,
      p95: 3,
      p99: 3,
    });
    expect(
      snapshot.spans['step.execute']?.attributes['step.stso_ms']
    ).toMatchObject({
      avg: 9,
      count: 2,
      p50: 7,
      p99: 11,
    });
    expect(snapshot.spans['step.execute']?.categories).toEqual({
      'step.status': { completed: 2 },
    });
  });

  it('resets all accumulated samples', async () => {
    const profile = new WorkflowProfile();
    profile.record(span('workflow.loadEvents', [0, 5_000_000]));
    await profile.reset();

    expect((await profile.snapshot()).spans).toEqual({});
  });

  it('captures spans from the registered OpenTelemetry provider', async () => {
    const profile = installWorkflowProfileTracing();
    await profile.reset();

    const otelSpan = trace
      .getTracer('workflow-benchmark-test')
      .startSpan('workflow.test');
    otelSpan.setAttribute('workflow.queue.overhead_ms', 4);
    otelSpan.end();

    expect((await profile.snapshot()).spans['workflow.test']).toBeDefined();
  });

  it('summarizes V8 samples by self time without retaining the full profile', () => {
    const profile = {
      endTime: 30_000,
      nodes: [
        {
          callFrame: {
            columnNumber: 3,
            functionName: 'replay',
            lineNumber: 41,
            scriptId: '1',
            url: 'file:///runtime.js',
          },
          hitCount: 2,
          id: 1,
        },
        {
          callFrame: {
            columnNumber: 7,
            functionName: 'append',
            lineNumber: 11,
            scriptId: '2',
            url: 'file:///world.js',
          },
          hitCount: 1,
          id: 2,
        },
      ],
      samples: [1, 2, 1],
      startTime: 0,
      timeDeltas: [5_000, 10_000, 5_000],
    } satisfies Profiler.Profile;

    const summary = summarizeCpuProfile(
      profile,
      5_000,
      new Date('2026-07-27T00:00:00Z'),
      new Date('2026-07-27T00:00:01Z')
    );

    expect(summary).toMatchObject({
      sampledTimeMs: 20,
      totalSamples: 3,
      wallTimeMs: 30,
    });
    expect(summary.topSelf).toEqual([
      expect.objectContaining({
        functionName: 'replay',
        selfSamples: 2,
        selfTimeMs: 10,
      }),
      expect.objectContaining({
        functionName: 'append',
        selfSamples: 1,
        selfTimeMs: 10,
      }),
    ]);
  });
});
