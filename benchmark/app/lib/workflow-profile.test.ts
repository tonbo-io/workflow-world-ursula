import { trace } from '@opentelemetry/api';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-node';
import { describe, expect, it } from 'vitest';
import {
  WorkflowProfile,
  installWorkflowProfileTracing,
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
  it('aggregates span duration, numeric attributes, and bounded categories', () => {
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

    const snapshot = profile.snapshot();
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

  it('resets all accumulated samples', () => {
    const profile = new WorkflowProfile();
    profile.record(span('workflow.loadEvents', [0, 5_000_000]));
    profile.reset();

    expect(profile.snapshot().spans).toEqual({});
  });

  it('captures spans from the registered OpenTelemetry provider', () => {
    const profile = installWorkflowProfileTracing();
    profile.reset();

    const otelSpan = trace
      .getTracer('workflow-benchmark-test')
      .startSpan('workflow.test');
    otelSpan.setAttribute('workflow.queue.overhead_ms', 4);
    otelSpan.end();

    expect(profile.snapshot().spans['workflow.test']).toBeDefined();
  });
});
