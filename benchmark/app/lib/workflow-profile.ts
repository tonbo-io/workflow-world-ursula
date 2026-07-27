import type { Context } from '@opentelemetry/api';
import {
  NodeTracerProvider,
  type ReadableSpan,
  type Span,
  type SpanProcessor,
} from '@opentelemetry/sdk-trace-node';

const PROFILE_GLOBAL = '__workflowBenchmarkProfile';
const PROVIDER_GLOBAL = '__workflowBenchmarkTracerProvider';
const MAX_SAMPLES_PER_MEASUREMENT = 200_000;

const CATEGORICAL_ATTRIBUTES = new Set([
  'error.type',
  'http.request.method',
  'http.response.status_code',
  'http.route',
  'messaging.destination.name',
  'messaging.operation.type',
  'step.status',
  'workflow.operation',
  'workflow.route.type',
  'workflow.run.status',
  'workflow.world.parse.format',
]);

const SPAN_PREFIXES = ['hook.', 'queue.', 'step.', 'workflow.', 'world.'];

interface Measurement {
  count: number;
  max: number;
  min: number;
  samples: number[];
  sum: number;
}

interface SpanMeasurements {
  attributes: Map<string, Measurement>;
  categories: Map<string, Map<string, number>>;
  durationMs: Measurement;
}

interface ProfileGlobal {
  [PROFILE_GLOBAL]?: WorkflowProfile;
  [PROVIDER_GLOBAL]?: NodeTracerProvider;
}

export interface Distribution {
  avg: number;
  count: number;
  max: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  sum: number;
}

export interface SpanProfile {
  attributes: Record<string, Distribution>;
  categories: Record<string, Record<string, number>>;
  durationMs: Distribution;
}

export interface WorkflowProfileSnapshot {
  enabled: boolean;
  pid: number;
  spans: Record<string, SpanProfile>;
  startedAt: string;
}

function newMeasurement(): Measurement {
  return {
    count: 0,
    max: Number.NEGATIVE_INFINITY,
    min: Number.POSITIVE_INFINITY,
    samples: [],
    sum: 0,
  };
}

function recordMeasurement(measurement: Measurement, value: number): void {
  if (!Number.isFinite(value)) return;
  measurement.count += 1;
  measurement.sum += value;
  measurement.min = Math.min(measurement.min, value);
  measurement.max = Math.max(measurement.max, value);
  if (measurement.samples.length < MAX_SAMPLES_PER_MEASUREMENT) {
    measurement.samples.push(value);
  }
}

function quantile(sorted: number[], percentile: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.ceil(percentile * sorted.length) - 1;
  return sorted[Math.max(0, index)] ?? 0;
}

function summarize(measurement: Measurement): Distribution {
  if (measurement.count === 0) {
    return { avg: 0, count: 0, max: 0, min: 0, p50: 0, p95: 0, p99: 0, sum: 0 };
  }
  const sorted = measurement.samples.toSorted((a, b) => a - b);
  return {
    avg: measurement.sum / measurement.count,
    count: measurement.count,
    max: measurement.max,
    min: measurement.min,
    p50: quantile(sorted, 0.5),
    p95: quantile(sorted, 0.95),
    p99: quantile(sorted, 0.99),
    sum: measurement.sum,
  };
}

function durationMs(span: ReadableSpan): number {
  return span.duration[0] * 1_000 + span.duration[1] / 1_000_000;
}

export class WorkflowProfile {
  private spans = new Map<string, SpanMeasurements>();
  private startedAt = new Date();

  record(span: ReadableSpan): void {
    if (!SPAN_PREFIXES.some((prefix) => span.name.startsWith(prefix))) return;
    let profile = this.spans.get(span.name);
    if (!profile) {
      profile = {
        attributes: new Map(),
        categories: new Map(),
        durationMs: newMeasurement(),
      };
      this.spans.set(span.name, profile);
    }
    recordMeasurement(profile.durationMs, durationMs(span));

    for (const [name, value] of Object.entries(span.attributes)) {
      if (typeof value === 'number') {
        let measurement = profile.attributes.get(name);
        if (!measurement) {
          measurement = newMeasurement();
          profile.attributes.set(name, measurement);
        }
        recordMeasurement(measurement, value);
      }
      if (!CATEGORICAL_ATTRIBUTES.has(name)) continue;
      if (typeof value !== 'string' && typeof value !== 'number') continue;
      let categories = profile.categories.get(name);
      if (!categories) {
        categories = new Map();
        profile.categories.set(name, categories);
      }
      const category = String(value);
      categories.set(category, (categories.get(category) ?? 0) + 1);
    }
  }

  reset(): void {
    this.spans.clear();
    this.startedAt = new Date();
  }

  snapshot(): WorkflowProfileSnapshot {
    const spans: Record<string, SpanProfile> = {};
    for (const [name, profile] of [...this.spans].sort(([a], [b]) =>
      a.localeCompare(b)
    )) {
      const attributes = Object.fromEntries(
        [...profile.attributes]
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([attribute, measurement]) => [
            attribute,
            summarize(measurement),
          ])
      );
      const categories = Object.fromEntries(
        [...profile.categories]
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([attribute, counts]) => [
            attribute,
            Object.fromEntries(
              [...counts].sort(([a], [b]) => a.localeCompare(b))
            ),
          ])
      );
      spans[name] = {
        attributes,
        categories,
        durationMs: summarize(profile.durationMs),
      };
    }
    return {
      enabled: true,
      pid: process.pid,
      spans,
      startedAt: this.startedAt.toISOString(),
    };
  }
}

class ProfileSpanProcessor implements SpanProcessor {
  constructor(private readonly profile: WorkflowProfile) {}

  onStart(_span: Span, _parentContext: Context): void {}

  onEnd(span: ReadableSpan): void {
    this.profile.record(span);
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

function profileGlobal(): typeof globalThis & ProfileGlobal {
  return globalThis as typeof globalThis & ProfileGlobal;
}

export function installWorkflowProfileTracing(): WorkflowProfile {
  const state = profileGlobal();
  if (state[PROFILE_GLOBAL]) return state[PROFILE_GLOBAL];

  const profile = new WorkflowProfile();
  const provider = new NodeTracerProvider({
    spanProcessors: [new ProfileSpanProcessor(profile)],
  });
  provider.register();
  state[PROFILE_GLOBAL] = profile;
  state[PROVIDER_GLOBAL] = provider;
  return profile;
}

export function getWorkflowProfile(): WorkflowProfile | undefined {
  return profileGlobal()[PROFILE_GLOBAL];
}

export function disabledWorkflowProfileSnapshot(): WorkflowProfileSnapshot {
  return {
    enabled: false,
    pid: process.pid,
    spans: {},
    startedAt: new Date().toISOString(),
  };
}
