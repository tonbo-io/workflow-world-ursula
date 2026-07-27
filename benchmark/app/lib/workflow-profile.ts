import type { Context } from '@opentelemetry/api';
import {
  NodeTracerProvider,
  type ReadableSpan,
  type Span,
  type SpanProcessor,
} from '@opentelemetry/sdk-trace-node';
import { Session } from 'node:inspector/promises';
import type { Profiler } from 'node:inspector';

const PROFILE_GLOBAL = '__workflowBenchmarkProfile';
const PROVIDER_GLOBAL = '__workflowBenchmarkTracerProvider';
const MAX_SAMPLES_PER_MEASUREMENT = 200_000;
const CPU_PROFILE_TOP_FRAMES = 50;
const DEFAULT_CPU_PROFILE_INTERVAL_US = 5_000;

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
  cpu?: CpuProfileSummary;
  enabled: boolean;
  pid: number;
  spans: Record<string, SpanProfile>;
  startedAt: string;
}

export interface CpuProfileFrame {
  columnNumber: number;
  functionName: string;
  lineNumber: number;
  selfSamples: number;
  selfTimeMs: number;
  url: string;
}

export interface CpuProfileSummary {
  intervalMicros: number;
  sampledTimeMs: number;
  startedAt: string;
  stoppedAt: string;
  topSelf: CpuProfileFrame[];
  totalSamples: number;
  wallTimeMs: number;
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

function cpuProfileIntervalMicros(): number {
  const value = Number(process.env.WORKFLOW_BENCH_CPU_PROFILE_INTERVAL_US);
  if (!Number.isSafeInteger(value) || value < 100) {
    return DEFAULT_CPU_PROFILE_INTERVAL_US;
  }
  return value;
}

export function summarizeCpuProfile(
  profile: Profiler.Profile,
  intervalMicros: number,
  startedAt: Date,
  stoppedAt: Date
): CpuProfileSummary {
  const nodes = new Map(profile.nodes.map((node) => [node.id, node]));
  const frameSamples = new Map<
    number,
    { selfSamples: number; selfTimeMicros: number }
  >();
  const samples = profile.samples ?? [];
  const timeDeltas = profile.timeDeltas ?? [];

  for (const [index, nodeId] of samples.entries()) {
    const current = frameSamples.get(nodeId) ?? {
      selfSamples: 0,
      selfTimeMicros: 0,
    };
    current.selfSamples += 1;
    current.selfTimeMicros += timeDeltas[index] ?? intervalMicros;
    frameSamples.set(nodeId, current);
  }

  const topSelf = [...frameSamples.entries()]
    .map(([nodeId, measurement]) => {
      const callFrame = nodes.get(nodeId)?.callFrame;
      return {
        columnNumber: callFrame?.columnNumber ?? 0,
        functionName: callFrame?.functionName || '(anonymous)',
        lineNumber: callFrame?.lineNumber ?? 0,
        selfSamples: measurement.selfSamples,
        selfTimeMs: measurement.selfTimeMicros / 1_000,
        url: callFrame?.url ?? '',
      };
    })
    .sort(
      (left, right) =>
        right.selfTimeMs - left.selfTimeMs ||
        right.selfSamples - left.selfSamples
    )
    .slice(0, CPU_PROFILE_TOP_FRAMES);

  return {
    intervalMicros,
    sampledTimeMs:
      [...frameSamples.values()].reduce(
        (sum, measurement) => sum + measurement.selfTimeMicros,
        0
      ) / 1_000,
    startedAt: startedAt.toISOString(),
    stoppedAt: stoppedAt.toISOString(),
    topSelf,
    totalSamples: samples.length,
    wallTimeMs: (profile.endTime - profile.startTime) / 1_000,
  };
}

export class WorkflowProfile {
  private cpuProfile?: CpuProfileSummary;
  private cpuProfileInterval = DEFAULT_CPU_PROFILE_INTERVAL_US;
  private cpuProfileSession?: Session;
  private cpuProfileStartedAt?: Date;
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

  async reset(): Promise<void> {
    await this.stopCpuProfile();
    this.spans.clear();
    this.startedAt = new Date();
    this.cpuProfile = undefined;
    this.cpuProfileInterval = cpuProfileIntervalMicros();
    this.cpuProfileStartedAt = new Date();
    const session = new Session();
    session.connect();
    await session.post('Profiler.enable');
    await session.post('Profiler.setSamplingInterval', {
      interval: this.cpuProfileInterval,
    });
    await session.post('Profiler.start');
    this.cpuProfileSession = session;
  }

  async snapshot(): Promise<WorkflowProfileSnapshot> {
    await this.stopCpuProfile();
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
      cpu: this.cpuProfile,
      enabled: true,
      pid: process.pid,
      spans,
      startedAt: this.startedAt.toISOString(),
    };
  }

  private async stopCpuProfile(): Promise<void> {
    const session = this.cpuProfileSession;
    const startedAt = this.cpuProfileStartedAt;
    if (!session || !startedAt) return;
    this.cpuProfileSession = undefined;
    this.cpuProfileStartedAt = undefined;
    try {
      const { profile } = await session.post('Profiler.stop');
      this.cpuProfile = summarizeCpuProfile(
        profile,
        this.cpuProfileInterval,
        startedAt,
        new Date()
      );
      await session.post('Profiler.disable');
    } finally {
      session.disconnect();
    }
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
