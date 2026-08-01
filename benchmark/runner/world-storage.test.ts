/**
 * Fair World-storage isolation benchmark.
 *
 * Both backends receive the exact same public World API sequence:
 *
 *   step_started -> step_completed
 *
 * Runs are created and started outside the timed window. This removes
 * Workflow replay, user-code execution, queue dispatch, and application HTTP
 * routing while retaining each World's real event validation, entity
 * materialization, transport, optimistic concurrency, and durable commit.
 */

import fs from 'node:fs';
import path from 'node:path';
import type { World } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { ulid } from 'ulid';
import { afterAll, test } from 'vitest';
import {
  type BackendMetricsSnapshot,
  captureBackendMetrics,
  deriveBackendMetrics,
  diffBackendMetrics,
} from './backend-metrics.js';

type BackendKind = 'ursula' | 'postgres';

interface Distribution {
  count: number;
  min: number;
  avg: number;
  p50: number;
  p75: number;
  p90: number;
  p95: number;
  p99: number;
  max: number;
}

interface IterationResult {
  elapsedMs: number;
  logicalSteps: number;
  mutations: number;
  stepsPerSecond: number;
  mutationsPerSecond: number;
  stepLatencyMs: number[];
  mutationLatencyMs: number[];
}

interface PreparedIteration {
  runIds: string[];
}

interface LevelResult {
  concurrentRuns: number;
  stepsPerRun: number;
  iterations: number;
  logicalSteps: number;
  elapsedMs: Distribution;
  stepsPerSecond: Distribution;
  mutationsPerSecond: Distribution;
  stepLatencyMs: Distribution;
  mutationLatencyMs: Distribution;
  backendUsage?: {
    before: BackendMetricsSnapshot;
    after: BackendMetricsSnapshot;
    delta: Record<string, number>;
    derived?: Record<string, number>;
  };
}

function envInt(name: string, fallback: number, min = 1): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(parsed) || parsed < min) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return parsed;
}

function envLevels(name: string, fallback: number[]): number[] {
  const raw = process.env[name];
  if (!raw) return fallback;
  const levels = raw
    .split(',')
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isSafeInteger(value) && value > 0);
  if (levels.length === 0) throw new Error(`Invalid ${name}: ${raw}`);
  return levels;
}

function percentile(sorted: readonly number[], fraction: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * fraction) - 1)
  );
  return sorted[index] ?? 0;
}

function distribution(values: readonly number[]): Distribution {
  const sorted = [...values].sort((left, right) => left - right);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    min: sorted[0] ?? 0,
    avg: sorted.length === 0 ? 0 : total / sorted.length,
    p50: percentile(sorted, 0.5),
    p75: percentile(sorted, 0.75),
    p90: percentile(sorted, 0.9),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted.at(-1) ?? 0,
  };
}

function elapsedMs(start: bigint): number {
  return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

async function createBenchmarkWorld(): Promise<{
  backend: BackendKind;
  world: World;
}> {
  const backend = process.env.WORKFLOW_BENCH_BACKEND?.startsWith('postgres')
    ? 'postgres'
    : 'ursula';
  if (backend === 'postgres') {
    const { createWorld } = await import('@workflow/world-postgres');
    return {
      backend,
      world: createWorld({
        connectionString:
          process.env.WORKFLOW_POSTGRES_URL ?? required('DATABASE_URL'),
        maxPoolSize: envInt('WORKFLOW_POSTGRES_MAX_POOL_SIZE', 128),
        queueConcurrency: 1,
      }),
    };
  }
  const { createWorld } = await import('@tonbo-io/world-ursula');
  return {
    backend,
    world: createWorld({
      baseUrl: required('WORKFLOW_URSULA_URL'),
      bucket: required('WORKFLOW_URSULA_BUCKET'),
      token: process.env.WORKFLOW_URSULA_TOKEN,
      deploymentId: 'world-storage-benchmark',
      // Preserve the public two-call contract for a fair comparison.
    }),
  };
}

async function createRun(world: World, runId: string): Promise<void> {
  await world.events.create(runId, {
    eventType: 'run_created',
    specVersion: SPEC_VERSION_CURRENT,
    eventData: {
      deploymentId: 'world-storage-benchmark',
      workflowName: 'workflow//benchmark//world-storage',
      input: Uint8Array.from([0]),
    },
  });
  await world.events.create(runId, {
    eventType: 'run_started',
    specVersion: SPEC_VERSION_CURRENT,
  });
}

async function executeStep(
  world: World,
  runId: string,
  stepId: string,
  mutationSamples: number[]
): Promise<number> {
  const stepStartedAt = process.hrtime.bigint();
  let mutationStartedAt = process.hrtime.bigint();
  await world.events.create(runId, {
    eventType: 'step_started',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: {
      stepName: 'step//benchmark//world-storage',
      workflowName: 'workflow//benchmark//world-storage',
      input: Uint8Array.from([1]),
      ownerMessageId: `msg_${runId}`,
    },
  });
  mutationSamples.push(elapsedMs(mutationStartedAt));

  mutationStartedAt = process.hrtime.bigint();
  await world.events.create(runId, {
    eventType: 'step_completed',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: {
      stepName: 'step//benchmark//world-storage',
      workflowName: 'workflow//benchmark//world-storage',
      result: Uint8Array.from([2]),
    },
  });
  mutationSamples.push(elapsedMs(mutationStartedAt));
  return elapsedMs(stepStartedAt);
}

async function prepareIteration(
  world: World,
  concurrentRuns: number
): Promise<PreparedIteration> {
  const runIds = Array.from(
    { length: concurrentRuns },
    () => `wrun_${ulid()}`
  );
  await Promise.all(runIds.map((runId) => createRun(world, runId)));
  return { runIds };
}

async function runIteration(
  world: World,
  prepared: PreparedIteration,
  stepsPerRun: number
): Promise<IterationResult> {
  const { runIds } = prepared;
  const mutationSamples: number[] = [];
  const stepSamples: number[] = [];
  const startedAt = process.hrtime.bigint();
  await Promise.all(
    runIds.map(async (runId) => {
      for (let step = 0; step < stepsPerRun; step += 1) {
        stepSamples.push(
          await executeStep(
            world,
            runId,
            `step_${ulid()}`,
            mutationSamples
          )
        );
      }
    })
  );
  const duration = elapsedMs(startedAt);
  const logicalSteps = runIds.length * stepsPerRun;
  const mutations = logicalSteps * 2;
  return {
    elapsedMs: duration,
    logicalSteps,
    mutations,
    stepsPerSecond: (logicalSteps * 1000) / duration,
    mutationsPerSecond: (mutations * 1000) / duration,
    stepLatencyMs: stepSamples,
    mutationLatencyMs: mutationSamples,
  };
}

const levels: LevelResult[] = [];
let output:
  | {
      schemaVersion: 1;
      kind: 'world-storage';
      backend: BackendKind;
      generatedAt: string;
      config: {
        concurrentRuns: number[];
        stepsPerRun: number;
        iterations: number;
      };
      levels: LevelResult[];
    }
  | undefined;
let worldToClose: World | undefined;

test(
  'fair World event storage scaling',
  { timeout: envInt('WORLD_STORAGE_TIMEOUT_MS', 900_000) },
  async () => {
    const concurrentRuns = envLevels(
      'WORLD_STORAGE_CONCURRENT_RUNS',
      [1, 8, 32, 128]
    );
    const stepsPerRun = envInt('WORLD_STORAGE_STEPS_PER_RUN', 50);
    const iterations = envInt('WORLD_STORAGE_ITERATIONS', 3);
    const { backend, world } = await createBenchmarkWorld();
    worldToClose = world;

    for (const concurrent of concurrentRuns) {
      // Build and warm the run state before capturing backend metrics. The
      // measured window therefore contains only the identical step mutations
      // below, not run registry or run-start setup.
      const prepared: PreparedIteration[] = [];
      for (let iteration = 0; iteration < iterations; iteration += 1) {
        prepared.push(await prepareIteration(world, concurrent));
      }
      const before = await captureBackendMetrics();
      const samples: IterationResult[] = [];
      for (const iteration of prepared) {
        samples.push(await runIteration(world, iteration, stepsPerRun));
      }
      const after = await captureBackendMetrics();
      const delta = diffBackendMetrics(before, after);
      const level: LevelResult = {
        concurrentRuns: concurrent,
        stepsPerRun,
        iterations,
        logicalSteps: concurrent * stepsPerRun * iterations,
        elapsedMs: distribution(samples.map((sample) => sample.elapsedMs)),
        stepsPerSecond: distribution(
          samples.map((sample) => sample.stepsPerSecond)
        ),
        mutationsPerSecond: distribution(
          samples.map((sample) => sample.mutationsPerSecond)
        ),
        stepLatencyMs: distribution(
          samples.flatMap((sample) => sample.stepLatencyMs)
        ),
        mutationLatencyMs: distribution(
          samples.flatMap((sample) => sample.mutationLatencyMs)
        ),
        ...(before && after && delta
          ? {
              backendUsage: {
                before,
                after,
                delta,
                derived: deriveBackendMetrics(delta),
              },
            }
          : {}),
      };
      levels.push(level);
      console.log(
        `[world-storage] ${backend} c${concurrent}: ${level.stepsPerSecond.avg.toFixed(1)} steps/s, mutation p50 ${level.mutationLatencyMs.p50.toFixed(2)}ms, p99 ${level.mutationLatencyMs.p99.toFixed(2)}ms`
      );
    }

    output = {
      schemaVersion: 1,
      kind: 'world-storage',
      backend,
      generatedAt: new Date().toISOString(),
      config: { concurrentRuns, stepsPerRun, iterations },
      levels,
    };
  }
);

afterAll(async () => {
  await worldToClose?.close?.();
  if (!output) return;
  const outputFile =
    process.env.WORLD_STORAGE_OUTPUT_FILE ??
    path.resolve(
      process.cwd(),
      `world-storage-${output.backend}-${Date.now()}.json`
    );
  fs.mkdirSync(path.dirname(outputFile), { recursive: true });
  fs.writeFileSync(outputFile, `${JSON.stringify(output, null, 2)}\n`);
  console.log(`WORLD_STORAGE_RESULT=${outputFile}`);
  console.log(
    `WORLD_STORAGE_RESULT_BASE64=${Buffer.from(JSON.stringify(output)).toString('base64')}`
  );
});
