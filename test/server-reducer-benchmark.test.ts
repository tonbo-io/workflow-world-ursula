import { performance } from 'node:perf_hooks';
import { describe, it } from 'vitest';
import { UrsulaClient } from '../src/client.js';
import { RunJournal } from '../src/run-journal.js';
import { createStorage } from '../src/storage.js';

const baseUrl = process.env.WORKFLOW_URSULA_URL;
const moduleId =
  process.env.WORKFLOW_URSULA_EXPERIMENTAL_SERVER_REDUCER_MODULE_ID;
const enabled =
  Boolean(baseUrl && moduleId) &&
  process.env.WORKFLOW_URSULA_REDUCER_BENCH === '1';

async function runTransitions(
  bucket: string,
  reducerModuleId: string | undefined,
  iterations: number
): Promise<number[]> {
  const client = new UrsulaClient({ baseUrl: baseUrl ?? '', bucket });
  const journal = new RunJournal(client);
  const { storage } = createStorage(client, {
    journal,
    serverReducerModuleId: reducerModuleId,
  });
  await client.append(
    '__bootstrap',
    { ready: true },
    {
      operationId: `bootstrap:${bucket}`,
      expectedRecord: 0,
      createIfMissing: true,
    }
  );
  const samples: number[] = [];
  for (let index = 0; index < iterations; index += 1) {
    const runId = `wrun_${bucket}_${Date.now()}_${index}`;
    const startedAt = performance.now();
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'deployment',
        workflowName: 'wasm-bench',
        input: { prompt: 'hello' },
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_started',
      eventData: {},
    });
    await storage.events.create(runId, {
      eventType: 'step_started',
      correlationId: 'step-1',
      eventData: {
        stepName: 'answer',
        input: { prompt: 'hello' },
        ownerMessageId: 'message-1',
        workflowName: 'wasm-bench',
      },
    });
    await storage.events.create(runId, {
      eventType: 'step_completed',
      correlationId: 'step-1',
      eventData: {
        stepName: 'answer',
        workflowName: 'wasm-bench',
        result: { text: 'hello' },
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: { text: 'hello' } },
    });
    samples.push(performance.now() - startedAt);
  }
  return samples;
}

function summary(samples: number[]): Record<string, number> {
  const sorted = [...samples].sort((left, right) => left - right);
  const percentile = (fraction: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ??
    0;
  return {
    averageMs:
      samples.reduce((total, sample) => total + sample, 0) / samples.length,
    p50Ms: percentile(0.5),
    p99Ms: percentile(0.99),
  };
}

describe.skipIf(!enabled)('server reducer micro benchmark', () => {
  it('compares five real World transitions with and without server reduction', async () => {
    const iterations = Number(
      process.env.WORKFLOW_URSULA_REDUCER_BENCH_ITERATIONS ?? 100
    );
    await runTransitions('workflow-control-warmup', undefined, 5);
    await runTransitions('workflow-wasm-warmup', moduleId, 5);
    const control = await runTransitions(
      `workflow-control-${Date.now()}`,
      undefined,
      iterations
    );
    const reducer = await runTransitions(
      `workflow-wasm-${Date.now()}`,
      moduleId,
      iterations
    );
    console.log(
      JSON.stringify(
        {
          iterations,
          transitionCount: 5,
          control: summary(control),
          reducer: summary(reducer),
        },
        null,
        2
      )
    );
  });
});
