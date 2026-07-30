import { beforeAll, describe, expect, it } from 'vitest';
import { UrsulaClient } from '../src/client.js';
import { RunJournal } from '../src/run-journal.js';
import { createStorage } from '../src/storage.js';

const baseUrl = process.env.WORKFLOW_URSULA_URL;
const bucket = process.env.WORKFLOW_URSULA_BUCKET ?? 'workflow-wasm-e2e';
const moduleId =
  process.env.WORKFLOW_URSULA_EXPERIMENTAL_SERVER_REDUCER_MODULE_ID;

describe.skipIf(!baseUrl || !moduleId)('Ursula WebAssembly run reducer', () => {
  const client = new UrsulaClient({
    baseUrl: baseUrl ?? '',
    bucket,
  });
  const journal = new RunJournal(client);
  const { storage } = createStorage(client, {
    journal,
    serverReducerModuleId: moduleId,
  });

  beforeAll(async () => {
    await client.append(
      '__wasm_reducer_bootstrap',
      { ready: true },
      {
        operationId: 'wasm-reducer-bootstrap',
        expectedRecord: 0,
        createIfMissing: true,
      }
    );
  });

  it('runs a complete run and lazy step through one reducer request per transition', async () => {
    const runId = `wrun_wasm_${Date.now()}`;
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'deployment',
        workflowName: 'wasm-e2e',
        input: { prompt: 'hello' },
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_started',
      eventData: {},
    });
    const started = await storage.events.create(runId, {
      eventType: 'step_started',
      correlationId: 'step-1',
      eventData: {
        stepName: 'answer',
        input: { prompt: 'hello' },
        ownerMessageId: 'message-1',
        workflowName: 'wasm-e2e',
      },
    });
    expect(started.stepCreated).toBe(true);
    await storage.events.create(runId, {
      eventType: 'step_completed',
      correlationId: 'step-1',
      eventData: {
        stepName: 'answer',
        workflowName: 'wasm-e2e',
        result: { text: 'hello' },
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: {
        output: { text: 'hello' },
      },
    });

    await expect(storage.runs.get(runId)).resolves.toMatchObject({
      runId,
      status: 'completed',
      output: { text: 'hello' },
    });
    await expect(storage.steps.get(runId, 'step-1')).resolves.toMatchObject({
      runId,
      stepId: 'step-1',
      status: 'completed',
      output: { text: 'hello' },
    });
    const events = await storage.events.list({
      runId,
      pagination: { limit: 100 },
    });
    expect(events.data.map(({ eventType }) => eventType)).toEqual([
      'run_created',
      'run_started',
      'step_created',
      'step_started',
      'step_completed',
      'run_completed',
    ]);
  });
});
