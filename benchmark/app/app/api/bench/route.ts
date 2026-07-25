import { start } from 'workflow/api';
import {
  benchHookStreamWorkflow,
  benchSequentialStepsWorkflow,
  benchSlWorkflow,
  benchSoWorkflow,
  benchStepWorkflow,
  benchStreamCatchupWorkflow,
  benchStreamWorkflow,
} from '../../../workflows/97_bench';

const workflows = {
  benchHookStreamWorkflow,
  benchSequentialStepsWorkflow,
  benchSlWorkflow,
  benchSoWorkflow,
  benchStepWorkflow,
  benchStreamCatchupWorkflow,
  benchStreamWorkflow,
} as const;

export async function POST(request: Request) {
  let body: { workflowFn?: string; args?: unknown[] };
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return Response.json({ error: 'Invalid JSON body' }, { status: 400 });
  }
  if (!body.workflowFn) {
    return Response.json(
      { error: '`workflowFn` is required' },
      { status: 400 }
    );
  }
  const workflow = workflows[body.workflowFn as keyof typeof workflows];
  if (!workflow) {
    return Response.json(
      { error: `Benchmark workflow "${body.workflowFn}" not found` },
      { status: 404 }
    );
  }
  try {
    const clientStart = Date.now();
    // The benchmark intentionally dispatches functions with different
    // signatures through one endpoint.
    const run = await start(
      workflow as (...args: unknown[]) => Promise<unknown>,
      body.args ?? []
    );
    return Response.json({ runId: run.runId, clientStart });
  } catch (error) {
    return Response.json(
      {
        error: 'Failed to start benchmark workflow',
        details: error instanceof Error ? error.message : String(error),
      },
      { status: 500 }
    );
  }
}
