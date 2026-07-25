import { getRun } from 'workflow/api';

interface RouteContext {
  params: Promise<{ runId: string }>;
}

export async function GET(_request: Request, context: RouteContext) {
  const { runId } = await context.params;
  try {
    const run = await getRun(runId);
    const status = await run.status;
    if (status !== 'completed') {
      if (status === 'failed' || status === 'cancelled') {
        await run.returnValue;
      }
      return Response.json({ status }, { status: 202 });
    }
    return Response.json({ status, returnValue: await run.returnValue });
  } catch (error) {
    return Response.json(
      {
        error: 'Failed to read benchmark workflow',
        details: error instanceof Error ? error.message : String(error),
      },
      { status: 500 }
    );
  }
}
