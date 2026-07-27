import {
  disabledWorkflowProfileSnapshot,
  getWorkflowProfile,
} from '../../../../lib/workflow-profile';

export const dynamic = 'force-dynamic';

export async function GET() {
  return Response.json(
    getWorkflowProfile()?.snapshot() ?? disabledWorkflowProfileSnapshot()
  );
}

export async function DELETE() {
  const profile = getWorkflowProfile();
  profile?.reset();
  return Response.json({
    enabled: profile !== undefined,
    reset: true,
  });
}
