import {
  disabledWorkflowProfileSnapshot,
  getWorkflowProfile,
} from '../../../../lib/workflow-profile';

export const dynamic = 'force-dynamic';

export async function GET(): Promise<Response> {
  const profile = getWorkflowProfile();
  return Response.json(
    profile ? await profile.snapshot() : disabledWorkflowProfileSnapshot()
  );
}

export async function DELETE(): Promise<Response> {
  const profile = getWorkflowProfile();
  await profile?.reset();
  return Response.json({
    enabled: profile !== undefined,
    reset: true,
  });
}
