import {
  disabledWorkflowProfileSnapshot,
  getWorkflowProfile,
} from '../../../../lib/workflow-profile';
import { getUrsulaRequestProfile } from '../../../../lib/ursula-request-profile';

export const dynamic = 'force-dynamic';

export async function GET(): Promise<Response> {
  const profile = getWorkflowProfile();
  const snapshot = profile
    ? await profile.snapshot()
    : disabledWorkflowProfileSnapshot();
  return Response.json({
    ...snapshot,
    ursulaRequests: getUrsulaRequestProfile()?.snapshot().requests ?? {},
  });
}

export async function DELETE(): Promise<Response> {
  const profile = getWorkflowProfile();
  await profile?.reset();
  getUrsulaRequestProfile()?.reset();
  return Response.json({
    enabled: profile !== undefined,
    reset: true,
  });
}
