export async function register() {
  if (process.env.NEXT_RUNTIME !== 'nodejs') return;
  if (process.env.WORKFLOW_BENCH_PROFILE === '1') {
    const { installWorkflowProfileTracing } = await import(
      './lib/workflow-profile'
    );
    installWorkflowProfileTracing();
  }
  const { getWorld } = await import('workflow/runtime');
  const world = await getWorld();
  await world.start?.();
}
