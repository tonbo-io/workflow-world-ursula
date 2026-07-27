import { resolve4 } from 'node:dns/promises';

interface Distribution {
  avg: number;
  count: number;
  max: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  sum: number;
}

interface SpanProfile {
  attributes: Record<string, Distribution>;
  categories: Record<string, Record<string, number>>;
  durationMs: Distribution;
}

interface WorkflowProfileSnapshot {
  cpu?: {
    intervalMicros: number;
    sampledTimeMs: number;
    startedAt: string;
    stoppedAt: string;
    topSelf: Array<{
      columnNumber: number;
      functionName: string;
      lineNumber: number;
      selfSamples: number;
      selfTimeMs: number;
      url: string;
    }>;
    totalSamples: number;
    wallTimeMs: number;
  };
  enabled: boolean;
  pid: number;
  spans: Record<string, SpanProfile>;
  startedAt: string;
}

export interface WorkflowProfileCollection {
  capturedAt: string;
  targets: Array<{
    address: string;
    error?: string;
    snapshot?: WorkflowProfileSnapshot;
  }>;
}

async function profileTargets(): Promise<URL[]> {
  const rawUrl = process.env.WORKFLOW_BENCH_PROFILE_URL;
  if (!rawUrl) return [];
  const source = new URL(rawUrl);
  let addresses: string[];
  try {
    addresses = await resolve4(source.hostname);
  } catch {
    addresses = [source.hostname];
  }
  return [...new Set(addresses)].sort().map((address) => {
    const target = new URL(source);
    target.hostname = address;
    return target;
  });
}

async function requestProfiles(
  method: 'DELETE' | 'GET'
): Promise<WorkflowProfileCollection | undefined> {
  const targets = await profileTargets();
  if (targets.length === 0) return undefined;
  return {
    capturedAt: new Date().toISOString(),
    targets: await Promise.all(
      targets.map(async (url) => {
        try {
          const response = await fetch(url, { method });
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          return {
            address: url.hostname,
            snapshot:
              method === 'GET'
                ? ((await response.json()) as WorkflowProfileSnapshot)
                : undefined,
          };
        } catch (error) {
          return {
            address: url.hostname,
            error: error instanceof Error ? error.message : String(error),
          };
        }
      })
    ),
  };
}

export async function captureWorkflowProfiles(): Promise<
  WorkflowProfileCollection | undefined
> {
  return requestProfiles('GET');
}

export async function resetWorkflowProfiles(): Promise<void> {
  const result = await requestProfiles('DELETE');
  if (!result) return;
  const failures = result.targets.filter((target) => target.error);
  if (failures.length > 0) {
    console.warn(
      `[bench] Failed to reset ${failures.length}/${result.targets.length} workflow profiles: ${failures
        .map((failure) => `${failure.address}: ${failure.error}`)
        .join('; ')}`
    );
  }
}
