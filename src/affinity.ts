import { createHash } from 'node:crypto';

export const DEFAULT_RUN_AFFINITY_LANES = 8;

/**
 * Maps an unbounded run-id space onto a bounded set of Ursula affinity lanes.
 *
 * Run-owned streams remain logically isolated by their local stream ids, while
 * queue journals and their live watchers can be shared by every run in a lane.
 */
export function runAffinity(runId: string, laneCount: number): string {
  if (!Number.isSafeInteger(laneCount) || laneCount < 1) {
    throw new Error('Ursula run affinity lane count must be a positive integer');
  }
  const lane =
    createHash('sha256').update(runId).digest().readUInt32BE(0) % laneCount;
  return `workflow-lane-${lane.toString().padStart(4, '0')}`;
}
