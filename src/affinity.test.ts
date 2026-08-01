import { describe, expect, it } from 'vitest';
import { runAffinity } from './affinity.js';

describe('run affinity', () => {
  it('is stable and bounded by the configured lane count', () => {
    const lanes = new Set(
      Array.from({ length: 1_000 }, (_, index) =>
        runAffinity(`wrun_${index}`, 8)
      )
    );

    expect(runAffinity('wrun_42', 8)).toBe(runAffinity('wrun_42', 8));
    expect(lanes.size).toBe(8);
    expect([...lanes]).toEqual(
      expect.arrayContaining(
        Array.from(
          { length: 8 },
          (_, lane) => `workflow-lane-${lane.toString().padStart(4, '0')}`
        )
      )
    );
  });

  it('rejects an invalid lane count', () => {
    expect(() => runAffinity('wrun_42', 0)).toThrow(
      'Ursula run affinity lane count must be a positive integer'
    );
  });
});
