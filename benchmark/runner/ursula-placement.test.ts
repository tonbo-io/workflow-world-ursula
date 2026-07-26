import { describe, expect, it } from 'vitest';
import {
  findStreamForGroup,
  raftGroupForStream,
} from './ursula-placement.js';

describe('Ursula benchmark placement', () => {
  it('matches the stable FNV placement for known stream identities', () => {
    expect(raftGroupForStream('agents', 'session-42', 64)).toBe(53);
    expect(raftGroupForStream('benchcmp', 'stream-1', 256)).toBe(254);
  });

  it('finds unique names in a requested group', () => {
    const first = findStreamForGroup('bench', 'run', 256, 17);
    const second = findStreamForGroup(
      'bench',
      'run',
      256,
      17,
      first.nextNonce
    );

    expect(raftGroupForStream('bench', first.stream, 256)).toBe(17);
    expect(raftGroupForStream('bench', second.stream, 256)).toBe(17);
    expect(second.stream).not.toBe(first.stream);
  });
});
