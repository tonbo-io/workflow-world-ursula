const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const U64_MASK = 0xffffffffffffffffn;

export function raftGroupForStream(
  bucket: string,
  stream: string,
  groupCount: number
): number {
  if (!Number.isSafeInteger(groupCount) || groupCount < 1) {
    throw new Error(`Invalid Ursula group count: ${groupCount}`);
  }
  let hash = FNV_OFFSET;
  for (const byte of Buffer.from(`${bucket}/${stream}`)) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & U64_MASK;
  }
  return Number(hash % BigInt(groupCount));
}

export function findStreamForGroup(
  bucket: string,
  base: string,
  groupCount: number,
  targetGroup: number,
  startingNonce = 0
): { stream: string; nextNonce: number } {
  if (
    !Number.isSafeInteger(targetGroup) ||
    targetGroup < 0 ||
    targetGroup >= groupCount
  ) {
    throw new Error(
      `Invalid Ursula target group ${targetGroup} for ${groupCount} groups`
    );
  }
  let nonce = startingNonce;
  for (;;) {
    const stream = `${base}-placement-${nonce}`;
    nonce += 1;
    if (raftGroupForStream(bucket, stream, groupCount) === targetGroup) {
      return { stream, nextNonce: nonce };
    }
  }
}
