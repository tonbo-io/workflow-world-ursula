import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createStreamer } from '../dist/index.js';

const baseUrl = process.env.URSULA_URL;
assert(baseUrl, 'URSULA_URL is required');

const runId = `wrun_${randomUUID().replaceAll('-', '')}`;
const name = 'agent-output';
const streamer = createStreamer({ baseUrl });

await streamer.streams.write(runId, name, 'first');
await streamer.streams.writeMulti(runId, name, [
  Uint8Array.from([0, 1, 2, 255]),
  'third',
]);

const firstPage = await streamer.streams.getChunks(runId, name, { limit: 2 });
assert.deepEqual(
  firstPage.data.map(({ data }) => Buffer.from(data)),
  [Buffer.from('first'), Buffer.from([0, 1, 2, 255])]
);
assert.equal(firstPage.hasMore, true);
assert(firstPage.cursor);

const secondPage = await streamer.streams.getChunks(runId, name, {
  cursor: firstPage.cursor,
  limit: 2,
});
assert.deepEqual(
  secondPage.data.map(({ data }) => Buffer.from(data)),
  [Buffer.from('third')]
);
assert.equal(secondPage.hasMore, false);
assert.equal(secondPage.done, false);

const tailReader = await streamer.streams.get(runId, name, -1);
const tail = tailReader.getReader();
const tailResult = await tail.read();
assert.equal(tailResult.done, false);
assert.deepEqual(Buffer.from(tailResult.value), Buffer.from('third'));
await tail.cancel();

const liveReader = await streamer.streams.get(runId, name, 3);
const live = liveReader.getReader();
const pendingLiveChunk = live.read();
await streamer.streams.write(runId, name, 'live');
const liveResult = await pendingLiveChunk;
assert.equal(liveResult.done, false);
assert.deepEqual(Buffer.from(liveResult.value), Buffer.from('live'));

await streamer.streams.close(runId, name);
const closed = await live.read();
assert.equal(closed.done, true);

const info = await streamer.streams.getInfo(runId, name);
assert.deepEqual(info, { tailIndex: 3, done: true });

const recovered = createStreamer({ baseUrl });
const recoveredChunks = await recovered.streams.getChunks(runId, name, {
  limit: 10,
});
assert.deepEqual(
  recoveredChunks.data.map(({ data }) => Buffer.from(data)),
  [
    Buffer.from('first'),
    Buffer.from([0, 1, 2, 255]),
    Buffer.from('third'),
    Buffer.from('live'),
  ]
);
assert.equal(recoveredChunks.done, true);
assert.deepEqual(await recovered.streams.list(runId), [name]);

console.log(
  JSON.stringify({
    runId,
    stream: name,
    chunks: recoveredChunks.data.length,
    tailIndex: info.tailIndex,
    done: info.done,
    recoveredAfterAdapterRestart: true,
  })
);
