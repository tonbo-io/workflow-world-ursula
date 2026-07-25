import { createHash } from 'node:crypto';
import type { QueuePayload, ValidQueueName } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  QueueJournal,
  UrsulaClient,
  UrsulaRequestError,
} from '../src/index.js';

const baseUrl = process.env.WORKFLOW_URSULA_URL;
const bucket = process.env.WORKFLOW_URSULA_BUCKET;

describe.skipIf(!baseUrl || !bucket)('Ursula retention recovery', () => {
  it(
    'rebuilds a lagging dispatcher after another instance crosses a checkpoint boundary',
    { timeout: 30_000 },
    async () => {
      if (!baseUrl || !bucket) throw new Error('Ursula test config missing');
      const first = new QueueJournal(new UrsulaClient({ baseUrl, bucket }));
      const lagging = new QueueJournal(new UrsulaClient({ baseUrl, bucket }));
      const queueName =
        `__wkf_workflow_retention_${Date.now()}` as ValidQueueName;
      const payload = {
        __healthCheck: true,
        correlationId: 'retention-recovery',
      } satisfies QueuePayload;
      const claimAt = new Date(Date.now() + 60_000);

      await expect(
        lagging.claim(queueName, claimAt, 1_000)
      ).resolves.toBeNull();
      for (let index = 0; index < 86; index += 1) {
        await first.enqueue(queueName, payload, {
          idempotencyKey: `retention-${index}`,
        });
        const lease = await first.claim(queueName, claimAt, 1_000);
        if (!lease) throw new Error('expected queue lease');
        await expect(first.ack(queueName, lease)).resolves.toBe(true);
      }

      const sourceStream = `queue-${createHash('sha256')
        .update(queueName)
        .digest('base64url')}`;
      const retainedRead = await new UrsulaClient({ baseUrl, bucket })
        .read(sourceStream, 0)
        .catch((error: unknown) => error);
      expect(retainedRead).toBeInstanceOf(UrsulaRequestError);
      expect((retainedRead as UrsulaRequestError).status).toBe(410);

      await expect(
        lagging.claim(queueName, claimAt, 1_000)
      ).resolves.toBeNull();
      await lagging.enqueue(queueName, payload, {
        idempotencyKey: 'after-retention',
      });
      await expect(
        lagging.claim(queueName, claimAt, 1_000)
      ).resolves.not.toBeNull();
    }
  );
});
