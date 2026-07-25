import type { AnyEventRequest } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import type { RunJournalState } from './run-journal.js';
import { mutationOperationId } from './storage.js';

const state = {
  runId: 'wrun_1',
  nextRecord: 7,
  steps: new Map(),
  hooks: new Map(),
  hookRetentionUntil: new Map(),
  waits: new Map(),
} satisfies RunJournalState;

const request = {
  eventType: 'run_started',
  specVersion: 5,
} satisfies AnyEventRequest;

describe('Ursula storage operation identity', () => {
  it('does not collapse identical concurrent calls without a request ID', () => {
    expect(mutationOperationId(state, request, 'call-1', undefined)).not.toBe(
      mutationOperationId(state, request, 'call-2', undefined)
    );
  });

  it('keeps a supplied request ID stable across process-local calls', () => {
    expect(mutationOperationId(state, request, 'call-1', 'request-1')).toBe(
      mutationOperationId(state, request, 'call-2', 'request-1')
    );
  });
});
