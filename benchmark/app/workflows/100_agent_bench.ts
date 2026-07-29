/**
 * Deterministic DurableAgent benchmarks derived from Vercel Workflow's
 * `100_durable_agent_e2e.ts`.
 *
 * These use the official `@workflow/ai/test` mock providers. That preserves the
 * real DurableAgent lifecycle, model-step boundaries, tool execution, default
 * output streaming, queueing, and World persistence while removing model API
 * latency and nondeterminism from the storage baseline.
 */
import { DurableAgent } from '@workflow/ai/agent';
import { mockSequenceModel, mockTextModel } from '@workflow/ai/test';
import { getWritable } from 'workflow';
import z from 'zod/v4';

export interface BenchAgentResult {
  /** Date.now() immediately before DurableAgent starts. */
  agentStartedAt: number;
  /** Date.now() immediately after DurableAgent and output streaming finish. */
  completedAt: number;
  /** Number of model turns returned by DurableAgent. */
  stepCount: number;
  /** Number of workflow tool steps expected in this scenario. */
  toolCallCount: number;
  /** Deterministic final text, used as a correctness assertion by the runner. */
  lastStepText: string | undefined;
}

async function echoStep(input: { step: number }): Promise<string> {
  'use step';
  return `step-${input.step}-done`;
}

/**
 * One model turn with a streamed text response and no tools.
 */
export async function benchAgentBasicWorkflow(): Promise<BenchAgentResult> {
  'use workflow';
  const agent = new DurableAgent({
    model: mockTextModel('Agent baseline complete.'),
    instructions: 'Return the deterministic benchmark response.',
  });
  const agentStartedAt = Date.now();
  const result = await agent.stream({
    messages: [{ role: 'user', content: 'Run the agent baseline.' }],
    writable: getWritable(),
  });
  return {
    agentStartedAt,
    completedAt: Date.now(),
    stepCount: result.steps.length,
    toolCallCount: 0,
    lastStepText: result.steps[result.steps.length - 1]?.text,
  };
}

/**
 * Four model turns interleaved with three durable workflow tool steps.
 */
export async function benchAgentToolLoopWorkflow(): Promise<BenchAgentResult> {
  'use workflow';
  const agent = new DurableAgent({
    model: mockSequenceModel([
      {
        type: 'tool-call',
        toolName: 'echoStep',
        input: JSON.stringify({ step: 1 }),
      },
      {
        type: 'tool-call',
        toolName: 'echoStep',
        input: JSON.stringify({ step: 2 }),
      },
      {
        type: 'tool-call',
        toolName: 'echoStep',
        input: JSON.stringify({ step: 3 }),
      },
      { type: 'text', text: 'All done!' },
    ]),
    tools: {
      echoStep: {
        description: 'Echo the benchmark step number.',
        inputSchema: z.object({ step: z.number() }),
        execute: echoStep,
      },
    },
    instructions: 'Execute all three benchmark tools in order.',
  });
  const agentStartedAt = Date.now();
  const result = await agent.stream({
    messages: [{ role: 'user', content: 'Run three steps.' }],
    writable: getWritable(),
  });
  return {
    agentStartedAt,
    completedAt: Date.now(),
    stepCount: result.steps.length,
    toolCallCount: 3,
    lastStepText: result.steps[result.steps.length - 1]?.text,
  };
}
