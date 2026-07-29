# `@tonbo-io/world-ursula`

An Ursula-backed World implementation for the Workflow SDK.

Ursula is the only durable source of truth for all three World surfaces:

- Storage: workflow events and materialized run, step, hook, and wait state;
- Streamer: ordered binary chunk streams, live reads, and close state;
- Queue: delayed messages, leases, attempts, retries, and acknowledgements.

The adapter keeps only disposable dispatcher state and read caches in memory.
Committed state can be reconstructed after an adapter restart by replaying its
Ursula journals.

```ts
import { createWorld } from '@tonbo-io/world-ursula';

const world = createWorld({
  baseUrl: 'https://ursula.example.com',
  token: process.env.URSULA_TOKEN,
  bucket: 'workflow',
});
```

It can also be loaded through Workflow's target-World mechanism:

```bash
export WORKFLOW_TARGET_WORLD=@tonbo-io/world-ursula
export WORKFLOW_URSULA_URL=https://ursula.example.com
export WORKFLOW_URSULA_TOKEN=...
export WORKFLOW_URSULA_BUCKET=workflow
```

No delegate database or queue is required. `withUrsulaStreams()` remains
available only as an incremental migration helper for an existing custom
World.

## Use with eve

[eve](https://eve.dev) selects a Workflow world by package name in the root
`agent/agent.ts`:

```ts
import { defineAgent } from 'eve';

export default defineAgent({
  model: 'openai/gpt-5.4-mini',
  experimental: { workflow: { world: '@tonbo-io/world-ursula' } },
});
```

Supply the connection through the environment; eve calls `createWorld()` with
no arguments, so every option has to come from `WORKFLOW_URSULA_*`:

```bash
export WORKFLOW_URSULA_URL=https://ursula.example.com
export WORKFLOW_URSULA_TOKEN=...
export WORKFLOW_URSULA_BUCKET=workflow
```

That is the whole integration. `eve build && eve start` then runs sessions,
turns, hooks and streams on Ursula with no other managed service.

Four things are worth knowing before deploying it:

**Version line.** At boot eve compares this package's declared `@workflow/core`
or `@workflow/world` dependency against the `@workflow/core` it bundles, and
refuses to start on a major or prerelease-tag mismatch. Track the version eve
pins; see [DESIGN.md](./DESIGN.md) for what a spec-version change implies for
committed journals.

**Queue delivery URL.** The dispatcher posts to Workflow's HTTP routes.
`eve start` exports `PORT`, which the default `localhost:$PORT` resolves
against. A custom host that serves the Nitro output directly may not, so set
`WORKFLOW_URSULA_QUEUE_DELIVERY_URL` to the origin serving
`/.well-known/workflow/v1/*` whenever eve is not the process manager.

**Queue namespace.** eve derives a per-agent queue namespace and exports it as
`WORKFLOW_QUEUE_NAMESPACE`, so topics arrive as
`__eve<hex>_wkf_workflow_<name>`. Nothing to configure — noted because queue
stream names are derived from the full topic, so two agents never share a
journal.

**Shutdown.** eve awaits `start()` but never calls `close()`, so
`WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS` does not apply under eve: in-flight
handlers end with the process. Committed state is unaffected — an expired lease
is redelivered.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `WORKFLOW_URSULA_URL` | Required Ursula HTTP endpoint |
| `WORKFLOW_URSULA_TOKEN` | Optional bearer token |
| `WORKFLOW_URSULA_BUCKET` | Ursula bucket, default `workflow` |
| `WORKFLOW_URSULA_DEPLOYMENT_ID` | Stable queue worker deployment identity |
| `WORKFLOW_URSULA_QUEUE_DISPATCHER_ENABLED` | Set to `0` on request-serving replicas when a smaller redundant dispatcher pool handles queue claims; default `1` |
| `WORKFLOW_URSULA_QUEUE_DELIVERY_URL` | Workflow HTTP origin for queue delivery; defaults to `localhost:$PORT` |
| `WORKFLOW_URSULA_LONG_POLL_TIMEOUT_MS` | Stream live-read timeout |
| `WORKFLOW_URSULA_STREAM_FLUSH_INTERVAL_MS` | Workflow chunk coalescing window; use `0` for immediate flushes |
| `WORKFLOW_URSULA_QUEUE_POLL_INTERVAL_MS` | Retry delay after a dispatcher wake/read error; normal delivery is wake-driven |
| `WORKFLOW_URSULA_QUEUE_LEASE_DURATION_MS` | Queue message lease duration |
| `WORKFLOW_URSULA_QUEUE_RETRY_DELAY_MS` | Default retry delay |
| `WORKFLOW_URSULA_QUEUE_CONCURRENCY` | Local queue dispatcher concurrency |
| `WORKFLOW_URSULA_QUEUE_PARTITIONS` | Physical journals per logical queue, default `8`; one execution lane always maps to one partition |
| `WORKFLOW_URSULA_EXPERIMENTAL_OWNED_STEP_TRANSACTIONS` | Set to `1` only when the runtime guarantees one active handler for an optimistic owned-lazy step; lets Turbo commit started + terminal state in one record |
| `WORKFLOW_URSULA_EXPERIMENTAL_COMPACT_COMPLETED_STEP_COMMITS` | Set to `1` only after every process can read compact v2 records; removes duplicated owned-step fields from the authoritative run append |
| `WORKFLOW_URSULA_QUEUE_SHUTDOWN_GRACE_MS` | Maximum graceful wait for in-flight handlers |

## Durability model

- Each accepted workflow event and the resulting materialized entities are
  stored in the same conditional Ursula append.
- Per-run mutation races are serialized with `Stream-Record-Match`.
- Within a claimed queue delivery, each step's started and terminal lifecycle
  is committed in one Ursula record. The experimental owned-step option also
  lets Turbo's optimistic owned-lazy path reuse the durable owner message as
  its fence and avoid the extra claim append.
- Hook tokens use dedicated Ursula claim streams to preserve global uniqueness.
- Queue messages retain one stable ID across lease expiry and redelivery.
- Local enqueues wake the dispatcher immediately; other instances wake through
  long-lived Ursula tail watchers on deliverable queue and registry streams.
- Run and queue checkpoints bound restart replay. Queue checkpoints also
  advance Ursula retention, recover lagging instances from `410 Gone`, keep
  only the latest checkpoint record, and omit acknowledged messages from the
  active set. Queue idempotency keys have a 24-hour durable retry window.
- Stream records store binary values with an explicit JSON codec.
- Run and queue registries are durable discovery metadata; query indexes are
  rebuildable projections, never authoritative state.

See [`DESIGN.md`](./DESIGN.md) for the stream layout and recovery invariants.
See [`BENCHMARK.md`](./BENCHMARK.md) for the Vercel/Postgres/Ursula comparison,
cost accounting, and Table Stream projection acceptance criteria.

## Current production gaps

The implementation targets the full World contract, but these areas still
need hardening before treating it as a production default:

- global run and correlation queries use bounded-concurrency reads but still
  rebuild from authoritative run journals; the intended replacement is a typed
  Ursula Table Stream projection rather than another JSON index;
- a process that dies after reserving a hook token but before committing any
  owner-run record leaves an orphan claim that needs timeout-based repair;
- Workflow event creation has no operation idempotency key, so an ambiguous
  mutation cannot always be reconstructed as the same operation after an
  adapter process restart;
- Streamer writes do not carry a Workflow operation ID, so an ambiguous
  in-flight append cannot be deduplicated across adapter process restart;
- Streamer chunks use base64 JSON records to preserve one stable Ursula record
  ordinal per Workflow chunk, adding roughly 33% payload overhead;
- queue dispatch needs metrics/tracing integration beyond structured error
  logging;
- large-scale contention, failover, and retention behavior still need soak and
  fault-injection coverage.

Ursula bucket listing/changefeeds and record-tail-only long polling would
improve index repair and wake-up cost, but are not required for the adapter's
correctness model.
