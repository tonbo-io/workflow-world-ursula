# Ursula World architecture

## Goal

`@tonbo-io/world-ursula` makes Ursula the sole durable source of truth for the
three World surfaces:

- Workflow event storage and materialized entities;
- chunk streams;
- delayed queue state.

Local memory, local files, and optional query indexes are caches only. Losing
all of them must not lose a committed event, entity, stream chunk, queued
message, delivery attempt, or hook-token claim.

## Required invariants

1. An accepted event and the entity state it produces are one atomic commit.
2. Concurrent mutations of one run are linearized in its Ursula stream.
3. Mutations that carry a stable idempotency key do not append twice across
   adapter restart; contracts without such a key document the weaker boundary.
4. A hook token has at most one live owner across all runs and processes.
5. A queue message has one stable ID across every delivery attempt.
6. An expired lease causes redelivery; an acknowledgement permanently prevents
   redelivery.
7. Delayed messages are not visible before their `availableAt`.
8. Every derived index is rebuildable from authoritative Ursula records.
9. One active execution lease fences every speculative step transition in its
   lane; a stale handler cannot commit after a newer lease generation takes
   over.

## Stream layout

All names are hashed where necessary to stay within Ursula's stream-ID limit.

| Stream family | Authority |
| --- | --- |
| `run/{runId}` | Ordered event log plus post-event Run/Step/Hook/Wait state |
| `stream/{runId}/{nameHash}` | Workflow chunk stream |
| `hook/{tokenHash}` | Hook-token reservation, ownership, retention, release |
| `queue/{queueHash}/{partition}` | Enqueue, lease, ack, retry and dead-letter transitions |
| `registry/runs/{shard}` | Durable run registration used for discovery and index repair |
| `index/*` | Rebuildable query projections; never canonical state |

## Run commits

One record in `run/{runId}` is a complete transaction:

```ts
interface RunCommit {
  version: 1;
  operationId: string;
  previousRecord: number;
  events: Event[];
  run?: WorkflowRun;
  steps?: Record<string, Step | null>;
  hooks?: Record<string, Hook | null>;
  waits?: Record<string, Wait | null>;
  executionLeases?: Record<string, RunExecutionLease | null>;
  externalStateUpdatedAt?: number;
}
```

The adapter loads the latest checkpoint and tail, applies the Workflow reducer,
then appends the commit with `Stream-Record-Match`. The event and its
materialized entity therefore cannot diverge. A 412 reloads state and retries.

`operationId` combines the prior record coordinate, request scope, and
canonical mutation request. A supplied `requestId` is the durable idempotency
scope. Without one, a per-call nonce stays stable across that call's CAS
retries, so a lost response is safe, while two distinct concurrent calls with
identical payloads cannot collapse into one server-side receipt.
The low-level Ursula client maps the operation to a deterministic producer ID.
The World event-create contract does not currently expose a true idempotency
key, so reconstructing the same logical operation after an adapter process
restart is not generally possible.

Every adapter process keeps an incremental materialization cache. Periodic
full-state checkpoint records bound cold-start replay to at most 127 subsequent
run commits; old event payloads remain authoritative and are read through a
separate incremental event cache. A failed checkpoint never changes the result
of an already-committed source mutation. Checkpoints run on an ordered
per-run background chain, so snapshot publication and retention never extend
the source mutation's response latency; a process crash merely leaves more
authoritative tail to replay. Both caches are LRU-bounded, terminal run state
is not retained in the materialization cache, and registry-wide query scans
bypass the caches.

An incremental cursor may briefly be ahead of a lagging Ursula follower even
though the leader already acknowledged the source commit. The adapter retries
`InvalidRecordBoundaries` on the same cursor during that bounded catch-up
window; it never treats a follower's lower local tail as authoritative state.

## Atomic step transactions

Workflow's public World contract exposes `step_started` and the terminal
`step_completed`, `step_failed`, or `step_retrying` mutation as separate
awaited calls. Persisting both literally costs two Raft commits per logical
step even when one queue delivery owns the entire inline execution.

For ordinary queue delivery, the Ursula adapter claims an execution lane in
the run journal before entering the Workflow handler. The claim contains a
unique fencing token, owner message ID, attempt, expiry, and monotonically
increasing generation. The `run` lane covers inline orchestration; a
background step uses `step:<stepId>`, so independent parallel steps do not
serialize on one lease. With
`WORKFLOW_URSULA_EXPERIMENTAL_OWNED_STEP_TRANSACTIONS=1`, Turbo's optimistic
lazy inline start can use its owning queue message as the transaction fence
without paying a separate execution-claim append. This remains opt-in because
the current World interface does not tell a backend whether a lazy start is
running under Turbo's single-handler guarantee.

Inside that claimed handler, `step_started` is materialized speculatively but
not appended. The terminal mutation replays the staged start against the
current run state and appends one record containing:

- `step_created` when the start was lazy;
- `step_started`;
- the terminal step event;
- the final Step entity; and
- background-lane lease release.

The execution claim, or Turbo's durable owner message, is the pre-body
linearization and crash-recovery boundary. Claimed terminal transactions
verify the same token and generation before their record-tail-guarded append.
If the handler dies, no speculative step event is visible and queue redelivery
can claim a newer generation or re-run the owned lazy step. If an old claimed
handler later returns, the generation check rejects its terminal write.
External run events committed after the claim do not revoke the owner;
terminal materialization rebases on them under `Stream-Record-Match`.

For a Turbo invocation that executes `N` owned lazy steps, the run write cost
is `N` atomic step appends instead of `2N` step appends. A non-Turbo queue
invocation pays one execution-claim append plus `N` atomic step appends.
Transports that provide neither a delivery lease nor an owned lazy start fall
back to the literal two-append contract.

The queue journal still owns ready-message discovery, delivery attempts,
delays, and acknowledgement. A later phase can move continuation readiness
into the run transaction and make the queue a rebuildable projection, but that
requires a bucket changefeed or equivalent durable projection repair path.

## Global indexes

Run logs remain authoritative Byte Streams. Global indexes should be typed
Table Stream projections, not a second family of schema-less JSON streams.
Initial projection families are expected to cover runs, steps, and hooks,
sharded to avoid one write-hot stream. Each row is a versioned upsert or
tombstone and carries its `source_run_id` and `source_next_record` watermark.
This gives DataFusion typed predicate/projection pushdown and Parquet
compaction without changing the per-run transaction boundary.

A projection write is idempotent and may lag, but a read that follows a
successful mutation must either:

1. wait until its requested source watermark is indexed; or
2. merge the authoritative run tail into the indexed result.

Run registration happens before the first run commit. A crash can therefore
leave an ignorable registry entry for a missing run, but cannot create an
undiscoverable committed run. Index repair walks the registry and compares
watermarks. Until Ursula Table Streams exist, the adapter uses bounded
registry-wide journal reads for correctness rather than adding a temporary JSON
projection that would need a second migration.

## Hook-token claims

The token stream is a small reservation state machine:

```text
available -> reserved(operationId, runId, hookId)
          -> committed(runRecord)
          -> retained(until) | released
```

Reservation happens before the run commit. Readers treat a reservation as
unavailable. The owner operation then appends the run commit and finalizes the
claim. Terminal runs remove non-retained Hooks immediately. Retained Hooks stay
readable and keep their token until their deadline; reads and later
reservations lazily append the cleanup transition after expiry. Reconciliation
also releases a terminal run's claim when its run commit removed the Hook but a
process died before appending the cross-stream release.

A process that dies after reserving a token but before committing any owner-run
record still leaves an orphan reservation. A future version needs a bounded
reservation timeout plus owner-journal reconciliation for that remaining crash
window.

## Queue state

Each logical Workflow queue is split into a fixed set of physical, append-only
queue journals. The stable execution-lane key (`runId` plus `stepId` when
present) selects one partition, so one lane remains ordered while unrelated
runs no longer contend on one global record tail. No ordering is promised
between independent execution lanes:

```text
enqueued(availableAt)
  -> leased(messageId, owner, expiresAt, attempt)
  -> acked
  -> retryScheduled(availableAt)
  -> leased(...)
```

Claiming a message is a record-tail-guarded append scoped to its partition.
Dispatchers incrementally replay queue transitions and may lease several
messages from one workflow topic. Active leases are keyed by execution lane:
messages for one lane stay serialized, while different runs or parallel steps
execute concurrently up to the configured process limit. The queue registry
records a partition only when its first message is enqueued, so workers keep
one long-lived watcher per active partition rather than eagerly opening every
possible partition on every replica. A local enqueue or completed delivery
wakes the pump immediately. Polling is used only as error backoff.

Small deployments use the zero-configuration topology in which every process
can dispatch. Larger deployments SHOULD separate request serving from queue
dispatch: request replicas set
`WORKFLOW_URSULA_QUEUE_DISPATCHER_ENABLED=0`, while a smaller redundant pool
keeps dispatch enabled and sends deliveries through the shared Workflow HTTP
origin. Request replicas still enqueue and execute delivered handlers; they
simply do not duplicate every registry and partition long poll. The dispatcher
pool is stateless. If all dispatchers disappear, delivery pauses until one
restarts, then pending messages or expired leases are recovered from Ursula.
At-least-once redelivery and execution fencing remain unchanged.

Every 256 transitions in one partition, the adapter writes a checkpoint
containing that partition's active messages and live 24-hour idempotency
window, publishes it as the partition stream's Ursula snapshot, then advances
source retention to that record boundary. This derived work runs on an ordered
per-partition background chain and
does not delay enqueue, lease, ack, or retry responses. The source snapshot is
required by Ursula as the safety proof for retention; adapter recovery reads
the derived checkpoint stream instead. Only the latest derived checkpoint
record is retained, so checkpoint schema changes MUST remain backward-readable
by every adapter version that can overlap during a rolling deployment. Writers
must not publish an incompatible checkpoint until older readers have been
removed. A dispatcher whose cursor falls behind retention discards its cache
and rebuilds from that checkpoint after Ursula returns `410 Gone`; if retention
moves again between reading the checkpoint and its source tail, it reloads the
checkpoint and retries once. Acknowledged messages are removed from the active
map immediately, so claim scans are bounded by live queue depth rather than
lifetime traffic. Handler delivery remains an adapter/runtime responsibility;
Ursula owns every durable queue transition.

## Time and delivery semantics

`availableAt` and lease expiry are absolute timestamps compared against each
adapter instance's local clock. Production deployments MUST synchronize clocks;
the maximum skew directly bounds how early a delayed message or redelivery can
occur. Queue delivery is at least once.

Lease IDs fence durable `extend`, `ack`, and `retry` transitions: a stale
handler cannot commit queue state after another worker has acquired a newer
lease. They do not stop already-running handler code from producing external
side effects. Handlers that require exactly-once effects must use their own
idempotency key or downstream fencing.

## Known Ursula primitive gaps

The design can be implemented over today's append, record-tail match, producer
deduplication, reads, and long polling. These additions would materially improve
production cost or recovery time but are not prerequisites for correctness:

- bucket stream listing or a bucket-wide changefeed for fast index repair;
- a server-side compare-and-append batch spanning several streams;
- record-tail long polling without reading record bodies.
