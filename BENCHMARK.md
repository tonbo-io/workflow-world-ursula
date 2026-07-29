# Ursula World benchmark plan

This benchmark answers two separate questions:

1. Does Ursula improve the Workflow workloads users report as slow?
2. What does that improvement cost at an equivalent durability and deployment
   topology?

It extends Workflow's existing deployment-side benchmark instead of maintaining
an Ursula-only harness. The same Workflow SDK commit, workflow definitions,
payloads, iteration counts, and clocks must be used for every backend.

## Comparators

Run the full matrix against:

1. **Vercel managed World** — the primary product baseline. The Workflow
   performance reports about growing run-history overhead and multi-minute
   stream visibility are against this backend.
2. **`@workflow/world-postgres`** — the primary self-hosted baseline. Vercel
   calls it the reference implementation and says real customers run it in
   production.
3. **Ursula World** — an EKS cluster in the same AWS region and availability
   profile as the application.

Community MongoDB, Redis, Turso, Jazz, and Cloudflare Worlds are useful
compatibility checks, but are not primary performance baselines yet. For
2026-07-17 through 2026-07-23, the npm downloads API reported 84,308 downloads
for `@workflow/world-postgres`, compared with 55 for
`@workflow-worlds/mongodb`, 22 for
`@fantasticfour/world-postgres-redis`, and 225 for
`@ai_kit/workflow-world`. Downloads include CI and transitive installs, so this
is adoption direction, not a unique-user count.

Sources:

- [Vercel Workflow GA and supported Worlds](https://vercel.com/blog/a-new-programming-model-for-durable-execution)
- [Postgres World package and production caveat](https://www.npmjs.com/package/@workflow/world-postgres)
- [Workflow performance megathread](https://github.com/vercel/workflow/issues/1930)
- [Run-history latency report](https://github.com/vercel/workflow/issues/3026)
- [Retained chat replay report](https://github.com/vercel/workflow/issues/1820)
- [Managed stream visibility report](https://github.com/vercel/workflow/issues/2767)

## User-facing metrics

The existing benchmark already measures:

- **TTFS**: trigger to first step start;
- **STSO**: gap between sequential steps, sampled early and after 100 and 1,000
  steps;
- **WO**: whole-run orchestration time outside step bodies;
- **SL**: live stream write-to-read propagation;
- **SO**: stream persistence/backpressure beyond the modelled token generation
  window.

The Ursula extension adds:

- **retained-first**: opening a closed stream to its first decoded chunk;
- **retained-drain**: draining a closed stream from either record zero or a
  resume index.
- **agent-e2e**: in-deployment trigger to completed DurableAgent execution;
- **agent-execution**: time inside DurableAgent, excluding workflow dispatch;
- **agent-dispatch**: in-deployment trigger to DurableAgent body entry.

The agent scenarios are derived from Vercel Workflow's official `100_durable_agent_e2e.ts` and use `DurableAgent` with the official `@workflow/ai/test` mock providers. They exercise the real model-step, tool-step, default-stream, queue, and World persistence lifecycle without including external model latency. The earlier `97_bench.ts` structured-stream scenario remains a synthetic AI-SDK-shaped payload benchmark; it is not presented as a complete agent.

The shared retained-stream scenarios write, close, and immediately replay the
stream, so they measure **hot retained replay** consistently across all
backends. A separate cold-replay scenario must verify that Ursula has flushed
the payload below the hot tier and use a fresh reader process before claiming
S3 replay performance.

Every run must also report correctness and operations metrics:

- completed, retried, duplicate, and failed steps;
- stream chunks written, replayed, duplicated, or missing;
- queue claim-to-delivery latency and lease redelivery count;
- run list/query latency at 1K, 10K, and 100K historical runs;
- adapter CPU, resident memory, network bytes, and request count;
- backend storage bytes, write amplification, object/row count, and retained
  history.

## Managed Vercel baseline

The latest successful public `main` benchmark available while this plan was
written used Workflow commit `62d570ed4bf38db333ae9fe9ba513c0d6a9d6b91`
on 2026-07-25. Its artifact provides the first fixed comparison baseline:

| Metric | Scenario | Average | p90 | p99 |
| --- | --- | ---: | ---: | ---: |
| TTFS | no-op step | 995.9 ms | 1,103 ms | 1,154 ms |
| TTFS | streaming step | 1,061.6 ms | 1,158 ms | 1,281 ms |
| TTFS | hook plus stream | 1,383.6 ms | 1,449 ms | 1,675 ms |
| SL | live stream propagation | 128.2 ms | 148 ms | 208 ms |
| SO | 300 text chunks at 100/s | 195.8 ms | 246 ms | 270 ms |
| SO | 300 structured chunks at 100/s | 202.4 ms | 249 ms | 696 ms |
| STSO | steps 1-20 | 248.4 ms | 344 ms | 387 ms |
| STSO | steps 101-120 | 256.7 ms | 298 ms | 377 ms |
| STSO | steps 1,001-1,020 | 529.3 ms | 596 ms | 605 ms |

The single 1,020-step run spent 412,515 ms outside step bodies. These values
come from the downloadable artifact on
[GitHub Actions run 30134851629](https://github.com/vercel/workflow/actions/runs/30134851629);
they are not substituted for a same-commit rerun when producing the final
comparison.

## DurableAgent baseline

The first production-shaped agent baseline ran on 2026-07-29 using Workflow commit `62d570ed4bf38db333ae9fe9ba513c0d6a9d6b91`, `@workflow/ai/test` deterministic mock providers, and benchmark images built from `dfbafbabdd4b8f982b9ce39c7df944f03d55a1f0`. The Ursula image index was `sha256:63366de927430d46d8ce060a05c73a3ceb1692a2224546f52dd0a3241e517783`; the Postgres image index was `sha256:be28f3cf004861c1e8fb05199f6b2af7e1558a92b687fb56fcdacce978289de9`.

Both backends used the same two `m7g.xlarge` application nodes and eight one-CPU application processes. Ursula used three `m7g.large` voters across three availability zones, 256 Raft groups, memory WAL, and S3 cold storage. PostgreSQL used RDS PostgreSQL 17.9 on a Multi-AZ `db.m7g.large` with 100 GiB gp3 storage and 3,000 provisioned IOPS. Backends ran serially so their application processes never competed for the two application nodes.

Each recorded repeat contained 30 successful basic runs and 30 successful tool-loop runs after two unrecorded warmups; every run validated the DurableAgent model-step count, tool count, and final text. The table reports the median of the three independent repeat summaries, not a percentile reconstructed from pooled raw samples.

| Scenario | Metric | Ursula | PostgreSQL | Ursula vs PostgreSQL |
| --- | --- | ---: | ---: | ---: |
| one model turn, no tool | average | 139 ms | 137 ms | 1% slower |
| one model turn, no tool | p75 | 134 ms | 137 ms | 2% faster |
| one model turn, no tool | p90 | 191 ms | 189 ms | 1% slower |
| one model turn, no tool | p99 | 352 ms | 327 ms | 8% slower |
| four model turns, three tools | average | 589 ms | 700 ms | 16% faster |
| four model turns, three tools | p75 | 655 ms | 771 ms | 15% faster |
| four model turns, three tools | p90 | 762 ms | 890 ms | 14% faster |
| four model turns, three tools | p99 | 1,285 ms | 1,032 ms | 25% slower |

The repeat-level summaries were:

| Backend | Scenario | Repeat averages | Repeat p90 | Repeat p99 |
| --- | --- | --- | --- | --- |
| Ursula | basic | 143.9 / 135.5 / 139.0 ms | 191 / 191 / 151 ms | 233 / 352 / 370 ms |
| PostgreSQL | basic | 159.1 / 137.0 / 136.2 ms | 304 / 189 / 183 ms | 327 / 243 / 364 ms |
| Ursula | three-tool loop | 572.1 / 640.3 / 589.2 ms | 708 / 908 / 762 ms | 879 / 1,653 / 1,285 ms |
| PostgreSQL | three-tool loop | 644.8 / 708.7 / 700.0 ms | 787 / 895 / 890 ms | 987 / 1,039 / 1,032 ms |

Ursula's measured append amplification was stable across all repeats: the basic scenario used 328-333 accepted appends per 30 runs, or about 11 appends per agent run; the three-tool loop used 869-872, or about 29 appends per agent run. Median Raft replication request bytes were about 47.9 KB per basic run and 101.5 KB per three-tool run. PostgreSQL's median physical database growth was about 4.4 KB and 15.0 KB per run respectively, but PostgreSQL page allocation makes such short-window size deltas quantized rather than exact logical-byte accounting. PostgreSQL transaction counters also include the eight workers' steady polling, so they are diagnostic, not a direct per-run billing unit.

The result establishes a narrower claim than the earlier synthetic benchmark: Ursula is not faster for the minimum one-turn agent lifecycle, but it is about 15% faster through the body of a deterministic multi-turn tool loop at average through p90. Ursula does not yet meet the lower-p99 goal; one slow sample dominates each 30-sample p99, and the three-repeat median p99 remains worse than PostgreSQL.

The equivalent Vercel managed DurableAgent run is still missing. The public managed artifact predates these two scenarios, and this environment has no linked Vercel benchmark deployment or credential. Do not substitute the synthetic `97_bench.ts` managed numbers for this agent baseline; run the same image commit, workflow definitions, warmups, and iteration counts on managed World before making a three-way product claim.

## Workloads

Use deterministic payloads for the measured runs; run a smaller real-model
validation separately so provider variance does not dominate storage results.

| Workload | Purpose |
| --- | --- |
| 1 no-op and 1 streaming step | cold/warm trigger overhead |
| 1,020 sequential steps | detect history-dependent replay cost |
| 300 chunks at 100 chunks/s | interactive agent streaming |
| 1,000 retained chunks from record 0 | full chat/history reconstruction |
| 1,000 retained chunks from record 900 | reconnect/resume behavior |
| 1,000 cold retained chunks from record 0/900 | S3 replay after a verified flush and fresh reader |
| 100 concurrent 50-step runs | queue fairness and backend contention |
| DurableAgent with one mock model turn | minimum production-shaped agent lifecycle |
| DurableAgent with four mock model turns and three tool steps | multi-turn agent persistence, streaming, and tool-loop overhead |
| 10K completed runs plus active runs | observability/list projection cost |
| worker restart during write and claim | idempotency and redelivery correctness |

The 10K-run query test remains a separate soak until Ursula Table Stream
projection exists. The authoritative per-run journals stay Byte Streams. The
planned projection should use versioned Table Stream rows for runs, steps, and
hooks, carrying `source_run_id` and `source_next_record` as a watermark.
Benchmark both the existing bounded JSON-journal scan and that projection once
[Ursula issue 81](https://github.com/tonbo-io/ursula/issues/81) lands.

## Fairness controls

- Pin the application and backend to the same AWS region. Record cross-AZ and
  cross-region traffic explicitly.
- Use the same Workflow repository commit and package versions.
- Warm each backend with the same number of unrecorded runs.
- Run recorded iterations sequentially unless the scenario explicitly tests
  concurrency.
- Use the same logical retention period and payload bytes.
- Keep durability comparable: Ursula uses its production voter topology and S3
  cold tier; Postgres uses multi-AZ production settings; Vercel uses the
  managed production World.
- Report medians and p75/p90/p99 plus raw result JSON. Do not compare only best
  cases.

## Running the shared benchmark

The EKS benchmark currently pins Vercel's public `main` tarballs at Workflow
commit `62d570ed4bf38db333ae9fe9ba513c0d6a9d6b91`
(`5.0.0-beta.36-62d570e`). This keeps the Ursula and Postgres runs on the same
Core revision as the managed baseline, including the stream group-commit and
immediate leading-edge dispatch changes that are newer than the published
`5.0.0-beta.36`. Replace the tarball pin with the next npm beta once those
changes are published.

For Ursula:

```sh
export WORKFLOW_TARGET_WORLD=@tonbo-io/world-ursula
export WORKFLOW_URSULA_URL=https://ursula.example.com
export WORKFLOW_URSULA_TOKEN=...
export WORKFLOW_URSULA_BUCKET=workflow-benchmark
export WORKFLOW_URSULA_QUEUE_DELIVERY_URL="$DEPLOYMENT_URL"
export WORKFLOW_BENCH_BACKEND=ursula-eks
```

Then run the same deployment and `packages/core/e2e/benchmark.test.ts` command
used by the Vercel benchmark workflow. Preserve the generated
`bench-results-<app>-<backend>.json` artifact.

For Postgres, set `WORKFLOW_TARGET_WORLD=@workflow/world-postgres` and
`WORKFLOW_POSTGRES_URL`. For managed Vercel, use the benchmark workflow's
standard Vercel environment.

The EKS comparison manifests assume two `m7g.xlarge` workers labelled
`workflow-benchmark-role=app`. They run eight application replicas spread
evenly across both nodes. Keep the same application topology for Ursula and
Postgres and run the backends serially; a single four-core application node
becomes replay-CPU-bound before either storage backend reaches its useful
throughput ceiling.

`WORKFLOW_BENCH_PROFILE=1` enables benchmark-only OpenTelemetry aggregation
and a low-frequency V8 CPU profile in every application process. The runner
resets and starts all profiles immediately before a suite, then stores span
distributions and the top 50 self-time frames from every pod in
`runtimeProfile`. The default sampling interval is 5 ms and can be changed with
`WORKFLOW_BENCH_CPU_PROFILE_INTERVAL_US`. Both comparators must use the same
setting; CPU-profiled results should not be compared with older unprofiled
results as if the methodology were identical.

The benchmark build also marks the installed `@workflow/world` package as side-effect-free before bundling. The package consists of declarations, schemas, constants, and pure helpers, but its published manifest currently omits this metadata. Without it, importing the `Run` serde registration entry causes esbuild to retain the whole World barrel in the code evaluated by every deterministic replay. DurableAgent legitimately brings the AI SDK and Zod into the VM, so the post-build guard now checks directly that neither `@workflow/world` nor `@tonbo-io/world-ursula` implementation code leaked into the bundle and enforces a 1 MiB budget around the measured official agent workload. This is a symmetric Workflow runtime optimization applied to both Ursula and Postgres images, not an Ursula-only benchmark shortcut.

The repository's EKS manifests implement this matrix directly:

- `deploy/eks-benchmark.yaml` uses the Depot-built `main-ursula` image and
  records aggregate counters from all three Ursula voters;
- `deploy/rds-benchmark/` provisions a private PostgreSQL 17 Multi-AZ RDS
  comparator in the canary VPC;
- `deploy/eks-postgres-benchmark.yaml` uses the separately compiled
  `main-postgres` image against that RDS instance.

Both Jobs append their result JSON as `BENCH_RESULT_BASE64` in the final log,
so Kubernetes log rotation or completed-Pod filesystem cleanup cannot discard
the artifact. The JSON includes a `backendUsage` before/after/delta block:
Ursula reports append, mutation, cold-upload, and GC counters; Postgres reports
database bytes, transactions, block activity, tuple activity, and temporary
I/O.

The result JSON also includes `scenarioBackendUsage` for the two agent scenarios. This is the unit-cost baseline: it records the marginal Ursula append/replication counters or PostgreSQL transaction/tuple counters for a fixed number of successful, correctness-checked agent runs.

## Cost collection

Take counters immediately before and after each isolated workload, then
normalize to:

- cost per 100,000 workflow steps;
- cost per 1,000 completed agent runs;
- cost per million stream chunks;
- cost per retained GB-month.

For Vercel, export billed Workflow steps and storage GB-hours plus function and
observability usage. For Postgres, record instance-hours, storage and backup
growth, I/O, row growth, and WAL bytes. For Ursula, record:

- EKS node, load-balancer, and EBS hours;
- Ursula append count, logical and replicated bytes, and cold flush counters;
- S3 PUT/GET/LIST requests, object count, object-size histogram, live bytes,
  noncurrent-version bytes, and lifecycle deletions;
- application-to-Ursula and Ursula-to-S3 network bytes.

Run `benchmark/capture-cost-snapshot.sh` immediately before and after each
backend. It records current/noncurrent S3 object counts and bytes, benchmark
Pod resource shapes and restarts, plus the non-secret RDS topology and storage
configuration. Keep these external snapshots beside the result JSON; the
in-result counters provide the marginal workload delta between them.

Fixed cluster cost and marginal workload cost must be shown separately.
Amortize fixed cost only at explicitly stated utilization levels; do not compare
a mostly idle production-sized EKS cluster directly with per-request managed
pricing.

The public Vercel price snapshot for this plan is **$2.50 per 100,000 Workflow
steps** and **$0.00069 per Workflow storage GB-hour**, before function and
observability charges. Record the price and retrieval date with every result
instead of treating those values as permanent. See
[Vercel managed infrastructure pricing](https://vercel.com/docs/limits#on-demand-resources-for-pro).

## Projection acceptance criteria

Table Stream projection is successful only if:

- global query latency is independent of total run-journal bytes for a fixed
  result size;
- query-time HTTP fan-out no longer scales with registered run count;
- projection storage and S3 request cost stay bounded through compaction;
- a reader can detect the projection watermark and merge the small,
  authoritative Byte Stream tail without returning stale terminal state;
- rebuilding or dropping the projection never changes authoritative workflow
  state.
