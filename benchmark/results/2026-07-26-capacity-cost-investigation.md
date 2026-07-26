# Workflow backend capacity and cost investigation

Last updated: 2026-07-26 16:50 CST

Status: the Workflow-level capacity sweep and the raw-storage isolation benchmark are complete. The raw benchmark's temporary ARM application node and RDS comparator remain active while the first server-side follow-ups are selected. The original `100 concurrent × 50 steps` result was a load point, not a saturation point, so the old `$0.412 / 100k` Ursula and `$0.266 / 100k` PostgreSQL figures remain withdrawn.

## Goal

Measure Ursula and PostgreSQL under the same application tier and EKS network conditions, find each backend's useful capacity under explicit latency SLOs, and use measured utilization rather than provisioned instance size to explain cost.

The comparison must answer three different questions:

1. What performance does a user see at a given concurrency?
2. How much backend capacity is available before latency becomes unacceptable?
3. What does 100,000 completed workflow steps cost at a stated utilization and SLO?

## Environment

| Component | Configuration |
| --- | --- |
| Ursula | 3 × `m6i.xlarge`, one voter per AZ, 256 Raft groups, memory WAL, S3 cold storage, Ursula 0.3.8 |
| PostgreSQL | RDS PostgreSQL 17.9, Multi-AZ `db.m7g.large`, 100 GiB gp3 |
| Application tier | Capacity sweeps used 8, 16, and 32 replicas on 2, 4, and 8 isolated `m6i.xlarge` EKS application nodes |
| Region | `us-east-1`, same VPC |
| Workload | Vercel Workflow-compatible sequential no-op steps through the same benchmark application |

Application compute is common to both backends and is excluded from backend price comparisons, but its CPU and memory must still be sampled to prove it did not become the load generator bottleneck.

## Numbers obtained so far

### Raw storage primitives: core versus adapter

The raw runner bypasses Workflow and both World adapters. It ran from one isolated `m7g.xlarge` ARM EKS worker in the same VPC against:

- Ursula 0.3.8 on three `m7g.large` ARM voters, 256 Raft groups, memory WAL, and S3 cold storage;
- RDS PostgreSQL 17.9 Multi-AZ on `db.m7g.large`, 100 GiB gp3, using a fixed pool of 32 warm TLS connections.

Every timed write is one request/query and one 256-byte payload. `create + first append` measures a new stream. `Warm append` pre-creates the streams outside the timed window and then issues ordinary POST/INSERT operations. PostgreSQL live delivery performs `INSERT + pg_notify` atomically and reads the committed row after the notification; the notification alone is not treated as durable data.

| Primitive | Ursula | PostgreSQL | Result |
| --- | ---: | ---: | --- |
| Sequential append p50 / p99 | 3.41 / 4.50 ms | 2.52 / 2.91 ms | PostgreSQL lower latency |
| New stream + first payload, 128 concurrency | 1,499 ops/s | 6,762 ops/s | PostgreSQL 4.5× throughput |
| Warm append, 32 concurrency | 2,514 ops/s | 6,584 ops/s | PostgreSQL 2.6× throughput |
| Warm append, 128 concurrency | 2,892 ops/s | 6,462 ops/s | PostgreSQL 2.2× throughput |
| Warm append, 256 concurrency | 2,516 ops/s | 6,578 ops/s | PostgreSQL 2.6× throughput |
| Live durable write-to-read p50 / p99 | 4.17 / 6.96 ms | 5.37 / 7.09 ms | Ursula lower p50, similar p99 |
| 1 MiB retained replay | 7.81 ms | 12.55 ms | Ursula 37.8% faster |

This changes the diagnosis:

1. Ursula's streaming read path is not intrinsically slower. It wins the live-delivery p50 and retained replay comparison.
2. PostgreSQL still wins the durable write path. Even after removing stream creation and warming routing, its steady 32-connection throughput is about 2.2–2.6× Ursula's.
3. Warming Ursula streams raises peak throughput from about 1.5k to 2.9k appends/s, proving that first-write routing and stream creation explain roughly half the original gap.
4. `raft_write_many_batches` remains zero under 128/256 concurrent ordinary POSTs. Code inspection confirms that the group actor coalesces only `AppendBatch` commands; standard POST uses the single-append handler. The server's main batching lever is therefore unreachable to correctness-sensitive adapter writes because the current batch endpoint lacks producer deduplication and CAS headers.
5. Gateway metrics captured through the three-replica ClusterIP are invalid for before/after subtraction because the two samples can hit different pods. Negative gateway deltas from this run are discarded; future automation must aggregate every gateway replica.

At the measured peak and theoretical 100% occupancy, the shared-EKS ARM estimate normalizes to about `$0.025 / 1M` raw warm appends for Ursula, versus `$0.016 / 1M` for the RDS comparator. These are compute-only primitive costs, not Workflow step costs and not a low-volume tenant allocation.

### Raw cold-object evidence

The Ursula run uploaded 12,187,648 data bytes through 2,152 cold flushes. S3 contained:

| Object class | Count | Bytes | Average |
| --- | ---: | ---: | ---: |
| `.bin` payload | 2,152 | 12,187,648 | 5,663 B |
| `.idx` cold-index page | 2,151 | 705,713 | 328 B |
| Total | 4,303 | 12,893,361 | 2,996 B |

Across all objects, p50 was 305 B, p99 was 331 B, and the maximum was 8 MiB. The configured `flush_min_hot_size` is already 8 MiB, but once a Raft group's aggregate hot bytes reaches the group threshold, the fallback planner drains individual streams with a one-byte minimum. The benchmark's many 256-byte streams therefore become individual S3 objects.

Existing cold compaction cannot repair this distribution: it only combines two or more contiguous undersized chunks from the same stream. A short stream with one small chunk is never eligible. Reaching an approximately 8 MiB object target for Workflow requires a group-level cross-stream packfile (with per-stream segment offsets and shared-object GC), not another increase to the same-stream compaction target.

The attempted 10 MiB cold replay completed in 502 ms, but `cold_store_reads=0`; it hit the cold read cache populated by the writer and is not reported as an S3 cold-read result.

### User-visible latency

All values are milliseconds. Managed World is a public external baseline and was not run in this VPC.

| Scenario | Ursula avg / p90 / p99 | PostgreSQL avg / p90 / p99 | Public Managed avg |
| --- | ---: | ---: | ---: |
| No-op TTFS | 55.5 / 66 / 112 | 50.9 / 58 / 181 | 995.9 |
| Streaming TTFS | 50.7 / 56 / 130 | 46.5 / 47 / 208 | 1061.6 |
| Hook + stream TTFS | 91.0 / 94 / 245 | 54.5 / 60 / 91 | 1383.6 |
| Live stream latency | 16.2 / 12 / 175 | 5.5 / 8 / 10 | 128.2 |
| Text stream overhead | 8.2 / 10 / 11 | 8.6 / 10 / 14 | 195.8 |
| Structured stream overhead | 8.1 / 10 / 13 | 7.9 / 9 / 11 | 202.4 |
| 1020-step workflow total | 74,525 | 78,302 | 412,515 |

### Existing concurrency point

| Metric, `100 runs × 50 steps` | Ursula | PostgreSQL |
| --- | ---: | ---: |
| Logical steps | 5,000 | 5,000 |
| Makespan | 106.974 s | 129.714 s |
| Throughput | 46.7 steps/s | 38.5 steps/s |
| Run duration avg / p99 | 69.586 s / 106.692 s | 111.795 s / 128.868 s |
| TTFS avg / p99 | 30.647 s / 87.881 s | 1.972 s / 4.166 s |

Ursula completed the batch 21.3% faster, but its run-level fairness was much worse: many runs waited tens of seconds before their first step. Therefore `46.7 steps/s` is not automatically a useful production capacity. Capacity must be reported together with TTFS and run-duration SLOs.

### Backend work over the complete benchmark run

The complete Ursula run lasted 441 seconds and included every benchmark scenario, not only the concurrency test.

| Ursula counter | Delta |
| --- | ---: |
| Accepted appends | 34,047 |
| Applied mutations | 35,145 |
| Routed requests | 579,119 |
| Raft apply entries | 112,674 |
| Group engine execution | 37.222 CPU-seconds |
| Mutation apply | 26.097 CPU-seconds |
| Mailbox send wait | 2.528 seconds |
| `raft_write_many` batches | 0 |
| Gateway leader-cache hits / misses | 39,530 / 1,902 |

The gateway cache hit ratio was 95.4%. The negative `gateway_leader_redirect_ns` delta in the raw result indicates a cumulative-counter reset or subtraction problem and must not be used for latency attribution.

PostgreSQL committed 343,167 transactions and grew by 25,354,240 bytes over its complete run. It reported 3,351,311 buffer hits and only 11 block reads.

### Observed resource utilization

CloudWatch basic EC2 monitoring provides five-minute averages, so this is only a coarse cluster-level bound:

| Ursula voter | CPU average during the final 441 s run | Maximum five-minute sample |
| --- | ---: | ---: |
| voter 1 | 5.60% | 6.75% |
| voter 2 | 6.05% | 6.74% |
| voter 3 | 5.60% | 6.65% |

Across 12 provisioned vCPUs, the raw average is about 0.69 vCPU. Pre-run CPU was already approximately 5.3–5.7%, so the workload's incremental CPU cannot be separated reliably with five-minute EC2 metrics and may be very small.

RDS CPU averaged approximately 13.27% during its benchmark window. On the two-vCPU primary this is approximately 0.265 primary vCPU; AWS does not expose the hidden Multi-AZ standby as a second instance in this metric.

These observations prove that neither backend was CPU-saturated at the measured point. They do **not** prove that Ursula can be downsized linearly: a three-voter topology has a quorum/availability floor, and per-node memory, network, single-core group ownership, application dispatch, connection limits, or queue fairness may become the actual constraint before aggregate CPU.

## Sharded capacity results

The backend-capacity probe exports eight byte-identical workflow functions. Each function has its own workflow queue, so requests are distributed across eight queues while executing the same 20-step no-op workload. This separates total backend capacity from the deliberately adversarial single-hot-queue result above.

### 16 application replicas on four nodes

| Concurrent runs × 20 steps | Ursula throughput / TTFS p99 / run p99 | PostgreSQL throughput / TTFS p99 / run p99 |
| --- | ---: | ---: |
| 25 | 84.2 steps/s / 0.642 s / 5.910 s | 69.9 steps/s / 1.037 s / 7.126 s |
| 50 | 114.9 steps/s / 1.578 s / 8.561 s | 96.4 steps/s / 1.245 s / 10.321 s |
| 100 | 116.1 steps/s / 3.669 s / 17.047 s | 92.5 steps/s / 2.212 s / 21.447 s |
| 250 | 121.0 steps/s / 16.227 s / 40.605 s | 132.7 steps/s / 3.158 s / 36.637 s |
| 500 | 156.4 steps/s / 50.666 s / 60.019 s | 129.1 steps/s / 51.773 s / 76.564 s |

This produces two different, both valid, answers:

- Under a p99 TTFS ≤ 2 s SLO, the measured useful capacity is 114.9 steps/s for Ursula and 96.4 steps/s for PostgreSQL.
- Under a p99 TTFS ≤ 5 s SLO, the measured useful capacity is 116.1 steps/s for Ursula and 132.7 steps/s for PostgreSQL.
- At the 500-run stress point, Ursula delivers 21.1% more throughput and a 21.6% lower run-duration p99, while both systems violate an interactive TTFS SLO.

The PostgreSQL application tier initially exceeded RDS's connection budget with 16 replicas × 66 connections. The successful run used 30 workers and a 32-connection pool per replica, approximately 512 possible application connections. RDS observed 408–544 connections, 25.9–31.1% CPU, 1.95–3.99 ms write latency, and 69–285 write IOPS. This is a real operational scaling constraint even though database CPU remained available.

During the Ursula sweep, the 16 application containers consumed about 10.8–12.5 CPU cores and were CFS-throttled by another 2.5–3.7 core-equivalents. The three Ursula voters together consumed about 0.8–1.3 cores in the sampled windows and held about 3.8 GiB of memory. The application/runtime tier was the immediate limiter.

### 32 application replicas on eight nodes

| Concurrent runs × 20 steps | Ursula throughput / TTFS p99 / run p99 | PostgreSQL throughput / TTFS p99 / run p99 |
| --- | ---: | ---: |
| 500 | 261.3 steps/s / 31.079 s / 36.573 s | 248.0 steps/s / 24.846 s / 38.618 s |
| 1,000 | adapter queue enqueue contention; no clean run | 276.3 steps/s / 53.787 s / 70.216 s |
| 2,000 | not reached | 282.7 steps/s / 120.287 s / 138.210 s |

Scaling the Ursula application tier from 16 to 32 replicas raised the 500-run result from 156.4 to 261.3 steps/s, a 67.1% gain. The 32 application containers used 21.0–27.7 cores with 4.3–6.3 core-equivalents of throttling. Ursula's three voters used 1.75–2.50 aggregate cores and 4.0–4.6 GiB during the sampled intervals. The current 12-vCPU voter topology was therefore only about 21% CPU-utilized at the busiest observed interval.

PostgreSQL reached 282.7 steps/s at 2,000 runs, but throughput improved only 2.3% over the 1,000-run level while TTFS p99 more than doubled. RDS CPU peaked at 61.7% in the one-minute samples, database connections reached 576, and write latency ranged from 1.84 to 11.08 ms. Its 32 application containers used up to 29.8 cores and were heavily throttled.

The backend ceiling is still not isolated perfectly:

- Both 32-replica runs nearly exhaust their 32 application CPU limits.
- Ursula fails admission at 1,000 runs because one of the eight queue journals remains contended during enqueue. This is an adapter queue-CAS ceiling, not a Raft or S3 capacity result.
- PostgreSQL can buffer more work, but its 1,000- and 2,000-run latency is not useful for interactive workflows.
- `raft_write_many_batches` remained zero, so even this workload did not reach Ursula's mailbox batch path.

## Cost interpretation

Pricing inputs and exclusions are unchanged from the [same-EKS comparison](./2026-07-26-eks-comparison.md): application compute is common and excluded; S3 requests, retained bytes, cross-AZ transfer, backups, and operations are separate line items. Fixed cost is normalized against the p99 TTFS ≤ 5 s useful-capacity points above, not the latency-violating stress ceiling.

The current Ursula topology costs approximately `$505.48/month` dedicated (`$420.48` compute + `$12` node storage + `$73` EKS control plane), or `$432.48/month` when the EKS control plane is already paid for. RDS Multi-AZ costs approximately `$269.01/month`.

The measured Ursula peak of 2.50 aggregate voter cores and 4.6 GiB does not justify charging the workload for twelve continuously busy vCPUs. A three-voter `m7g.large` estimate provides six aggregate vCPUs and 24 GiB while preserving the three-AZ quorum topology. It costs approximately `$263.70/month` dedicated or `$190.70/month` on shared EKS. This is a sizing estimate, not a benchmark of that instance type; it must be validated before becoming a deployment recommendation.

### Fixed cost per 100,000 useful steps

| Backend/topology | 30% utilization | 60% utilization | 80% utilization |
| --- | ---: | ---: | ---: |
| Ursula current x86, dedicated EKS | $0.552 | $0.276 | $0.207 |
| Ursula current x86, shared EKS | $0.472 | $0.236 | $0.177 |
| Ursula estimated 3 × `m7g.large`, dedicated EKS | $0.288 | $0.144 | $0.108 |
| Ursula estimated 3 × `m7g.large`, shared EKS | $0.208 | $0.104 | $0.078 |
| RDS Multi-AZ `db.m7g.large` | $0.257 | $0.129 | $0.096 |
| Managed World step charge | $2.500 | $2.500 | $2.500 |

At Ursula's 100-run useful-capacity point, 347 data uploads were issued for 2,000 logical steps. The earlier estimate treated each data-upload counter increment as the only PUT and reported `$0.087 / 100k` at `$0.005 / 1,000 PUTs`. The raw S3 inventory proves that nearly every `.bin` upload also rewrites one `.idx` page. The corrected request-cost lower bound is therefore approximately `$0.174 / 100k`, before snapshots, object versions, and other metadata writes. The uploaded data objects averaged only 20.8 KiB, so request cost is material and cross-stream packing is required.

At 60% utilization, including that observed PUT ratio:

| Backend/topology | Fixed | Observed PUT estimate | Current total |
| --- | ---: | ---: | ---: |
| Ursula current x86, shared EKS | $0.236 | ≥$0.174 | ≥$0.410 |
| Ursula estimated 3 × `m7g.large`, shared EKS | $0.104 | ≥$0.174 | ≥$0.278 |
| Ursula estimated 3 × `m7g.large`, dedicated EKS | $0.144 | ≥$0.174 | ≥$0.318 |
| RDS Multi-AZ | $0.129 | included in provisioned gp3 baseline for this run | $0.129 plus backups/transfer |
| Managed World | $2.500 | storage/function charges excluded | at least $2.500 |

The defensible conclusion is not that Ursula is already universally cheaper than PostgreSQL. It is:

1. The earlier “three nodes make Ursula much more expensive” comparison materially overcharged unused x86 capacity.
2. The three-voter ARM topology is now validated for raw primitives, but its Workflow capacity remains projected from the x86 sweep; the projection makes fixed cost competitive with RDS, especially on shared EKS.
3. Today, data plus index-page PUTs add at least `$0.174 / 100k` and keep the total above this RDS comparator at the p99 TTFS ≤ 5 s capacity point.
4. Ursula's strongest measured result is its stress throughput and long-history behavior; its most urgent production blockers are queue admission/fairness and cold-object packing.
5. Managed World's `$2.50 / 100k` step charge remains an order of magnitude above either self-hosted backend before application compute and operations are considered.

## Current interpretation

### Confirmed

- The previous fixed-cost comparison charged all 12 Ursula vCPUs to a workload that used only a small fraction of them.
- The previous `$0.412 per 100k steps` Ursula and `$0.266 per 100k steps` PostgreSQL figures assumed the single observed throughput point was continuous maximum capacity. That assumption is unproven, so those figures are withdrawn as capacity-normalized results.
- Ursula already has a real throughput advantage at the tested point, but the very high concurrent TTFS shows that throughput alone hides a scheduling/fairness problem.
- PostgreSQL's smaller provisioned topology is also lightly loaded. A fair conclusion cannot be “Ursula is expensive because it has three nodes” without measuring how much useful load those nodes sustain.
- `raft_write_many=0` means the tested workload did not exercise Ursula's server-side mailbox batching path.
- Raw primitive isolation shows that Ursula already beats PostgreSQL for live durable delivery p50 and retained replay, but loses sequential append latency and warm append throughput.
- Ordinary POST appends cannot reach the current `AppendBatch` coalescing handler. This is an implementation/API gap, not evidence that Raft cannot batch.
- The cold request-cost model previously omitted the near-1:1 `.idx` PUT paired with each `.bin` PUT.
- Same-stream cold compaction cannot solve the dominant short-stream object distribution.

### Working hypotheses, not yet proven

1. Extending the batch endpoint with producer deduplication and per-entry CAS, then adopting it in the adapter, should close part of the 2.2–2.6× warm-write throughput gap.
2. Deterministic gateway routing or a complete group-leader map should primarily improve new-stream and cold-route traffic; it cannot by itself explain the remaining warm-append gap.
3. Ursula's useful Workflow capacity may be reached first by a TTFS/fairness SLO violation rather than by aggregate voter CPU saturation.
4. A group-level packfile should reduce S3 PUT cost by orders of magnitude, but its read amplification, shared-object GC, and failure atomicity must be benchmarked rather than assumed.

### Single hot-queue capacity probe

The clean probe used eight application replicas on two isolated `m6i.xlarge` app nodes and a fresh Ursula bucket. Every run used the same workflow name and therefore the same workflow queue. This is a useful hot-key/burst test, not a measurement of total Ursula Raft-group capacity.

| Concurrent runs × 20 steps | Ursula throughput / TTFS p99 / run p99 | PostgreSQL throughput / TTFS p99 / run p99 |
| --- | ---: | ---: |
| 25 | 50.3 steps/s / 1.724 s / 9.885 s | 56.4 steps/s / 1.495 s / 8.855 s |
| 50 | 70.6 steps/s / 11.587 s / 13.895 s | 57.5 steps/s / 1.075 s / 17.247 s |
| 100 | 78.2 steps/s / 21.601 s / 25.334 s | 35.9 steps/s / 6.520 s / 55.730 s |
| 250 | enqueue contention, no clean result | 65.1 steps/s / 8.866 s / 74.318 s |
| 500 | not reached | client `fetch failed`, no clean result |

This proves that the old 46.7 steps/s point was not Ursula's maximum throughput. It also identifies a production-facing hot-key ceiling: at an instantaneous 250-run burst, Ursula's `queue()` returned HTTP 500 because the shared workflow queue remained contended during enqueue. PostgreSQL admitted 250 but the 500-run burst failed at the application/client boundary.

The curves are non-monotonic because the application and queue scheduling layers are saturated before either backend. Cgroup samples show the eight application containers consuming 6.1–6.9 CPU cores during the Ursula run and 5.9–6.4 cores during the loaded PostgreSQL windows, with substantial CFS throttling. By contrast, all three Ursula voters together consumed only about 0.69–0.77 core during the useful loaded windows. RDS CPU was only 18–19%, while database connections rose to 514–544, almost exactly the eight application pools' configured total. These are application/adapter ceilings, not database CPU ceilings.

The next probe therefore uses more app nodes and 16 app replicas, and distributes identical runs over eight distinct workflow names/queues. The one-queue numbers remain valuable as the user-visible hot-key test.

## Capacity sweep methodology

The benchmark harness uses an isolated `BENCH_CAPACITY_ONLY=1` mode. It runs increasing concurrent workflow counts with a fixed step count and records an exact time window plus backend counters for every level.

Initial levels:

```text
25, 50, 100, 250, 500 concurrent runs
20 sequential no-op steps per run
```

If throughput is still rising and resource/SLO limits are not reached at 500, extend to 1,000 and 2,000 runs.

For every level, retain:

- logical steps and wall-clock window;
- makespan and steps/s;
- run duration avg, p90, and p99;
- TTFS avg, p90, and p99;
- Ursula or PostgreSQL backend counter deltas;
- app pod CPU/memory;
- voter pod and node CPU/memory, or RDS CPU/connections/IOPS/latency;
- failures, retries, timeouts, and queue backlog if exposed.

Candidate useful-capacity SLOs must be published with the result. The first pass will show at least:

- throughput-max capacity;
- capacity with p99 TTFS ≤ 5 seconds;
- capacity with p99 run duration ≤ 2× the low-concurrency duration;
- zero-error capacity.

The exact SLO can be changed for product positioning, but no cost figure may omit it.

## Cost model to produce

For each backend and selected SLO:

```text
monthly useful steps = measured useful steps/s × 2,628,000 s/month × target utilization
fixed cost per 100k steps = monthly fixed backend cost / monthly useful steps × 100,000
variable cost per 100k steps = storage requests + storage bytes + cross-AZ bytes + backup/IO charges
```

Report at multiple utilization assumptions, at minimum 30%, 60%, and 80%. A continuously saturated 100% figure may be shown only as a theoretical ceiling.

Cost views:

1. Dedicated Ursula cluster including the EKS control plane.
2. Ursula on an already-paid shared EKS control plane.
3. Current x86 topology.
4. Clearly marked Graviton/smaller-node estimate after resource headroom is measured.
5. RDS Multi-AZ measured comparator.
6. Managed World at the public `$2.50 / 100k steps`, with function duration, storage, and observability listed separately where public prices allow.

S3 PUTs, retained bytes, cross-AZ Raft traffic, and RDS storage/IO must remain separate line items. The prior observed Ursula cold-flush sample averaged only about 12.9 KiB per uploaded object; it is useful as a warning about request cost, not yet a steady-state value for this capacity run.

## Next steps

- [x] Establish the same-EKS Ursula and PostgreSQL functional/latency baseline.
- [x] Recover coarse CloudWatch CPU for both completed runs.
- [x] Identify that the previous cost-per-throughput calculation used an unsaturated load point.
- [x] Finish the capacity-only harness and type-check it.
- [x] Commit and merge the harness through GitHub; use Depot-published `main-ursula` and `main-postgres` images.
- [x] Restore isolated application capacity and RDS Multi-AZ temporarily.
- [x] Run identical Ursula and PostgreSQL sweeps with exact per-level time windows.
- [x] Sample app and backend resources during every level.
- [x] Extend concurrency until throughput plateaus, errors appear, or the selected latency SLO is crossed.
- [x] Attribute the first limiting resource/path using backend counters and resource samples.
- [x] Recalculate cost per 100,000 useful steps at 30%, 60%, and 80% utilization.
- [x] Update this investigation report and keep raw JSON/resource samples beside it.
- [ ] Benchmark Workflow capacity on the 3 × `m7g.large` Ursula topology instead of treating it as a linear sizing estimate.
- [x] Isolate raw create, append, live-delivery, and replay primitives from the World adapter.
- [x] Correct the S3 request model to include cold index-page PUTs.
- [ ] Extend append-batch with producer deduplication and per-entry CAS, adopt it in the adapter, and rerun warm append.
- [ ] Add a cross-stream group packfile targeting approximately 8 MiB objects, including safe shared-object GC.
- [ ] Aggregate gateway metrics replica-by-replica and rerun route attribution.
- [ ] Force a cold-cache miss via leadership transfer and measure real S3 replay.
- [ ] Remove the queue enqueue-CAS admission ceiling and rerun the 1,000/2,000-run Ursula levels.
- [ ] Reduce cold PUTs toward 8 MiB objects and rerun the request-cost measurement.
- [ ] Add cross-AZ byte accounting and an operations/backup cost sensitivity.
- [ ] Publish the final comparison after the right-sized topology and queue fix are measured.
- [ ] Destroy the current raw-benchmark RDS and temporary application node after the first optimization target is chosen; verify the canary returns to 3/3 ready Ursula voters.

## Evidence

- [`ursula-v038-final.json`](./ursula-v038-final.json)
- [`postgres-rds-multi-az.json`](./postgres-rds-multi-az.json)
- [`ursula-v038-capacity.json`](./ursula-v038-capacity.json)
- [`postgres-rds-capacity.json`](./postgres-rds-capacity.json)
- [`ursula-v038-capacity-sharded.json`](./ursula-v038-capacity-sharded.json)
- [`postgres-rds-capacity-sharded.json`](./postgres-rds-capacity-sharded.json)
- [`ursula-v038-capacity-scale.json`](./ursula-v038-capacity-scale.json)
- [`postgres-rds-capacity-scale.json`](./postgres-rds-capacity-scale.json)
- [`2026-07-26-eks-comparison.md`](./2026-07-26-eks-comparison.md)
