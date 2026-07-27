# Workflow backend capacity and cost investigation

Last updated: 2026-07-27

Status: the Workflow-level capacity sweep, raw-storage isolation benchmark, high-cardinality server follow-ups, and the first structural Workflow-runtime pass are complete. Separating request-serving pods from queue dispatchers raised the median complete-Workflow result to 666.7 steps/s for Ursula versus 428.6 steps/s for PostgreSQL, a 1.556× advantage, with lower run and TTFS p99 on the same eight application cores. This meets the performance gate at the measured 500-run stress point. Fixed Ursula backend cost normalizes approximately 54% below the RDS comparator and current packed cold objects are approximately 8 MiB, but total cost is not signed off: exact cross-AZ traffic and snapshot/reference churn can still consume the cost margin. The active server change makes identical per-voter Raft snapshots one compressed, content-addressed S3 object before the comparison is repeated.

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

## 2026-07-27 structural Workflow rerun

This rerun supersedes the earlier complete-Workflow comparison, but does not replace the historical primitive and failure-analysis sections below. It used Ursula 0.3.22 on three `m7g.large` voters across three availability zones with memory WAL, S3 cold storage, and 256 Raft groups. Both backends used eight one-core application pods spread four-per-node over the same two `m7g.xlarge` ARM workers. PostgreSQL remained RDS PostgreSQL 17 Multi-AZ on `db.m7g.large` with 100 GiB gp3.

The measured path includes the structural changes accumulated through `workflow-world-ursula` PRs #64–#72:

- one owned logical step commits its run events and entity deltas in one authoritative Ursula append;
- the benchmark tree-shakes the Workflow VM bundle from 624,177 bytes to 32,584 bytes without changing replay semantics;
- hot run-journal reads slice the requested event page instead of cloning the entire retained event history;
- idempotent reads and producer-deduplicated writes retry temporary leader-unknown `503` responses;
- Ursula JSON decoding uses native `JSON.parse` plus an object-only tagged-value walk instead of invoking a reviver callback for every primitive.

Each sample below is an independent no-profiler `500 runs × 50 sequential steps` job on the same warmed application and backend topology. Reporting the median of three jobs avoids selecting the previous 636.7 steps/s high sample as the headline.

| Complete Workflow metric | Ursula samples | PostgreSQL samples | Median comparison |
| --- | ---: | ---: | ---: |
| Throughput | 524.7, 584.8, 609.6 steps/s | 402.2, 428.6, 472.6 steps/s | **584.8 vs 428.6; Ursula 1.364×** |
| Run-duration p99 | 40.081, 41.343, 45.749 s | 52.103, 57.314, 60.879 s | **41.343 vs 57.314 s; Ursula 27.9% lower** |
| TTFS p99 | 26.753, 27.650, 33.718 s | 46.271, 46.625, 52.327 s | **27.650 vs 46.625 s; Ursula 40.7% lower** |
| Backend state transitions | 28,533–28,627 accepted appends | 212,137–218,771 committed transactions | approximately 1.14 appends vs 8.49 transactions per logical step |

The p99 goal is met at this stress point. The throughput goal is not: 1.5× the PostgreSQL median is 642.9 steps/s, so Ursula still needs another 9.9% over its measured median without worsening tails. Increasing physical queue partitions from 8 to 32 was tested and rejected: it delivered 521.2 steps/s with 46.317 s run p99 and 40.786 s TTFS p99 because every application replica opened more partition watchers.

CPU profiles show why more Raft parameter tuning is unlikely to close this gap. Ursula's storage mutation path is already faster, and one logical step is already approximately one append. The remaining wall time is dominated by Workflow VM/replay scheduling, garbage collection, application `runMicrotasks`, JSON decoding, and duplicated dispatcher/watch activity. The next structural experiment is explicit partition ownership or a smaller dedicated dispatcher set so that eight application replicas do not all maintain every queue-partition long poll.

### Dedicated dispatcher sweep

PR #74 added an explicit `dispatcherEnabled` role without changing the public World contract. All configurations below retain eight one-core application pods and the same `500 runs × 50 steps` workload; only the number of pods that own queue watchers and claims changes. Each cell is the median of three independent no-profiler jobs.

| Queue dispatchers + request-only pods | Throughput | Run-duration p99 | TTFS p99 |
| --- | ---: | ---: | ---: |
| 8 + 0, original topology | 584.8 steps/s | 41.343 s | 27.650 s |
| 2 + 6 | 603.6 steps/s | 40.434 s | 37.961 s |
| 4 + 4 | 619.3 steps/s | 39.074 s | 32.743 s |
| **5 + 3** | **666.7 steps/s** | **36.299 s** | **31.846 s** |

Two dispatchers reduce duplicate watchers most aggressively but concentrate claims enough to regress TTFS. Five dispatchers preserve work stealing while removing three full sets of queue watchers; its three throughput samples were 550.7, 676.1, and 666.7 steps/s. The median is 1.556× PostgreSQL's 428.6 steps/s, run p99 is 36.7% lower than PostgreSQL's 57.314 s, and TTFS p99 is 31.7% lower than PostgreSQL's 46.625 s. This is the first complete-Workflow configuration to meet the 1.5× throughput and lower-p99 gate, but the low first sample means it must be reproduced after the snapshot-cost change rather than treated as a final release result.

### Current per-million-step cost boundary

This is an always-busy shared-service normalization, not a per-tenant fixed-cost allocation. It excludes the identical application tier.

| Backend | Monthly fixed backend | Median throughput | Fixed cost / 1M steps |
| --- | ---: | ---: | ---: |
| Ursula, shared EKS, `3 × m7g.large` | $190.70 | 666.7 steps/s | **$0.109** |
| RDS Multi-AZ `db.m7g.large` + 100 GiB gp3 | $269.01 | 428.6 steps/s | **$0.239** |

The fixed Ursula component is 54.4% lower. Across the combined 75,000-step Ursula measurement window, the cold counters advanced by two physical uploads and 16,550,524 bytes while publishing 202 logical stream slices. The two shared objects averaged 8,275,262 bytes, matching the intended approximately 8 MiB pack target. Normalized to one million steps, that observed window is approximately 26.7 physical PUTs, 221 MB of retained payload, about `$0.00013` in PUT charges, and about `$0.005` for the first retained month. Flush is asynchronous—the individual job deltas were zero, two, and zero uploads—so this combined window is evidence for object shape and order of magnitude, not an exact per-job bill.

On that limited evidence, fixed backend plus packed payload is approximately `$0.114 / 1M` steps before snapshot/reference writes, reads, cross-AZ transfer, backups, and operations, versus `$0.239 / 1M` for the RDS baseline. Ursula must remain below `$0.167 / 1M` to satisfy the 30% total-cost target, leaving about `$0.053 / 1M` for the unmeasured items. An initial S3 inventory found that every voter uploaded the same full Raft snapshot, while an initial packet sample found material 256-group heartbeat traffic even when the workload was idle. These are now explicit blockers rather than exclusions: the server must share compressed snapshot objects, and the next benchmark must measure equal-window loaded and idle cross-AZ bytes before total cost is signed off.

### Immediate next gates

1. Merge and deploy content-addressed compressed Raft snapshots so identical per-voter snapshots produce one physical S3 object.
2. Repeat the exact three-job five-dispatcher Ursula comparison and require at least 642.9 median steps/s with no run-p99 regression.
3. Instrument equal-window idle and loaded cross-AZ bytes plus exact snapshot/reference objects; require total Ursula backend cost below `$0.167 / 1M` steps.
4. Upstream the `@workflow/world` side-effect metadata fix so the 94.8% VM-bundle reduction is not benchmark-local.

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

### Standard-append coalescing follow-up: Ursula 0.3.9

Ursula 0.3.9 lets one Raft-group actor collect already-queued ordinary POST appends and commit them as one `GroupWriteCommand::Batch`. Each request keeps its own producer deduplication, CAS, close, and stream-sequence semantics; a single append stays on the original no-wait path. The release was deployed to the same three `m7g.large` ARM voters and rerun from the same isolated application worker. PostgreSQL was rerun immediately afterward, not concurrently.

The first post-upgrade Ursula run was affected by cold gateway leader state. The table uses the second stable Ursula run and the new PostgreSQL run:

| Primitive | Ursula 0.3.9 | PostgreSQL | Result |
| --- | ---: | ---: | --- |
| Sequential append p50 / p99 | 3.45 / 5.42 ms | 2.38 / 9.00 ms | PostgreSQL lower p50; Ursula lower p99 in this sample |
| Warm append, 32 concurrency | 3,020 ops/s | 6,425 ops/s | PostgreSQL 2.13× throughput |
| Warm append, 128 concurrency | 3,084 ops/s | 6,550 ops/s | PostgreSQL 2.12× throughput |
| Warm append, 256 concurrency | 3,173 ops/s | 6,645 ops/s | PostgreSQL 2.09× throughput |
| Live durable write-to-read p50 / p99 | 4.30 / 7.37 ms | 5.14 / 6.38 ms | Ursula lower p50; PostgreSQL lower p99 |
| 1 MiB retained replay | 7.64 ms | 13.97 ms | Ursula 45.4% faster |

Relative to the 0.3.8 sample, Ursula's warm throughput changed by +20.1% at concurrency 32, +6.6% at 128, and +26.1% at 256. This is a measured run-to-run improvement, but it is not evidence that coalescing caused the whole change:

- only 9 `raft_write_many` calls carrying 21 logical commands appeared across 3,688 accepted appends, so only 0.57% of appends reached the new multi-command path;
- the 21 logical commands saved only 12 Raft entries, far too little to explain the full throughput delta;
- gateway leader state, cluster cache warmth, S3 flush overlap, and ordinary run variance remain confounders;
- the stable result still gives PostgreSQL a consistent approximately 2.1× warm-write throughput advantage.

The trigger rate is low for a structural reason. The benchmark spreads 512 independent requests over 256 Raft groups: at concurrency 128 there are only 0.5 in-flight requests per group on average. The actor drains a command immediately, so two requests rarely coexist in the same group mailbox. More code on the zero-wait drain path will not make batching common.

The next server experiment must therefore be explicit and falsifiable:

1. add an adaptive, bounded coalescing window only when a group/core mailbox already shows contention; do not add delay to the uncontended single-append path;
2. sweep a small set of windows, such as 25, 50, 100, and 200 microseconds;
3. report sequential p50/p99, warm c32/c128/c256 throughput, batch-size distribution, and logical-commands-per-Raft-entry;
4. reject the change if it does not materially raise the batch ratio or if it harms sequential p99 beyond the stated budget;
5. separately add a hot-group workload, because the current independent-stream test measures broad distribution rather than the queue/run-journal contention that Workflow can create.

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

## ARM canary rerun after queue correctness fixes

The projected `3 × m7g.large` Ursula topology is now measured rather than extrapolated. The canary ran Ursula 0.3.13 with 256 groups, memory WAL, and S3 cold storage. Both backends used eight application replicas on the same two `m7g.xlarge` ARM nodes, ran serially, and executed the same `25,50,100,250,500,1000` run sweep with 20 steps per run and eight workflow queues. PostgreSQL used 30 workers and a 32-connection pool per application replica; the initial 64/66 setting reached 544 RDS connections and made every application readiness probe fail at 500 runs even though RDS CPU was only 26.5%.

Two adapter fixes were required before the clean run:

- Queue contention now refreshes only the missing journal suffix instead of discarding the cache and rebuilding from a checkpoint after every 412.
- A local mutation freezes its optimistic record tail before waiting behind another local append. This prevents a stale lease or ack transition from adopting the newer tail and committing after an earlier mutation removed the referenced message.

The second defect was discovered by the first ARM rerun: it failed at 100 runs with `Queue transition references missing message`. The regression test reproduces concurrent ack and lease extension on one journal, and the fixed image subsequently completed both the single-queue and eight-queue 1,000-run levels.

### Eight-queue capacity

| Concurrent runs × 20 steps | Ursula throughput / TTFS p99 / run p99 | PostgreSQL throughput / TTFS p99 / run p99 |
| --- | ---: | ---: |
| 25 | 58.0 steps/s / 0.838 s / 8.587 s | 69.2 steps/s / 0.372 s / 7.072 s |
| 50 | 91.4 steps/s / 1.557 s / 10.852 s | 94.3 steps/s / 0.643 s / 10.385 s |
| 100 | 93.4 steps/s / 4.086 s / 20.918 s | 81.0 steps/s / 1.466 s / 24.307 s |
| 250 | 111.2 steps/s / 29.043 s / 44.302 s | 97.8 steps/s / 39.145 s / 48.852 s |
| 500 | 115.3 steps/s / 62.274 s / 82.183 s | 96.3 steps/s / 76.133 s / 96.891 s |
| 1,000 | 116.1 steps/s / 147.652 s / 160.959 s | 93.1 steps/s / 160.732 s / 196.831 s |

At 1,000 runs, Ursula delivered 24.7% more throughput and an 18.2% lower run-duration p99. At 500 runs it delivered 19.7% more throughput and a 15.2% lower run-duration p99. PostgreSQL still has the better low-load TTFS, while Ursula's advantage appears under sustained concurrent pressure.

The one-hot-queue probe also completed all levels after the correctness fix:

| Concurrent runs × 20 steps | Steps/s | TTFS p99 | Run p99 |
| --- | ---: | ---: | ---: |
| 25 | 70.1 | 1.723 s | 7.022 s |
| 50 | 66.5 | 11.211 s | 14.852 s |
| 100 | 69.8 | 23.980 s | 27.963 s |
| 250 | 104.1 | 43.674 s | 46.840 s |
| 500 | 102.4 | 87.993 s | 94.887 s |
| 1,000 | 103.8 | 171.859 s | 178.580 s |

This confirms that the old admission failure was removed, but a single workflow queue remains a deliberate hot-key workload with poor fairness. Eight queues improve the 1,000-run throughput by 11.8% and run p99 by 9.9%.

### Measured backend utilization

The three Ursula voter instances averaged 19.1%, 21.3%, and 21.0% CPU in the first five-minute loaded sample and 13.1%, 15.7%, and 14.7% in the second partially loaded sample. Their post-run idle baseline was approximately 6.8–7.1%. Ursula therefore still had substantial voter CPU headroom; the eight-replica application/dispatcher tier capped this curve before the backend.

The clean PostgreSQL run held 247–272 database connections and used 25.7–34.5% primary CPU. The failed 66-connection-pool run reached 544 connections. This is a material operational difference: Ursula's queue fix admits the 1,000-run burst without a per-process connection budget, while PostgreSQL requires explicit pool sizing.

### Per-million-step backend cost

This table is backend-only: the two shared ARM application nodes are common to both self-hosted runs and excluded. Prices are current us-east-1 on-demand catalog prices captured with the run: `m7g.large = $0.0816/hour`, PostgreSQL Multi-AZ `db.m7g.large = $0.337/hour`, and Multi-AZ gp3 storage `100 GiB × $0.23/GiB-month`. The fixed component is divided by the measured 1,000-run steady throughput, so the unit price is constant and does not increase when one customer sends fewer steps.

| Backend | Fixed compute/storage per 1M steps | Request cost per 1M steps | First-month retained bytes | Measured first-month total |
| --- | ---: | ---: | ---: | ---: |
| Ursula 0.3.13, shared EKS, `3 × m7g.large` | $0.586 | $2.265 data/index PUTs + $0.046 snapshot PUTs | 3.91 GB, about $0.090 | **$2.986** |
| Ursula 0.3.14/0.3.15 group packing, same topology | $0.581 | $0.062 all PUTs | 2.43 GB, about $0.056 | **$0.699** |
| PostgreSQL Multi-AZ `db.m7g.large` + 100 GiB gp3 | $1.099 | Included in provisioned gp3 baseline | Included in the fixed 100 GiB allocation | **$1.099** plus backup/transfer |
| Managed World public step charge | — | — | Storage and function charges excluded | **$25.00** |

The 0.3.13 baseline beat PostgreSQL on the measured backend compute component by 46.7%, but lost total cost by 2.7× because of S3 PUT amplification. Its eight-queue sweep produced 38,500 logical steps and 17,440 versioned cold objects: 8,720 `.bin` PUTs and 8,720 `.idx` PUTs. Snapshot activity added 352 PUTs. The retained `.bin` objects averaged only 14.8 KiB (`p50 13.5 KiB`, `p90 31.5 KiB`, `p99 59.6 KiB`), so same-stream compaction could not solve the dominant short-stream distribution.

Ursula 0.3.14 replaced the snapshot driver's forced partial cold flushes with group-scoped packing. The same 38,500-step workload ran at 117.1 steps/s with a 159.445-second 1,000-run p99, versus 116.1 steps/s and 160.959 seconds on 0.3.13. During the exact job window, S3 recorded no `.bin` or `.idx` versions: the 129 MB workload was spread over 256 groups and no group reached the 8 MiB pack threshold. The bounded hot tails were instead persisted in 239 group snapshots plus 239 reference updates:

| 0.3.14 workload object class | Versions | Bytes | Average | p50 | p90 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Snapshot | 239 | 93,483,227 | 391.1 KiB | 354.9 KiB | 698.2 KiB | 1.03 MiB |
| Reference | 239 | 38,843 | 163 B | 163 B | 163 B | 164 B |

That is 478 PUTs rather than 17,792, a 97.3% reduction. At the same public S3 prices it is `$0.062/M` steps in PUTs and 2.43 GB, or about `$0.056/M` steps for the first retained month. Including measured ARM voter compute, the backend total is approximately `$0.699/M` steps, 36.4% below the `$1.099/M` RDS comparator before the exclusions shared by this table.

A same-group canary then forced the actual pack path with 48 streams and 12 MiB of payload. Ursula 0.3.14 produced one 8 MiB pack but also 32 tiny `.idx` pages. The cause was a production-path mismatch: the in-memory engine skipped index pages for shared slices, while the OpenRaft engine wrote them unconditionally. Ursula 0.3.15 fixed that path and added a three-node regression test. The repeated canary, combined with the previous group's 4 MiB hot tail, produced exactly two 8 MiB packs, zero `.idx` objects, and successful readback from every sampled stream.

This validates both intended regimes: sparse groups pay bounded snapshot writes instead of one PUT per tiny stream, while busy groups converge on 8 MiB cross-stream data objects without per-slice index PUTs. The remaining storage-cost question is whether long-running snapshot and reference version churn stays bounded under continuous mixed load; it needs a multi-hour soak rather than another short capacity run.

### High-cardinality snapshot and distributed raw-write follow-up: Ursula 0.3.18–0.3.22

The first four-generator raw run exposed a state-cardinality bug: every append scanned all streams in its Raft group to recompute hot payload bytes. Ursula 0.3.18 replaced that scan with an incrementally maintained gauge. At approximately 524,000 streams the conservative warm-append floor improved from about 5.91k to 8.52k operations/s, but fixed 5,000-log automatic snapshots still reduced fresh-to-aged throughput by approximately 18% and raised p99 to 458–467 ms.

Disabling snapshots was diagnostic, not a production configuration: it produced 10.57k aged operations/s and 59–79 ms p99, proving that the remaining collapse came from full-state snapshot work rather than the append or Raft commit path. Raising the fixed threshold to 20,000 logs made snapshots less frequent but larger and synchronized; aged throughput fell to 8.01k operations/s and p99 remained 461–466 ms.

Ursula 0.3.21 moved inline snapshot frame encoding and assembly off the async worker. This raised the aged floor to 9.16k operations/s and reduced p99 to 277–436 ms, but all groups and replicas still crossed the same threshold together. Ursula 0.3.22 keeps 5,000 logs as the minimum while deterministically spreading each group/replica threshold over `[5,000, 10,000)`. The same fresh/aged pair then produced:

| 4 × 128 raw workload | Conservative global warm throughput | Per-generator warm p50 | Per-generator warm p99 |
| --- | ---: | ---: | ---: |
| Ursula 0.3.22 fresh, approximately 262k streams | 11.72k ops/s | 35–41 ms | 87–108 ms |
| Ursula 0.3.22 aged, approximately 524k streams | 11.51k ops/s | 31–40 ms | 132–139 ms |
| RDS PostgreSQL 17.9 Multi-AZ, fresh table/run IDs | 8.46k ops/s | 56–57 ms | 135–148 ms |

The conservative global value is four times the slowest generator rather than the sum of independently completed rates. On warm append, aged Ursula is 36.1% faster than PostgreSQL, has materially lower p50, and has comparable p99. Fresh-to-aged Ursula throughput now falls only 1.8%.

New-stream throughput is much closer: Ursula delivered approximately 8.40k operations/s versus PostgreSQL's 8.28k, but Ursula's create p99 was 303–316 ms versus 135–143 ms for PostgreSQL. Snapshot staggering therefore removes the broad warm-write regression; it does not solve new-stream tail latency.

This comparison is intentionally a high-load primitive test, not a complete durability-cost claim. Ursula used three `m7g.xlarge`-hosted voters with memory WAL, inline snapshots, 32 Raft groups, and cold storage disabled. The four generators shared those nodes, while the PostgreSQL generators used the same four nodes after the Ursula load ended. RDS used a 32-connection pool per generator and reached 51.6% primary CPU in the one-minute CloudWatch sample. The co-location makes the Ursula throughput result conservative for application CPU interference, but memory WAL plus disabled cold storage means the result cannot price production retention.

At the agreed symmetric 40% shared-compute allocation, current public inputs produce nearly tied raw compute economics:

| Raw warm append cost view | Ursula 0.3.22 | RDS PostgreSQL |
| --- | ---: | ---: |
| Allocated hourly compute | `3 × $0.1632 × 40% = $0.1958` | `$0.337 × 40% = $0.1348` |
| Compute per 1M warm appends | $0.0047 | $0.0044 |
| Full 100 GiB RDS storage allocation per 1M | n/a | $0.0010 |
| Compute plus that fixed storage baseline | $0.0047 before S3/retention | $0.0055 |

The compute-only difference is about 7%, in PostgreSQL's favor; charging the comparator's full 100 GiB baseline makes Ursula about 14% cheaper before Ursula S3, cross-AZ, and retained-byte costs. The honest conclusion is cost parity at this primitive layer, not a decisive Ursula win. Production Workflow cost still requires rerunning the six user-facing scenarios with cold storage enabled and accounting for packed S3 PUTs, retained bytes, and snapshot/reference churn.

### Queue coordination experiments on the production memory-WAL topology

The production target is three memory-WAL voters across three availability zones with S3 cold storage and snapshots. Disk WAL and local fsync are not part of this service contract. The next Workflow experiment therefore reused that canary topology, eight ARM application replicas on two `m7g.xlarge` nodes, eight workflow queues, and a fresh bucket per variant.

The measured 0.3.14 baseline emitted approximately 2.32 accepted appends per logical step while the voters remained lightly utilized. Its 1,000-run admission failure came from queue-tail CAS exhaustion in the adapter rather than Raft, CPU, or S3. Two attempts to amortize dispatcher claims were net-negative: an unbounded claim batch concentrated delivery ownership in one process, and a four-message bounded batch still weakened cross-replica work stealing. Both changes were reverted.

The next experiment treated enqueue as a commutative append while retaining strict tail CAS for lease, ack, retry, and extension:

| Concurrent runs × 20 steps | Guarded enqueue baseline | Commutative enqueue | Result |
| ---: | ---: | ---: | --- |
| 25 | 54.2 steps/s | 78.6 steps/s | low-load improvement |
| 50 | 91.6 steps/s | 87.2 steps/s | approximately tied |
| 100 | 83.9 steps/s | 84.0 steps/s | tied |
| 250 | 108.5 steps/s | 95.2 steps/s | regression |
| 500 | 117.7 steps/s | 97.4 steps/s | regression |
| 1,000 | 117.1 steps/s | 80.1 steps/s | admission succeeded, throughput regressed |

Removing producer CAS eliminated the 1,000-run enqueue failure, but every message still advanced the shared stream tail and made state-dependent consumer CAS collide more often. Coalescing same-turn enqueues into bounded 64-record appends then measured 65.6, 79.0, and 62.7 steps/s at 25, 50, and 100 runs, respectively, so it was stopped early. The commutative and batching changes were reverted rather than leaving an unproven optimization on `main`.

The structural conclusion is that one logical workflow queue stored as one Durable Stream is a global sequencing point for enqueue, lease, and ack. Adapter-side batching can move contention between producers, consumers, and replicas, but cannot remove it. The next scalable design needs partitioned physical queue journals plus an efficient discovery/changefeed primitive, or a server-side conditional queue transition that validates message state inside one Raft command without comparing the entire stream tail.

The follow-up split each logical queue into fixed physical journals by execution-lane hash and used the existing queue registry only to discover partitions that had received work. The first 64-partition implementation removed the 1,000-run admission failure, but a full single-iteration sweep plateaued at 104.8 steps/s rather than the guarded baseline's 117.1 steps/s. A watcher-driven ready set then removed empty-partition probes. Because single iterations remained noisy, the comparison was narrowed to three identical 100-run iterations on the same 0.3.22 memory-WAL cluster, eight ARM app replicas, and a fresh bucket per setting:

| Physical journals per logical queue | Throughput | Run duration p99 | TTFS p99 |
| ---: | ---: | ---: | ---: |
| 1 | 81.7 steps/s | 28.147 s | 5.992 s |
| 8 | 87.2 steps/s | 22.531 s | 6.966 s |
| 64 | 87.7 steps/s | 27.012 s | 8.452 s |

Eight partitions improved throughput by 6.7% and run-duration p99 by 20.0% over the contemporaneous one-partition control, but TTFS p99 worsened by 16.3%. Sixty-four partitions added only 0.6% throughput over eight while worsening both tail metrics and multiplying active watchers. The adapter therefore defaults to eight, not 64. Partitioning is a bounded coordination improvement and removes the enqueue admission ceiling; it is not evidence for the earlier 300 steps/s target. The remaining capacity and TTFS limit is now dominated by the Workflow application/dispatcher path rather than voter CPU or one queue-tail CAS.

### One-record step transaction experiment

The next experiment changed the authoritative run layout instead of tuning queue parameters. An owned step stages `step_started` in memory, then one record-tail-guarded append commits `step_created` when needed, `step_started`, the terminal event, and the final Step entity together. Ordinary queue delivery first commits a fenced execution-lane claim. Turbo can avoid that claim by reusing its owning queue message, but this path is explicitly experimental because the current World interface does not expose whether a lazy start is running under Turbo's single-handler guarantee.

The exact `100 runs × 20 steps × 3 iterations`, eight-partition ARM comparison produced:

| Variant | Accepted appends | Appends / logical step | Throughput | Run p99 | TTFS p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Contemporaneous literal lifecycle baseline | 14,160 | 2.36 | 87.2 steps/s | 22.531 s | 6.966 s |
| One-record owned step, per-run commit ordering | 8,163 | 1.36 | 89.6 steps/s | 23.026 s | 10.621 s |

The structural write goal was achieved: accepted appends fell by 42.4%, almost exactly one removed append per logical step. Throughput improved only 2.8%, while the sampled tail metrics did not improve. This falsifies the stronger hypothesis that two run-journal Raft commits were the dominant Workflow capacity blocker at this load point. The voters remained lightly loaded and application/dispatcher scheduling still determined completion and fairness.

Two failed intermediate runs are excluded from the table but produced useful guards. A queue-only execution context left Turbo on the old path and still emitted 2.36 appends per step. The first Turbo fallback then used one process-wide commit mutex, reducing throughput to 68.2 steps/s; changing that mutex to per-run ordering restored concurrency, and a regression test now proves different runs do not serialize.

This experiment should not become a default compatibility behavior until the upstream World contract exposes an explicit atomic-step capability or transaction method. Without that signal, an adapter cannot distinguish Turbo's optimistic single-owner lazy start from a conservative lazy start whose successful `step_started` call is itself the ownership gate. The safe default therefore remains the literal two-append contract outside a fenced queue-delivery context.

### World-storage isolation: why the Workflow result looked tied

The one-record experiment removed 42.4% of Ursula appends but barely moved the complete Workflow benchmark. A call-chain audit then corrected an important assumption: a sequential inline Workflow does not enqueue one continuation after every step. One queue delivery repeatedly executes `terminal append → inline replay → next step` until suspension or timeout. Queue ack/outbox fusion therefore cannot explain or fix the `100 × 20` result.

A new isolation runner measures the storage contract shared by both Worlds without Workflow replay, dispatcher scheduling, application HTTP routing, or user code. Each run is created and started before the measurement window. Every timed logical step performs the same two public World mutations, `step_started` followed by `step_completed`. The runner uses valid ULID identifiers, runs from the same isolated `m7g.xlarge` ARM EKS node, and tests the production-shaped backends serially:

- Ursula 0.3.22: three `m7g.large` voters across three AZs, memory WAL, S3 cold tier, and a three-replica gateway;
- RDS PostgreSQL 17: Multi-AZ `db.m7g.large`, 100 GiB gp3, TLS, and a 128-connection client pool.

| Concurrent runs | Ursula steps/s | PostgreSQL steps/s | Ursula advantage | Ursula mutation p50 / p99 | PostgreSQL mutation p50 / p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 130.6 | 96.3 | 1.36× | 3.66 / 8.97 ms | 5.11 / 9.70 ms |
| 8 | 641.1 | 393.9 | 1.63× | 5.21 / 12.97 ms | 8.53 / 20.12 ms |
| 32 | 969.5 | 497.3 | 1.95× | 15.22 / 27.53 ms | 24.93 / 63.26 ms |
| 128 | 918.2 | 500.9 | 1.83× | 68.26 / 153.90 ms | 105.03 / 247.39 ms |

This resolves the first-principles question: Ursula's specialized append path does beat the general OLTP backend when storage is the limiting layer. It reaches almost twice the logical-step throughput and keeps substantially lower tails at the useful 32-run point. The complete Workflow benchmark looked tied because its approximately 87–90 steps/s is only about 9% of the isolated Ursula storage capacity; deterministic replay and the application/runtime tier dominate there.

The high-concurrency Ursula curve still has a real knee. Throughput peaks near 32 runs and falls 5.3% at 128 while mutation p99 rises 5.6×. The measurement rules out several old explanations:

- accepted appends equal exactly two per logical step, so run setup is not contaminating the counters;
- the gateway reports a 100% leader-cache hit ratio during timed writes, so follower redirects are not the cause;
- the three voters averaged only about 13.8–20.9% CPU over the run minute, and the generator node averaged about 9.9%, so aggregate CPU is not saturated;
- per-replica mutation apply and group-engine time increase with concurrency, while standard-append coalescing remains sparse until the 128-run level.

The next backend investigation should therefore profile the 32→128 knee inside the per-core group/OpenRaft response path instead of adding more adapter-level step semantics. Separately, the next end-to-end experiment should scale the Workflow application/runtime tier until it consumes a meaningful fraction of the measured approximately 970 steps/s storage capacity.

At the measured peak and current on-demand prices, backend compute normalizes to approximately `$0.070 / 1M logical steps` for three shared-EKS Ursula `m7g.large` voters, versus approximately `$0.206 / 1M logical steps` for Multi-AZ RDS including its provisioned 100 GiB gp3 baseline. This narrow calculation excludes Ursula S3 requests/retention and shared application compute; it establishes storage-engine efficiency, not the final service bill.

### Complete Workflow CPU-headroom rerun

The storage-isolation result did not establish complete Workflow throughput because the original application tier was saturated. The same `100 concurrent × 50 sequential no-op steps` suite was therefore run first with four one-core application pods on one `m7g.xlarge`, then with eight one-core pods spread exactly four-per-node across two identical `m7g.xlarge` workers. Ursula and PostgreSQL ran serially against the same app topology; Ursula used the production three-voter memory-WAL plus S3 canary and PostgreSQL used the Multi-AZ `db.m7g.large` comparator.

| Application topology | Ursula throughput / run avg / p99 | PostgreSQL throughput / run avg / p99 | Verdict |
| --- | ---: | ---: | --- |
| 4 pods, 1 node | 48.6 steps/s / 66.393 s / 102.479 s | 44.7 steps/s / 98.366 s / 110.776 s | Ursula 1.09× throughput |
| 8 pods, 2 nodes | 81.1 steps/s / 36.874 s / 61.251 s | 81.8 steps/s / 44.425 s / 60.815 s | Throughput and p99 tied; Ursula run average 17.0% lower |

Adding only application CPU raised Ursula throughput by 66.9% and PostgreSQL by 83.0%. The eight-pod comparison is the fair result: comparing eight-core Ursula with four-core PostgreSQL would manufacture a 1.81× advantage from unequal shared runtime resources.

The first benchmark-only OpenTelemetry profile captured every app replica. Its durations are wall time and include asynchronous waits, so they are attribution clues rather than CPU samples:

| Eight-pod aggregate span | Ursula avg | PostgreSQL avg | Interpretation |
| --- | ---: | ---: | --- |
| `step.execute timedNoopStep` | 384.0 ms | 606.9 ms | Ursula completes the step lifecycle sooner |
| `workflow.run ...benchSequentialStepsWorkflow` | 177.5 ms | 85.3 ms | Replay/scheduling wall time consumes Ursula's storage advantage |
| Actual no-op user body | 0.091 ms | 0.072 ms | User code is irrelevant |
| `step.hydrate` | 0.158 ms | 0.127 ms | Serialization is not the main limiter |
| `step.dehydrate` | 0.247 ms | 0.223 ms | Serialization is not the main limiter |

This falsifies the idea that more Ursula server tuning alone can produce the complete-Workflow 1.5× target at this load point. The specialized storage layer already wins the isolated World contract by 1.95× at 32 concurrent runs, but the Workflow handler re-enters `runWorkflow` after every durable step boundary and replays the deterministic program. PostgreSQL's longer storage waits leave more application CPU headroom and overlap across runs; when the common runtime reaches its CPU ceiling, both backends converge near 81 steps/s.

The follow-up low-overhead V8 profile sampled all eight pods for both backends at 5 ms. It reproduced the tied complete-Workflow result: Ursula delivered 81.5 steps/s with a 60.942 s run p99, while PostgreSQL delivered 84.2 steps/s with a 59.065 s p99. Across the whole suite, the top-frame summaries attributed approximately:

| Aggregated self time across 8 pods | Ursula | PostgreSQL |
| --- | ---: | ---: |
| Workflow VM bundle frames | 121.9 CPU-s | 111.0 CPU-s |
| Garbage collection | 134.4 CPU-s | 128.6 CPU-s |
| `node:vm` context creation | 2.8 CPU-s | 3.3 CPU-s |

The dominant bundle frames are not the benchmark's no-op function. They are top-level module initialization: Zod schema constructors, export wiring, and related initialization repeated when the same compiled `vm.Script` is evaluated in each fresh replay context. The generated Workflow VM bundle was 624,177 bytes and contained the complete Zod 4 runtime, every locale, and all `@workflow/world` schemas. This happens because the builder correctly imports the `Run` serde registration entry in both contexts, but the published `@workflow/world` package does not declare its modules side-effect-free; esbuild therefore retains every re-exported schema module even though the registration path only uses lightweight constants and helpers.

Adding only `"sideEffects": false` to the installed `@workflow/world` manifest reduced the generated bundle from 624,177 to 32,584 bytes, a 94.8% reduction, and removed Zod completely. This is a build-graph fix rather than a durable-execution shortcut: it does not cache mutable VM state, serialize a JavaScript stack, change replay inputs, or alter any World operation. It applies symmetrically to Ursula and PostgreSQL. The benchmark carries a temporary reproducible pre-build metadata patch plus a post-build size/Zod guard while the same change is proposed upstream.

This result replaces the earlier replay-prefix hypothesis as the first runtime experiment. Reusing a mutable VM context remains unsafe without a much larger design because module globals, pending promises, clocks, and deterministic hooks belong to one replay. Tree-shaking unused pure modules removes the measured initialization waste while preserving a fresh context and the existing replay correctness model. If the EKS rerun does not materially increase both backends' complete throughput or reveal more of Ursula's 1.95× isolated storage advantage, the next profile will identify the remaining reducer/VM cost before any continuation design is attempted.

### Benchmark infrastructure caveats

The benchmark image now activates and caches pnpm in both build and runtime stages. Before that fix, a newly scaled private-subnet node attempted a Corepack npm download and crash-looped while an older node's cache hid the problem.

The temporary managed node group also attached only the EKS cluster security group, whereas the original benchmark and voter nodes carried the shared OpenTofu node security group. Direct pod traffic worked after ingress was added, but ClusterIP traffic remained unavailable until the second node's ENIs carried the shared node group as well. Future benchmark node groups need a launch template that attaches both groups; this was an environment defect, not a backend result.

## Current interpretation

### Confirmed

- The previous fixed-cost comparison charged all 12 Ursula vCPUs to a workload that used only a small fraction of them.
- The previous `$0.412 per 100k steps` Ursula and `$0.266 per 100k steps` PostgreSQL figures assumed the single observed throughput point was continuous maximum capacity. That assumption is unproven, so those figures are withdrawn as capacity-normalized results.
- Ursula already has a real throughput advantage at the tested point, but the very high concurrent TTFS shows that throughput alone hides a scheduling/fairness problem.
- PostgreSQL's smaller provisioned topology is also lightly loaded. A fair conclusion cannot be “Ursula is expensive because it has three nodes” without measuring how much useful load those nodes sustain.
- Ursula 0.3.8 recorded `raft_write_many=0`; Ursula 0.3.9 made the ordinary POST path reachable but batched only 21 of 3,688 appends in the stable raw run.
- Raw primitive isolation shows that Ursula already beats PostgreSQL for retained replay and, after the cardinality and snapshot fixes, wins the distributed warm-append throughput and p50 comparison while keeping comparable p99.
- Ordinary POST appends can now coalesce without weakening producer/CAS semantics, but the zero-wait mailbox drain yields a 0.57% trigger rate under the broad 256-group raw workload.
- The cold request-cost model previously omitted the near-1:1 `.idx` PUT paired with each `.bin` PUT.
- Same-stream cold compaction cannot solve the dominant short-stream object distribution.
- Group-scoped packing reduces the measured Workflow PUT count by 97.3% without regressing the saturated throughput point.
- The production OpenRaft pack path now creates 8 MiB shared objects without per-stream `.idx` pages; a static three-node test protects this distinction.
- Queue enqueue admission is an adapter coordination limit: removing its CAS lets 1,000 runs complete, but transfers contention to lease/ack and reduces high-load throughput.

### Working hypotheses, not yet proven

1. An adaptive, bounded server-side coalescing window may raise the batch ratio without changing the protocol, but its remaining value must be measured against the new 0.3.22 distributed baseline rather than the obsolete approximately 2.1× PostgreSQL gap.
2. Deterministic gateway routing or a complete group-leader map should primarily improve new-stream and cold-route traffic; it cannot by itself explain the remaining warm-append gap.
3. Ursula's useful Workflow capacity may be reached first by a TTFS/fairness SLO violation rather than by aggregate voter CPU saturation.
4. Snapshot/reference version churn may become the next S3 cost floor during a continuous mixed workload even though short-run data/index PUT amplification is removed.
5. Partitioned queue journals need a bucket changefeed or equivalent discovery primitive; polling every partition from every adapter replica would replace CAS contention with connection and request amplification.

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
- [x] Run raw Ursula and PostgreSQL primitives from the same ARM EKS application worker.
- [x] Make ordinary producer/CAS-safe POST appends eligible for same-group Raft batching and deploy Ursula 0.3.9.
- [x] Rerun the raw comparison and measure the actual coalescing trigger rate.
- [ ] Add an adaptive bounded coalescing-window experiment with batch-size distribution.
- [ ] Add a hot-group raw workload that complements the 256-group independent-stream workload.
- [x] Commit and merge the harness through GitHub; use Depot-published `main-ursula` and `main-postgres` images.
- [x] Restore isolated application capacity and RDS Multi-AZ temporarily.
- [x] Run identical Ursula and PostgreSQL sweeps with exact per-level time windows.
- [x] Sample app and backend resources during every level.
- [x] Extend concurrency until throughput plateaus, errors appear, or the selected latency SLO is crossed.
- [x] Attribute the first limiting resource/path using backend counters and resource samples.
- [x] Recalculate cost per 100,000 useful steps at 30%, 60%, and 80% utilization.
- [x] Update this investigation report and keep raw JSON/resource samples beside it.
- [x] Benchmark Workflow capacity on the 3 × `m7g.large` memory-WAL plus S3 Ursula topology instead of treating it as a linear sizing estimate.
- [x] Isolate raw create, append, live-delivery, and replay primitives from the World adapter.
- [x] Correct the S3 request model to include cold index-page PUTs.
- [ ] Extend append-batch with producer deduplication and per-entry CAS, adopt it in the adapter, and rerun warm append.
- [x] Add a cross-stream group packfile targeting approximately 8 MiB objects, including safe shared-object GC.
- [ ] Aggregate gateway metrics replica-by-replica and rerun route attribution.
- [ ] Force a cold-cache miss via leadership transfer and measure real S3 replay.
- [x] Remove the O(stream-count) append scan, offload inline snapshot encoding, stagger automatic snapshots, and rerun a four-generator PostgreSQL comparison.
- [x] Test CAS-free and locally batched enqueue at the 1,000-run level; revert both after they moved contention to consumer transitions and reduced throughput.
- [x] Partition queue journals by execution lane, replace empty scans with a watcher-driven ready set, and rerun the capacity sweep.
- [x] Prototype one-record owned step transactions and verify the expected 42.4% append reduction on EKS.
- [x] Add a fair public-World storage isolation runner and compare Ursula with Multi-AZ PostgreSQL from the same EKS ARM node.
- [x] Confirm that Ursula wins the isolated World mutation path at every tested concurrency, peaking at 1.95× PostgreSQL throughput.
- [ ] Profile the Ursula 32→128 concurrency knee inside the per-core group/OpenRaft response path.
- [x] Scale the Workflow application/runtime tier until the end-to-end run reaches a material fraction of isolated storage capacity.
- [x] Repeat the complete Workflow comparison with eight one-core app replicas evenly spread over two ARM nodes.
- [x] Add per-pod Workflow span aggregation and prove the full run converges at the shared replay/application-CPU ceiling.
- [x] Capture low-frequency V8 CPU self-time from every app pod for both backends and identify repeated workflow-bundle initialization as the replay hotspot.
- [ ] Benchmark the 94.8% smaller tree-shaken Workflow VM bundle on both backends without weakening durable step semantics.
- [ ] Upstream the `@workflow/world` side-effect metadata and remove the temporary benchmark pre-build patch after an official package contains it.
- [ ] Propose an explicit atomic-step capability/transaction method upstream; keep Turbo owner-based staging experimental until the runtime provides that signal.
- [ ] Replace the interim active-partition registry/watchers with a bucket changefeed only if idle connection and discovery cost justify the Ursula server primitive; it is not expected to fix loaded throughput.
- [x] Reduce cold PUTs toward 8 MiB objects and rerun the request-cost measurement.
- [ ] Run a multi-hour mixed-load soak and measure snapshot/reference version churn, pack fill ratio, and shared-pack GC.
- [ ] Add cross-AZ byte accounting and an operations/backup cost sensitivity.
- [x] Publish the ARM comparison after the right-sized topology and queue fix are measured.
- [x] Destroy the raw-benchmark RDS and temporary workloads, return the ARM application node group to one node, and leave the canary's 3/3 Ursula voters untouched.

## Evidence

- [`ursula-v038-final.json`](./ursula-v038-final.json)
- [`postgres-rds-multi-az.json`](./postgres-rds-multi-az.json)
- [`ursula-v038-capacity.json`](./ursula-v038-capacity.json)
- [`postgres-rds-capacity.json`](./postgres-rds-capacity.json)
- [`ursula-v038-capacity-sharded.json`](./ursula-v038-capacity-sharded.json)
- [`postgres-rds-capacity-sharded.json`](./postgres-rds-capacity-sharded.json)
- [`ursula-v038-capacity-scale.json`](./ursula-v038-capacity-scale.json)
- [`postgres-rds-capacity-scale.json`](./postgres-rds-capacity-scale.json)
- [`ursula-v0313-capacity-hot-queuefix.json`](./ursula-v0313-capacity-hot-queuefix.json)
- [`ursula-v0313-capacity-hot-casfix.json`](./ursula-v0313-capacity-hot-casfix.json)
- [`ursula-v0313-capacity-sharded-casfix.json`](./ursula-v0313-capacity-sharded-casfix.json)
- [`ursula-v0314-capacity-pack.json`](./ursula-v0314-capacity-pack.json)
- [`ursula-commutative-enqueue.json`](./ursula-commutative-enqueue.json)
- [`ursula-v0314-v0315-s3-inventory.json`](./ursula-v0314-v0315-s3-inventory.json)
- [`postgres-rds-capacity-arm-pool66-failure.json`](./postgres-rds-capacity-arm-pool66-failure.json)
- [`postgres-rds-capacity-arm-pool32.json`](./postgres-rds-capacity-arm-pool32.json)
- [`world-storage-comparison-2026-07-27.json`](./world-storage-comparison-2026-07-27.json)
- [`ursula-v0322-workflow-profile.json`](./ursula-v0322-workflow-profile.json)
- [`postgres-rds-v0322-workflow-profile.json`](./postgres-rds-v0322-workflow-profile.json)
- [`ursula-v0322-workflow-profile-8app.json`](./ursula-v0322-workflow-profile-8app.json)
- [`postgres-rds-v0322-workflow-profile-8app.json`](./postgres-rds-v0322-workflow-profile-8app.json)
- [`ursula-v0322-workflow-cpu-8app.json`](./ursula-v0322-workflow-cpu-8app.json)
- [`postgres-rds-v0322-workflow-cpu-8app.json`](./postgres-rds-v0322-workflow-cpu-8app.json)
- [`postgres-rds-v0322-workflow-tree-capacity-8app.json`](./postgres-rds-v0322-workflow-tree-capacity-8app.json)
- `ursula-v0322-workflow-dispatch{2,4,5}-capacity50-8app-r{1,2,3}.json`
- [`2026-07-26-eks-comparison.md`](./2026-07-26-eks-comparison.md)
