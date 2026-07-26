# Workflow backend capacity and cost investigation

Last updated: 2026-07-26 14:05 CST

Status: measurement, first-pass analysis, publication, and temporary infrastructure cleanup are complete. The original `100 concurrent × 50 steps` result was a load point, not a saturation point, so the old `$0.412 / 100k` Ursula and `$0.266 / 100k` PostgreSQL figures remain withdrawn.

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

At Ursula's 100-run useful-capacity point, 347 cold uploads were issued for 2,000 logical steps: 17,350 PUTs per 100,000 steps, or about `$0.087 / 100k` at `$0.005 / 1,000 PUTs`. The uploaded objects averaged only 20.8 KiB, so request cost is material and the cold-object packing work remains economically important.

At 60% utilization, including that observed PUT ratio:

| Backend/topology | Fixed | Observed PUT estimate | Current total |
| --- | ---: | ---: | ---: |
| Ursula current x86, shared EKS | $0.236 | $0.087 | $0.323 |
| Ursula estimated 3 × `m7g.large`, shared EKS | $0.104 | $0.087 | $0.191 |
| Ursula estimated 3 × `m7g.large`, dedicated EKS | $0.144 | $0.087 | $0.231 |
| RDS Multi-AZ | $0.129 | included in provisioned gp3 baseline for this run | $0.129 plus backups/transfer |
| Managed World | $2.500 | storage/function charges excluded | at least $2.500 |

The defensible conclusion is not that Ursula is already universally cheaper than PostgreSQL. It is:

1. The earlier “three nodes make Ursula much more expensive” comparison materially overcharged unused x86 capacity.
2. A plausible right-sized three-voter topology makes Ursula fixed cost competitive with RDS, especially on shared EKS.
3. Today, small cold objects add roughly `$0.087 / 100k` and keep the total above this RDS comparator at the p99 TTFS ≤ 5 s capacity point.
4. Ursula's strongest measured result is its stress throughput and long-history behavior; its most urgent production blockers are queue admission/fairness and cold-object packing.
5. Managed World's `$2.50 / 100k` step charge remains an order of magnitude above either self-hosted backend before application compute and operations are considered.

## Current interpretation

### Confirmed

- The previous fixed-cost comparison charged all 12 Ursula vCPUs to a workload that used only a small fraction of them.
- The previous `$0.412 per 100k steps` Ursula and `$0.266 per 100k steps` PostgreSQL figures assumed the single observed throughput point was continuous maximum capacity. That assumption is unproven, so those figures are withdrawn as capacity-normalized results.
- Ursula already has a real throughput advantage at the tested point, but the very high concurrent TTFS shows that throughput alone hides a scheduling/fairness problem.
- PostgreSQL's smaller provisioned topology is also lightly loaded. A fair conclusion cannot be “Ursula is expensive because it has three nodes” without measuring how much useful load those nodes sustain.
- `raft_write_many=0` means the tested workload did not exercise Ursula's server-side mailbox batching path.

### Working hypotheses, not yet proven

1. Ursula has substantial unused throughput headroom because voter CPU remained near idle.
2. The `100 × 50` result may be limited by workflow queue scheduling, application replicas, per-run serialization, or HTTP request amplification rather than Raft.
3. Raising concurrency should eventually make group-actor mailbox batching visible, but only if the adapter/app tier can feed enough independent appends.
4. Ursula's useful capacity may be reached first by a TTFS/fairness SLO violation rather than by aggregate CPU saturation.
5. The three-node availability floor makes a dedicated low-volume Ursula cluster look expensive; shared nodes, smaller Graviton voters, or multiple tenants amortizing the cluster could improve economics, but these are deployment alternatives and must not be presented as measured results.

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
- [ ] Benchmark the estimated 3 × `m7g.large` Ursula topology instead of treating it as a linear sizing estimate.
- [ ] Remove the queue enqueue-CAS admission ceiling and rerun the 1,000/2,000-run Ursula levels.
- [ ] Reduce cold PUTs toward 8 MiB objects and rerun the request-cost measurement.
- [ ] Add cross-AZ byte accounting and an operations/backup cost sensitivity.
- [ ] Publish the final comparison after the right-sized topology and queue fix are measured.
- [x] Destroy RDS and remove temporary application nodes after evidence is saved; verify the canary returns to 3/3 ready Ursula voters.

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
