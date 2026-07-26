# Workflow backend capacity and cost investigation

Last updated: 2026-07-26

Status: in progress. The existing `100 concurrent × 50 steps` result is a load point, not a measured saturation point. Any cost per 100,000 steps derived by dividing fixed monthly cost by that throughput is provisional and must not be used as a capacity-normalized conclusion.

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
| Application tier | 4 application replicas on an isolated EKS application node |
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

## Capacity sweep methodology

The benchmark harness is being extended with an isolated `BENCH_CAPACITY_ONLY=1` mode. It will run increasing concurrent workflow counts with a fixed step count and record an exact time window plus backend counters for every level.

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
- [ ] Finish the capacity-only harness and type-check it.
- [ ] Commit and merge the harness through GitHub; use Depot-published `main-ursula` and `main-postgres` images.
- [ ] Restore isolated application capacity and RDS Multi-AZ temporarily.
- [ ] Run identical Ursula and PostgreSQL sweeps with exact per-level time windows.
- [ ] Sample app and backend resources during every level.
- [ ] Extend concurrency until throughput plateaus, errors appear, or the selected latency SLO is crossed.
- [ ] Attribute the first limiting resource/path using backend counters and resource samples.
- [ ] Recalculate cost per 100,000 useful steps at 30%, 60%, and 80% utilization.
- [ ] Update the final comparison report and keep raw JSON/resource samples beside it.
- [ ] Destroy RDS and remove temporary application nodes after evidence is saved.

## Evidence

- [`ursula-v038-final.json`](./ursula-v038-final.json)
- [`postgres-rds-multi-az.json`](./postgres-rds-multi-az.json)
- [`2026-07-26-eks-comparison.md`](./2026-07-26-eks-comparison.md)
