# EKS Workflow backend comparison — 2026-07-26

This run compares `@tonbo-io/world-ursula` with `@workflow/world-postgres` from the same EKS cluster and the same isolated `m6i.xlarge` application node in `us-east-1`. Both backends used four application replicas. Application compute is excluded from the backend cost comparison.

## Backends

| Backend | Configuration |
| --- | --- |
| Ursula | Three `m6i.xlarge` voter nodes across three availability zones, 256 Raft groups, memory WAL, S3 cold storage, Ursula 0.3.8 plus adapter commit `53401a5` |
| PostgreSQL | RDS PostgreSQL 17.9, Multi-AZ `db.m7g.large`, 100 GiB gp3 |
| Managed World | Public Vercel Workflow benchmark baseline; not rerun inside this VPC and no public 100 × 50 concurrency result |

Raw results:

- [`ursula-v038-final.json`](./ursula-v038-final.json)
- [`ursula-v037-final.json`](./ursula-v037-final.json)
- [`postgres-rds-multi-az.json`](./postgres-rds-multi-az.json)
- [`ursula-v036.json`](./ursula-v036.json)

## Results

All latency values are milliseconds.

| Scenario | Ursula avg / p90 / p99 | PostgreSQL avg / p90 / p99 | Public Managed avg |
| --- | ---: | ---: | ---: |
| No-op TTFS | 55.5 / 66 / 112 | 50.9 / 58 / 181 | 995.9 |
| Streaming TTFS | 50.7 / 56 / 130 | 46.5 / 47 / 208 | 1061.6 |
| Hook + stream TTFS | 91.0 / 94 / 245 | 54.5 / 60 / 91 | 1383.6 |
| Live stream latency | 16.2 / 12 / 175 | 5.5 / 8 / 10 | 128.2 |
| Text stream overhead | 8.2 / 10 / 11 | 8.6 / 10 / 14 | 195.8 |
| Structured stream overhead | 8.1 / 10 / 13 | 7.9 / 9 / 11 | 202.4 |
| 1020-step workflow | 74,525 total | 78,302 total | 412,515 total |

The concurrent workload completed 5,000 logical steps in 107.0 seconds on Ursula and 129.9 seconds on PostgreSQL: 46.7 versus 38.5 steps/s, a 21.3% Ursula throughput advantage. Compared with the previous Ursula build, throughput rose from 41.3 to 46.7 steps/s (+13.1%), no-op TTFS fell from 165.5 to 55.5 ms, streaming TTFS from 83.2 to 50.7 ms, hook TTFS from 163.4 to 91.0 ms, and live stream latency from 38.3 to 16.2 ms.

The final Ursula and PostgreSQL runs completed every recorded scenario without a retry. An intermediate Ursula run exposed two separate production defects—transient cross-replica step visibility and run-wide queue leases that could deadlock parallel steps—which were fixed in PRs #34 and #35 before the final run.

## Backend work

The final Ursula run accepted 34,047 appends and applied 35,145 mutations. Gateway leader affinity hit 39,530 times, missed 1,902 times, and learned from 545 redirects, for a 95.4% cache hit ratio. Standard single-record POSTs did not exercise `raft_write_many`; its counters remained zero.

The fresh final cluster did not flush cold objects during its 7.4-minute observation window, so it cannot establish steady-state S3 request cost. The immediately preceding run on the warm cluster uploaded 2,158 objects containing 27,831,017 bytes—only 12.9 KiB per object—so the cost model below conservatively retains that observed PUT ratio rather than treating the final run's zero as a durable steady state.

PostgreSQL committed 343,167 transactions and grew the database by 25,354,240 bytes during the run. Its buffer cache served nearly all blocks (`3,351,311` hits and `11` reads).

## Cost model

Price snapshot: 2026-07-26, `us-east-1`, 730 hours/month.

Pricing inputs come from the official [Vercel limits and pricing table](https://vercel.com/docs/limits), [Amazon EKS pricing](https://aws.amazon.com/eks/pricing/), [Amazon EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/), [Amazon RDS for PostgreSQL pricing](https://aws.amazon.com/rds/postgresql/pricing/), and [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/).

| Backend | Fixed backend cost/month | Sustained benchmark capacity | Fixed cost / 100k steps |
| --- | ---: | ---: | ---: |
| Ursula, measured x86 topology | $505.48 | 46.7 steps/s | $0.412 |
| Ursula, if the EKS control plane is already shared | $432.48 | 46.7 steps/s | $0.352 |
| Ursula, equivalent three-node Graviton estimate | $442.41 | not measured | — |
| RDS PostgreSQL comparator | $269.01 | 38.5 steps/s | $0.266 |
| Managed World | usage-priced | n/a | $2.50 |

Ursula fixed cost includes three `m6i.xlarge` instances ($420.48), one EKS control plane ($73), and 150 GiB gp3 ($12). If Ursula shares an EKS cluster that the application already requires, the $73 control plane is common cost rather than Ursula incremental cost; both views are shown. The estimate excludes S3, cross-AZ transfer, backups, and operations. At this workload's observed cold-upload ratio, S3 PUT requests add approximately 34,200 PUTs or $0.171 per 100,000 logical steps, before compaction; payload storage itself is negligible at current volume. Cross-AZ Raft traffic was not measured and must not be represented as zero.

The RDS estimate includes Multi-AZ `db.m7g.large` compute ($246.01) and 100 GiB Multi-AZ gp3 ($23). The managed estimate includes Workflow step charges only; function duration, observability, and retained World storage are additional.

At zero variable cost, managed step pricing crosses the measured Ursula fixed topology at about 20.2 million steps/month (17.3 million when the EKS control plane is shared) and the RDS comparator at about 10.8 million steps/month. Under continuous saturation, Ursula is cheaper than managed on step charges but remains about 2.18× the PostgreSQL backend cost per delivered 100,000 steps after adding observed S3 PUTs (about 1.96× with a shared EKS control plane). Ursula's current advantage is capacity and long-history behavior, not lowest cost at this small three-node topology. Larger sustained concurrency, Graviton voters, cross-stream cold packing, and fewer adapter appends are the direct levers for changing that result.

## Interpretation limits

- The Managed World numbers are a public external baseline, not a same-VPC run; no public managed concurrency number exists for this workload.
- The Ursula before/after comparison combines server and adapter changes, so it is not an isolated TCP, compiler-profile, or gateway-routing A/B test.
- The PostgreSQL comparator has substantially less provisioned compute than the Ursula voter topology. The cost-per-throughput calculation is more meaningful than comparing monthly totals alone.
- The workload price model does not include application/function compute because it is common to all backends.
