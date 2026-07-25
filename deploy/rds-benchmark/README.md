# RDS benchmark comparator

This stack creates an ephemeral production-shaped PostgreSQL comparator in the
same VPC and region as `ursula-canary-eks`: encrypted gp3 storage, Multi-AZ
standby, seven-day backups, and private-only networking. It also writes the
connection URL into the `workflow-benchmark-postgres` namespace.

Initialize with the existing infrastructure-state bucket, using a distinct key:

```sh
tofu init \
  -backend-config=bucket=ursula-opentofu-state-232814779190-us-east-1 \
  -backend-config=key=benchmarks/workflow-postgres.tfstate \
  -backend-config=region=us-east-1 \
  -backend-config=encrypt=true \
  -backend-config=use_lockfile=true
tofu plan -out=workflow-postgres.tfplan
tofu apply workflow-postgres.tfplan
```

After the image matrix has published `main-postgres`, apply
`../eks-postgres-benchmark.yaml`. Destroy this stack after recording both
performance and RDS configuration/cost inputs:

```sh
tofu destroy
```
