#!/usr/bin/env bash
set -euo pipefail

output_file=${1:?usage: capture-cost-snapshot.sh OUTPUT.json}
: "${KUBECONFIG:?KUBECONFIG must point at the benchmark EKS cluster}"
: "${URSULA_S3_BUCKET:?URSULA_S3_BUCKET is required}"

snapshot_dir=$(mktemp -d)
trap 'rm -rf "$snapshot_dir"' EXIT

aws s3api list-object-versions \
  --bucket "$URSULA_S3_BUCKET" \
  --output json >"$snapshot_dir/s3.json"

kubectl get pods \
  --all-namespaces \
  -l 'app in (workflow-benchmark-app,workflow-benchmark-runner)' \
  -o json >"$snapshot_dir/pods.json"

if [[ -n ${WORKFLOW_RDS_IDENTIFIER:-} ]]; then
  aws rds describe-db-instances \
    --db-instance-identifier "$WORKFLOW_RDS_IDENTIFIER" \
    --output json >"$snapshot_dir/rds.json"
else
  printf '{"DBInstances":[]}\n' >"$snapshot_dir/rds.json"
fi

jq -n \
  --arg captured_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg s3_bucket "$URSULA_S3_BUCKET" \
  --slurpfile s3 "$snapshot_dir/s3.json" \
  --slurpfile pods "$snapshot_dir/pods.json" \
  --slurpfile rds "$snapshot_dir/rds.json" \
  '{
    capturedAt: $captured_at,
    s3: {
      bucket: $s3_bucket,
      currentObjects: (($s3[0].Versions // []) | map(select(.IsLatest)) | length),
      currentBytes: (($s3[0].Versions // []) | map(select(.IsLatest) | .Size) | add // 0),
      noncurrentObjects: (($s3[0].Versions // []) | map(select(.IsLatest | not)) | length),
      noncurrentBytes: (($s3[0].Versions // []) | map(select(.IsLatest | not) | .Size) | add // 0),
      deleteMarkers: (($s3[0].DeleteMarkers // []) | length)
    },
    kubernetes: [
      $pods[0].items[] | {
        namespace: .metadata.namespace,
        pod: .metadata.name,
        node: .spec.nodeName,
        containers: [
          .spec.containers[] | {
            name,
            requests: (.resources.requests // {}),
            limits: (.resources.limits // {})
          }
        ],
        restarts: ([.status.containerStatuses[]?.restartCount] | add // 0)
      }
    ],
    rds: (
      if ($rds[0].DBInstances | length) == 0 then null
      else $rds[0].DBInstances[0] | {
        identifier: .DBInstanceIdentifier,
        engine: .Engine,
        engineVersion: .EngineVersion,
        instanceClass: .DBInstanceClass,
        multiAZ: .MultiAZ,
        storageType: .StorageType,
        allocatedStorageGiB: .AllocatedStorage,
        iops: .Iops,
        storageThroughput: .StorageThroughput,
        encrypted: .StorageEncrypted,
        backupRetentionDays: .BackupRetentionPeriod,
        performanceInsights: .PerformanceInsightsEnabled
      }
      end
    )
  }' >"$output_file"

printf 'cost snapshot written to %s\n' "$output_file"
