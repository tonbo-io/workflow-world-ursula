import { Client } from 'pg';

export interface BackendMetricsSnapshot {
  kind: 'ursula' | 'postgres';
  capturedAt: string;
  counters: Record<string, number>;
  targets: number;
}

const URSULA_COUNTERS = [
  'accepted_appends',
  'append_post_commit_ns',
  'applied_mutations',
  'cold_flush_publish_bytes',
  'cold_flush_publishes',
  'cold_flush_upload_bytes',
  'cold_flush_uploads',
  'cold_gc_reclaimed',
  'cold_hot_bytes',
  'cold_orphan_bytes',
  'cold_orphan_cleanup_attempts',
  'cold_store_read_bytes',
  'cold_store_reads',
  'live_read_backpressure_events',
  'mailbox_send_wait_ns',
  'mutation_apply_ns',
  'group_engine_exec_ns',
  'group_lock_wait_ns',
  'raft_apply_entries',
  'raft_apply_ns',
  'raft_grpc_append_stream_fallbacks',
  'raft_grpc_append_stream_batch_frames',
  'raft_grpc_append_stream_batch_items_max',
  'raft_grpc_append_stream_inflight_max',
  'raft_grpc_append_stream_request_bytes',
  'raft_grpc_append_stream_request_frames',
  'raft_grpc_append_stream_requests',
  'raft_grpc_append_stream_response_bytes',
  'raft_grpc_append_stream_response_frames',
  'raft_grpc_append_stream_responses',
  'raft_grpc_append_stream_session_failures',
  'raft_grpc_append_stream_sessions_opened',
  'raft_grpc_append_unary_calls',
  'raft_snapshot_body_bytes',
  'raft_snapshot_builds',
  'raft_snapshot_external_uploads',
  'raft_snapshot_pointer_bytes',
  'raft_write_many_batches',
  'raft_write_many_commands',
  'raft_write_many_logical_commands',
  'raft_write_many_response_ns',
  'raft_write_many_responses',
  'raft_write_many_submit_ns',
  'read_watcher_notify_calls',
  'read_watcher_notify_ns',
  'read_watcher_replans',
  'routed_requests',
  'wal_batches',
  'wal_records',
  'wal_sync_ns',
  'wal_write_ns',
] as const;

const URSULA_GATEWAY_COUNTERS = [
  'leader_cache_hits',
  'leader_cache_misses',
  'leader_cache_updates',
  'leader_cache_evictions',
  'leader_redirect_ns',
  'leader_redirects',
  'requests',
] as const;

function finiteNumber(value: unknown): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function addCounter(
  counters: Record<string, number>,
  name: string,
  value: unknown
): void {
  counters[name] = (counters[name] ?? 0) + finiteNumber(value);
}

async function captureUrsula(
  rawUrls: string,
  gatewayMetricsUrl?: string
): Promise<BackendMetricsSnapshot> {
  const urls = rawUrls
    .split(',')
    .map((url) => url.trim())
    .filter(Boolean);
  const payloads = await Promise.all(
    urls.map(async (url) => {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(
          `Ursula metrics request failed: ${url} returned HTTP ${response.status}`
        );
      }
      return (await response.json()) as Record<string, unknown>;
    })
  );
  const counters: Record<string, number> = {};
  for (const payload of payloads) {
    for (const name of URSULA_COUNTERS) {
      addCounter(counters, name, payload[name]);
    }
  }
  if (gatewayMetricsUrl) {
    const response = await fetch(gatewayMetricsUrl);
    // Pre-metrics Ursula releases do not expose this optional diagnostic
    // endpoint. Keeping 404 backward-compatible lets one benchmark image run
    // a clean old/new server A/B.
    if (response.status === 404) {
      return {
        kind: 'ursula',
        capturedAt: new Date().toISOString(),
        counters,
        targets: urls.length,
      };
    }
    if (!response.ok) {
      throw new Error(
        `Ursula gateway metrics request failed: ${gatewayMetricsUrl} returned HTTP ${response.status}`
      );
    }
    const payload = (await response.json()) as Record<string, unknown>;
    for (const name of URSULA_GATEWAY_COUNTERS) {
      addCounter(counters, `gateway_${name}`, payload[name]);
    }
  }
  return {
    kind: 'ursula',
    capturedAt: new Date().toISOString(),
    counters,
    targets: urls.length,
  };
}

async function capturePostgres(
  connectionString: string
): Promise<BackendMetricsSnapshot> {
  const client = new Client({ connectionString });
  await client.connect();
  try {
    const result = await client.query<{
      database_bytes: string;
      xact_commit: string;
      xact_rollback: string;
      blks_read: string;
      blks_hit: string;
      tup_returned: string;
      tup_fetched: string;
      tup_inserted: string;
      tup_updated: string;
      tup_deleted: string;
      temp_files: string;
      temp_bytes: string;
    }>(`
      SELECT
        pg_database_size(current_database())::text AS database_bytes,
        xact_commit::text,
        xact_rollback::text,
        blks_read::text,
        blks_hit::text,
        tup_returned::text,
        tup_fetched::text,
        tup_inserted::text,
        tup_updated::text,
        tup_deleted::text,
        temp_files::text,
        temp_bytes::text
      FROM pg_stat_database
      WHERE datname = current_database()
    `);
    const row = result.rows[0];
    if (!row) throw new Error('Postgres metrics query returned no database row');
    return {
      kind: 'postgres',
      capturedAt: new Date().toISOString(),
      counters: Object.fromEntries(
        Object.entries(row).map(([name, value]) => [
          name,
          finiteNumber(value),
        ])
      ),
      targets: 1,
    };
  } finally {
    await client.end();
  }
}

export async function captureBackendMetrics(): Promise<
  BackendMetricsSnapshot | undefined
> {
  const ursulaUrls = process.env.WORKFLOW_URSULA_METRICS_URLS;
  if (ursulaUrls) {
    return captureUrsula(
      ursulaUrls,
      process.env.WORKFLOW_URSULA_GATEWAY_METRICS_URL
    );
  }
  const postgresUrl =
    process.env.WORKFLOW_POSTGRES_URL ?? process.env.DATABASE_URL;
  if (postgresUrl) return capturePostgres(postgresUrl);
  return undefined;
}

export function diffBackendMetrics(
  before: BackendMetricsSnapshot | undefined,
  after: BackendMetricsSnapshot | undefined
): Record<string, number> | undefined {
  if (!before || !after || before.kind !== after.kind) return undefined;
  return Object.fromEntries(
    Object.entries(after.counters).map(([name, value]) => [
      name,
      value - (before.counters[name] ?? 0),
    ])
  );
}

function per(
  counters: Record<string, number>,
  numerator: string,
  denominator: string
): number | undefined {
  const count = counters[denominator] ?? 0;
  if (count <= 0) return undefined;
  return (counters[numerator] ?? 0) / count;
}

/** Derived averages are diagnostic only. The source metrics are cumulative
 * sums rather than per-request histograms, so these values cannot represent
 * p95/p99 or reconstruct one particular client request. */
export function deriveBackendMetrics(
  counters: Record<string, number> | undefined
): Record<string, number> | undefined {
  if (!counters) return undefined;
  return Object.fromEntries(
    Object.entries({
      mutationApplyNsPerMutation: per(
        counters,
        'mutation_apply_ns',
        'applied_mutations'
      ),
      groupEngineExecNsPerMutation: per(
        counters,
        'group_engine_exec_ns',
        'applied_mutations'
      ),
      groupLockWaitNsPerMutation: per(
        counters,
        'group_lock_wait_ns',
        'applied_mutations'
      ),
      appendPostCommitNsPerAppend: per(
        counters,
        'append_post_commit_ns',
        'accepted_appends'
      ),
      readWatcherNotifyNsPerCall: per(
        counters,
        'read_watcher_notify_ns',
        'read_watcher_notify_calls'
      ),
      readWatcherReplansPerCall: per(
        counters,
        'read_watcher_replans',
        'read_watcher_notify_calls'
      ),
      mailboxSendWaitNsPerRequest: per(
        counters,
        'mailbox_send_wait_ns',
        'routed_requests'
      ),
      raftApplyNsPerEntry: per(
        counters,
        'raft_apply_ns',
        'raft_apply_entries'
      ),
      raftWriteManySubmitNsPerBatch: per(
        counters,
        'raft_write_many_submit_ns',
        'raft_write_many_batches'
      ),
      raftWriteManyResponseNsPerBatch: per(
        counters,
        'raft_write_many_response_ns',
        'raft_write_many_batches'
      ),
      raftGrpcAppendStreamRequestBytesPerRequest: per(
        counters,
        'raft_grpc_append_stream_request_bytes',
        'raft_grpc_append_stream_requests'
      ),
      raftGrpcAppendStreamResponseBytesPerResponse: per(
        counters,
        'raft_grpc_append_stream_response_bytes',
        'raft_grpc_append_stream_responses'
      ),
      raftGrpcAppendStreamItemsPerRequestFrame: per(
        counters,
        'raft_grpc_append_stream_requests',
        'raft_grpc_append_stream_request_frames'
      ),
      raftGrpcAppendStreamItemsPerResponseFrame: per(
        counters,
        'raft_grpc_append_stream_responses',
        'raft_grpc_append_stream_response_frames'
      ),
      raftGrpcAppendStreamBatchFrameRatio:
        (counters.raft_grpc_append_stream_batch_frames ?? 0) /
        Math.max(1, counters.raft_grpc_append_stream_request_frames ?? 0),
      raftGrpcAppendStreamUsageRatio:
        (counters.raft_grpc_append_stream_requests ?? 0) /
        Math.max(
          1,
          (counters.raft_grpc_append_stream_requests ?? 0) +
            (counters.raft_grpc_append_unary_calls ?? 0)
        ),
      walWriteNsPerBatch: per(counters, 'wal_write_ns', 'wal_batches'),
      walSyncNsPerBatch: per(counters, 'wal_sync_ns', 'wal_batches'),
      gatewayRedirectNsPerRedirect: per(
        counters,
        'gateway_leader_redirect_ns',
        'gateway_leader_redirects'
      ),
      gatewayLeaderCacheHitRatio:
        (counters.gateway_leader_cache_hits ?? 0) /
        Math.max(
          1,
          (counters.gateway_leader_cache_hits ?? 0) +
            (counters.gateway_leader_cache_misses ?? 0)
        ),
    }).filter((entry): entry is [string, number] => entry[1] !== undefined)
  );
}
