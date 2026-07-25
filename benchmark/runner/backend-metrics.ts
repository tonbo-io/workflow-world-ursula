import { Client } from 'pg';

export interface BackendMetricsSnapshot {
  kind: 'ursula' | 'postgres';
  capturedAt: string;
  counters: Record<string, number>;
  targets: number;
}

const URSULA_COUNTERS = [
  'accepted_appends',
  'applied_mutations',
  'cold_flush_publish_bytes',
  'cold_flush_publishes',
  'cold_flush_upload_bytes',
  'cold_flush_uploads',
  'cold_gc_reclaimed',
  'cold_orphan_bytes',
  'cold_orphan_cleanup_attempts',
  'cold_store_read_bytes',
  'cold_store_reads',
  'live_read_backpressure_events',
  'raft_apply_entries',
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
  rawUrls: string
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
  if (ursulaUrls) return captureUrsula(ursulaUrls);
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
