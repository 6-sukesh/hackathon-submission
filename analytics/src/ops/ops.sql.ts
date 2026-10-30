// Platform-internal pipeline SQL. Still tenant-scoped: every read filters tenant_id = {tenant:String}.
// Timestamps (plays/clicks arrive via Kafka): received_at = producer receive time, cdc_ts = Kafka broker
// append time, ingested_at = start of the ClickHouse consume block (can be up to ~1s before cdc_ts).
import type { Collection } from './dto/ops-query.dto';

const T = `tenant_id = {tenant:String}`;
const RECENT_INGEST = `${T} AND ingested_at >= now64(3) - toIntervalMinute({minutes:UInt32})`;
const ms = (c: string) => `toUnixTimestamp64Milli(${c})`;

/** Recent sample for raw-key discovery (most recent events first, capped at {sample} rows). */
const sample = (table: Collection) => `
  SELECT client_time, schema_version, raw
  FROM music.${table} FINAL
  WHERE ${T} AND client_time >= now64(3) - toIntervalDay({days:UInt32})
  ORDER BY client_time DESC
  LIMIT {sample:UInt32}`;

// Keys present in raw that are not typed columns of the table. `_id` is the Mongo document key, not an
// event attribute, so it is never a promotion candidate.
const schemaFields = (table: Collection) => `
  SELECT key, count() AS occurrences,
         min(client_time) AS first_seen, max(client_time) AS last_seen,
         arraySort(groupUniqArray(schema_version)) AS schema_versions,
         arraySort(groupUniqArray(toString(JSONType(raw, key)))) AS json_types
  FROM (${sample(table)})
  ARRAY JOIN JSONExtractKeys(raw) AS key
  WHERE key != '_id'
    AND key NOT IN (SELECT name FROM system.columns WHERE database = 'music' AND table = {table:String})
  GROUP BY key
  ORDER BY occurrences DESC, key`;

const sampleStats = (table: Collection) => `
  SELECT schema_version, count() AS events FROM (${sample(table)})
  GROUP BY schema_version ORDER BY schema_version`;

// The table name is chosen from this fixed map by a validated enum, never interpolated from input.
export const OPS_SQL = {
  pipelineLag: `
    SELECT collection, count() AS events,
           min(ingested_at) AS first_ingested_at, max(ingested_at) AS last_ingested_at,
           quantilesExact(0.5, 0.95)(kafka_ms) AS kafka_q, max(kafka_ms) AS kafka_max,
           quantilesExact(0.5, 0.95)(producer_ms) AS producer_q, max(producer_ms) AS producer_max,
           countIf(producer_ms > 3600000) AS producer_over_1h,
           quantilesExactIf(0.5, 0.95)(producer_ms, producer_ms <= 3600000) AS producer_streaming_q,
           maxIf(producer_ms, producer_ms <= 3600000) AS producer_streaming_max
    FROM (
      SELECT 'plays' AS collection, ingested_at,
             ${ms('ingested_at')} - ${ms('cdc_ts')} AS kafka_ms, ${ms('cdc_ts')} - ${ms('received_at')} AS producer_ms
      FROM music.plays FINAL WHERE ${RECENT_INGEST}
      UNION ALL
      SELECT 'clicks' AS collection, ingested_at,
             ${ms('ingested_at')} - ${ms('cdc_ts')} AS kafka_ms, ${ms('cdc_ts')} - ${ms('received_at')} AS producer_ms
      FROM music.clicks FINAL WHERE ${RECENT_INGEST}
    )
    GROUP BY collection`,
  schemaFields: { plays: schemaFields('plays'), clicks: schemaFields('clicks') } satisfies Record<Collection, string>,
  sampleStats: { plays: sampleStats('plays'), clicks: sampleStats('clicks') } satisfies Record<Collection, string>,
};
