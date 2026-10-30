-- Per-minute rollups for the tenant dashboards (/tenants/me/overview, /tenants/me/live).
--
-- Chained materialized views on the TYPED tables (music.plays / music.clicks), so they see exactly what
-- the typed tables receive, whichever path fills them. Buckets are event time: toStartOfMinute(client_time),
-- so a late event lands in its old bucket automatically (AggregatingMergeTree merges it in).
--
-- Duplicates: an MV sees every inserted row, including at-least-once replays that ReplacingMergeTree later
-- collapses. So counts are distinct counts, not row counts:
--   * plays / clicks / skips = uniq(event_id), users = uniq(user_id), sessions = uniq(session_id)
--     -> dedup-safe: a replayed event is counted once.
--   * max(ingested_at) -> dedup-safe (idempotent).
--   * sum(ms_played), avg(lateness_sec) -> NOT dedup-safe: a replayed duplicate is added again
--     (sum over-counts; the average is skewed toward replayed events).
--   uniq is exact for small sets (a few thousand per merged window here) and an approximation
--   (~1% error) at large cardinalities.
-- The exact path is still the raw table with FINAL; only dashboard aggregates read these rollups.
--
-- Read with the -Merge combinators: uniqMerge(plays), sumMerge(ms_played), avgMerge(...), maxMerge(...).
--
-- Re-seeding: TRUNCATE of plays/clicks does not cascade to these tables; truncate them too, then reload
-- plays/clicks and the MVs refill them.

-- ── plays ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS music.plays_rollup_1m
(
    tenant_id         LowCardinality(String),
    minute            DateTime('UTC'),
    plays             AggregateFunction(uniq, String),                  -- event_id
    users             AggregateFunction(uniq, String),                  -- user_id
    sessions          AggregateFunction(uniq, String),                  -- session_id (non-empty)
    skips             AggregateFunction(uniq, String),                  -- event_id where skipped
    plays_by_platform AggregateFunction(uniqMap, Map(String, String)),  -- platform -> uniq(event_id)
    ms_played         AggregateFunction(sum, UInt64),                   -- over-counts replayed duplicates
    lateness_sec      AggregateFunction(avg, Int64),                    -- skewed by replayed duplicates
    last_ingested_at  AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS music.plays_rollup_1m_mv TO music.plays_rollup_1m AS
SELECT
    tenant_id,
    toStartOfMinute(client_time)                    AS minute,
    uniqState(event_id)                             AS plays,
    uniqState(user_id)                              AS users,
    uniqStateIf(session_id, session_id != '')       AS sessions,
    uniqStateIf(event_id, skipped = 1)              AS skips,
    uniqMapState(map(toString(platform), event_id)) AS plays_by_platform,
    sumState(toUInt64(ms_played))                   AS ms_played,
    avgState(lateness_sec)                          AS lateness_sec,
    maxState(ingested_at)                           AS last_ingested_at
FROM music.plays
GROUP BY tenant_id, minute;

-- ── clicks ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS music.clicks_rollup_1m
(
    tenant_id        LowCardinality(String),
    minute           DateTime('UTC'),
    clicks           AggregateFunction(uniq, String),  -- event_id
    users            AggregateFunction(uniq, String),  -- user_id
    sessions         AggregateFunction(uniq, String),  -- session_id (non-empty)
    last_ingested_at AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS music.clicks_rollup_1m_mv TO music.clicks_rollup_1m AS
SELECT
    tenant_id,
    toStartOfMinute(client_time)              AS minute,
    uniqState(event_id)                       AS clicks,
    uniqState(user_id)                        AS users,
    uniqStateIf(session_id, session_id != '') AS sessions,
    maxState(ingested_at)                     AS last_ingested_at
FROM music.clicks
GROUP BY tenant_id, minute;

-- ── backfill ────────────────────────────────────────────────────────────
-- Rows that reached plays/clicks before the MVs existed (no-op on a fresh volume). The cutoff is the
-- MV's creation time: earlier rows are backfilled here, later ones arrive through the MV, so nothing is
-- counted twice (to within the 1 s resolution of metadata_modification_time).
-- Run once: re-running adds the same rows again (uniq counts are unaffected, sums/averages are not).
-- To rebuild from scratch: TRUNCATE the rollup, then re-run its INSERT without the cutoff while writes are paused.
INSERT INTO music.plays_rollup_1m
SELECT
    tenant_id,
    toStartOfMinute(client_time)                    AS minute,
    uniqState(event_id)                             AS plays,
    uniqState(user_id)                              AS users,
    uniqStateIf(session_id, session_id != '')       AS sessions,
    uniqStateIf(event_id, skipped = 1)              AS skips,
    uniqMapState(map(toString(platform), event_id)) AS plays_by_platform,
    sumState(toUInt64(ms_played))                   AS ms_played,
    avgState(lateness_sec)                          AS lateness_sec,
    maxState(ingested_at)                           AS last_ingested_at
FROM music.plays
WHERE ingested_at < (SELECT metadata_modification_time FROM system.tables
                     WHERE database = 'music' AND name = 'plays_rollup_1m_mv')
GROUP BY tenant_id, minute;

INSERT INTO music.clicks_rollup_1m
SELECT
    tenant_id,
    toStartOfMinute(client_time)              AS minute,
    uniqState(event_id)                       AS clicks,
    uniqState(user_id)                        AS users,
    uniqStateIf(session_id, session_id != '') AS sessions,
    maxState(ingested_at)                     AS last_ingested_at
FROM music.clicks
WHERE ingested_at < (SELECT metadata_modification_time FROM system.tables
                     WHERE database = 'music' AND name = 'clicks_rollup_1m_mv')
GROUP BY tenant_id, minute;
