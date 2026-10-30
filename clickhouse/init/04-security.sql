-- Database-enforced tenant isolation for the analytics API. Runs after 03-rollups.sql (the rollup tables
-- must exist for the grants below).
--
-- The API connects as analytics_app and sends the authenticated tenant as the custom setting
-- SQL_tenant_id on every query (SQL_ is ClickHouse's default custom_settings_prefixes). Row policies
-- then filter every tenant table to that tenant, so a query that forgets its `tenant_id = {tenant}`
-- filter still cannot read another tenant's rows.
--
-- Fail closed: without SQL_tenant_id, getSetting() throws (UNKNOWN_SETTING), so the query errors
-- instead of returning every tenant's rows.
--
-- Row policies also apply to anyone NOT listed in them: once a table has a policy, a user without one
-- sees no rows. That includes materialized-view pushes made by a user's INSERT, so every user that
-- writes these tables (or feeds them through MVs) needs a permissive policy — today only `default`
-- (Flink, seed, admin). Background inserts (Kafka engine) run without a user and are not filtered.

-- ── app user ────────────────────────────────────────────────────────────
-- Local-dev password; must match CLICKHOUSE_PASSWORD in analytics/.env. In any shared environment set a
-- real secret: ALTER USER analytics_app IDENTIFIED WITH sha256_password BY '<secret>'.
-- readonly = 2: may read and change settings (needed for SQL_tenant_id), may not write or run DDL,
-- and may not change `readonly` itself.
CREATE USER IF NOT EXISTS analytics_app
    IDENTIFIED WITH sha256_password BY 'analytics_app_pw'
    DEFAULT DATABASE music
    SETTINGS readonly = 2;

-- Only what the API reads. No events_raw, no ingest tables.
GRANT SELECT ON music.plays            TO analytics_app;
GRANT SELECT ON music.clicks           TO analytics_app;
GRANT SELECT ON music.users            TO analytics_app;
GRANT SELECT ON music.tracks           TO analytics_app;  -- global catalog, no tenant column
GRANT SELECT ON music.plays_rollup_1m  TO analytics_app;
GRANT SELECT ON music.clicks_rollup_1m TO analytics_app;
-- /ops/schema-fields compares raw JSON keys with the typed columns of plays/clicks.
GRANT SELECT(database, table, name) ON system.columns TO analytics_app;

-- ── row policies ────────────────────────────────────────────────────────
-- tracks is a global catalog (no tenant_id), so it has no policy.
--
-- Pipeline / admin user keeps seeing (and feeding MVs with) every row. Created first so `default` is never
-- without a policy while this file runs.
CREATE ROW POLICY IF NOT EXISTS unrestricted
    ON music.plays, music.clicks, music.users, music.plays_rollup_1m, music.clicks_rollup_1m
    USING 1
    TO default;

CREATE ROW POLICY IF NOT EXISTS tenant_isolation
    ON music.plays, music.clicks, music.users, music.plays_rollup_1m, music.clicks_rollup_1m
    USING tenant_id = getSetting('SQL_tenant_id')
    TO analytics_app;
