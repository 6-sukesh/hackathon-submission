# analytics

NestJS service that reads ClickHouse (`music` DB, fed by MongoDB → Flink CDC) and serves per-user
listening reports and content-based song recommendations. Multi-tenant: every request (except `/health`)
needs `Authorization: Bearer <jwt>`; the token's `tenant_id` claim is the caller's tenant.

## Run

```bash
cp .env.example .env      # CLICKHOUSE_URL / USER / PASSWORD / DB, PORT, JWT_SECRET
npm install
npm run build && npm run start     # or: npm run start:dev

# dev tokens, signed with JWT_SECRET from .env (used by the examples below)
ACME=$(npm run -s token -- t_acme)
GLOBEX=$(npm run -s token -- t_globex)
INITECH=$(npm run -s token -- t_initech)
```

The service connects to ClickHouse as `analytics_app` (created by `clickhouse/init/04-security.sql`), not `default`.

## Auth

- `Authorization: Bearer <jwt>`, HS256 signed with `JWT_SECRET` (≥ 32 chars; the app refuses to start
  otherwise). Required claims: `tenant_id`, `sub`, `exp`. Other algorithms (incl. `none`) are rejected.
- Missing, malformed, wrongly signed or expired token, or a token without those claims → 401.
- A global `AuthGuard` verifies the token and pins the tenant on the request; handlers read it only via
  `@Tenant()`. `@Public()` (only `/health`) skips it.
- Scope is the tenant only: any valid token for a tenant can read every user of that tenant. Per-user
  authorization (e.g. a listener only seeing their own `/users/:userId/...`, roles for `/ops`) is the next step.

Dev tokens: `npm run -s token -- <tenant_id> [sub] [ttl]` prints a token (`sub` defaults to `dev`, `ttl` to
`1h`; seconds or `15m`/`12h`/`7d`; a negative ttl mints an already-expired token for tests).

```bash
npm run -s token -- t_acme                 # 1h token for t_acme, sub "dev"
npm run -s token -- t_globex alice 15m     # sub "alice", 15 minutes
npm run -s token -- t_acme dev -60         # expired a minute ago -> 401
```

## Endpoints

| Method | Path | Notes |
|---|---|---|
| GET | `/health` | Pings ClickHouse (no token needed). 503 if down. |
| GET | `/users/:userId/report?from=YYYY-MM-DD&to=YYYY-MM-DD` | Defaults to last 30 days. Bounds on `client_time` (event time), `to` inclusive, UTC. |
| GET | `/users/:userId/recommendations?limit=10` | `limit` 1–50. Cold start falls back to tenant top tracks, then popularity. |
| GET | `/tenants/me/overview` | Last 24h (whole minutes) for the caller's tenant: plays, active users, minutes listened, skip rate, plays by platform, clicks; plays per minute (gaps zero-filled). Served from the rollups. |

```bash
curl localhost:3000/health
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_04/report?from=2026-10-01&to=2026-10-30'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_04/recommendations?limit=5'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/overview'
```

Errors: missing/invalid/expired token → 401; bad query params → 400; user not in the caller's tenant → 404.

## Insights

User-facing insights (`src/insights/`, SQL in `insights.sql.ts`). All `/users/:userId/...` routes are tenant-scoped
(another tenant's user → 404). Times are UTC on `client_time`; feature averages are `ms_played`-weighted.

| Method | Path | Notes |
|---|---|---|
| GET | `/users/:userId/wrapped?year=2026` | Year in review: totals, top 5 tracks/artists/genres (share %), most active month/hour, longest day, listening personality + feature averages. `year` 2000–2100, default current year. |
| GET | `/users/:userId/history?limit=20&before=<ISO>&before_id=<event_id>` | Recently played, newest first. `limit` 1–100. Keyset on `client_time`: pass `next_before` (+ `next_before_id` as tie-breaker for plays at the same ms). `before` needs a timezone (`Z`/offset) or is a plain date. |
| GET | `/users/:userId/streaks` | Current streak (ending today or yesterday) and longest streak of consecutive UTC days with ≥1 play, plus active days in the last 30. |
| GET | `/users/:userId/taste-drift?months=6` | Per calendar month (zero-filled): plays, minutes, top genre, weighted danceability/energy/valence/acousticness/tempo; `change` = last vs first month with plays. `months` 1–24. |
| GET | `/users/:userId/recommendations/mood?mood=chill&limit=10` | `mood` ∈ chill (energy<0.5), focus (energy<0.6, acousticness>0.4), workout (energy>0.75, tempo>115), happy (valence>0.6). Same taste-profile score as `/recommendations`; tracks played in the last 14 days sort last. Falls back to popular tracks in range. `limit` 1–50. |
| GET | `/users/:userId/rediscover?limit=10` | Tracks played ≥3 times but not in the last 30 days, by past play count, with `last_played_at`. |
| GET | `/tracks/:trackId/similar?limit=10` | Nearest catalog tracks by euclidean distance on (danceability, energy, valence, acousticness, tempo/250). Global catalog (a token is still required); unknown track → 404. |

```bash
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_04/wrapped?year=2026'
curl -H "Authorization: Bearer $GLOBEX" 'localhost:3000/users/u_globex_03/history?limit=20'
curl -H "Authorization: Bearer $INITECH" 'localhost:3000/users/u_initech_02/streaks'
curl -H "Authorization: Bearer $GLOBEX" 'localhost:3000/users/u_globex_03/taste-drift?months=6'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_02/recommendations/mood?mood=workout&limit=5'
curl -H "Authorization: Bearer $GLOBEX" 'localhost:3000/users/u_globex_03/rediscover?limit=10'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tracks/spotify:track:a1G3BmW1AqUoJR23YaFeCI/similar?limit=5'
```

## Tenant dashboards & ops

All scoped to the token's tenant (every table read filters `tenant_id`, including both sides of
unions/joins, and row policies enforce it underneath). Event-time bucketing on `client_time` (UTC) unless
noted. Bad params → 400.

| Method | Path | Notes |
|---|---|---|
| GET | `/tenants/me/live?window=15m` | `5m`\|`15m`\|`60m`, as whole minutes from the minute containing now − window through the current minute. Plays/min (gap-filled), plays, clicks, active users/sessions (plays + clicks), `last_ingested_at`, `freshness_sec` from the rollups; top 5 tracks from `plays FINAL`. Events dated after the current minute excluded. |
| GET | `/tenants/me/trending?window=24h` | `24h`\|`7d`, `min_plays` (default 3), `limit` (≤50). Current vs previous equal window, `growth_pct` (null + `is_new` when previous = 0). |
| GET | `/tenants/me/retention?cohort=week&weeks=8` | Cohort = Monday of the user's first play (last 26 weeks); `weeks` = horizon 0..N (≤26); % of cohort with ≥1 play in week N, null for weeks not started. |
| GET | `/tenants/me/engagement?days=30` | Daily DAU/plays/skip rate (gap-filled), WAU, MAU, avg DAU (30d)/MAU stickiness, session count, avg/median session length, plays per session. |
| GET | `/tenants/me/funnel?days=30` | Per session: page_view → search → play (plays row or play click) → like → add_to_playlist. `sessions` (any order) and `sessions_in_order` (`windowFunnel`), step + overall conversion %. |
| GET | `/tenants/me/data-quality?days=7` | Window on `received_at`. Per collection: late rate (lateness > 1h), lateness p50/p90/p99/max, closed-day events (UTC day ended > 24h before receipt) + affected days, future-dated (client_time > received_at + 5 min), schema_version mix. |
| GET | `/ops/pipeline-lag?minutes=60` | Window on `ingested_at`. Per collection: Kafka → ClickHouse lag (ingested_at − cdc_ts; small negatives are normal because ingested_at is fixed per consume block) and producer → Kafka lag (cdc_ts − received_at), p50/p95/max in ms; buffered publishes over 1h are counted and excluded from `producer_to_kafka_streaming_ms`. |
| GET | `/ops/schema-fields?collection=plays` | `plays`\|`clicks`, `days` (default 30). Keys in `raw` (`JSONExtractKeys`) that are not typed columns (`system.columns`): occurrences, first/last seen, schema_versions, JSON types — promotion candidates (e.g. `audio_quality`, `device_model`). |

```bash
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/live?window=15m'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/trending?window=24h'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/retention?cohort=week&weeks=8'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/engagement?days=30'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/funnel?days=30'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/tenants/me/data-quality?days=7'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/ops/pipeline-lag?minutes=60'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/ops/schema-fields?collection=plays'
```

## Design notes

- **Tenant isolation, layer 1 (SQL)**: the tenant comes only from the verified token (`@Tenant()`). Every
  query is parameterized (`{tenant:String}`) and filters `tenant_id`. Users are resolved inside the tenant
  first, so another tenant's user id is a 404. The `tracks` catalog is global; recommendation profiles use
  only the tenant's own plays/clicks.
- **Tenant isolation, layer 2 (ClickHouse row policies)**, `clickhouse/init/04-security.sql`:
  - The API connects as `analytics_app`: `readonly = 2` (can read and set settings, cannot write, run DDL
    or lower `readonly`), `SELECT` only on `plays`, `clicks`, `users`, `tracks`, the two rollups and
    `system.columns(database, table, name)` (for `/ops/schema-fields`). No `events_raw`.
  - Row policy `tenant_isolation` on `plays`, `clicks`, `users`, `plays_rollup_1m`, `clicks_rollup_1m`:
    `USING tenant_id = getSetting('SQL_tenant_id') TO analytics_app` (`SQL_` is ClickHouse's default
    custom-setting prefix). `tracks` has no tenant and no policy. Policy `unrestricted` (`USING 1 TO default`)
    keeps the pipeline/admin user seeing every row.
  - The auth guard stores the tenant in an `AsyncLocalStorage` request context (opened per request by
    `RequestContextMiddleware`); `ClickHouseService` adds `SQL_tenant_id` to every query from it. Code
    cannot forget it, and a query that drops its `tenant_id` filter still only sees the token's tenant.
  - Fail closed: without `SQL_tenant_id` (no authenticated request) `getSetting` throws, so reading a
    tenant table errors instead of returning every tenant. `/health` only pings and needs no tenant.
  - The setting is chosen by the app, so the policy guards against app bugs (a missing filter), not against
    a compromised app holding the `analytics_app` password.
- **Rollups** (`clickhouse/init/03-rollups.sql`): `plays_rollup_1m` and `clicks_rollup_1m`,
  AggregatingMergeTree `ORDER BY (tenant_id, minute)`, filled by materialized views chained on the typed
  `plays`/`clicks` tables, bucketed by `toStartOfMinute(client_time)` (late events merge into their old
  bucket). Counts are `uniq` states of `event_id` / `user_id` / `session_id` (plus `skips` and
  `plays_by_platform` as `uniq(event_id)`), so at-least-once duplicates are not double counted;
  `sum(ms_played)` and `avg(lateness_sec)` *are* inflated by duplicates. `/tenants/me/overview` and
  `/tenants/me/live` read them with `uniqMerge`/`sumMerge`/`avgMerge`/`maxMerge`; every other endpoint
  stays on `FINAL`, which remains the exact path. A re-seed must truncate the rollups along with
  `plays`/`clicks`; reloading refills them through the MVs.
- **CDC correctness**: all reads use `FINAL` (ReplacingMergeTree dedups CDC replays); users/tracks also
  filter `is_deleted = 0`. Event bucketing uses `client_time`, so late events land where they belong;
  the report shows `avg_lateness_sec` and `data_freshness` (max `ingested_at` / `client_time`).
- **Recommendations** (`src/recommendations/recommendations.sql.ts`, one query): taste vector =
  ms_played-weighted mean of danceability, energy, valence, acousticness, tempo/250 over the last 90 days
  (×3 for liked tracks) + top-3 genres. Candidates = unplayed, non-deleted tracks, minus artists the user
  skips ≥70% (≥3 plays). `score = 1/(1+euclidean) + 0.15·genre_match + 0.05·popularity/100`.

## Layout

```
src/
  clickhouse/       client provider + ClickHouseService (query/one/ping; sets SQL_tenant_id per query)
  common/           AuthGuard (JWT), request context, @Tenant(), @Public(), UserIdPipe, UserScopeService (tenant-scoped 404)
  health/           GET /health
  reports/          GET /users/:id/report (SQL in reports.sql.ts)
  recommendations/  GET /users/:id/recommendations (SQL in recommendations.sql.ts)
  insights/         wrapped, history, streaks, taste-drift, mood recs, rediscover, similar tracks (SQL in insights.sql.ts)
  tenants/          GET /tenants/me/{overview,live,trending,retention,engagement,funnel,data-quality} (SQL in tenants.sql.ts)
  ops/              GET /ops/{pipeline-lag,schema-fields} (SQL in ops.sql.ts)
scripts/token.js    dev token minting (npm run token)
```

## Tests

`scripts/demo/smoke.sh` (repo root, service running): mints tokens with the token script, checks every
endpoint's status code, auth failures (no token, wrong signature, expired → 401), cross-tenant 404s, and
the row policies directly as `analytics_app` (only `t_acme` rows with `SQL_tenant_id = 't_acme'`, error
without the setting, writes denied).
