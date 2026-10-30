# Music analytics: Mongo + Kafka → ClickHouse → NestJS

```
Mongo (users, tracks) ─► Flink CDC ─► Kafka cdc.music ─────────────┐
other teams' producers ─► Kafka events.plays, events.clicks ───────┼─► ClickHouse Kafka engine ─► events_raw
                                                     bad messages ─┘─► music.ingest_errors     │ materialized views
web-app ─► analytics (NestJS :3000) ◄── plays / clicks / users / tracks ◄────────────────────────┘
```
Kafka is Redpanda: `localhost:19092` from the host, `redpanda:9092` inside compose; Console on http://localhost:8080.
The plays/clicks message format is in [contracts/events.md](contracts/events.md).

- [design.md](design.md) — problem, system boundary, architecture, how each constraint is handled, roadmap
- [DEMO.md](DEMO.md) — 10-minute demo script and Q&A prep
- [analytics/README.md](analytics/README.md) — every API endpoint with example curls

## Run
```bash
docker compose up -d                                       # redpanda (+ topics, console), mongo, clickhouse, flink
docker exec -i mongo mongosh --quiet < mongo/init-schema.js # clickhouse/init runs automatically on a fresh volume
./flink/submit.sh                                          # build jar, submit (resumes from a savepoint if running; --fresh)
cd scripts/seed && npm i && node seed.js && cd ../..       # FULL RESET: users/tracks → Mongo, plays/clicks → Kafka
./flink/check.sh                                           # job state, Mongo vs ClickHouse, topic offsets, consumer lag
cd analytics && npm i && npm run start                     # API on :3000
node scripts/seed/seed.js --live 20                        # optional: stream ~20 events/s, 10% late, 10% ISO timestamps
scripts/demo/kafka-tests.sh [--replay --savepoint --restart | --all]   # end-to-end tests of the Kafka path
```

## How the requirements are handled
- **Schema changes**: `events_raw.raw` keeps the whole event (or normalized Mongo document); typed tables keep `raw` too.
  New fields need no pipeline change: query `JSONExtract*(raw, 'field')`, then promote to a column when needed.
- **Out-of-order / late events**: every event has `client_time` (device), `received_at` (producer),
  `cdc_ts` (Kafka broker time; Mongo change stream for users/tracks) and `ingested_at` (ClickHouse). Tables are
  partitioned/sorted by `client_time`, so late events land in the right bucket; `ReplacingMergeTree(cdc_ts)` collapses
  replays and duplicate `event_id`s — query with `FINAL`.
- **Bad messages** never block ingestion: they go to `music.ingest_errors` with topic/partition/offset and the error.
- **Tenant isolation**: the tenant comes from a signed JWT (`tenant_id` claim). The API connects to ClickHouse as the
  read-only `analytics_app` user and sets `SQL_tenant_id` on every query; row policies return only that tenant's rows
  and fail the query if the setting is missing. Queries also filter `tenant_id` explicitly.
- **Dashboards at scale**: `plays_rollup_1m` / `clicks_rollup_1m` (per tenant × minute, fed by views) serve the
  overview and live endpoints; late events add into their old minute.

## API
```bash
ACME=$(npm --prefix analytics run -s token -- t_acme)       # dev JWT signed with analytics/.env JWT_SECRET
curl localhost:3000/health
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_04/report?from=2026-10-01&to=2026-10-30'
curl -H "Authorization: Bearer $ACME" 'localhost:3000/users/u_acme_04/recommendations?limit=10'
curl -H "Authorization: Bearer $ACME" localhost:3000/tenants/me/overview
scripts/demo/smoke.sh                                     # every endpoint + auth + row-policy checks
```
