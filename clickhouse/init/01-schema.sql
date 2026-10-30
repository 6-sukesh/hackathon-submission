-- Analytics DB. Everything lands in music.events_raw (from Kafka, see 02-kafka.sql); materialized views fan out
-- into typed tables.
--
-- Schema changes: the full normalized Mongo document is kept in `raw`. A new Mongo field needs no
--   Flink change — it is already in `raw` and queryable via JSONExtract*(raw, 'field'); promote it to a
--   typed column later with ALTER TABLE ... ADD COLUMN + updating the MV.
-- Out-of-order / late data: tables are partitioned and sorted by client_time (event time), so a late event
--   lands in the bucket it belongs to. ReplacingMergeTree(cdc_ts) collapses CDC replays/duplicates and keeps
--   the newest version of a row. Query with FINAL (or argMax) for exact counts.
-- Times: client_time = device clock, received_at = producer receive time,
--   cdc_ts = Mongo change-stream time (users/tracks) or Kafka broker time (plays/clicks), ingested_at = ClickHouse write time.
--   plays/clicks timestamps may arrive as epoch-ms numbers or ISO-8601 strings (contracts/events.md).

CREATE DATABASE IF NOT EXISTS music;

-- Landing table written by the Kafka views in 02-kafka.sql.
-- raw = the event JSON as published (plays/clicks), or the normalized Mongo document (users/tracks):
--   dates as epoch millis, ObjectId as hex string, numbers as plain JSON numbers.
CREATE TABLE IF NOT EXISTS music.events_raw
(
    source_db   LowCardinality(String),
    collection  LowCardinality(String),
    op          LowCardinality(String),          -- r = snapshot, c = insert, u = update/replace, d = delete
    doc_id      String,                          -- Mongo _id (users/tracks) or event_id (plays/clicks)
    raw         String,                          -- '{}' for deletes
    cdc_ts_ms   Int64,                           -- change-stream time (epoch ms; snapshot rows use read time) or Kafka broker time
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (collection, ingested_at)
TTL toDateTime(ingested_at) + INTERVAL 30 DAY;

-- ── plays ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS music.plays
(
    event_id       String,
    tenant_id      LowCardinality(String),
    user_id        String,
    track_id       String,
    track_name     String,
    artist_name    String,
    genre          LowCardinality(String),
    ms_played      UInt32,
    reason_start   LowCardinality(String),
    reason_end     LowCardinality(String),
    skipped        UInt8,
    shuffle        UInt8,
    platform       LowCardinality(String),
    conn_country   LowCardinality(String),
    session_id     String,
    schema_version UInt16,
    client_time    DateTime64(3, 'UTC'),
    received_at    DateTime64(3, 'UTC'),
    cdc_ts         DateTime64(3, 'UTC'),
    ingested_at    DateTime64(3, 'UTC'),
    lateness_sec   Int64 ALIAS dateDiff('second', client_time, received_at),
    raw            String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(cdc_ts)
PARTITION BY toYYYYMM(client_time)
ORDER BY (tenant_id, user_id, client_time, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS music.plays_mv TO music.plays AS
SELECT
    JSONExtractString(raw, 'event_id')                        AS event_id,
    JSONExtractString(raw, 'tenant_id')                       AS tenant_id,
    JSONExtractString(raw, 'user_id')                         AS user_id,
    JSONExtractString(raw, 'track_id')                        AS track_id,
    JSONExtractString(raw, 'track_name')                      AS track_name,
    JSONExtractString(raw, 'artist_name')                     AS artist_name,
    JSONExtractString(raw, 'genre')                           AS genre,
    toUInt32(greatest(JSONExtractInt(raw, 'ms_played'), 0))   AS ms_played,
    JSONExtractString(raw, 'reason_start')                    AS reason_start,
    JSONExtractString(raw, 'reason_end')                      AS reason_end,
    toUInt8(JSONExtractBool(raw, 'skipped'))                  AS skipped,
    toUInt8(JSONExtractBool(raw, 'shuffle'))                  AS shuffle,
    JSONExtractString(raw, 'platform')                        AS platform,
    JSONExtractString(raw, 'conn_country')                    AS conn_country,
    JSONExtractString(raw, 'session_id')                      AS session_id,
    toUInt16(greatest(JSONExtractInt(raw, 'schema_version'), 1)) AS schema_version,
    if(JSONType(raw, 'client_time') = 'String',
       parseDateTime64BestEffortOrZero(JSONExtractString(raw, 'client_time'), 3, 'UTC'),
       fromUnixTimestamp64Milli(JSONExtractInt(raw, 'client_time'), 'UTC'))   AS client_time,
    if(JSONType(raw, 'received_at') = 'String',
       parseDateTime64BestEffortOrZero(JSONExtractString(raw, 'received_at'), 3, 'UTC'),
       fromUnixTimestamp64Milli(JSONExtractInt(raw, 'received_at'), 'UTC'))   AS received_at,
    fromUnixTimestamp64Milli(cdc_ts_ms, 'UTC')                AS cdc_ts,
    ingested_at,
    raw
FROM music.events_raw
WHERE collection = 'plays' AND op IN ('r', 'c', 'u');

-- ── clicks ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS music.clicks
(
    event_id       String,
    tenant_id      LowCardinality(String),
    user_id        String,
    session_id     String,
    click_type     LowCardinality(String),
    target_type    LowCardinality(String),
    target_id      String,
    page           String,
    search_query   String,
    platform       LowCardinality(String),
    schema_version UInt16,
    client_time    DateTime64(3, 'UTC'),
    received_at    DateTime64(3, 'UTC'),
    cdc_ts         DateTime64(3, 'UTC'),
    ingested_at    DateTime64(3, 'UTC'),
    lateness_sec   Int64 ALIAS dateDiff('second', client_time, received_at),
    raw            String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(cdc_ts)
PARTITION BY toYYYYMM(client_time)
ORDER BY (tenant_id, user_id, client_time, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS music.clicks_mv TO music.clicks AS
SELECT
    JSONExtractString(raw, 'event_id')     AS event_id,
    JSONExtractString(raw, 'tenant_id')    AS tenant_id,
    JSONExtractString(raw, 'user_id')      AS user_id,
    JSONExtractString(raw, 'session_id')   AS session_id,
    JSONExtractString(raw, 'click_type')   AS click_type,
    JSONExtractString(raw, 'target_type')  AS target_type,
    JSONExtractString(raw, 'target_id')    AS target_id,
    JSONExtractString(raw, 'page')         AS page,
    JSONExtractString(raw, 'search_query') AS search_query,
    JSONExtractString(raw, 'platform')     AS platform,
    toUInt16(greatest(JSONExtractInt(raw, 'schema_version'), 1)) AS schema_version,
    if(JSONType(raw, 'client_time') = 'String',
       parseDateTime64BestEffortOrZero(JSONExtractString(raw, 'client_time'), 3, 'UTC'),
       fromUnixTimestamp64Milli(JSONExtractInt(raw, 'client_time'), 'UTC'))   AS client_time,
    if(JSONType(raw, 'received_at') = 'String',
       parseDateTime64BestEffortOrZero(JSONExtractString(raw, 'received_at'), 3, 'UTC'),
       fromUnixTimestamp64Milli(JSONExtractInt(raw, 'received_at'), 'UTC'))   AS received_at,
    fromUnixTimestamp64Milli(cdc_ts_ms, 'UTC') AS cdc_ts,
    ingested_at,
    raw
FROM music.events_raw
WHERE collection = 'clicks' AND op IN ('r', 'c', 'u');

-- ── users (mutable reference data; deletes honoured via is_deleted) ─────
CREATE TABLE IF NOT EXISTS music.users
(
    doc_id       String,
    user_id      String,
    tenant_id    LowCardinality(String),
    display_name String,
    email        String,
    country      LowCardinality(String),
    plan         LowCardinality(String),
    created_at   DateTime64(3, 'UTC'),
    cdc_ts       DateTime64(3, 'UTC'),
    ingested_at  DateTime64(3, 'UTC'),
    is_deleted   UInt8,
    raw          String
)
ENGINE = ReplacingMergeTree(cdc_ts, is_deleted)
ORDER BY doc_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS music.users_mv TO music.users AS
SELECT
    doc_id,
    JSONExtractString(raw, 'user_id')      AS user_id,
    JSONExtractString(raw, 'tenant_id')    AS tenant_id,
    JSONExtractString(raw, 'display_name') AS display_name,
    JSONExtractString(raw, 'email')        AS email,
    JSONExtractString(raw, 'country')      AS country,
    JSONExtractString(raw, 'plan')         AS plan,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'created_at'), 'UTC') AS created_at,
    fromUnixTimestamp64Milli(cdc_ts_ms, 'UTC') AS cdc_ts,
    ingested_at,
    toUInt8(op = 'd') AS is_deleted,
    raw
FROM music.events_raw
WHERE collection = 'users';

-- ── tracks (catalog + audio features, used for recommendations) ─────────
CREATE TABLE IF NOT EXISTS music.tracks
(
    doc_id       String,
    track_id     String,
    track_name   String,
    artist_id    String,
    artist_name  String,
    album_name   String,
    genre        LowCardinality(String),
    duration_ms  UInt32,
    popularity   UInt8,
    release_year UInt16,
    danceability Float32,
    energy       Float32,
    valence      Float32,
    tempo        Float32,
    acousticness Float32,
    cdc_ts       DateTime64(3, 'UTC'),
    ingested_at  DateTime64(3, 'UTC'),
    is_deleted   UInt8,
    raw          String
)
ENGINE = ReplacingMergeTree(cdc_ts, is_deleted)
ORDER BY doc_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS music.tracks_mv TO music.tracks AS
SELECT
    doc_id,
    JSONExtractString(raw, 'track_id')    AS track_id,
    JSONExtractString(raw, 'track_name')  AS track_name,
    JSONExtractString(raw, 'artist_id')   AS artist_id,
    JSONExtractString(raw, 'artist_name') AS artist_name,
    JSONExtractString(raw, 'album_name')  AS album_name,
    JSONExtractString(raw, 'genre')       AS genre,
    toUInt32(greatest(JSONExtractInt(raw, 'duration_ms'), 0))  AS duration_ms,
    toUInt8(least(greatest(JSONExtractInt(raw, 'popularity'), 0), 100)) AS popularity,
    toUInt16(greatest(JSONExtractInt(raw, 'release_year'), 0)) AS release_year,
    JSONExtractFloat(raw, 'danceability') AS danceability,
    JSONExtractFloat(raw, 'energy')       AS energy,
    JSONExtractFloat(raw, 'valence')      AS valence,
    JSONExtractFloat(raw, 'tempo')        AS tempo,
    JSONExtractFloat(raw, 'acousticness') AS acousticness,
    fromUnixTimestamp64Milli(cdc_ts_ms, 'UTC') AS cdc_ts,
    ingested_at,
    toUInt8(op = 'd') AS is_deleted,
    raw
FROM music.events_raw
WHERE collection = 'tracks';
