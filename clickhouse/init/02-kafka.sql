-- Kafka (Redpanda) ingestion. Runs after 01-schema.sql; everything lands in music.events_raw, so the typed
-- views in 01-schema.sql serve both paths unchanged.
--
--   events.plays, events.clicks  JSON events published by other teams (contract: contracts/events.md)
--   cdc.music                    users/tracks change envelopes from the Flink CDC job (key collection:doc_id)
--
-- Bad messages go to music.ingest_errors with topic/partition/offset — never silently dropped and never
--   blocking the consumer (kafka_handle_error_mode = 'stream' turns parse failures into rows with _error set).
-- Delivery is at-least-once: offsets are committed after the insert, so a crash can replay a block.
--   ReplacingMergeTree in the typed tables collapses the replays (query with FINAL).
-- For Kafka-sourced events cdc_ts_ms / cdc_ts holds the Kafka broker timestamp of the message (the events topics
--   use message.timestamp.type=LogAppendTime). It grows with every (re)publish, so it is still a valid
--   replay/dedup version for ReplacingMergeTree(cdc_ts).
-- ingested_at on Kafka-sourced rows is when ClickHouse started the consume block (now64() is fixed per streaming
--   query), so it can be up to kafka_flush_interval_ms earlier than the actual write and even precede cdc_ts.
-- Consumer groups: clickhouse-events, clickhouse-cdc. Pause ingestion with DETACH TABLE music.kafka_events
--   (Kafka keeps buffering), resume with ATTACH TABLE; reprocess with `rpk group seek` while detached.

-- ── dead-letter table ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS music.ingest_errors
(
    topic       LowCardinality(String),
    partition   UInt64,
    offset      UInt64,
    key         String,
    raw_message String,
    error       String,
    received_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_at)
ORDER BY (topic, received_at)
TTL toDateTime(received_at) + INTERVAL 30 DAY;

-- ── events.plays / events.clicks ────────────────────────────────────────
-- JSONAsString keeps each message as one string, so new producer fields flow into `raw` untouched.
CREATE TABLE IF NOT EXISTS music.kafka_events
(
    message String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'events.plays,events.clicks',
         kafka_group_name = 'clickhouse-events',
         kafka_format = 'JSONAsString',
         kafka_handle_error_mode = 'stream',
         kafka_flush_interval_ms = 1000;   -- default 7.5s; flush every second for near-real-time

-- Validation runs once here; the two views below split valid rows and errors. Null engine stores nothing.
CREATE TABLE IF NOT EXISTS music.kafka_events_checked
(
    topic       String,
    partition   UInt64,
    offset      UInt64,
    key         String,
    kafka_ts_ms Int64,
    message     String,
    error       String   -- '' = valid
)
ENGINE = Null;

-- Required by contracts/events.md: non-empty string ids, client_time/received_at as epoch ms or ISO-8601,
-- plays also need track_id + numeric ms_played, clicks need click_type.
CREATE MATERIALIZED VIEW IF NOT EXISTS music.kafka_events_check_mv TO music.kafka_events_checked AS
WITH
    if(_topic = 'events.plays', ['event_id', 'tenant_id', 'user_id', 'track_id'],
                                ['event_id', 'tenant_id', 'user_id', 'click_type']) AS required_strings,
    arrayFirst(f -> JSONType(message, f) != 'String' OR JSONExtractString(message, f) = '',
               required_strings) AS bad_string,
    arrayFirst(f -> NOT (JSONType(message, f) IN ('Int64', 'UInt64', 'Double')
                         OR (JSONType(message, f) = 'String'
                             AND parseDateTime64BestEffortOrNull(JSONExtractString(message, f), 3, 'UTC') IS NOT NULL)),
               ['client_time', 'received_at']) AS bad_time
SELECT
    _topic                                                        AS topic,
    _partition                                                    AS partition,
    _offset                                                       AS offset,
    _key                                                          AS key,
    coalesce(toUnixTimestamp64Milli(_timestamp_ms), toUnixTimestamp64Milli(now64(3))) AS kafka_ts_ms,
    if(_error = '', message, _raw_message)                        AS message,
    multiIf(_error != '',                     _error,
            JSONType(message) != 'Object',    'not a JSON object',
            bad_string != '',                 concat('missing or empty field: ', bad_string),
            bad_time != '',                   concat('missing or unparseable timestamp: ', bad_time),
            _topic = 'events.plays' AND JSONType(message, 'ms_played') NOT IN ('Int64', 'UInt64', 'Double'),
                                              'missing or non-numeric field: ms_played',
            '')                                                   AS error
FROM music.kafka_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS music.kafka_events_raw_mv TO music.events_raw AS
SELECT
    'kafka'                                    AS source_db,
    multiIf(topic = 'events.plays', 'plays', topic = 'events.clicks', 'clicks', topic) AS collection,
    'c'                                        AS op,
    JSONExtractString(message, 'event_id')     AS doc_id,
    message                                    AS raw,
    kafka_ts_ms                                AS cdc_ts_ms
FROM music.kafka_events_checked
WHERE error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS music.kafka_events_errors_mv TO music.ingest_errors AS
SELECT topic, partition, offset, key, message AS raw_message, error
FROM music.kafka_events_checked
WHERE error != '';

-- ── cdc.music (users / tracks from Flink) ───────────────────────────────
-- One envelope per message, already in the events_raw shape: source_db, collection, op, doc_id, raw, cdc_ts_ms.
CREATE TABLE IF NOT EXISTS music.kafka_cdc
(
    source_db  String,
    collection String,
    op         String,
    doc_id     String,
    raw        String,
    cdc_ts_ms  Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'cdc.music',
         kafka_group_name = 'clickhouse-cdc',
         kafka_format = 'JSONEachRow',
         kafka_handle_error_mode = 'stream',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS music.kafka_cdc_raw_mv TO music.events_raw AS
SELECT source_db, collection, op, doc_id, raw, cdc_ts_ms
FROM music.kafka_cdc
WHERE _error = '' AND collection != '' AND doc_id != '' AND op IN ('r', 'c', 'u', 'd');

CREATE MATERIALIZED VIEW IF NOT EXISTS music.kafka_cdc_errors_mv TO music.ingest_errors AS
SELECT
    _topic     AS topic,
    _partition AS partition,
    _offset    AS offset,
    _key       AS key,
    if(_error != '', _raw_message,
       formatRowNoNewline('JSONEachRow', source_db, collection, op, doc_id, raw, cdc_ts_ms)) AS raw_message,
    if(_error != '', _error, 'invalid envelope: needs collection, doc_id and op in (r, c, u, d)') AS error
FROM music.kafka_cdc
WHERE NOT (_error = '' AND collection != '' AND doc_id != '' AND op IN ('r', 'c', 'u', 'd'));
