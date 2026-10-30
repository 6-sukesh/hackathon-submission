# Event contract: `events.plays`, `events.clicks`

Producers (other teams' services) publish user interaction events to Kafka (Redpanda). The analytics
pipeline consumes them into ClickHouse (`clickhouse/init/02-kafka.sql`). This file is the contract
between the two sides.

| | |
|---|---|
| Brokers | `redpanda:9092` (inside the compose network), `localhost:19092` (host) |
| Topics | `events.plays` (6 partitions), `events.clicks` (6 partitions) |
| Message key | `tenant_id:user_id` (UTF-8), e.g. `t_acme:u_acme_04`: keeps one user's events in order on one partition |
| Message value | one JSON object (UTF-8), no envelope, no batching of several events in one message |
| Delivery | at-least-once is fine: re-sending an event with the same `event_id` is deduplicated |
| Current `schema_version` | plays `2`, clicks `1` |

## Common rules

- **`event_id`** is a globally unique id for the event (a UUID). It is the deduplication key, so a retry
  must reuse the same `event_id`.
- **Timestamps** (`client_time`, `received_at`) are accepted in either form:
  - epoch milliseconds as a JSON number: `1793356800000`
  - ISO-8601 string with a zone offset: `"2026-10-30T10:15:00.123Z"` or `"2026-10-30T15:45:00+05:30"`.
    A string without an offset is read as UTC.
- `client_time` is when the device says it happened (may be hours or days old for offline clients; the
  event is still counted on that day). `received_at` is when the producer's backend received it.
- Unknown extra fields are allowed and kept: they are stored in ClickHouse `raw` and queryable at once.
- A message that is not a JSON object, misses a required field, or has an unparseable timestamp is not
  dropped: it goes to `music.ingest_errors` (topic, partition, offset, key, message, error) and the
  consumer moves on.

## `events.plays`

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | string | yes | UUID, dedup key |
| `tenant_id` | string | yes | e.g. `t_acme` |
| `user_id` | string | yes | e.g. `u_acme_04` |
| `track_id` | string | yes | e.g. `spotify:track:...` |
| `client_time` | number (epoch ms) or ISO-8601 string | yes | device time |
| `received_at` | number (epoch ms) or ISO-8601 string | yes | backend receive time |
| `ms_played` | integer | yes | ≥ 0 |
| `schema_version` | integer | no (default 1) | see versioning |
| `track_name` | string | no | denormalized for convenience |
| `artist_name` | string | no | |
| `genre` | string | no | |
| `reason_start` | string | no | e.g. `clickrow`, `trackdone` |
| `reason_end` | string | no | e.g. `trackdone`, `fwdbtn` |
| `skipped` | boolean | no | |
| `shuffle` | boolean | no | |
| `platform` | string | no | `web`, `android`, `ios`, `desktop` |
| `conn_country` | string | no | ISO country code |
| `session_id` | string | no | |
| `audio_quality` | string | no | v2: `normal`, `high`, `lossless` |
| `device_model` | string | no | v2 |

```json
{
  "event_id": "7f1c2a7e-3a1b-4f55-9a0e-8f2a4c1d9b10",
  "tenant_id": "t_acme",
  "user_id": "u_acme_04",
  "track_id": "spotify:track:4cOdK2wGLETKBW3PvgPWqT",
  "track_name": "Blinding Lights",
  "artist_name": "The Weeknd",
  "genre": "pop",
  "client_time": 1793356800000,
  "received_at": 1793356801250,
  "ms_played": 201000,
  "reason_start": "clickrow",
  "reason_end": "trackdone",
  "skipped": false,
  "shuffle": true,
  "platform": "android",
  "conn_country": "IN",
  "session_id": "c0b8e7c4-58a8-4d0f-9d0c-2b6f3f6c1e22",
  "schema_version": 2,
  "audio_quality": "high",
  "device_model": "Pixel 8"
}
```

## `events.clicks`

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | string | yes | UUID, dedup key |
| `tenant_id` | string | yes | |
| `user_id` | string | yes | |
| `click_type` | string | yes | `play`, `pause`, `skip`, `like`, `unlike`, `add_to_playlist`, `search`, `follow_artist`, `share`, `page_view` |
| `client_time` | number (epoch ms) or ISO-8601 string | yes | |
| `received_at` | number (epoch ms) or ISO-8601 string | yes | |
| `schema_version` | integer | no (default 1) | |
| `session_id` | string | no | |
| `target_type` | string | no | `track`, `artist`, `album`, `playlist`, `page`, `search` |
| `target_id` | string | no | |
| `page` | string | no | e.g. `/home`, `/player` |
| `search_query` | string | no | only for `search` |
| `platform` | string | no | |

```json
{
  "event_id": "1e0f8a4b-6a55-4d6e-a1f4-0c6d2c7b8e90",
  "tenant_id": "t_globex",
  "user_id": "u_globex_03",
  "session_id": "c0b8e7c4-58a8-4d0f-9d0c-2b6f3f6c1e22",
  "click_type": "like",
  "target_type": "track",
  "target_id": "spotify:track:4cOdK2wGLETKBW3PvgPWqT",
  "page": "/player",
  "platform": "web",
  "client_time": "2026-10-30T10:15:00.123Z",
  "received_at": "2026-10-30T10:15:01.004Z",
  "schema_version": 1
}
```

## Versioning

- `schema_version` is an integer, bumped by the producer whenever it adds fields.
- Within a topic only **additive, optional** changes are allowed: new optional fields, new enum values.
  Consumers must keep working with old and new versions at the same time.
- Never remove or rename a field, change its type or meaning, or make an optional field required.
  That is a **breaking change**: publish a new event type on a new topic (e.g. `events.plays.v2`)
  and run both until consumers have moved.
