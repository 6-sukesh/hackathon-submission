// Tenant dashboard SQL. Every query filters `tenant_id = {tenant:String}` on every table it reads
// (including both sides of joins/unions), so aggregates never mix tenants; ClickHouse row policies
// enforce the same filter (SQL_tenant_id) underneath. All values are server-side `{name:Type}` params.
// FINAL collapses CDC replays. Days are UTC.
//
// overview and live read the per-minute rollups (clickhouse/init/03-rollups.sql) with -Merge
// combinators: plays/clicks/users/sessions/skips are distinct counts (replay-safe, exact at these
// sizes); minutes_listened and avg_lateness_sec are not replay-safe (see the caveat there). Rollup
// windows are whole minutes (event time). Everything else reads the raw tables with FINAL.

const T = `tenant_id = {tenant:String}`;

// ── overview: minute buckets from the one containing now - 24h onwards ─────
const LAST_24H = `${T} AND minute >= toStartOfMinute(now() - INTERVAL 24 HOUR)`;

// ── live: minute buckets [start of minute(now - minutes), current minute]; events dated after the
//    current minute (clock-skewed) are excluded. LIVE_RAW is the same window on client_time. ──
const LIVE = `${T}
  AND minute >= toStartOfMinute(now() - toIntervalMinute({minutes:UInt32}))
  AND minute <= toStartOfMinute(now())`;
const LIVE_RAW = `${T}
  AND client_time >= toStartOfMinute(now() - toIntervalMinute({minutes:UInt32}))
  AND client_time <  toStartOfMinute(now()) + 60`;

// ── trending: current = [now - h, now), previous = [now - 2h, now - h) ────
const CUR = `client_time >= now64(3) - toIntervalHour({hours:UInt32})`;
const TRENDING_RANGE = `${T} AND client_time >= now64(3) - toIntervalHour(2 * {hours:UInt32}) AND client_time < now64(3)`;

// ── day ranges: the last {days} UTC days including today ──────────────────
const DAYS = `${T}
  AND client_time >= toDateTime64(today() - ({days:UInt32} - 1), 3, 'UTC')
  AND client_time <  toDateTime64(today() + 1, 3, 'UTC')`;

// ── data quality: events RECEIVED in the last {days} days (arrival window, so late events count) ──
const RECEIVED = `${T} AND received_at >= now64(3) - toIntervalDay({days:UInt32})`;

export const TENANT_SQL = {
  // In skip_rate, `plays` is the alias (UInt64) — it shadows the state column of the same name.
  overviewTotals: `
    SELECT uniqMerge(plays) AS plays, uniqMerge(users) AS active_users,
           round(sumMerge(ms_played) / 60000, 1) AS minutes_listened,
           round(ifNotFinite(uniqMerge(skips) / plays, 0), 4) AS skip_rate,
           round(ifNotFinite(avgMerge(lateness_sec), 0), 1) AS avg_lateness_sec,
           maxMerge(last_ingested_at) AS last_ingested_at,
           uniqMapMerge(plays_by_platform) AS plays_by_platform
    FROM music.plays_rollup_1m WHERE ${LAST_24H}`,
  overviewPerMinute: `
    SELECT minute, uniqMerge(plays) AS plays, uniqMerge(users) AS users
    FROM music.plays_rollup_1m WHERE ${LAST_24H}
    GROUP BY minute
    ORDER BY minute WITH FILL
      FROM toStartOfMinute(now() - INTERVAL 24 HOUR) TO toStartOfMinute(now()) + 60 STEP 60`,
  overviewClicks: `
    SELECT uniqMerge(clicks) AS clicks FROM music.clicks_rollup_1m WHERE ${LAST_24H}`,

  livePerMinute: `
    SELECT minute, uniqMerge(plays) AS plays, uniqMerge(users) AS users
    FROM music.plays_rollup_1m WHERE ${LIVE}
    GROUP BY minute
    ORDER BY minute WITH FILL
      FROM toStartOfMinute(now('UTC') - toIntervalMinute({minutes:UInt32}))
      TO toStartOfMinute(now('UTC')) + 60 STEP 60`,
  // Users/sessions are merged across both rollups, so someone who only clicked still counts as active.
  liveTotals: `
    SELECT uniqMergeIf(events, src = 'play') AS plays, uniqMergeIf(events, src = 'click') AS clicks,
           uniqMerge(users) AS active_users, uniqMerge(sessions) AS active_sessions
    FROM (
      SELECT 'play' AS src, plays AS events, users, sessions FROM music.plays_rollup_1m WHERE ${LIVE}
      UNION ALL
      SELECT 'click' AS src, clicks AS events, users, sessions FROM music.clicks_rollup_1m WHERE ${LIVE}
    )`,
  // Per-track grain is not in the rollup; the live window is at most 60 minutes of raw rows.
  liveTopTracks: `
    SELECT track_id, any(track_name) AS track_name, any(artist_name) AS artist_name,
           count() AS plays, uniqExact(user_id) AS listeners
    FROM music.plays FINAL WHERE ${LIVE_RAW}
    GROUP BY track_id ORDER BY plays DESC, listeners DESC, track_id LIMIT 5`,
  // Not window-bound: "how current is this tenant's data at all". buckets = rollup rows (0 = no data).
  freshness: `
    SELECT sum(n) AS buckets, max(t) AS last_ingested_at,
           round((toUnixTimestamp64Milli(now64(3)) - toUnixTimestamp64Milli(max(t))) / 1000, 1) AS freshness_sec
    FROM (
      SELECT count() AS n, maxMerge(last_ingested_at) AS t FROM music.plays_rollup_1m WHERE ${T}
      UNION ALL
      SELECT count() AS n, maxMerge(last_ingested_at) AS t FROM music.clicks_rollup_1m WHERE ${T}
    )`,

  trendingTotals: `
    SELECT countIf(${CUR}) AS current_plays, countIf(NOT (${CUR})) AS previous_plays
    FROM music.plays FINAL WHERE ${TRENDING_RANGE}`,
  // Only growing tracks with >= minPlays in the current window; new tracks (previous = 0) first,
  // then by growth %, then by current plays.
  trending: `
    SELECT track_id, any(track_name) AS track_name, any(artist_name) AS artist_name,
           countIf(${CUR}) AS current_plays,
           countIf(NOT (${CUR})) AS previous_plays,
           toInt64(current_plays) - toInt64(previous_plays) AS delta,
           if(previous_plays = 0, NULL, round(100 * delta / previous_plays, 1)) AS growth_pct,
           previous_plays = 0 AS is_new,
           uniqExactIf(user_id, ${CUR}) AS current_listeners
    FROM music.plays FINAL WHERE ${TRENDING_RANGE}
    GROUP BY track_id
    HAVING current_plays >= {minPlays:UInt32} AND current_plays > previous_plays
    ORDER BY growth_pct DESC NULLS FIRST, current_plays DESC, track_id
    LIMIT {limit:UInt32}`,

  // Cohort = Monday of the user's first-ever play in this tenant (users.created_at predates the data).
  // Rows: (cohort_week, week_n, distinct users with >= 1 play in week_n after the cohort week).
  retention: `
    WITH first_play AS (
      SELECT user_id, toMonday(min(client_time)) AS cohort_week
      FROM music.plays FINAL
      WHERE ${T} AND client_time <= now64(3)
      GROUP BY user_id
    )
    SELECT f.cohort_week AS cohort_week,
           intDiv(dateDiff('day', f.cohort_week, a.week), 7) AS week_n,
           uniqExact(a.user_id) AS users
    FROM (
      SELECT DISTINCT user_id, toMonday(client_time) AS week
      FROM music.plays FINAL
      WHERE ${T}
        AND client_time >= toDateTime64(toMonday(today()) - toIntervalWeek({lookback:UInt32}), 3, 'UTC')
        AND client_time <= now64(3)
    ) AS a
    INNER JOIN first_play AS f ON a.user_id = f.user_id
    WHERE f.cohort_week >= toMonday(today()) - toIntervalWeek({lookback:UInt32})
      AND week_n <= {weeks:UInt32}
    GROUP BY cohort_week, week_n
    ORDER BY cohort_week, week_n`,

  engagementDaily: `
    SELECT toDate(client_time) AS day, uniqExact(user_id) AS dau, count() AS plays,
           sum(skipped) AS skips
    FROM music.plays FINAL WHERE ${DAYS}
    GROUP BY day
    ORDER BY day WITH FILL FROM today() - ({days:UInt32} - 1) TO today() + 1`,
  // WAU / MAU / stickiness are fixed 7- and 30-UTC-day windows ending today, independent of {days}.
  engagementActive: `
    SELECT uniqExactIf(user_id, client_time >= toDateTime64(today() - 6, 3, 'UTC')) AS wau,
           uniqExact(user_id) AS mau,
           round(uniqExact((user_id, toDate(client_time))) / 30, 2) AS avg_dau_30d
    FROM music.plays FINAL
    WHERE ${T}
      AND client_time >= toDateTime64(today() - 29, 3, 'UTC')
      AND client_time <  toDateTime64(today() + 1, 3, 'UTC')`,
  // Session length = last play start - first play start + ms_played of the last play.
  engagementSessions: `
    SELECT count() AS sessions,
           sum(n_plays) AS plays,
           round(ifNotFinite(sum(n_skips) / sum(n_plays), 0), 4) AS skip_rate,
           round(ifNotFinite(avg(duration_ms), 0) / 1000, 1) AS avg_session_sec,
           round(ifNotFinite(quantileExact(0.5)(duration_ms), 0) / 1000, 1) AS median_session_sec,
           round(ifNotFinite(avg(n_plays), 0), 2) AS avg_plays_per_session
    FROM (
      SELECT session_id,
             toUnixTimestamp64Milli(max(client_time)) - toUnixTimestamp64Milli(min(client_time))
               + argMax(ms_played, client_time) AS duration_ms,
             count() AS n_plays, sum(skipped) AS n_skips
      FROM music.plays FINAL WHERE ${DAYS} AND session_id != ''
      GROUP BY session_id
    )`,

  // One row per session: which steps it contains (any order) and how deep it got in strict order
  // (windowFunnel, events may be interleaved). "play" = a row in plays OR a click of type play.
  funnel: `
    SELECT count() AS sessions,
           countIf(s1) AS any_1, countIf(s1 AND s2) AS any_2, countIf(s1 AND s2 AND s3) AS any_3,
           countIf(s1 AND s2 AND s3 AND s4) AS any_4, countIf(s1 AND s2 AND s3 AND s4 AND s5) AS any_5,
           countIf(depth >= 1) AS ord_1, countIf(depth >= 2) AS ord_2, countIf(depth >= 3) AS ord_3,
           countIf(depth >= 4) AS ord_4, countIf(depth >= 5) AS ord_5
    FROM (
      SELECT session_id,
             max(ev = 'page_view') AS s1, max(ev = 'search') AS s2, max(ev = 'play') AS s3,
             max(ev = 'like') AS s4, max(ev = 'add_to_playlist') AS s5,
             windowFunnel(86400000)(ts, ev = 'page_view', ev = 'search', ev = 'play', ev = 'like',
                                    ev = 'add_to_playlist') AS depth
      FROM (
        SELECT session_id, toUInt64(toUnixTimestamp64Milli(client_time)) AS ts, toString(click_type) AS ev
        FROM music.clicks FINAL
        WHERE ${DAYS} AND session_id != ''
          AND click_type IN ('page_view', 'search', 'play', 'like', 'add_to_playlist')
        UNION ALL
        SELECT session_id, toUInt64(toUnixTimestamp64Milli(client_time)) AS ts, 'play' AS ev
        FROM music.plays FINAL
        WHERE ${DAYS} AND session_id != ''
      )
      GROUP BY session_id
    )`,

  // Closed day: the event's UTC day had ended more than 24h before it was received.
  dataQuality: `
    SELECT collection, count() AS events,
           countIf(lateness_sec > 3600) AS late_events,
           round(ifNotFinite(late_events / events, 0), 4) AS late_rate,
           quantilesExact(0.5, 0.9, 0.99)(lateness_sec) AS lateness_q,
           max(lateness_sec) AS max_lateness_sec,
           countIf(received_at >= toStartOfDay(client_time) + INTERVAL 2 DAY) AS closed_day_events,
           countIf(client_time > received_at + INTERVAL 5 MINUTE) AS future_dated_events,
           max(-lateness_sec) AS max_clock_ahead_sec
    FROM (
      SELECT 'plays' AS collection, client_time, received_at, lateness_sec FROM music.plays FINAL WHERE ${RECEIVED}
      UNION ALL
      SELECT 'clicks' AS collection, client_time, received_at, lateness_sec FROM music.clicks FINAL WHERE ${RECEIVED}
    )
    GROUP BY collection`,
  closedDays: `
    SELECT toDate(client_time) AS day, count() AS events,
           countIf(collection = 'plays') AS plays, countIf(collection = 'clicks') AS clicks
    FROM (
      SELECT 'plays' AS collection, client_time, received_at FROM music.plays FINAL WHERE ${RECEIVED}
      UNION ALL
      SELECT 'clicks' AS collection, client_time, received_at FROM music.clicks FINAL WHERE ${RECEIVED}
    )
    WHERE received_at >= toStartOfDay(client_time) + INTERVAL 2 DAY
    GROUP BY day ORDER BY day DESC LIMIT 30`,
  schemaMix: `
    SELECT 'plays' AS collection, schema_version, count() AS events
    FROM music.plays FINAL WHERE ${RECEIVED} GROUP BY schema_version
    UNION ALL
    SELECT 'clicks' AS collection, schema_version, count() AS events
    FROM music.clicks FINAL WHERE ${RECEIVED} GROUP BY schema_version`,
};
