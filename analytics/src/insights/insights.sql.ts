import { CATALOG, TASTE_PROFILE_CTES, USER_PLAYS } from '../recommendations/recommendations.sql';

// All per-user queries are scoped by tenant + user and bucket on client_time (event time, UTC).
// FINAL collapses CDC replays; the `tracks` catalog is global (FINAL + is_deleted = 0 via CATALOG).
const USER = `tenant_id = {tenant:String} AND user_id = {userId:String}`;

// [from, to) on client_time; both are Dates computed by the service.
const RANGE = `${USER}
  AND client_time >= toDateTime64({from:Date}, 3, 'UTC')
  AND client_time <  toDateTime64({to:Date}, 3, 'UTC')`;

// ms_played-weighted feature averages over (track_id, ms) rows in `weights`; NULL when nothing matched the catalog.
const WEIGHTED_FEATURES = `
  round(sum(w.ms * c.dance) / nullIf(sum(w.ms), 0), 3) AS danceability,
  round(sum(w.ms * c.energy) / nullIf(sum(w.ms), 0), 3) AS energy,
  round(sum(w.ms * c.valence) / nullIf(sum(w.ms), 0), 3) AS valence,
  round(sum(w.ms * c.acoustic) / nullIf(sum(w.ms), 0), 3) AS acousticness,
  round(250 * sum(w.ms * c.tempo_n) / nullIf(sum(w.ms), 0), 1) AS tempo`;

export const WRAPPED_SQL = {
  totals: `
    SELECT count() AS plays,
           round(sum(ms_played) / 60000, 1) AS minutes_listened,
           uniqExact(track_id) AS unique_tracks,
           uniqExact(artist_name) AS unique_artists,
           uniqExact(toDate(client_time)) AS active_days
    FROM music.plays FINAL WHERE ${RANGE}`,
  topTracks: `
    SELECT track_id, any(track_name) AS track_name, any(artist_name) AS artist_name,
           count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${RANGE}
    GROUP BY track_id ORDER BY plays DESC, minutes DESC, track_id LIMIT 5`,
  topArtists: `
    SELECT artist_name, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${RANGE}
    GROUP BY artist_name ORDER BY plays DESC, minutes DESC, artist_name LIMIT 5`,
  genres: `
    SELECT genre, plays, minutes, round(100 * plays / sum(plays) OVER (), 1) AS share_pct
    FROM (
      SELECT genre, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
      FROM music.plays FINAL WHERE ${RANGE} GROUP BY genre
    )
    ORDER BY plays DESC, genre LIMIT 5`,
  topMonth: `
    SELECT toMonth(client_time) AS month, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${RANGE}
    GROUP BY month ORDER BY plays DESC, minutes DESC, month LIMIT 1`,
  topHour: `
    SELECT toHour(client_time) AS hour, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${RANGE}
    GROUP BY hour ORDER BY plays DESC, minutes DESC, hour LIMIT 1`,
  longestDay: `
    SELECT toDate(client_time) AS date, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${RANGE}
    GROUP BY date ORDER BY sum(ms_played) DESC, date LIMIT 1`,
  features: `
    WITH catalog AS (${CATALOG})
    SELECT ${WEIGHTED_FEATURES}, round(sum(w.ms) / 60000, 1) AS based_on_minutes
    FROM (
      SELECT track_id, sum(ms_played) AS ms FROM music.plays FINAL WHERE ${RANGE} GROUP BY track_id
    ) AS w
    INNER JOIN catalog AS c ON c.track_id = w.track_id`,
};

/** Recently played, newest first. Fetches limit+1 rows so the service can tell whether there is a next page. */
const HISTORY_COLUMNS = `
  SELECT event_id, track_id, track_name, artist_name, genre, client_time, ms_played, skipped, platform
  FROM music.plays FINAL`;
const HISTORY_ORDER = `ORDER BY client_time DESC, event_id DESC LIMIT {limit:UInt32}`;

export const HISTORY_SQL = `${HISTORY_COLUMNS} WHERE ${USER} ${HISTORY_ORDER}`;

// Keyset: (client_time, event_id) < (before, before_id). before_id = '' means "strictly before `before`".
export const HISTORY_BEFORE_SQL = `${HISTORY_COLUMNS}
  WHERE ${USER}
    AND (client_time < toDateTime64({before:String}, 3, 'UTC')
         OR (client_time = toDateTime64({before:String}, 3, 'UTC') AND event_id < {beforeId:String}))
  ${HISTORY_ORDER}`;

/** Distinct UTC days with at least one play (streaks are computed in the service). */
export const ACTIVE_DAYS_SQL = `
  SELECT DISTINCT toDate(client_time) AS day
  FROM music.plays FINAL WHERE ${USER}
  ORDER BY day`;

/** Per month: plays, minutes, weighted features (catalog-matched plays only) and top genre by plays. */
export const TASTE_DRIFT_SQL = `
WITH
  catalog AS (${CATALOG}),
  scoped AS (
    SELECT toStartOfMonth(client_time) AS month, track_id, genre, ms_played
    FROM music.plays FINAL WHERE ${RANGE}
  ),
  feats AS (
    SELECT w.month AS month, ${WEIGHTED_FEATURES}
    FROM (SELECT month, track_id, sum(ms_played) AS ms FROM scoped GROUP BY month, track_id) AS w
    INNER JOIN catalog AS c ON c.track_id = w.track_id
    GROUP BY w.month
  ),
  totals AS (
    SELECT month, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM scoped GROUP BY month
  ),
  genres AS (
    SELECT month, argMax(genre, (plays, ms)) AS top_genre
    FROM (SELECT month, genre, count() AS plays, sum(ms_played) AS ms FROM scoped GROUP BY month, genre)
    GROUP BY month
  )
SELECT toString(t.month) AS month, t.plays AS plays, t.minutes AS minutes, g.top_genre AS top_genre,
       f.danceability AS danceability, f.energy AS energy, f.valence AS valence,
       f.acousticness AS acousticness, f.tempo AS tempo
FROM totals AS t
LEFT JOIN genres AS g ON g.month = t.month
LEFT JOIN feats AS f ON f.month = t.month
ORDER BY t.month
SETTINGS join_use_nulls = 1`;

/**
 * Mood recs: the same taste profile as /recommendations, restricted to a feature box per mood.
 * Bounds are exclusive and passed as params. Tracks the user played in the last {recentDays} days sort last;
 * artists the user mostly skips are excluded. No rows when the user has no plays in 90 days.
 */
const MOOD_BOX = `
  c.energy > {energyLo:Float64} AND c.energy < {energyHi:Float64}
  AND c.valence > {valenceLo:Float64} AND c.valence < {valenceHi:Float64}
  AND c.acoustic > {acousticLo:Float64} AND c.acoustic < {acousticHi:Float64}
  AND c.dance > {danceLo:Float64} AND c.dance < {danceHi:Float64}
  AND c.tempo_n * 250 > {tempoLo:Float64} AND c.tempo_n * 250 < {tempoHi:Float64}`;


export const MOOD_RECS_SQL = `
WITH
  catalog AS (${CATALOG}),
  plays AS (${USER_PLAYS}),${TASTE_PROFILE_CTES},
  recent AS (
    SELECT track_id, count() AS recent_plays FROM plays
    WHERE client_time >= now64(3) - toIntervalDay({recentDays:UInt32})
    GROUP BY track_id
  )
SELECT
  c.track_id AS track_id, c.track_name AS track_name, c.artist_name AS artist_name, c.genre AS genre,
  c.energy AS energy, c.valence AS valence, c.dance AS danceability, c.acoustic AS acousticness,
  round(c.tempo_n * 250, 1) AS tempo,
  sqrt(pow(c.dance - pr.p_dance, 2) + pow(c.energy - pr.p_energy, 2) + pow(c.valence - pr.p_valence, 2)
     + pow(c.acoustic - pr.p_acoustic, 2) + pow(c.tempo_n - pr.p_tempo, 2)) AS distance,
  has(g.genres, c.genre) AS genre_match,
  round(1 / (1 + distance) + 0.15 * genre_match + 0.05 * c.popularity / 100, 4) AS score,
  r.recent_plays AS recent_plays  -- unmatched -> 0 (join_use_nulls=0)
FROM catalog AS c
CROSS JOIN profile AS pr
CROSS JOIN top_genres AS g
LEFT JOIN recent AS r ON r.track_id = c.track_id
WHERE pr.profile_tracks > 0
  AND ${MOOD_BOX}
  AND c.artist_name NOT IN (SELECT artist_name FROM skipped_artists)
ORDER BY recent_plays > 0, score DESC, c.track_id
LIMIT {limit:UInt32}`;

/** Fallback: tracks in the mood box ranked by tenant plays (30 days), then catalog popularity. */
export const MOOD_POPULAR_SQL = `
WITH
  catalog AS (${CATALOG}),
  tenant_counts AS (
    SELECT track_id, count() AS tenant_plays
    FROM music.plays FINAL
    WHERE tenant_id = {tenant:String} AND client_time >= now64(3) - INTERVAL 30 DAY
    GROUP BY track_id
  )
SELECT c.track_id AS track_id, c.track_name AS track_name, c.artist_name AS artist_name, c.genre AS genre,
       c.energy AS energy, c.valence AS valence, c.dance AS danceability, c.acoustic AS acousticness,
       round(c.tempo_n * 250, 1) AS tempo, t.tenant_plays AS tenant_plays,
       round(t.tenant_plays + c.popularity / 100, 4) AS score
FROM catalog AS c
LEFT JOIN tenant_counts AS t ON t.track_id = c.track_id  -- unmatched -> 0 (join_use_nulls=0)
WHERE ${MOOD_BOX}
ORDER BY tenant_plays DESC, c.popularity DESC, c.track_id
LIMIT {limit:UInt32}`;

/** Tracks played >= 3 times before the last 30 days and not at all since (excludes deleted catalog tracks). */
export const REDISCOVER_SQL = `
  SELECT track_id, any(track_name) AS track_name, any(artist_name) AS artist_name, any(genre) AS genre,
         count() AS past_plays, round(sum(ms_played) / 60000, 1) AS past_minutes,
         max(client_time) AS last_played_at,
         dateDiff('day', max(client_time), now64(3)) AS days_since_last_play
  FROM music.plays FINAL
  WHERE ${USER}
    AND track_id IN (SELECT track_id FROM music.tracks FINAL WHERE is_deleted = 0)
  GROUP BY track_id
  HAVING countIf(client_time >= now64(3) - INTERVAL 30 DAY) = 0 AND past_plays >= 3
  ORDER BY past_plays DESC, last_played_at DESC, track_id
  LIMIT {limit:UInt32}`;

const TRACK_COLUMNS = `
  track_id, track_name, artist_name, genre,
  dance AS danceability, energy, valence, acoustic AS acousticness, round(tempo_n * 250, 1) AS tempo`;

export const TRACK_SQL = `
  WITH catalog AS (${CATALOG})
  SELECT ${TRACK_COLUMNS} FROM catalog WHERE track_id = {trackId:String} LIMIT 1`;

/** Nearest catalog tracks by euclidean distance on (danceability, energy, valence, acousticness, tempo/250). */
export const SIMILAR_TRACKS_SQL = `
WITH
  catalog AS (${CATALOG}),
  target AS (SELECT * FROM catalog WHERE track_id = {trackId:String} LIMIT 1)
SELECT
  c.track_id AS track_id, c.track_name AS track_name, c.artist_name AS artist_name, c.genre AS genre,
  c.dance AS danceability, c.energy AS energy, c.valence AS valence, c.acoustic AS acousticness,
  round(c.tempo_n * 250, 1) AS tempo,
  round(sqrt(pow(c.dance - t.dance, 2) + pow(c.energy - t.energy, 2) + pow(c.valence - t.valence, 2)
     + pow(c.acoustic - t.acoustic, 2) + pow(c.tempo_n - t.tempo_n, 2)), 4) AS distance,
  round(1 / (1 + distance), 4) AS similarity
FROM catalog AS c
CROSS JOIN target AS t
WHERE c.track_id != t.track_id
ORDER BY distance, c.track_id
LIMIT {limit:UInt32}`;
