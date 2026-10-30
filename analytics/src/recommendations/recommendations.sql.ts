// Feature vector: danceability, energy, valence, acousticness (all 0..1) + tempo / 250 (clamped 0..1).
// Only the tenant's own plays/clicks feed the profile; the `tracks` catalog is global.
export const CATALOG = `
  SELECT track_id, track_name, artist_name, genre, popularity,
         danceability AS dance, energy, valence, acousticness AS acoustic,
         least(greatest(tempo, 0), 250) / 250 AS tempo_n
  FROM music.tracks FINAL
  WHERE is_deleted = 0`;

export const USER_PLAYS = `
  SELECT track_id, artist_name, ms_played, skipped, client_time
  FROM music.plays FINAL
  WHERE tenant_id = {tenant:String} AND user_id = {userId:String}`;

/**
 * Taste-profile CTEs shared with insights (mood recs). Expect `catalog` and `plays` CTEs before them;
 * define liked, weighted, profile (p_* + profile_tracks), top_genres and skipped_artists.
 */
export const TASTE_PROFILE_CTES = `
  liked AS (
    SELECT DISTINCT target_id AS track_id
    FROM music.clicks FINAL
    WHERE tenant_id = {tenant:String} AND user_id = {userId:String}
      AND click_type = 'like' AND target_type = 'track'
  ),
  -- weight = ms_played over the last 90 days, x3 for liked tracks
  weighted AS (
    SELECT c.*, p.ms * if(c.track_id IN (SELECT track_id FROM liked), 3, 1) AS w
    FROM (
      SELECT track_id, sum(ms_played) AS ms FROM plays
      WHERE client_time >= now64(3) - INTERVAL 90 DAY
      GROUP BY track_id
    ) AS p
    INNER JOIN catalog AS c ON c.track_id = p.track_id
    WHERE p.ms > 0
  ),
  profile AS (
    SELECT sum(w * dance) / sum(w) AS p_dance, sum(w * energy) / sum(w) AS p_energy,
           sum(w * valence) / sum(w) AS p_valence, sum(w * acoustic) / sum(w) AS p_acoustic,
           sum(w * tempo_n) / sum(w) AS p_tempo, count() AS profile_tracks
    FROM weighted
  ),
  top_genres AS (
    SELECT groupArray(genre) AS genres
    FROM (SELECT genre, sum(w) AS gw FROM weighted GROUP BY genre ORDER BY gw DESC LIMIT 3)
  ),
  -- artists the user skips a lot (>= 3 plays, >= 70% skipped) are excluded
  skipped_artists AS (
    SELECT artist_name FROM plays GROUP BY artist_name
    HAVING count() >= 3 AND avg(skipped) >= 0.7
  )
`;

/** Content-based recs in one query. Returns no rows when the user has no plays in 90 days (cold start). */
export const PERSONAL_RECS_SQL = `
WITH
  catalog AS (${CATALOG}),
  plays AS (${USER_PLAYS}),${TASTE_PROFILE_CTES}SELECT
  c.track_id AS track_id, c.track_name AS track_name, c.artist_name AS artist_name, c.genre AS genre,
  c.popularity AS popularity,
  sqrt(pow(c.dance - pr.p_dance, 2) + pow(c.energy - pr.p_energy, 2) + pow(c.valence - pr.p_valence, 2)
     + pow(c.acoustic - pr.p_acoustic, 2) + pow(c.tempo_n - pr.p_tempo, 2)) AS distance,
  1 / (1 + distance) AS similarity,
  has(g.genres, c.genre) AS genre_match,
  round(similarity + 0.15 * genre_match + 0.05 * c.popularity / 100, 4) AS score,
  c.dance AS danceability, c.energy AS energy, c.valence AS valence, c.acoustic AS acousticness,
  pr.p_dance, pr.p_energy, pr.p_valence, pr.p_acoustic, pr.p_tempo, pr.profile_tracks, g.genres AS top_genres
FROM catalog AS c
CROSS JOIN profile AS pr
CROSS JOIN top_genres AS g
WHERE pr.profile_tracks > 0
  AND c.track_id NOT IN (SELECT DISTINCT track_id FROM plays)
  AND c.artist_name NOT IN (SELECT artist_name FROM skipped_artists)
ORDER BY score DESC, c.track_id
LIMIT {limit:UInt32}`;

/** Cold start: tenant-wide most-played (last 30 days), then catalog popularity. */
export const COLD_START_SQL = `
WITH
  catalog AS (${CATALOG}),
  tenant_counts AS (
    SELECT track_id, count() AS tenant_plays
    FROM music.plays FINAL
    WHERE tenant_id = {tenant:String} AND client_time >= now64(3) - INTERVAL 30 DAY
    GROUP BY track_id
  )
SELECT c.track_id AS track_id, c.track_name AS track_name, c.artist_name AS artist_name,
       c.genre AS genre, c.popularity AS popularity, t.tenant_plays AS tenant_plays,
       round(t.tenant_plays + c.popularity / 100, 4) AS score
FROM catalog AS c
LEFT JOIN tenant_counts AS t ON t.track_id = c.track_id  -- unmatched -> 0 (join_use_nulls=0)
WHERE c.track_id NOT IN (
  SELECT DISTINCT track_id FROM music.plays FINAL
  WHERE tenant_id = {tenant:String} AND user_id = {userId:String}
)
ORDER BY tenant_plays DESC, c.popularity DESC, c.track_id
LIMIT {limit:UInt32}`;
