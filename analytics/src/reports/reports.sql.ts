// Every query below is scoped by tenant + user and bounded on client_time (event time).
// `to` is inclusive: client_time < to + 1 day. FINAL collapses CDC replays.
const SCOPE = `tenant_id = {tenant:String} AND user_id = {userId:String}
  AND client_time >= toDateTime64({from:Date}, 3, 'UTC')
  AND client_time <  toDateTime64(addDays({to:Date}, 1), 3, 'UTC')`;

export const REPORT_SQL = {
  totals: `
    SELECT count() AS plays,
           round(sum(ms_played) / 60000, 1) AS minutes_listened,
           uniqExact(track_id) AS unique_tracks,
           uniqExact(artist_name) AS unique_artists,
           round(ifNotFinite(avg(skipped), 0), 4) AS skip_rate,
           round(ifNotFinite(avg(lateness_sec), 0), 1) AS avg_lateness_sec,
           max(lateness_sec) AS max_lateness_sec
    FROM music.plays FINAL WHERE ${SCOPE}`,
  topTracks: `
    SELECT track_id, any(track_name) AS track_name, any(artist_name) AS artist_name,
           count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${SCOPE}
    GROUP BY track_id ORDER BY plays DESC, minutes DESC LIMIT 10`,
  topArtists: `
    SELECT artist_name, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${SCOPE}
    GROUP BY artist_name ORDER BY plays DESC, minutes DESC LIMIT 5`,
  genres: `
    SELECT genre, plays, round(100 * plays / sum(plays) OVER (), 1) AS share_pct
    FROM (SELECT genre, count() AS plays FROM music.plays FINAL WHERE ${SCOPE} GROUP BY genre)
    ORDER BY plays DESC LIMIT 10`,
  byHour: `
    SELECT toHour(client_time) AS hour, count() AS plays
    FROM music.plays FINAL WHERE ${SCOPE} GROUP BY hour ORDER BY hour`,
  byWeekday: `
    SELECT toDayOfWeek(client_time) AS weekday, count() AS plays
    FROM music.plays FINAL WHERE ${SCOPE} GROUP BY weekday ORDER BY weekday`,
  byPlatform: `
    SELECT platform, count() AS plays, round(sum(ms_played) / 60000, 1) AS minutes
    FROM music.plays FINAL WHERE ${SCOPE} GROUP BY platform ORDER BY plays DESC`,
  clickTypes: `
    SELECT click_type, count() AS clicks
    FROM music.clicks FINAL WHERE ${SCOPE} GROUP BY click_type ORDER BY clicks DESC`,
  searches: `
    SELECT lower(trim(search_query)) AS query, count() AS searches
    FROM music.clicks FINAL WHERE ${SCOPE} AND search_query != ''
    GROUP BY query ORDER BY searches DESC LIMIT 10`,
  // Freshness is not range-bounded: it answers "how current is this user's data at all".
  freshness: `
    SELECT max(ingested_at) AS last_ingested_at, max(client_time) AS last_event_time, count() AS n
    FROM music.plays FINAL WHERE tenant_id = {tenant:String} AND user_id = {userId:String}
    UNION ALL
    SELECT max(ingested_at), max(client_time), count()
    FROM music.clicks FINAL WHERE tenant_id = {tenant:String} AND user_id = {userId:String}`,
};
