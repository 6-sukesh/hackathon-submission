import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { UserScopeService } from '../common/user-scope.service';
import { HistoryQueryDto } from './dto/history-query.dto';
import { Mood } from './dto/mood-recs-query.dto';
import {
  ACTIVE_DAYS_SQL,
  HISTORY_BEFORE_SQL,
  HISTORY_SQL,
  MOOD_POPULAR_SQL,
  MOOD_RECS_SQL,
  REDISCOVER_SQL,
  SIMILAR_TRACKS_SQL,
  TASTE_DRIFT_SQL,
  TRACK_SQL,
  WRAPPED_SQL as W,
} from './insights.sql';

type Row = Record<string, any>;

interface Features {
  danceability: number | null;
  energy: number | null;
  valence: number | null;
  acousticness: number | null;
  tempo: number | null;
}

const DAY_MS = 86_400_000;
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const RECENT_DAYS = 14;
const REDISCOVER_WINDOW_DAYS = 30;

/** Exclusive feature bounds per mood (tempo in BPM). Missing bounds are open. */
type Bounds = Partial<Record<'energy' | 'valence' | 'acoustic' | 'dance' | 'tempo', [number | null, number | null]>>;
const MOOD_BOUNDS: Record<Mood, Bounds> = {
  chill: { energy: [null, 0.5] },
  focus: { energy: [null, 0.6], acoustic: [0.4, null] },
  workout: { energy: [0.75, null], tempo: [115, null] },
  happy: { valence: [0.6, null] },
};

const r3 = (n: number) => Math.round(n * 1000) / 1000;
const r1 = (n: number) => Math.round(n * 10) / 10;
const ymd = (ms: number) => new Date(ms).toISOString().slice(0, 10);

@Injectable()
export class InsightsService {
  constructor(
    private readonly ch: ClickHouseService,
    private readonly users: UserScopeService,
  ) {}

  // ── 1. Wrapped ───────────────────────────────────────────────────────────
  async wrapped(tenant: string, userId: string, year: number) {
    const profile = await this.users.resolve(tenant, userId); // 404 outside tenant
    const p = { tenant, userId, from: `${year}-01-01`, to: `${year + 1}-01-01` };

    const [totals, topTracks, topArtists, genres, month, hour, day, feats] = await Promise.all([
      this.ch.one<Row>(W.totals, p),
      this.ch.query<Row>(W.topTracks, p),
      this.ch.query<Row>(W.topArtists, p),
      this.ch.query<Row>(W.genres, p),
      this.ch.one<Row>(W.topMonth, p),
      this.ch.one<Row>(W.topHour, p),
      this.ch.one<Row>(W.longestDay, p),
      this.ch.one<Row>(W.features, p),
    ]);

    const features: Features = {
      danceability: feats?.danceability ?? null,
      energy: feats?.energy ?? null,
      valence: feats?.valence ?? null,
      acousticness: feats?.acousticness ?? null,
      tempo: feats?.tempo ?? null,
    };

    return {
      user: profile,
      year,
      range: { from: p.from, to_exclusive: p.to, time_column: 'client_time', timezone: 'UTC' },
      totals: {
        minutes_listened: totals?.minutes_listened ?? 0,
        plays: totals?.plays ?? 0,
        unique_tracks: totals?.unique_tracks ?? 0,
        unique_artists: totals?.unique_artists ?? 0,
        active_days: totals?.active_days ?? 0,
      },
      top_tracks: topTracks,
      top_artists: topArtists,
      top_genres: genres,
      most_active_month: month ? { month: month.month, name: MONTHS[month.month - 1], plays: month.plays, minutes: month.minutes } : null,
      most_active_hour: hour ? { hour: hour.hour, plays: hour.plays, minutes: hour.minutes } : null,
      longest_listening_day: day ? { date: day.date, minutes: day.minutes, plays: day.plays } : null,
      listening_personality: personality(features),
      audio_features: { ...features, weighting: 'ms_played', based_on_minutes: feats?.based_on_minutes ?? 0 },
    };
  }

  // ── 2. History (keyset on client_time, event_id as tie-breaker) ──────────
  async history(tenant: string, userId: string, q: HistoryQueryDto) {
    if (q.before_id && !q.before) throw new BadRequestException('before_id requires before');
    const before = q.before ? toChDateTime(q.before) : undefined;
    await this.users.resolve(tenant, userId); // 404 outside tenant

    const params = { tenant, userId, limit: q.limit + 1, before, beforeId: q.before_id ?? '' };
    const rows = await this.ch.query<Row>(before ? HISTORY_BEFORE_SQL : HISTORY_SQL, params);
    const hasMore = rows.length > q.limit;
    const page = rows.slice(0, q.limit);
    const last = page.at(-1);

    return {
      user_id: userId,
      items: page.map((r) => ({
        event_id: r.event_id,
        track_id: r.track_id,
        track_name: r.track_name,
        artist_name: r.artist_name,
        genre: r.genre,
        client_time: r.client_time,
        ms_played: r.ms_played,
        skipped: r.skipped === 1,
        platform: r.platform,
      })),
      // Pass both back as ?before=...&before_id=... (before alone also works; ties at the same ms may then be skipped).
      next_before: hasMore && last ? last.client_time : null,
      next_before_id: hasMore && last ? last.event_id : null,
    };
  }

  // ── 3. Streaks (UTC days with >= 1 play) ─────────────────────────────────
  async streaks(tenant: string, userId: string) {
    await this.users.resolve(tenant, userId); // 404 outside tenant
    const rows = await this.ch.query<{ day: string }>(ACTIVE_DAYS_SQL, { tenant, userId });

    const todayMs = Date.parse(new Date().toISOString().slice(0, 10));
    // Ignore future-dated days (device clock skew) so they can't extend a streak.
    const days = rows.map((r) => Date.parse(r.day)).filter((d) => d <= todayMs);

    // Collapse consecutive days into runs.
    const runs: { start: number; end: number; days: number }[] = [];
    for (const d of days) {
      const cur = runs.at(-1);
      if (cur && d - cur.end === DAY_MS) {
        cur.end = d;
        cur.days++;
      } else {
        runs.push({ start: d, end: d, days: 1 });
      }
    }

    // Longest: ties go to the most recent run.
    const longest = runs.reduce<(typeof runs)[number] | null>((best, r) => (!best || r.days >= best.days ? r : best), null);
    // Current: the run ending today, or yesterday (still alive until today ends without a play).
    const lastRun = runs.at(-1);
    const current = lastRun && todayMs - lastRun.end <= DAY_MS ? lastRun : null;
    const fmt = (r: (typeof runs)[number] | null) =>
      r ? { days: r.days, start_date: ymd(r.start), end_date: ymd(r.end) } : { days: 0, start_date: null, end_date: null };

    const windowStart = todayMs - 29 * DAY_MS;
    return {
      user_id: userId,
      timezone: 'UTC',
      today: ymd(todayMs),
      current_streak: { ...fmt(current), played_today: lastRun?.end === todayMs },
      longest_streak: fmt(longest),
      active_days_last_30: days.filter((d) => d >= windowStart).length,
      total_active_days: days.length,
      first_active_date: days.length ? ymd(days[0]) : null,
      last_active_date: days.length ? ymd(days[days.length - 1]) : null,
    };
  }

  // ── 4. Taste drift (per UTC calendar month) ──────────────────────────────
  async tasteDrift(tenant: string, userId: string, months: number) {
    await this.users.resolve(tenant, userId); // 404 outside tenant
    const now = new Date();
    const y = now.getUTCFullYear();
    const m = now.getUTCMonth();
    const from = ymd(Date.UTC(y, m - (months - 1), 1));
    const to = ymd(Date.UTC(y, m + 1, 1));

    const rows = await this.ch.query<Row>(TASTE_DRIFT_SQL, { tenant, userId, from, to });
    const byMonth = new Map(rows.map((r) => [r.month.slice(0, 7), r]));

    // Zero-fill months without plays so the series is contiguous.
    const series = Array.from({ length: months }, (_, i) => {
      const key = ymd(Date.UTC(y, m - (months - 1) + i, 1)).slice(0, 7);
      const r = byMonth.get(key);
      return {
        month: key,
        plays: r?.plays ?? 0,
        minutes: r?.minutes ?? 0,
        top_genre: r?.top_genre ?? null,
        danceability: r?.danceability ?? null,
        energy: r?.energy ?? null,
        valence: r?.valence ?? null,
        acousticness: r?.acousticness ?? null,
        tempo: r?.tempo ?? null,
      };
    });

    const withData = series.filter((s) => s.plays > 0 && s.energy !== null);
    const first = withData[0];
    const last = withData.at(-1);
    let change = null;
    if (first && last && first !== last) {
      const d = (k: 'danceability' | 'energy' | 'valence' | 'acousticness') => r3((last[k] as number) - (first[k] as number));
      change = {
        from_month: first.month,
        to_month: last.month,
        danceability: d('danceability'),
        energy: d('energy'),
        valence: d('valence'),
        acousticness: d('acousticness'),
        tempo: r1((last.tempo as number) - (first.tempo as number)),
        top_genre: { from: first.top_genre, to: last.top_genre, changed: first.top_genre !== last.top_genre },
        summary: driftSummary(first, last),
      };
    }

    return {
      user_id: userId,
      months,
      range: { from, to_exclusive: to, time_column: 'client_time', timezone: 'UTC', weighting: 'ms_played' },
      series,
      change, // first vs last month with plays; null if fewer than two such months
    };
  }

  // ── 5. Mood recommendations ──────────────────────────────────────────────
  async moodRecommendations(tenant: string, userId: string, mood: Mood, limit: number) {
    await this.users.resolve(tenant, userId); // 404 outside tenant
    const bounds = MOOD_BOUNDS[mood];
    const params = { tenant, userId, limit, recentDays: RECENT_DAYS, ...boundParams(bounds) };
    const constraints = describeBounds(bounds);

    const rows = await this.ch.query<Row>(MOOD_RECS_SQL, params);
    if (rows.length) {
      return {
        user_id: userId,
        mood,
        constraints,
        strategy: 'mood_content_based',
        recommendations: rows.map((r) => ({
          ...trackOut(r),
          score: r.score,
          played_recently: r.recent_plays > 0,
          reason: moodReason(mood, r),
        })),
      };
    }

    // No taste profile (no plays in 90 days) or nothing left after exclusions: popular tracks in range.
    const popular = await this.ch.query<Row>(MOOD_POPULAR_SQL, params);
    return {
      user_id: userId,
      mood,
      constraints,
      strategy: 'mood_popular',
      recommendations: popular.map((r) => ({
        ...trackOut(r),
        score: r.score,
        reason: r.tenant_plays > 0 ? `popular ${mood} pick on your team (${r.tenant_plays} plays)` : `popular ${mood} pick in the catalog`,
      })),
    };
  }

  // ── 6. Rediscover ────────────────────────────────────────────────────────
  async rediscover(tenant: string, userId: string, limit: number) {
    await this.users.resolve(tenant, userId); // 404 outside tenant
    const rows = await this.ch.query<Row>(REDISCOVER_SQL, { tenant, userId, limit });
    return {
      user_id: userId,
      criteria: { min_past_plays: 3, not_played_in_last_days: REDISCOVER_WINDOW_DAYS },
      tracks: rows,
    };
  }

  // ── 7. Similar tracks (global catalog) ───────────────────────────────────
  async similarTracks(trackId: string, limit: number) {
    const [track, similar] = await Promise.all([
      this.ch.one<Row>(TRACK_SQL, { trackId }),
      this.ch.query<Row>(SIMILAR_TRACKS_SQL, { trackId, limit }),
    ]);
    if (!track) throw new NotFoundException(`Track ${trackId} not found`);
    return {
      track: trackOut(track),
      features: ['danceability', 'energy', 'valence', 'acousticness', 'tempo/250'],
      similar: similar.map((r) => ({ ...trackOut(r), distance: r.distance, similarity: r.similarity })),
    };
  }
}

/** ISO 8601 (validated by the DTO) → 'YYYY-MM-DD hh:mm:ss.sss' UTC for toDateTime64(…, 3, 'UTC'). */
function toChDateTime(iso: string): string {
  const ms = Date.parse(iso);
  const year = new Date(ms).getUTCFullYear();
  if (Number.isNaN(ms) || year < 1970 || year > 2299) throw new BadRequestException('before is not a valid timestamp');
  return new Date(ms).toISOString().replace('T', ' ').slice(0, 23);
}

function trackOut(r: Row) {
  return {
    track_id: r.track_id,
    track_name: r.track_name,
    artist_name: r.artist_name,
    genre: r.genre,
    danceability: r3(r.danceability),
    energy: r3(r.energy),
    valence: r3(r.valence),
    acousticness: r3(r.acousticness),
    tempo: r.tempo,
  };
}

function boundParams(b: Bounds) {
  const lo = (k: keyof Bounds) => b[k]?.[0] ?? -1e9;
  const hi = (k: keyof Bounds) => b[k]?.[1] ?? 1e9;
  return {
    energyLo: lo('energy'), energyHi: hi('energy'),
    valenceLo: lo('valence'), valenceHi: hi('valence'),
    acousticLo: lo('acoustic'), acousticHi: hi('acoustic'),
    danceLo: lo('dance'), danceHi: hi('dance'),
    tempoLo: lo('tempo'), tempoHi: hi('tempo'),
  };
}

function describeBounds(b: Bounds): Record<string, { gt?: number; lt?: number }> {
  const names: Record<keyof Bounds, string> = {
    energy: 'energy', valence: 'valence', acoustic: 'acousticness', dance: 'danceability', tempo: 'tempo',
  };
  const out: Record<string, { gt?: number; lt?: number }> = {};
  for (const [k, [lo, hi]] of Object.entries(b) as [keyof Bounds, [number | null, number | null]][]) {
    out[names[k]] = { ...(lo !== null ? { gt: lo } : {}), ...(hi !== null ? { lt: hi } : {}) };
  }
  return out;
}

function moodReason(mood: Mood, r: Row): string {
  const parts = [`fits your ${mood} mood`];
  if (r.genre_match) parts.push(`matches your ${r.genre} taste`);
  parts.push(r.recent_plays > 0 ? `played ${r.recent_plays}x in the last ${RECENT_DAYS} days` : `not played in the last ${RECENT_DAYS} days`);
  return parts.join(' · ');
}

/** Simple, ordered rules on the ms_played-weighted feature averages; first match wins. */
function personality(f: Features): { label: string; description: string } | null {
  const { energy: e, valence: v, danceability: d, acousticness: a } = f;
  if (e === null || v === null || d === null || a === null) return null;
  if (e >= 0.7 && d >= 0.65) return { label: 'High-energy dancer', description: 'Loud, fast and made for moving.' };
  if (e >= 0.7) return { label: 'Adrenaline seeker', description: 'High-intensity tracks that hit hard.' };
  if (d >= 0.75) return { label: 'Groove machine', description: 'Rhythm first: beats you can nod to all day.' };
  if (a >= 0.5 && e < 0.5) return { label: 'Chill acoustic', description: 'Soft, organic sounds and low-key vibes.' };
  if (v <= 0.45) return { label: 'Melancholic explorer', description: 'Drawn to moodier, bittersweet sounds.' };
  if (v >= 0.55) return { label: 'Sunny optimist', description: 'Bright, feel-good music most of the time.' };
  if (e < 0.5) return { label: 'Mellow wanderer', description: 'Easygoing tracks at an unhurried pace.' };
  return { label: 'Eclectic all-rounder', description: 'A balanced mix of moods and energy.' };
}

function driftSummary(first: Row, last: Row): string {
  const moves: string[] = [];
  const add = (k: string, up: string, down: string) => {
    const delta = last[k] - first[k];
    if (Math.abs(delta) >= 0.05) moves.push(delta > 0 ? up : down);
  };
  add('energy', 'more energetic', 'calmer');
  add('valence', 'happier', 'moodier');
  add('danceability', 'more danceable', 'less danceable');
  add('acousticness', 'more acoustic', 'more electronic');
  if (first.top_genre !== last.top_genre) moves.push(`top genre ${first.top_genre} → ${last.top_genre}`);
  return moves.length ? `Since ${first.month}: ${moves.join(', ')}.` : `Since ${first.month}: taste is stable.`;
}
