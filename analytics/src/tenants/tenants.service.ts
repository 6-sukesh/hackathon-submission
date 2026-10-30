import { Injectable } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import {
  DataQualityQueryDto,
  DaysQueryDto,
  LIVE_WINDOWS,
  LiveQueryDto,
  RetentionQueryDto,
  TRENDING_WINDOWS,
  TrendingQueryDto,
} from './dto/dashboard-query.dto';
import { TENANT_SQL as SQL } from './tenants.sql';

type Row = Record<string, any>;

/** Cohorts considered by /retention: first-play weeks within this many weeks before the current week. */
const RETENTION_LOOKBACK_WEEKS = 26;
const FUNNEL_STEPS = ['page_view', 'search', 'play', 'like', 'add_to_playlist'] as const;

const pct = (n: number, d: number): number | null => (d > 0 ? Math.round((1000 * n) / d) / 10 : null);

@Injectable()
export class TenantsService {
  constructor(private readonly ch: ClickHouseService) {}

  /** Last 24h of the caller's tenant at minute granularity (event time), gaps filled with 0. */
  async overview(tenant: string) {
    const [totals, perMinute, clicks] = await Promise.all([
      this.ch.one<Row>(SQL.overviewTotals, { tenant }),
      this.ch.query<{ minute: string; plays: number; users: number }>(SQL.overviewPerMinute, { tenant }),
      this.ch.one<{ clicks: number }>(SQL.overviewClicks, { tenant }),
    ]);
    return {
      tenant_id: tenant,
      window: 'last_24h',
      totals: { ...totals, clicks: clicks?.clicks ?? 0, last_ingested_at: totals?.plays ? totals.last_ingested_at : null },
      plays_per_minute: perMinute,
    };
  }

  async live(tenant: string, q: LiveQueryDto) {
    const p = { tenant, minutes: LIVE_WINDOWS[q.window] };
    const [perMinute, totals, topTracks, fresh] = await Promise.all([
      this.ch.query<Row>(SQL.livePerMinute, p),
      this.ch.one<Row>(SQL.liveTotals, p),
      this.ch.query<Row>(SQL.liveTopTracks, p),
      this.ch.one<Row>(SQL.freshness, p),
    ]);
    const hasData = (fresh?.buckets ?? 0) > 0;
    return {
      tenant_id: tenant,
      window: q.window,
      time_column: 'client_time',
      totals,
      plays_per_minute: perMinute,
      top_tracks: topTracks,
      freshness: {
        last_ingested_at: hasData ? fresh!.last_ingested_at : null,
        freshness_sec: hasData ? fresh!.freshness_sec : null,
      },
    };
  }

  async trending(tenant: string, q: TrendingQueryDto) {
    const p = { tenant, hours: TRENDING_WINDOWS[q.window], minPlays: q.min_plays, limit: q.limit };
    const [totals, tracks] = await Promise.all([
      this.ch.one<Row>(SQL.trendingTotals, p),
      this.ch.query<Row>(SQL.trending, p),
    ]);
    return {
      tenant_id: tenant,
      window: q.window,
      min_plays: q.min_plays,
      definition: 'current = last window ending now; previous = the equal window before it; growth_pct null when previous = 0 (is_new)',
      totals: {
        ...totals,
        growth_pct: totals?.previous_plays
          ? Math.round((1000 * (totals.current_plays - totals.previous_plays)) / totals.previous_plays) / 10
          : null,
      },
      tracks: tracks.map((t) => ({ ...t, is_new: Boolean(t.is_new) })),
    };
  }

  async retention(tenant: string, q: RetentionQueryDto) {
    const rows = await this.ch.query<{ cohort_week: string; week_n: number; users: number }>(SQL.retention, {
      tenant,
      weeks: q.weeks,
      lookback: RETENTION_LOOKBACK_WEEKS,
    });
    const currentWeek = mondayOf(new Date());
    const byCohort = new Map<string, Map<number, number>>();
    for (const r of rows) {
      if (!byCohort.has(r.cohort_week)) byCohort.set(r.cohort_week, new Map());
      byCohort.get(r.cohort_week)!.set(Number(r.week_n), r.users);
    }
    const cohorts = [...byCohort.entries()].map(([cohortWeek, weeks]) => {
      const size = weeks.get(0) ?? 0;
      const elapsed = Math.round((Date.parse(currentWeek) - Date.parse(cohortWeek)) / (7 * 86_400_000));
      return {
        cohort_week: cohortWeek,
        cohort_size: size,
        // null = week N hasn't started yet for this cohort; the current week is partial.
        retention: Array.from({ length: q.weeks + 1 }, (_, n) =>
          n > elapsed
            ? { week: n, users: null, pct: null }
            : { week: n, users: weeks.get(n) ?? 0, pct: pct(weeks.get(n) ?? 0, size) },
        ),
      };
    });
    return {
      tenant_id: tenant,
      cohort: q.cohort,
      weeks: q.weeks,
      definition: `cohort = Monday (UTC) of the user's first play; week N = % of cohort with >= 1 play in week N; cohorts from the last ${RETENTION_LOOKBACK_WEEKS} weeks`,
      cohorts,
    };
  }

  async engagement(tenant: string, q: DaysQueryDto) {
    const p = { tenant, days: q.days };
    const [daily, active, sessions] = await Promise.all([
      this.ch.query<Row>(SQL.engagementDaily, p),
      this.ch.one<Row>(SQL.engagementActive, p),
      this.ch.one<Row>(SQL.engagementSessions, p),
    ]);
    return {
      tenant_id: tenant,
      days: q.days,
      time_column: 'client_time',
      active_users: {
        wau: active?.wau ?? 0,
        mau: active?.mau ?? 0,
        avg_dau_30d: active?.avg_dau_30d ?? 0,
        stickiness_dau_mau: active?.mau ? Math.round((10000 * active.avg_dau_30d) / active.mau) / 10000 : null,
        definition: 'active = >= 1 play; WAU/MAU = last 7/30 UTC days incl. today; stickiness = avg DAU over 30 days / MAU',
      },
      sessions,
      daily: daily.map((d) => ({
        day: d.day,
        dau: d.dau,
        plays: d.plays,
        skip_rate: d.plays ? Math.round((10000 * d.skips) / d.plays) / 10000 : null,
      })),
    };
  }

  async funnel(tenant: string, q: DaysQueryDto) {
    const r = (await this.ch.one<Row>(SQL.funnel, { tenant, days: q.days })) ?? {};
    const steps = FUNNEL_STEPS.map((step, i) => {
      const k = i + 1;
      const sessions = r[`any_${k}`] ?? 0;
      const ordered = r[`ord_${k}`] ?? 0;
      return {
        step,
        sessions,
        step_conversion_pct: k === 1 ? null : pct(sessions, r[`any_${k - 1}`] ?? 0),
        overall_conversion_pct: pct(sessions, r.any_1 ?? 0),
        sessions_in_order: ordered,
        step_conversion_in_order_pct: k === 1 ? null : pct(ordered, r[`ord_${k - 1}`] ?? 0),
      };
    });
    return {
      tenant_id: tenant,
      days: q.days,
      total_sessions: r.sessions ?? 0,
      definition:
        'sessions = sessions containing every step up to this one (any order); sessions_in_order = strict order via windowFunnel; play = plays row or play click',
      steps,
    };
  }

  async dataQuality(tenant: string, q: DataQualityQueryDto) {
    const p = { tenant, days: q.days };
    const [stats, closedDays, mix] = await Promise.all([
      this.ch.query<Row>(SQL.dataQuality, p),
      this.ch.query<Row>(SQL.closedDays, p),
      this.ch.query<Row>(SQL.schemaMix, p),
    ]);
    const collections = Object.fromEntries(
      ['plays', 'clicks'].map((c) => {
        const s = stats.find((r) => r.collection === c);
        const versions = mix.filter((m) => m.collection === c).sort((a, b) => a.schema_version - b.schema_version);
        const total = versions.reduce((n, v) => n + v.events, 0);
        return [
          c,
          {
            events: s?.events ?? 0,
            late_events: s?.late_events ?? 0,
            late_rate: s?.late_rate ?? 0,
            lateness_sec: s
              ? { p50: s.lateness_q[0], p90: s.lateness_q[1], p99: s.lateness_q[2], max: s.max_lateness_sec }
              : null,
            closed_day_events: s?.closed_day_events ?? 0,
            future_dated_events: s?.future_dated_events ?? 0,
            max_clock_ahead_sec: s && s.max_clock_ahead_sec > 0 ? s.max_clock_ahead_sec : 0,
            schema_versions: versions.map((v) => ({
              schema_version: v.schema_version,
              events: v.events,
              pct: pct(v.events, total),
            })),
          },
        ];
      }),
    );
    return {
      tenant_id: tenant,
      days: q.days,
      window_column: 'received_at',
      definitions: {
        late: 'lateness_sec = received_at - client_time > 3600',
        closed_day: "event's UTC day ended more than 24h before received_at (day needs re-aggregation)",
        future_dated: 'client_time > received_at + 5 min (device clock ahead)',
      },
      collections,
      closed_days: closedDays,
    };
  }
}

function mondayOf(d: Date): string {
  const day = (d.getUTCDay() + 6) % 7; // Mon = 0
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - day)).toISOString().slice(0, 10);
}
