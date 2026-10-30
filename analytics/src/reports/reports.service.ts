import { BadRequestException, Injectable } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { UserScopeService } from '../common/user-scope.service';
import { ReportQueryDto } from './dto/report-query.dto';
import { REPORT_SQL as SQL } from './reports.sql';

const DAY_MS = 86_400_000;
const MAX_RANGE_DAYS = 366;
const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

type Row = Record<string, any>;

@Injectable()
export class ReportsService {
  constructor(
    private readonly ch: ClickHouseService,
    private readonly users: UserScopeService,
  ) {}

  async userReport(tenant: string, userId: string, q: ReportQueryDto) {
    const { from, to } = resolveRange(q);
    const profile = await this.users.resolve(tenant, userId); // 404 outside tenant
    const p = { tenant, userId, from, to };

    const [totals, topTracks, topArtists, genres, byHour, byWeekday, byPlatform, clickTypes, searches, fresh] =
      await Promise.all([
        this.ch.one<Row>(SQL.totals, p),
        this.ch.query<Row>(SQL.topTracks, p),
        this.ch.query<Row>(SQL.topArtists, p),
        this.ch.query<Row>(SQL.genres, p),
        this.ch.query<Row>(SQL.byHour, p),
        this.ch.query<Row>(SQL.byWeekday, p),
        this.ch.query<Row>(SQL.byPlatform, p),
        this.ch.query<Row>(SQL.clickTypes, p),
        this.ch.query<Row>(SQL.searches, p),
        this.ch.query<Row>(SQL.freshness, p),
      ]);

    const hourMap = new Map(byHour.map((r) => [r.hour, r.plays]));
    const dayMap = new Map(byWeekday.map((r) => [r.weekday, r.plays]));
    const clickCounts = Object.fromEntries(clickTypes.map((r) => [r.click_type, r.clicks]));
    const withData = fresh.filter((r) => r.n > 0);
    const latest = (k: string) => (withData.length ? withData.map((r) => r[k]).sort().at(-1) : null);

    return {
      tenant_id: tenant,
      user: profile,
      range: { from, to, time_column: 'client_time', timezone: 'UTC' },
      totals,
      top_tracks: topTracks,
      top_artists: topArtists,
      top_genres: genres,
      listening_by_hour: Array.from({ length: 24 }, (_, h) => ({ hour: h, plays: hourMap.get(h) ?? 0 })),
      listening_by_weekday: WEEKDAYS.map((d, i) => ({ weekday: d, plays: dayMap.get(i + 1) ?? 0 })),
      listening_by_platform: byPlatform,
      clicks: {
        total: clickTypes.reduce((s, r) => s + r.clicks, 0),
        by_type: clickCounts,
        likes: clickCounts['like'] ?? 0,
        top_search_queries: searches,
      },
      data_freshness: {
        last_ingested_at: latest('last_ingested_at'),
        last_event_time: latest('last_event_time'),
      },
    };
  }
}

function resolveRange(q: ReportQueryDto): { from: string; to: string } {
  const today = new Date().toISOString().slice(0, 10);
  const to = q.to ?? today;
  const from = q.from ?? new Date(Date.parse(to) - 29 * DAY_MS).toISOString().slice(0, 10);
  const days = (Date.parse(to) - Date.parse(from)) / DAY_MS;
  if (days < 0) throw new BadRequestException('from must be on or before to');
  if (days >= MAX_RANGE_DAYS) throw new BadRequestException(`range must be at most ${MAX_RANGE_DAYS} days`);
  return { from, to };
}
