import { Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export const LIVE_WINDOWS = { '5m': 5, '15m': 15, '60m': 60 } as const;
export const TRENDING_WINDOWS = { '24h': 24, '7d': 168 } as const;

export class LiveQueryDto {
  @IsOptional()
  @IsIn(Object.keys(LIVE_WINDOWS), { message: 'window must be one of 5m, 15m, 60m' })
  window: keyof typeof LIVE_WINDOWS = '15m';
}

export class TrendingQueryDto {
  @IsOptional()
  @IsIn(Object.keys(TRENDING_WINDOWS), { message: 'window must be one of 24h, 7d' })
  window: keyof typeof TRENDING_WINDOWS = '24h';

  /** Tracks need at least this many plays in the current window (filters tiny counts). */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(1000)
  min_plays: number = 3;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(50)
  limit: number = 10;
}

export class RetentionQueryDto {
  @IsOptional()
  @IsIn(['week'], { message: 'cohort must be week' })
  cohort: 'week' = 'week';

  /** Retention horizon: week 0..weeks after the cohort week. */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(26)
  weeks: number = 8;
}

export class DaysQueryDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(90)
  days: number = 30;
}

export class DataQualityQueryDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(90)
  days: number = 7;
}
