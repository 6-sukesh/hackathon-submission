import { Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export const COLLECTIONS = ['plays', 'clicks'] as const;
export type Collection = (typeof COLLECTIONS)[number];

export class PipelineLagQueryDto {
  /** Look-back on ingested_at (ClickHouse write time), 1 minute to 7 days. */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(10080)
  minutes: number = 60;
}

export class SchemaFieldsQueryDto {
  @IsOptional()
  @IsIn(COLLECTIONS, { message: 'collection must be one of plays, clicks' })
  collection: Collection = 'plays';

  /** Sample events with client_time in the last N days (most recent first, capped). */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(90)
  days: number = 30;
}
