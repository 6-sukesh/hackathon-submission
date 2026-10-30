import { Type } from 'class-transformer';
import { IsInt, IsOptional, Matches, Max, Min } from 'class-validator';

// ISO 8601 date or date-time; a time part must carry Z or an offset so the cursor is unambiguous.
const ISO_TS_RE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:?\d{2}))?$/;

export class HistoryQueryDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limit: number = 20;

  /** Keyset cursor: only plays with client_time strictly before this instant (pass `next_before`). */
  @IsOptional()
  @Matches(ISO_TS_RE, { message: 'before must be an ISO 8601 timestamp with timezone (e.g. 2026-10-30T12:00:00.000Z)' })
  before?: string;

  /** Optional tie-breaker (pass `next_before_id`): plays at exactly `before` with a smaller event_id are included. */
  @IsOptional()
  @Matches(/^[A-Za-z0-9_.:-]{1,128}$/, { message: 'before_id is invalid' })
  before_id?: string;
}
