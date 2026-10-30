import { Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export const MOODS = ['chill', 'focus', 'workout', 'happy'] as const;
export type Mood = (typeof MOODS)[number];

export class MoodRecsQueryDto {
  @IsIn(MOODS, { message: `mood must be one of: ${MOODS.join(', ')}` })
  mood!: Mood;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(50)
  limit: number = 10;
}
