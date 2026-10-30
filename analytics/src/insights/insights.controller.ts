import { Controller, Get, Param, Query } from '@nestjs/common';
import { Tenant } from '../common/tenant.decorator';
import { UserIdPipe } from '../common/user-id.pipe';
import { HistoryQueryDto } from './dto/history-query.dto';
import { LimitQueryDto } from './dto/limit-query.dto';
import { MoodRecsQueryDto } from './dto/mood-recs-query.dto';
import { TasteDriftQueryDto } from './dto/taste-drift-query.dto';
import { WrappedQueryDto } from './dto/wrapped-query.dto';
import { InsightsService } from './insights.service';
import { TrackIdPipe } from './track-id.pipe';

/** Per-user insights. Every route resolves the user inside the caller's tenant first (404 otherwise). */
@Controller('users/:userId')
export class UserInsightsController {
  constructor(private readonly insights: InsightsService) {}

  @Get('wrapped')
  wrapped(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string, @Query() q: WrappedQueryDto) {
    return this.insights.wrapped(tenant, userId, q.year ?? new Date().getUTCFullYear());
  }

  @Get('history')
  history(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string, @Query() q: HistoryQueryDto) {
    return this.insights.history(tenant, userId, q);
  }

  @Get('streaks')
  streaks(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string) {
    return this.insights.streaks(tenant, userId);
  }

  @Get('taste-drift')
  tasteDrift(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string, @Query() q: TasteDriftQueryDto) {
    return this.insights.tasteDrift(tenant, userId, q.months);
  }

  @Get('recommendations/mood')
  moodRecs(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string, @Query() q: MoodRecsQueryDto) {
    return this.insights.moodRecommendations(tenant, userId, q.mood, q.limit);
  }

  @Get('rediscover')
  rediscover(@Tenant() tenant: string, @Param('userId', UserIdPipe) userId: string, @Query() q: LimitQueryDto) {
    return this.insights.rediscover(tenant, userId, q.limit);
  }
}

/** Global catalog lookups. The tenant header is still enforced by the global guard; no tenant data is read. */
@Controller('tracks')
export class TrackInsightsController {
  constructor(private readonly insights: InsightsService) {}

  @Get(':trackId/similar')
  similar(@Param('trackId', TrackIdPipe) trackId: string, @Query() q: LimitQueryDto) {
    return this.insights.similarTracks(trackId, q.limit);
  }
}
