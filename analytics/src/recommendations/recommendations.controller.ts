import { Controller, Get, Param, Query } from '@nestjs/common';
import { Tenant } from '../common/tenant.decorator';
import { UserIdPipe } from '../common/user-id.pipe';
import { RecommendationsQueryDto } from './dto/recommendations-query.dto';
import { RecommendationsService } from './recommendations.service';

@Controller('users/:userId/recommendations')
export class RecommendationsController {
  constructor(private readonly recs: RecommendationsService) {}

  @Get()
  get(
    @Tenant() tenant: string,
    @Param('userId', UserIdPipe) userId: string,
    @Query() q: RecommendationsQueryDto,
  ) {
    return this.recs.recommend(tenant, userId, q.limit);
  }
}
