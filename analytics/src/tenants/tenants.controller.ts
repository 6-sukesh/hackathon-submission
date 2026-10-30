import { Controller, Get, Query } from '@nestjs/common';
import { Tenant } from '../common/tenant.decorator';
import {
  DataQualityQueryDto,
  DaysQueryDto,
  LiveQueryDto,
  RetentionQueryDto,
  TrendingQueryDto,
} from './dto/dashboard-query.dto';
import { TenantsService } from './tenants.service';

/** Tenant-level dashboards. The tenant comes only from @Tenant() (the token), never from params. */
@Controller('tenants/me')
export class TenantsController {
  constructor(private readonly tenants: TenantsService) {}

  /** Last 24h of the caller's tenant at minute granularity (event time), gaps filled with 0. */
  @Get('overview')
  overview(@Tenant() tenant: string) {
    return this.tenants.overview(tenant);
  }

  @Get('live')
  live(@Tenant() tenant: string, @Query() q: LiveQueryDto) {
    return this.tenants.live(tenant, q);
  }

  @Get('trending')
  trending(@Tenant() tenant: string, @Query() q: TrendingQueryDto) {
    return this.tenants.trending(tenant, q);
  }

  @Get('retention')
  retention(@Tenant() tenant: string, @Query() q: RetentionQueryDto) {
    return this.tenants.retention(tenant, q);
  }

  @Get('engagement')
  engagement(@Tenant() tenant: string, @Query() q: DaysQueryDto) {
    return this.tenants.engagement(tenant, q);
  }

  @Get('funnel')
  funnel(@Tenant() tenant: string, @Query() q: DaysQueryDto) {
    return this.tenants.funnel(tenant, q);
  }

  @Get('data-quality')
  dataQuality(@Tenant() tenant: string, @Query() q: DataQualityQueryDto) {
    return this.tenants.dataQuality(tenant, q);
  }
}
