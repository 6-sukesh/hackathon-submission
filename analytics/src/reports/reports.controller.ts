import { Controller, Get, Param, Query } from '@nestjs/common';
import { Tenant } from '../common/tenant.decorator';
import { UserIdPipe } from '../common/user-id.pipe';
import { ReportQueryDto } from './dto/report-query.dto';
import { ReportsService } from './reports.service';

@Controller('users/:userId/report')
export class ReportsController {
  constructor(private readonly reports: ReportsService) {}

  @Get()
  get(
    @Tenant() tenant: string,
    @Param('userId', UserIdPipe) userId: string,
    @Query() q: ReportQueryDto,
  ) {
    return this.reports.userReport(tenant, userId, q);
  }
}
