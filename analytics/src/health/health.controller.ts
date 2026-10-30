import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { Public } from '../common/public.decorator';

@Controller('health')
export class HealthController {
  constructor(private readonly ch: ClickHouseService) {}

  @Public()
  @Get()
  async health() {
    const ok = await this.ch.ping().catch(() => false);
    if (!ok) throw new ServiceUnavailableException({ status: 'down', clickhouse: 'unreachable' });
    return { status: 'ok', clickhouse: 'up' };
  }
}
