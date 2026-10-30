import { Controller, Get, Query } from '@nestjs/common';
import { Tenant } from '../common/tenant.decorator';
import { PipelineLagQueryDto, SchemaFieldsQueryDto } from './dto/ops-query.dto';
import { OpsService } from './ops.service';

/** Pipeline operations. Platform-internal, but still behind AuthGuard and scoped to the caller's tenant. */
@Controller('ops')
export class OpsController {
  constructor(private readonly ops: OpsService) {}

  @Get('pipeline-lag')
  pipelineLag(@Tenant() tenant: string, @Query() q: PipelineLagQueryDto) {
    return this.ops.pipelineLag(tenant, q);
  }

  @Get('schema-fields')
  schemaFields(@Tenant() tenant: string, @Query() q: SchemaFieldsQueryDto) {
    return this.ops.schemaFields(tenant, q);
  }
}
