import { createClient } from '@clickhouse/client';
import { Global, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CLICKHOUSE_CLIENT } from './clickhouse.constants';
import { ClickHouseService } from './clickhouse.service';

@Global()
@Module({
  providers: [
    {
      provide: CLICKHOUSE_CLIENT,
      inject: [ConfigService],
      useFactory: (config: ConfigService) =>
        createClient({
          url: config.get('CLICKHOUSE_URL', 'http://localhost:8123'),
          username: config.get('CLICKHOUSE_USER', 'analytics_app'),
          password: config.get('CLICKHOUSE_PASSWORD', ''),
          database: config.get('CLICKHOUSE_DB', 'music'),
          clickhouse_settings: {
            // Return UInt64 counts as JSON numbers and timestamps as ISO strings.
            output_format_json_quote_64bit_integers: 0,
            date_time_output_format: 'iso',
          },
        }),
    },
    ClickHouseService,
  ],
  exports: [ClickHouseService],
})
export class ClickHouseModule {}
