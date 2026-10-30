import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { APP_GUARD } from '@nestjs/core';
import { JwtModule } from '@nestjs/jwt';
import { ClickHouseModule } from './clickhouse/clickhouse.module';
import { AuthGuard } from './common/auth.guard';
import { CommonModule } from './common/common.module';
import { RequestContextMiddleware } from './common/request-context';
import { HealthController } from './health/health.controller';
import { InsightsModule } from './insights/insights.module';
import { OpsModule } from './ops/ops.module';
import { RecommendationsModule } from './recommendations/recommendations.module';
import { ReportsModule } from './reports/reports.module';
import { TenantsModule } from './tenants/tenants.module';

/** HS256 needs a key at least as long as its 256-bit output. */
const MIN_SECRET_LENGTH = 32;

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    JwtModule.registerAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const secret = config.getOrThrow<string>('JWT_SECRET');
        if (secret.length < MIN_SECRET_LENGTH) {
          throw new Error(`JWT_SECRET must be at least ${MIN_SECRET_LENGTH} characters`);
        }
        return { secret, verifyOptions: { algorithms: ['HS256'] } };
      },
    }),
    ClickHouseModule,
    CommonModule,
    ReportsModule,
    RecommendationsModule,
    InsightsModule,
    TenantsModule,
    OpsModule,
  ],
  controllers: [HealthController],
  // Auth guard runs on every route; only routes marked @Public() skip it.
  providers: [{ provide: APP_GUARD, useClass: AuthGuard }],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(RequestContextMiddleware).forRoutes('*path');
  }
}
