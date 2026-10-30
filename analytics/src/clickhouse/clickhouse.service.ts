import type { ClickHouseClient } from '@clickhouse/client';
import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { requestContext } from '../common/request-context';
import { CLICKHOUSE_CLIENT } from './clickhouse.constants';

/**
 * Thin wrapper over @clickhouse/client. All values go through `params`
 * (ClickHouse server-side `{name:Type}` substitution) — never string interpolation.
 *
 * Every query carries the authenticated tenant as the custom setting SQL_tenant_id, taken from the
 * request context (set by AuthGuard), so callers cannot forget it. ClickHouse row policies filter
 * every tenant table on it for the analytics_app user; without a tenant (no authenticated request)
 * the setting is absent and any read of a tenant table fails instead of returning all tenants.
 */
@Injectable()
export class ClickHouseService implements OnApplicationShutdown {
  constructor(@Inject(CLICKHOUSE_CLIENT) private readonly client: ClickHouseClient) {}

  async query<T>(query: string, params: Record<string, unknown> = {}): Promise<T[]> {
    const tenant = requestContext()?.tenantId;
    const rs = await this.client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
      clickhouse_settings: tenant ? { SQL_tenant_id: tenant } : {},
    });
    return rs.json<T>();
  }

  async one<T>(query: string, params: Record<string, unknown> = {}): Promise<T | undefined> {
    return (await this.query<T>(query, params))[0];
  }

  async ping(): Promise<boolean> {
    const res = await this.client.ping();
    return res.success;
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
  }
}
