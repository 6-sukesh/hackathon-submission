import { Injectable, NotFoundException } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';

export interface UserProfile {
  user_id: string;
  display_name: string;
  plan: string;
  country: string;
}

/**
 * Resolves a user strictly inside a tenant. A user id that belongs to another
 * tenant (or doesn't exist) yields 404 — never data.
 * Falls back to "has plays in this tenant" so a user whose profile row hasn't
 * landed via CDC yet still resolves (with an empty profile).
 */
@Injectable()
export class UserScopeService {
  constructor(private readonly ch: ClickHouseService) {}

  async resolve(tenant: string, userId: string): Promise<UserProfile> {
    const profile = await this.ch.one<UserProfile>(
      `SELECT user_id, display_name, plan, country
       FROM music.users FINAL
       WHERE tenant_id = {tenant:String} AND user_id = {userId:String} AND is_deleted = 0
       LIMIT 1`,
      { tenant, userId },
    );
    if (profile) return profile;

    const hasEvents = await this.ch.one<{ n: number }>(
      `SELECT count() AS n FROM (
         SELECT 1 FROM music.plays WHERE tenant_id = {tenant:String} AND user_id = {userId:String} LIMIT 1
       )`,
      { tenant, userId },
    );
    if (hasEvents?.n) return { user_id: userId, display_name: '', plan: '', country: '' };

    throw new NotFoundException(`User ${userId} not found`);
  }
}
