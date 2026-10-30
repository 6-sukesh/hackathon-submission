import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { JwtService, TokenExpiredError } from '@nestjs/jwt';
import type { Request } from 'express';
import { IS_PUBLIC } from './public.decorator';
import { requestContext } from './request-context';

const TENANT_RE = /^[A-Za-z0-9_.:-]{1,64}$/;

interface Claims {
  tenant_id?: unknown;
  sub?: unknown;
  exp?: unknown;
}

/**
 * Requires `Authorization: Bearer <jwt>` (HS256, JWT_SECRET) with `tenant_id`, `sub` and `exp` claims.
 * Pins the tenant on the request (read via @Tenant()) and in the request context, from which
 * ClickHouseService sets SQL_tenant_id on every query (row policies). Downstream code never reads the
 * tenant from params/query/body.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly jwt: JwtService,
  ) {}

  async canActivate(ctx: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [
      ctx.getHandler(),
      ctx.getClass(),
    ]);
    if (isPublic) return true;

    const req = ctx.switchToHttp().getRequest<Request & { tenantId?: string }>();
    const [scheme, token] = (req.headers.authorization ?? '').split(' ');
    if (scheme !== 'Bearer' || !token) throw new UnauthorizedException('Missing bearer token');

    let claims: Claims;
    try {
      // Algorithm is pinned to HS256 in JwtModule's verifyOptions; exp is checked when present.
      claims = await this.jwt.verifyAsync<Claims>(token);
    } catch (err) {
      throw new UnauthorizedException(err instanceof TokenExpiredError ? 'Token expired' : 'Invalid token');
    }
    const { tenant_id: tenant, sub, exp } = claims;
    if (typeof tenant !== 'string' || !TENANT_RE.test(tenant) || typeof sub !== 'string' || !sub) {
      throw new UnauthorizedException('Token must carry tenant_id and sub claims');
    }
    if (typeof exp !== 'number') throw new UnauthorizedException('Token must carry an exp claim');

    req.tenantId = tenant;
    const store = requestContext();
    if (store) Object.assign(store, { tenantId: tenant, sub });
    return true;
  }
}
