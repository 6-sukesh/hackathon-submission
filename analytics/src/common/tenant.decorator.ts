import { createParamDecorator, ExecutionContext, UnauthorizedException } from '@nestjs/common';

/** The tenant id from the verified token (AuthGuard). */
export const Tenant = createParamDecorator((_: unknown, ctx: ExecutionContext): string => {
  const tenant = ctx.switchToHttp().getRequest<{ tenantId?: string }>().tenantId;
  if (!tenant) throw new UnauthorizedException('Missing tenant');
  return tenant;
});
