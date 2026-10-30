import { AsyncLocalStorage } from 'node:async_hooks';
import { Injectable, NestMiddleware } from '@nestjs/common';

/** Per-request state that must reach code with no access to the request (e.g. ClickHouseService). */
export interface RequestContext {
  /** Authenticated tenant, set by AuthGuard. Unset on @Public() routes. */
  tenantId?: string;
  /** Token subject (caller id), set by AuthGuard. */
  sub?: string;
}

const storage = new AsyncLocalStorage<RequestContext>();

/** The current request's context, or undefined outside a request (startup, shutdown). */
export const requestContext = (): RequestContext | undefined => storage.getStore();

/** Opens an empty context for every request; AuthGuard fills it in once the token is verified. */
@Injectable()
export class RequestContextMiddleware implements NestMiddleware {
  use(_req: unknown, _res: unknown, next: () => void): void {
    storage.run({}, next);
  }
}
