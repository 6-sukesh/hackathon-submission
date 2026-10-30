#!/usr/bin/env node
// Prints a dev API token: HS256 JWT signed with JWT_SECRET (environment first, then analytics/.env).
//
//   npm run -s token -- <tenant_id> [sub] [ttl]
//
//   sub  token subject (caller id), default "dev"
//   ttl  lifetime: seconds or a number with s|m|h|d (e.g. 900, 15m, 12h), default 1h.
//        A negative ttl mints an already-expired token (for tests).
const path = require('node:path');
const { JwtService } = require('@nestjs/jwt');

try {
  process.loadEnvFile(path.join(__dirname, '..', '.env')); // does not override variables already set
} catch {
  // no .env: rely on the environment
}

const UNIT_SEC = { s: 1, m: 60, h: 3600, d: 86400 };
const [tenant, sub = 'dev', ttl = '1h'] = process.argv.slice(2);
const ttlMatch = /^(-?\d+)([smhd]?)$/.exec(ttl);

if (!tenant || !ttlMatch) {
  console.error('usage: npm run -s token -- <tenant_id> [sub] [ttl, e.g. 3600 | 15m | 12h | 7d]');
  process.exit(1);
}
const secret = process.env.JWT_SECRET;
if (!secret) {
  console.error('JWT_SECRET is not set (environment or analytics/.env)');
  process.exit(1);
}

const expiresIn = Number(ttlMatch[1]) * UNIT_SEC[ttlMatch[2] || 's'];
const token = new JwtService().sign(
  { tenant_id: tenant },
  { secret, algorithm: 'HS256', subject: sub, expiresIn },
);
console.log(token);
