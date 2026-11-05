const crypto = require('crypto');
const logger = require('../../utils/logger');

/**
 * Login sessions behind the JWTs. Every access/refresh token carries the
 * session id (sid); a token whose session is gone is rejected, so logging out
 * or revoking a session takes effect at the next request on every instance.
 *
 * The refresh token is single use: each refresh rotates its jti. Presenting
 * an already used refresh token means it was copied, and the whole session is
 * revoked ('reuse'). The token replaced last is still accepted for
 * REUSE_INTERVAL_MS, so two browser tabs refreshing at once keep the session.
 *
 * Session: { id, userId, ip, userAgent, createdAt, lastSeenAt, expiresAt }
 *
 * RedisSessionStore (BROKER_REDIS_URL) keeps them in Redis with a TTL;
 * PgSessionStore uses the auth_sessions table when Redis is not configured.
 */

const newId = () => crypto.randomBytes(16).toString('base64url');
// lastSeenAt is written at most once per interval per session
const TOUCH_INTERVAL_MS = 60 * 1000;
const REUSE_INTERVAL_MS = Number(process.env.BROKER_REFRESH_REUSE_INTERVAL_MS) || 30 * 1000;

class BaseSessionStore {
  constructor() {
    this.touched = new Map();
  }

  /** Records activity, throttled per session. */
  touch(sid) {
    const now = Date.now();
    if (now - (this.touched.get(sid) || 0) < TOUCH_INTERVAL_MS) return;
    this.touched.set(sid, now);
    if (this.touched.size > 10000) {
      for (const [k, t] of this.touched) if (now - t > TOUCH_INTERVAL_MS) this.touched.delete(k);
    }
    this._touch(sid, new Date(now)).catch(e => logger.warn(`Session touch failed: ${e.message}`));
  }
}

class PgSessionStore extends BaseSessionStore {
  constructor(pool) {
    super();
    this.pool = pool;
    this.kind = 'postgres';
  }

  async start() {}
  async close() {}

  async create({ userId, ip, userAgent, expiresAt }) {
    const id = newId();
    const jti = newId();
    await this.pool.query(
      `INSERT INTO auth_sessions (id, user_id, refresh_jti, ip, user_agent, expires_at) VALUES ($1, $2, $3, $4, $5, $6)`,
      [id, userId, jti, ip || null, userAgent || null, expiresAt]
    );
    return { id, jti };
  }

  async get(sid) {
    const { rows } = await this.pool.query('SELECT * FROM auth_sessions WHERE id = $1 AND expires_at > now()', [sid]);
    return rows.length ? fromRow(rows[0]) : null;
  }

  async rotate(sid, jti, expiresAt) {
    const next = newId();
    const { rowCount } = await this.pool.query(
      `UPDATE auth_sessions
          SET prev_jti = CASE WHEN refresh_jti = $2 THEN refresh_jti ELSE prev_jti END,
              rotated_at = CASE WHEN refresh_jti = $2 THEN now() ELSE rotated_at END,
              refresh_jti = $3, expires_at = $4, last_seen_at = now()
        WHERE id = $1 AND expires_at > now()
          AND (refresh_jti = $2 OR (prev_jti = $2 AND rotated_at > now() - make_interval(secs => $5)))`,
      [sid, jti, next, expiresAt, REUSE_INTERVAL_MS / 1000]
    );
    if (rowCount) return { jti: next };
    return (await this.get(sid)) ? { reuse: true } : null;
  }

  async _touch(sid, at) {
    await this.pool.query('UPDATE auth_sessions SET last_seen_at = $2 WHERE id = $1', [sid, at]);
  }

  async revoke(sid) {
    const { rows } = await this.pool.query('DELETE FROM auth_sessions WHERE id = $1 RETURNING user_id', [sid]);
    return rows.length ? rows[0].user_id : null;
  }

  async revokeUser(userId) {
    const { rows } = await this.pool.query('DELETE FROM auth_sessions WHERE user_id = $1 RETURNING id', [userId]);
    return rows.map(r => r.id);
  }

  async list({ userId } = {}) {
    await this.pool.query('DELETE FROM auth_sessions WHERE expires_at <= now()');
    const { rows } = await this.pool.query(
      `SELECT * FROM auth_sessions WHERE ($1::uuid IS NULL OR user_id = $1) ORDER BY last_seen_at DESC`,
      [userId || null]
    );
    return rows.map(fromRow);
  }
}

function fromRow(r) {
  return {
    id: r.id,
    userId: r.user_id,
    ip: r.ip,
    userAgent: r.user_agent,
    createdAt: r.created_at.toISOString(),
    lastSeenAt: r.last_seen_at.toISOString(),
    expiresAt: r.expires_at.toISOString()
  };
}

// Atomic refresh rotation: 1 rotated, 0 reused (wrong jti), -1 no session.
// ARGV: presented jti, new jti, expiresAt, now (ISO), ttl ms, now ms, reuse interval ms
const ROTATE_LUA = `
local cur = redis.call('HGET', KEYS[1], 'jti')
if not cur then return -1 end
if cur == ARGV[1] then
  redis.call('HSET', KEYS[1], 'prevJti', cur, 'rotatedAt', ARGV[6])
else
  local prev = redis.call('HGET', KEYS[1], 'prevJti')
  local at = tonumber(redis.call('HGET', KEYS[1], 'rotatedAt') or '0')
  if prev ~= ARGV[1] or tonumber(ARGV[6]) - at > tonumber(ARGV[7]) then return 0 end
end
redis.call('HSET', KEYS[1], 'jti', ARGV[2], 'expiresAt', ARGV[3], 'lastSeenAt', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1`;

class RedisSessionStore extends BaseSessionStore {
  constructor(url) {
    super();
    const Redis = require('ioredis');
    this.kind = 'redis';
    this.redis = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 2, enableOfflineQueue: true });
    this.redis.on('error', (e) => logger.warn(`Redis (sessions): ${e.message}`));
    this.redis.defineCommand('rotateSession', { numberOfKeys: 1, lua: ROTATE_LUA });
  }

  async start() {
    if (this.redis.status === 'wait') await this.redis.connect();
  }

  async close() {
    this.redis.disconnect();
  }

  _key(sid) { return `udc:session:${sid}`; }
  _userKey(userId) { return `udc:user-sessions:${userId}`; }

  async create({ userId, ip, userAgent, expiresAt }) {
    const id = newId();
    const jti = newId();
    const now = new Date().toISOString();
    const ttl = Math.max(1000, expiresAt.getTime() - Date.now());
    await this.redis.multi()
      .hset(this._key(id), {
        userId, jti, ip: ip || '', userAgent: userAgent || '', createdAt: now, lastSeenAt: now, expiresAt: expiresAt.toISOString()
      })
      .pexpire(this._key(id), ttl)
      .sadd(this._userKey(userId), id)
      .exec();
    return { id, jti };
  }

  async get(sid) {
    const h = await this.redis.hgetall(this._key(sid));
    return h && h.userId ? fromHash(sid, h) : null;
  }

  async rotate(sid, jti, expiresAt) {
    const next = newId();
    const ttl = Math.max(1000, expiresAt.getTime() - Date.now());
    const now = Date.now();
    const r = await this.redis.rotateSession(this._key(sid), jti, next, expiresAt.toISOString(), new Date(now).toISOString(), ttl, now, REUSE_INTERVAL_MS);
    if (r === 1) return { jti: next };
    return r === 0 ? { reuse: true } : null;
  }

  async _touch(sid, at) {
    // Only when the session still exists: a revoked one must not be recreated
    await this.redis.eval(
      "if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('HSET', KEYS[1], 'lastSeenAt', ARGV[1]) end return 0",
      1, this._key(sid), at.toISOString()
    );
  }

  async revoke(sid) {
    const userId = await this.redis.hget(this._key(sid), 'userId');
    await this.redis.del(this._key(sid));
    if (userId) await this.redis.srem(this._userKey(userId), sid);
    return userId || null;
  }

  async revokeUser(userId) {
    const sids = await this.redis.smembers(this._userKey(userId));
    const pipeline = this.redis.multi().del(this._userKey(userId));
    for (const sid of sids) pipeline.del(this._key(sid));
    await pipeline.exec();
    return sids;
  }

  async list({ userId } = {}) {
    const userKeys = userId ? [this._userKey(userId)] : await this._scan('udc:user-sessions:*');
    const sessions = [];
    for (const key of userKeys) {
      const sids = await this.redis.smembers(key);
      if (!sids.length) continue;
      const pipeline = this.redis.pipeline();
      for (const sid of sids) pipeline.hgetall(this._key(sid));
      const results = await pipeline.exec();
      const expired = [];
      results.forEach(([err, h], i) => {
        if (!err && h && h.userId) sessions.push(fromHash(sids[i], h));
        else expired.push(sids[i]);
      });
      // Sessions expire through their TTL; drop them from the per-user index
      if (expired.length) await this.redis.srem(key, ...expired);
    }
    return sessions.sort((a, b) => b.lastSeenAt.localeCompare(a.lastSeenAt));
  }

  async _scan(pattern) {
    const keys = [];
    let cursor = '0';
    do {
      const [next, batch] = await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', 500);
      cursor = next;
      keys.push(...batch);
    } while (cursor !== '0');
    return keys;
  }
}

function fromHash(id, h) {
  return {
    id,
    userId: h.userId,
    ip: h.ip || null,
    userAgent: h.userAgent || null,
    createdAt: h.createdAt,
    lastSeenAt: h.lastSeenAt,
    expiresAt: h.expiresAt
  };
}

function createSessionStore(pool, url = process.env.BROKER_REDIS_URL) {
  return url ? new RedisSessionStore(url) : new PgSessionStore(pool);
}

module.exports = { createSessionStore, PgSessionStore, RedisSessionStore };
