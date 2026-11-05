const logger = require('../../utils/logger');

const MAX_ATTEMPTS = 5;
const WINDOW_MS = 15 * 60 * 1000;

/**
 * Brute-force protection for login: at most MAX_ATTEMPTS attempts per
 * (username, ip) within WINDOW_MS; a successful login resets the count.
 *
 * The attempt is counted before the password is checked, so parallel
 * requests cannot slip through while the (slow) verification runs.
 * With Redis the count is shared by all instances, otherwise it is per instance.
 */
class LoginLimiter {
  constructor(redis = null) {
    this.redis = redis;
    this.attempts = new Map();
  }

  _key(username, ip) {
    return `${String(username).toLowerCase()}|${ip}`;
  }

  /** Counts an attempt; returns the milliseconds to wait when over the limit, 0 otherwise. */
  async attempt(username, ip) {
    const key = this._key(username, ip);
    if (this.redis) {
      try {
        const rkey = `udc:login:${key}`;
        const [[, count], , [, ttl]] = await this.redis.multi().incr(rkey).pexpire(rkey, WINDOW_MS, 'NX').pttl(rkey).exec();
        return count > MAX_ATTEMPTS ? Math.max(ttl, 1000) : 0;
      } catch (error) {
        logger.warn(`Login limiter (Redis) unavailable, using local counts: ${error.message}`);
      }
    }
    const now = Date.now();
    let entry = this.attempts.get(key);
    if (!entry || now - entry.first > WINDOW_MS) {
      entry = { count: 0, first: now };
      this.attempts.set(key, entry);
    }
    entry.count++;
    if (this.attempts.size > 10000) {
      for (const [k, e] of this.attempts) if (now - e.first > WINDOW_MS) this.attempts.delete(k);
    }
    return entry.count > MAX_ATTEMPTS ? WINDOW_MS - (now - entry.first) : 0;
  }

  async succeed(username, ip) {
    const key = this._key(username, ip);
    this.attempts.delete(key);
    if (this.redis) await this.redis.del(`udc:login:${key}`).catch(() => {});
  }
}

module.exports = LoginLimiter;
