const MAX_FAILURES = 5;
const WINDOW_MS = 15 * 60 * 1000;

/**
 * In-memory brute-force protection for login: after MAX_FAILURES failed
 * attempts within WINDOW_MS, the (username, ip) pair is blocked until the
 * window expires. Per instance only; good enough until the cluster phase.
 */
class LoginLimiter {
  constructor() {
    this.failures = new Map();
  }

  _key(username, ip) {
    return `${String(username).toLowerCase()}|${ip}`;
  }

  /** Milliseconds until the pair may retry, 0 if not blocked. */
  blockedFor(username, ip) {
    const entry = this.failures.get(this._key(username, ip));
    if (!entry) return 0;
    const elapsed = Date.now() - entry.first;
    if (elapsed > WINDOW_MS) {
      this.failures.delete(this._key(username, ip));
      return 0;
    }
    return entry.count >= MAX_FAILURES ? WINDOW_MS - elapsed : 0;
  }

  fail(username, ip) {
    const key = this._key(username, ip);
    const entry = this.failures.get(key);
    if (!entry || Date.now() - entry.first > WINDOW_MS) {
      this.failures.set(key, { count: 1, first: Date.now() });
    } else {
      entry.count++;
    }
  }

  succeed(username, ip) {
    this.failures.delete(this._key(username, ip));
  }
}

module.exports = LoginLimiter;
