const logger = require('../../utils/logger');

const KINDS = ['login', 'login_failed', 'login_blocked', 'logout', 'refresh_reuse', 'session_revoked', 'user_sessions_revoked'];
const RETENTION_DAYS = Number(process.env.BROKER_AUTH_LOG_RETENTION_DAYS) || 90;
const PRUNE_INTERVAL_MS = 6 * 60 * 60 * 1000;

/** Access log (auth_events): who logged in, from where, failed attempts, logouts and revocations. */
class AuthEventService {
  constructor(pool) {
    this.pool = pool;
    this.lastPrune = 0;
  }

  /** Never fails the request that triggered it. */
  record({ kind, userId, username, sessionId, ip, userAgent, actor, detail }) {
    this.pool.query(
      `INSERT INTO auth_events (kind, user_id, username, session_id, ip, user_agent, actor, detail)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [kind, userId || null, username || null, sessionId || null, ip || null, userAgent || null, actor || null, detail || null]
    ).catch(e => logger.warn(`Auth event not recorded: ${e.message}`));
    this._prune();
  }

  async list({ userId, kind, from, to, limit = 100, offset = 0 } = {}) {
    const where = [];
    const params = [];
    const add = (sql, value) => { params.push(value); where.push(sql.replace('?', `$${params.length}`)); };
    if (userId) add('user_id = ?', userId);
    if (kind) add('kind = ?', kind);
    if (from) add('at >= ?', from);
    if (to) add('at <= ?', to);
    params.push(limit, offset);
    const { rows } = await this.pool.query(
      `SELECT *, count(*) OVER () AS total FROM auth_events ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY at DESC, id DESC LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );
    return {
      total: rows.length ? Number(rows[0].total) : 0,
      events: rows.map(r => ({
        id: Number(r.id),
        at: r.at.toISOString(),
        kind: r.kind,
        userId: r.user_id,
        username: r.username,
        sessionId: r.session_id,
        ip: r.ip,
        userAgent: r.user_agent,
        actor: r.actor,
        detail: r.detail
      }))
    };
  }

  _prune() {
    if (Date.now() - this.lastPrune < PRUNE_INTERVAL_MS) return;
    this.lastPrune = Date.now();
    this.pool.query(`DELETE FROM auth_events WHERE at < now() - make_interval(days => $1)`, [RETENTION_DAYS])
      .catch(e => logger.warn(`Auth log pruning failed: ${e.message}`));
  }
}

AuthEventService.KINDS = KINDS;
module.exports = AuthEventService;
