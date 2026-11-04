const { validateCreate, validateMerged } = require('./subscriptionSchema');
const { notFound } = require('../errors');

const STATS_COLUMNS = `s.matched, s.delivered, s.failed_attempts, s.dead, s.last_notification_at,
  s.last_success_at, s.last_failure_at, s.last_error, s.last_latency_ms,
  (SELECT count(*)::int FROM notification_dlq d WHERE d.subscription_id = sub.id) AS dlq_count`;

/**
 * Subscription persistence. The engine keeps a compiled copy in memory and is
 * told about changes through the onChange callback (wired by the Broker).
 */
class SubscriptionService {
  constructor(pool) {
    this.pool = pool;
    this.onChange = () => {};
  }

  async list(tenant) {
    const { rows } = await this.pool.query(
      `SELECT sub.*, ${STATS_COLUMNS} FROM subscriptions sub
         LEFT JOIN subscription_stats s ON s.subscription_id = sub.id
        WHERE sub.tenant = $1 ORDER BY sub.name, sub.id`,
      [tenant]
    );
    return rows.map(toSubscription);
  }

  /** All subscriptions of every tenant, for the engine. */
  async loadAll() {
    const { rows } = await this.pool.query('SELECT * FROM subscriptions');
    return rows.map(toSubscription);
  }

  async loadOne(id) {
    const { rows } = await this.pool.query('SELECT * FROM subscriptions WHERE id = $1', [id]);
    return rows[0] ? toSubscription(rows[0]) : null;
  }

  async get(tenant, id) {
    const { rows } = await this.pool.query(
      `SELECT sub.*, ${STATS_COLUMNS} FROM subscriptions sub
         LEFT JOIN subscription_stats s ON s.subscription_id = sub.id
        WHERE sub.tenant = $1 AND sub.id = $2`,
      [tenant, id]
    );
    if (!rows.length) throw notFound(`Subscription '${id}' not found`);
    return toSubscription(rows[0]);
  }

  async create(tenant, input, createdBy) {
    const s = validateCreate(input);
    const { rows } = await this.pool.query(
      `INSERT INTO subscriptions (tenant, name, description, enabled, selector, trigger, filter, filter_mode,
                                  throttle_ms, periodic_ms, notify, delivery, expires_at, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING *`,
      [tenant, s.name, s.description || null, s.enabled, s.select, s.trigger, s.filter || null, s.filterMode,
        s.throttleMs || null, s.periodicMs || null, s.notify, s.delivery, s.expiresAt || null, createdBy || null]
    );
    const sub = toSubscription(rows[0]);
    await this.onChange(tenant, sub.id);
    return sub;
  }

  /** Partial update: top-level fields replace the stored ones, then the result is validated as a whole. */
  async update(tenant, id, patch) {
    const current = await this.get(tenant, id);
    const merged = validateMerged({
      ...pickEditable(current),
      ...patch
    });
    const { rows } = await this.pool.query(
      `UPDATE subscriptions SET name = $3, description = $4, enabled = $5, selector = $6, trigger = $7,
              filter = $8, filter_mode = $9, throttle_ms = $10, periodic_ms = $11, notify = $12,
              delivery = $13, expires_at = $14, updated_at = now()
        WHERE tenant = $1 AND id = $2 RETURNING *`,
      [tenant, id, merged.name, merged.description || null, merged.enabled, merged.select, merged.trigger,
        merged.filter || null, merged.filterMode, merged.throttleMs || null, merged.periodicMs || null,
        merged.notify, merged.delivery, merged.expiresAt || null]
    );
    await this.onChange(tenant, id);
    return toSubscription(rows[0]);
  }

  async remove(tenant, id) {
    const { rowCount } = await this.pool.query('DELETE FROM subscriptions WHERE tenant = $1 AND id = $2', [tenant, id]);
    if (!rowCount) throw notFound(`Subscription '${id}' not found`);
    await this.onChange(tenant, id);
  }

  // ─── Statistics ─────────────────────────────────────────────────────────

  /** Adds counter deltas: { [subId]: { matched, delivered, failedAttempts, dead, last* } } */
  async addStats(deltas) {
    for (const [id, d] of Object.entries(deltas)) {
      await this.pool.query(
        `INSERT INTO subscription_stats AS s (subscription_id, matched, delivered, failed_attempts, dead,
                last_notification_at, last_success_at, last_failure_at, last_error, last_latency_ms)
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 WHERE EXISTS (SELECT 1 FROM subscriptions WHERE id = $1)
         ON CONFLICT (subscription_id) DO UPDATE SET
           matched = s.matched + EXCLUDED.matched,
           delivered = s.delivered + EXCLUDED.delivered,
           failed_attempts = s.failed_attempts + EXCLUDED.failed_attempts,
           dead = s.dead + EXCLUDED.dead,
           last_notification_at = GREATEST(s.last_notification_at, EXCLUDED.last_notification_at),
           last_success_at = GREATEST(s.last_success_at, EXCLUDED.last_success_at),
           last_failure_at = GREATEST(s.last_failure_at, EXCLUDED.last_failure_at),
           last_error = CASE WHEN EXCLUDED.last_failure_at IS NOT NULL
                              AND EXCLUDED.last_failure_at >= COALESCE(s.last_failure_at, EXCLUDED.last_failure_at)
                             THEN EXCLUDED.last_error ELSE s.last_error END,
           last_latency_ms = COALESCE(EXCLUDED.last_latency_ms, s.last_latency_ms)`,
        [id, d.matched || 0, d.delivered || 0, d.failedAttempts || 0, d.dead || 0,
          d.lastNotificationAt || null, d.lastSuccessAt || null, d.lastFailureAt || null,
          d.lastError || null, d.lastLatencyMs ?? null]
      );
    }
  }

  async resetStats(tenant, id) {
    await this.get(tenant, id);
    await this.pool.query('DELETE FROM subscription_stats WHERE subscription_id = $1', [id]);
  }

  // ─── Dead-letter queue ──────────────────────────────────────────────────

  async addDead(tenant, subscriptionId, notification, error, attempts) {
    await this.pool.query(
      `INSERT INTO notification_dlq (tenant, subscription_id, notification, error, attempts)
       SELECT $1, $2, $3, $4, $5 WHERE EXISTS (SELECT 1 FROM subscriptions WHERE id = $2)`,
      [tenant, subscriptionId, notification, error, attempts]
    );
  }

  async listDead(tenant, subscriptionId, { limit = 100, offset = 0 } = {}) {
    await this.get(tenant, subscriptionId);
    const { rows } = await this.pool.query(
      `SELECT * FROM notification_dlq WHERE tenant = $1 AND subscription_id = $2
        ORDER BY failed_at DESC LIMIT $3 OFFSET $4`,
      [tenant, subscriptionId, Math.min(Number(limit) || 100, 1000), Number(offset) || 0]
    );
    return rows.map(r => ({
      id: r.id,
      notification: r.notification,
      error: r.error,
      attempts: r.attempts,
      failedAt: r.failed_at.toISOString()
    }));
  }

  /** Removes DLQ entries (all, or the given ids) and returns their notifications. */
  async takeDead(tenant, subscriptionId, ids) {
    await this.get(tenant, subscriptionId);
    const params = [tenant, subscriptionId];
    let filter = '';
    if (Array.isArray(ids) && ids.length) {
      params.push(ids);
      filter = 'AND id = ANY($3::uuid[])';
    }
    const { rows } = await this.pool.query(
      `DELETE FROM notification_dlq WHERE tenant = $1 AND subscription_id = $2 ${filter} RETURNING notification`,
      params
    );
    return rows.map(r => r.notification);
  }
}

function pickEditable(s) {
  return {
    name: s.name,
    description: s.description,
    enabled: s.enabled,
    select: s.select,
    trigger: s.trigger,
    filter: s.filter,
    filterMode: s.filterMode,
    throttleMs: s.throttleMs,
    periodicMs: s.periodicMs,
    notify: s.notify,
    delivery: s.delivery,
    expiresAt: s.expiresAt
  };
}

function toSubscription(row) {
  const sub = {
    id: row.id,
    tenant: row.tenant,
    name: row.name,
    description: row.description,
    enabled: row.enabled,
    select: row.selector,
    trigger: row.trigger,
    filter: row.filter,
    filterMode: row.filter_mode,
    throttleMs: row.throttle_ms,
    periodicMs: row.periodic_ms,
    notify: row.notify,
    delivery: row.delivery,
    expiresAt: row.expires_at ? row.expires_at.toISOString() : null,
    createdBy: row.created_by,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString()
  };
  if ('dlq_count' in row) {
    const iso = (d) => (d ? d.toISOString() : null);
    sub.stats = {
      matched: Number(row.matched || 0),
      delivered: Number(row.delivered || 0),
      failedAttempts: Number(row.failed_attempts || 0),
      dead: Number(row.dead || 0),
      dlq: row.dlq_count,
      lastNotificationAt: iso(row.last_notification_at),
      lastSuccessAt: iso(row.last_success_at),
      lastFailureAt: iso(row.last_failure_at),
      lastError: row.last_error,
      lastLatencyMs: row.last_latency_ms
    };
  }
  return sub;
}

module.exports = SubscriptionService;
module.exports.pickEditable = pickEditable;
