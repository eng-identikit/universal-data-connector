const logger = require('../../utils/logger');
const { badRequest } = require('../errors');

const MAX_POINTS = 2000;
const MAX_RAW = 10000;
const DEFAULT_RANGE_MS = 24 * 60 * 60 * 1000;

/**
 * Attribute history (TimescaleDB hypertable when available).
 * A row is recorded each time an attribute gets a new timestamp, i.e. its
 * value changed or the producer sent an explicit ts.
 */
class HistoryService {
  constructor(pool, { enabled = process.env.BROKER_HISTORY !== 'off' } = {}) {
    this.pool = pool;
    this.enabled = enabled;
  }

  /** Applies BROKER_HISTORY_RETENTION (e.g. '90 days') as a TimescaleDB retention policy. */
  async configureRetention() {
    const retention = process.env.BROKER_HISTORY_RETENTION;
    if (!retention) return;
    try {
      await this.pool.query(
        `SELECT add_retention_policy('attribute_history', $1::interval, if_not_exists => true)`, [retention]
      );
      logger.info(`Broker history retention: ${retention}`);
    } catch (error) {
      logger.warn(`Broker history retention not applied (${error.message})`);
    }
  }

  /** History rows for the attributes that changed between previous and entity. */
  rows(tenant, previous, entity) {
    if (!this.enabled || !entity) return [];
    const out = [];
    for (const [name, attr] of Object.entries(entity.attrs)) {
      const before = previous?.attrs?.[name];
      if (before && before.ts === attr.ts) continue;
      const value = attr.type === 'relationship' ? attr.target : attr.value;
      if (value === undefined) continue;
      out.push({
        time: attr.ts,
        entity: entity.id,
        attr: name,
        num: typeof value === 'number' ? value : typeof value === 'boolean' ? Number(value) : null,
        val: value,
        quality: attr.quality || null
      });
    }
    return out;
  }

  /** Inserts rows inside the caller's transaction. */
  async insert(client, tenant, rows) {
    if (!rows.length) return;
    // One JSON document: much cheaper to serialize than parallel pg arrays
    await client.query(
      `INSERT INTO attribute_history (time, tenant, entity_id, attr, num, val, quality)
       SELECT x.time, $1, x.entity, x.attr, x.num, x.val, x.quality
         FROM jsonb_to_recordset($2::jsonb)
              AS x(time timestamptz, entity text, attr text, num float8, val jsonb, quality text)`,
      [tenant, JSON.stringify(rows)]
    );
  }

  /**
   * Downsampled series per attribute: numeric attributes are bucketed
   * (avg/min/max/last), the others return their last value per bucket.
   */
  async series(tenant, entityId, { attrs, from, to, maxPoints } = {}) {
    const range = parseRange(from, to);
    const points = clampInt(maxPoints, 500, 1, MAX_POINTS, 'maxPoints');
    const bucketMs = Math.max(1000, Math.ceil((range.to - range.from) / points));
    const names = attrs ? String(attrs).split(',').map(s => s.trim()).filter(Boolean) : null;

    const { rows } = await this.pool.query(
      `SELECT attr,
              date_bin($5::interval, time, $3::timestamptz) AS bucket,
              avg(num) AS avg, min(num) AS min, max(num) AS max,
              (array_agg(val ORDER BY time DESC))[1] AS last,
              count(*)::int AS samples
         FROM attribute_history
        WHERE tenant = $1 AND entity_id = $2 AND time >= $3 AND time <= $4
          AND ($6::text[] IS NULL OR attr = ANY($6))
        GROUP BY attr, bucket ORDER BY attr, bucket`,
      [tenant, entityId, range.from, range.to, `${bucketMs} milliseconds`, names]
    );

    const series = {};
    for (const r of rows) {
      const s = series[r.attr] || (series[r.attr] = []);
      s.push({
        t: r.bucket.toISOString(),
        avg: r.avg === null ? null : Number(r.avg),
        min: r.min === null ? null : Number(r.min),
        max: r.max === null ? null : Number(r.max),
        last: r.last,
        samples: r.samples
      });
    }
    return { from: range.from.toISOString(), to: range.to.toISOString(), bucketMs, series };
  }

  /** Raw samples of one attribute, newest first. */
  async raw(tenant, entityId, attr, { from, to, limit } = {}) {
    const range = parseRange(from, to);
    const max = clampInt(limit, 1000, 1, MAX_RAW, 'limit');
    const { rows } = await this.pool.query(
      `SELECT time, val, quality FROM attribute_history
        WHERE tenant = $1 AND entity_id = $2 AND attr = $3 AND time >= $4 AND time <= $5
        ORDER BY time DESC LIMIT $6`,
      [tenant, entityId, attr, range.from, range.to, max]
    );
    return rows.map(r => ({ t: r.time.toISOString(), value: r.val, quality: r.quality }));
  }

  /** Attributes with history for an entity, with sample count and time range. */
  async attributes(tenant, entityId) {
    const { rows } = await this.pool.query(
      `SELECT attr, count(*)::int AS samples, min(time) AS first, max(time) AS last,
              bool_or(num IS NOT NULL) AS numeric
         FROM attribute_history WHERE tenant = $1 AND entity_id = $2 GROUP BY attr ORDER BY attr`,
      [tenant, entityId]
    );
    return rows.map(r => ({ attr: r.attr, samples: r.samples, numeric: r.numeric, first: r.first.toISOString(), last: r.last.toISOString() }));
  }
}

function parseRange(from, to) {
  const end = to ? new Date(to) : new Date();
  const start = from ? new Date(from) : new Date(end.getTime() - DEFAULT_RANGE_MS);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) throw badRequest('from/to must be ISO-8601 dates');
  if (start >= end) throw badRequest('from must be before to');
  return { from: start, to: end };
}

function clampInt(value, fallback, min, max, name) {
  if (value === undefined || value === '') return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) throw badRequest(`${name} must be an integer between ${min} and ${max}`);
  return n;
}

module.exports = HistoryService;
