const fs = require('fs').promises;
const path = require('path');
const logger = require('../utils/logger');

/**
 * TimescaleHistory - read-only queries over the data stored by TimescaleDBAdapter
 *
 * Rows are written as: time, source_id, value JSONB where value is
 *   { sourceId, timestamp, data: { id, type, measurements: [{ id, type, value }], metadata } }
 * Measurements are extracted in SQL with jsonb_array_elements, so no schema change is needed.
 *
 * Uses its own pg Pool so history stays browsable even when the engine is
 * currently writing to another storage (e.g. after a restart, which defaults to memory).
 */

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

// Candidate bucket sizes for automatic downsampling (seconds → interval)
const BUCKETS = [
  [1, '1 second'], [5, '5 seconds'], [10, '10 seconds'], [30, '30 seconds'],
  [60, '1 minute'], [300, '5 minutes'], [600, '10 minutes'], [1800, '30 minutes'],
  [3600, '1 hour'], [10800, '3 hours'], [21600, '6 hours'], [43200, '12 hours'],
  [86400, '1 day'], [604800, '7 days'], [2592000, '30 days']
];

// Measurements array, tolerant to both the engine format and flat records
const MEASUREMENTS_SQL = `
  CASE
    WHEN jsonb_typeof(value->'data'->'measurements') = 'array' THEN value->'data'->'measurements'
    WHEN jsonb_typeof(value->'measurements') = 'array' THEN value->'measurements'
    ELSE '[]'::jsonb
  END`;

const NUMERIC_VALUE_SQL = `
  CASE jsonb_typeof(m->'value')
    WHEN 'number' THEN (m->>'value')::double precision
    WHEN 'boolean' THEN CASE WHEN (m->>'value')::boolean THEN 1 ELSE 0 END
    ELSE NULL
  END`;

function chooseBucket(startTime, endTime, maxPoints) {
  const rangeSeconds = Math.max(1, (endTime - startTime) / 1000);
  const target = rangeSeconds / Math.max(1, maxPoints);
  const found = BUCKETS.find(([seconds]) => seconds >= target);
  return (found || BUCKETS[BUCKETS.length - 1])[1];
}

function parseTime(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    const error = new Error(`Invalid time '${value}'`);
    error.status = 400;
    throw error;
  }
  return date;
}

class TimescaleHistory {
  constructor() {
    this.pool = null;
    this.poolKey = null;
    this.table = 'sensor_data';
  }

  /**
   * Resolve the TimescaleDB configuration: active engine storage first, then config/storage.json
   * (storage or alternatives.timescaledb). Returns null when none is configured.
   */
  async resolveConfig(engine) {
    const isTimescale = type => type === 'timescaledb' || type === 'timescale';

    const active = engine?.dataStore?.storageConfig;
    if (active && isTimescale(active.type)) {
      return { config: active.config || {}, origin: 'active' };
    }

    try {
      const file = path.join(process.cwd(), 'config', 'storage.json');
      const json = JSON.parse(await fs.readFile(file, 'utf8'));
      if (json.storage && isTimescale(json.storage.type)) {
        return { config: json.storage.config || {}, origin: 'storage.json' };
      }
      const alt = json.alternatives && (json.alternatives.timescaledb || json.alternatives.timescale);
      if (alt) {
        return { config: alt.config || {}, origin: 'storage.json (alternatives)' };
      }
    } catch (error) {
      logger.debug(`TimescaleHistory: cannot read storage.json: ${error.message}`);
    }
    return null;
  }

  async getPool(engine) {
    const resolved = await this.resolveConfig(engine);
    if (!resolved) {
      const error = new Error('TimescaleDB is not configured (set it in Storage or in config/storage.json)');
      error.status = 409;
      throw error;
    }

    const { config } = resolved;
    const table = config.table || 'sensor_data';
    if (!IDENT_RE.test(table)) {
      throw new Error(`Invalid TimescaleDB table name '${table}'`);
    }

    const connection = {
      host: config.host || 'localhost',
      port: config.port || 5432,
      database: config.database,
      user: config.username || config.user,
      password: config.password,
      ...(config.options || {})
    };
    const key = JSON.stringify(connection);

    if (!this.pool || this.poolKey !== key) {
      await this.close();
      const { Pool } = require('pg');
      this.pool = new Pool({ ...connection, max: 4, idleTimeoutMillis: 30000, connectionTimeoutMillis: 5000 });
      this.pool.on('error', err => logger.warn(`TimescaleHistory pool error: ${err.message}`));
      this.poolKey = key;
    }
    this.table = table;
    this.origin = resolved.origin;
    return this.pool;
  }

  async close() {
    if (this.pool) {
      const pool = this.pool;
      this.pool = null;
      this.poolKey = null;
      await pool.end().catch(() => {});
    }
  }

  async status(engine) {
    const resolved = await this.resolveConfig(engine);
    if (!resolved) {
      return { available: false, configured: false, reason: 'TimescaleDB is not configured' };
    }
    const info = engine?.dataStore?.getStorageInfo ? engine.dataStore.getStorageInfo() : null;
    const activeType = info?.type || 'memory';
    try {
      const pool = await this.getPool(engine);
      const result = await pool.query(
        `SELECT count(DISTINCT source_id)::int AS sources, min(time) AS oldest, max(time) AS newest
         FROM ${this.table}`
      );
      const row = result.rows[0];
      return {
        available: true,
        configured: true,
        origin: resolved.origin,
        table: this.table,
        recording: activeType === 'timescaledb' || activeType === 'timescale',
        activeStorage: activeType,
        storageFallback: info?.fallback || null,
        sources: row.sources,
        oldest: row.oldest,
        newest: row.newest
      };
    } catch (error) {
      return {
        available: false,
        configured: true,
        origin: resolved.origin,
        activeStorage: activeType,
        reason: error.code === '42P01'
          ? `Table '${this.table}' does not exist yet (no data recorded)`
          : error.message || error.code || (error.errors && error.errors[0] && (error.errors[0].message || error.errors[0].code)) || String(error)
      };
    }
  }

  async sources(engine, { startTime, endTime } = {}) {
    const pool = await this.getPool(engine);
    const params = [];
    const where = [];
    if (startTime) { params.push(parseTime(startTime)); where.push(`time >= $${params.length}`); }
    if (endTime) { params.push(parseTime(endTime)); where.push(`time <= $${params.length}`); }

    const result = await pool.query(
      `SELECT source_id, count(*)::bigint AS records, min(time) AS first, max(time) AS last
       FROM ${this.table}
       ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
       GROUP BY source_id
       ORDER BY source_id`,
      params
    );
    return result.rows.map(r => ({ sourceId: r.source_id, records: Number(r.records), first: r.first, last: r.last }));
  }

  /**
   * Distinct measurements of a source (sampled from the most recent rows in the range).
   */
  async measurements(engine, { sourceId, startTime, endTime, sample = 2000 }) {
    const pool = await this.getPool(engine);
    const end = parseTime(endTime, new Date());
    const start = parseTime(startTime, new Date(end.getTime() - 24 * 3600 * 1000));

    const result = await pool.query(
      `WITH recent AS (
         SELECT value FROM ${this.table}
         WHERE source_id = $1 AND time >= $2 AND time <= $3
         ORDER BY time DESC
         LIMIT $4
       )
       SELECT m->>'id' AS id,
              max(m->>'type') AS type,
              max(jsonb_typeof(m->'value')) AS json_type,
              max(recent.value->'data'->>'id') AS device
       FROM recent, jsonb_array_elements(${MEASUREMENTS_SQL}) AS m
       WHERE m ? 'id'
       GROUP BY m->>'id'
       ORDER BY m->>'id'`,
      [sourceId, start, end, Math.min(Math.max(parseInt(sample, 10) || 2000, 1), 20000)]
    );
    return result.rows.map(r => ({
      id: r.id,
      type: r.type,
      device: r.device,
      numeric: r.json_type === 'number' || r.json_type === 'boolean'
    }));
  }

  /**
   * Downsampled time series (avg/min/max/last per bucket) for numeric/boolean measurements.
   */
  async series(engine, { sourceId, measurements, startTime, endTime, maxPoints = 500, bucket }) {
    const pool = await this.getPool(engine);
    const end = parseTime(endTime, new Date());
    const start = parseTime(startTime, new Date(end.getTime() - 3600 * 1000));
    if (start >= end) {
      const error = new Error('startTime must be before endTime');
      error.status = 400;
      throw error;
    }

    const ids = (Array.isArray(measurements) ? measurements : String(measurements || '').split(','))
      .map(s => s.trim()).filter(Boolean).slice(0, 20);
    if (!ids.length) {
      const error = new Error('At least one measurement is required');
      error.status = 400;
      throw error;
    }

    const points = Math.min(Math.max(parseInt(maxPoints, 10) || 500, 10), 5000);
    const bucketInterval = bucket && BUCKETS.some(([, b]) => b === bucket) ? bucket : chooseBucket(start, end, points);

    const result = await pool.query(
      `SELECT time_bucket($5::interval, time) AS bucket,
              m->>'id' AS measurement,
              avg(${NUMERIC_VALUE_SQL}) AS avg,
              min(${NUMERIC_VALUE_SQL}) AS min,
              max(${NUMERIC_VALUE_SQL}) AS max,
              last(${NUMERIC_VALUE_SQL}, time) AS last,
              count(*)::int AS samples
       FROM ${this.table}, jsonb_array_elements(${MEASUREMENTS_SQL}) AS m
       WHERE source_id = $1 AND time >= $2 AND time <= $3 AND m->>'id' = ANY($4)
       GROUP BY bucket, measurement
       ORDER BY bucket`,
      [sourceId, start, end, ids, bucketInterval]
    );

    const series = {};
    for (const id of ids) series[id] = [];
    for (const row of result.rows) {
      if (row.avg === null) continue;
      series[row.measurement].push({
        t: row.bucket,
        avg: row.avg,
        min: row.min,
        max: row.max,
        last: row.last,
        samples: row.samples
      });
    }

    return { sourceId, startTime: start, endTime: end, bucket: bucketInterval, series };
  }

  /**
   * Raw records, newest first, keyset-friendly pagination via limit/offset.
   */
  async records(engine, { sourceId, startTime, endTime, limit = 100, offset = 0, order = 'desc' }) {
    const pool = await this.getPool(engine);
    const end = parseTime(endTime, new Date());
    const start = parseTime(startTime, new Date(end.getTime() - 3600 * 1000));
    const lim = Math.min(Math.max(parseInt(limit, 10) || 100, 1), 1000);
    const off = Math.max(parseInt(offset, 10) || 0, 0);
    const dir = String(order).toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    const params = [start, end];
    let sourceFilter = '';
    if (sourceId) {
      params.push(sourceId);
      sourceFilter = `AND source_id = $${params.length}`;
    }
    params.push(lim + 1, off);

    const result = await pool.query(
      `SELECT time, source_id, value
       FROM ${this.table}
       WHERE time >= $1 AND time <= $2 ${sourceFilter}
       ORDER BY time ${dir}
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );

    const rows = result.rows.slice(0, lim).map(r => ({
      timestamp: r.time,
      sourceId: r.source_id,
      data: r.value && r.value.data !== undefined ? r.value.data : r.value
    }));

    return { startTime: start, endTime: end, limit: lim, offset: off, hasMore: result.rows.length > lim, records: rows };
  }
}

module.exports = new TimescaleHistory();
module.exports.TimescaleHistory = TimescaleHistory;
module.exports.chooseBucket = chooseBucket;
