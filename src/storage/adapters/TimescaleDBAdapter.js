const BaseStorageAdapter = require('../BaseStorageAdapter');
const { Client } = require('pg');
const logger = require('../../utils/logger');

/**
 * TimescaleDB Storage Adapter
 * 
 * TimescaleDB is an extension of PostgreSQL optimized for time-series data.
 * It provides automatic partitioning (hypertables), compression, and advanced
 * time-series analytics capabilities.
 * 
 * Features:
 * - Automatic time-based partitioning (hypertables)
 * - Data compression for older data
 * - Continuous aggregates for real-time rollups
 * - Time-series specific functions
 * - Full PostgreSQL compatibility
 */
class TimescaleDBAdapter extends BaseStorageAdapter {
  constructor(config) {
    super({ type: 'timescaledb', ...config });
    
    this.client = null;
    this.table = config.table || 'sensor_data';
    this.hypertable = config.hypertable !== false; // Enable by default
    this.chunkTimeInterval = config.chunkTimeInterval || '1 day';
    this.compressionEnabled = config.compression || false;
    this.compressionAfter = config.compressionAfter || '7 days';
    this.retentionPolicy = config.retentionPolicy || null; // e.g., '30 days'
  }

  async connect() {
    try {
      this.client = new Client({
        host: this.config.host || 'localhost',
        port: this.config.port || 5432,
        database: this.config.database,
        user: this.config.username || this.config.user,
        password: this.config.password,
        ...this.config.options
      });

      await this.client.connect();
      
      // Check if TimescaleDB extension is available
      const extensionCheck = await this.client.query(
        "SELECT * FROM pg_extension WHERE extname = 'timescaledb'"
      );
      
      if (extensionCheck.rows.length === 0) {
        logger.warn('TimescaleDB extension not found, creating...');
        await this.client.query('CREATE EXTENSION IF NOT EXISTS timescaledb');
      }
      
      await this.initializeSchema();
      
      this.connected = true;
      this.isConnected = true;
      logger.info(`Connected to TimescaleDB at ${this.config.host}:${this.config.port}`);
      
      return true;
    } catch (error) {
      logger.error('Failed to connect to TimescaleDB:', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.client) {
      await this.client.end();
      this.client = null;
      this.connected = false;
      this.isConnected = false;
      logger.info('Disconnected from TimescaleDB');
    }
  }

  async initializeSchema() {
    try {
      // Create regular table first
      await this.client.query(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          id SERIAL,
          time TIMESTAMPTZ NOT NULL,
          source_id VARCHAR(255) NOT NULL,
          data_type VARCHAR(100),
          value JSONB NOT NULL,
          metadata JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      // Convert to hypertable if enabled
      if (this.hypertable) {
        try {
          await this.client.query(`
            SELECT create_hypertable(
              '${this.table}', 
              'time',
              chunk_time_interval => INTERVAL '${this.chunkTimeInterval}',
              if_not_exists => TRUE
            )
          `);
          logger.info(`Hypertable created for ${this.table} with chunk interval ${this.chunkTimeInterval}`);
        } catch (error) {
          // Hypertable might already exist
          if (!error.message.includes('already a hypertable')) {
            logger.warn('Could not create hypertable:', error.message);
          }
        }
      }

      // Enable compression if configured
      if (this.compressionEnabled) {
        try {
          await this.client.query(`
            ALTER TABLE ${this.table} SET (
              timescaledb.compress,
              timescaledb.compress_segmentby = 'source_id',
              timescaledb.compress_orderby = 'time DESC'
            )
          `);
          
          await this.client.query(`
            SELECT add_compression_policy('${this.table}', INTERVAL '${this.compressionAfter}')
          `);
          
          logger.info(`Compression enabled for ${this.table} after ${this.compressionAfter}`);
        } catch (error) {
          logger.warn('Could not enable compression:', error.message);
        }
      }

      // Set retention policy if configured
      if (this.retentionPolicy) {
        try {
          await this.client.query(`
            SELECT add_retention_policy('${this.table}', INTERVAL '${this.retentionPolicy}')
          `);
          logger.info(`Retention policy set to ${this.retentionPolicy}`);
        } catch (error) {
          logger.warn('Could not set retention policy:', error.message);
        }
      }

      // Create indexes for better query performance
      await this.client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.table}_source_time 
        ON ${this.table} (source_id, time DESC)
      `);

      await this.client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.table}_data_type 
        ON ${this.table} (data_type, time DESC)
      `);

      logger.info(`TimescaleDB schema initialized for table ${this.table}`);
    } catch (error) {
      logger.error('Failed to initialize TimescaleDB schema:', error);
      throw error;
    }
  }

  async store(data) {
    if (!this.connected) {
      throw new Error('Not connected to TimescaleDB');
    }

    try {
      const timestamp = data.timestamp || new Date().toISOString();
      
      const query = `
        INSERT INTO ${this.table} (time, source_id, data_type, value, metadata)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
      `;

      const values = [
        timestamp,
        data.sourceId || 'unknown',
        data.type || null,
        JSON.stringify(data.value || data),
        data.metadata ? JSON.stringify(data.metadata) : null
      ];

      const result = await this.client.query(query, values);
      
      return {
        success: true,
        id: result.rows[0].id,
        timestamp
      };
    } catch (error) {
      logger.error('Failed to store data in TimescaleDB:', error);
      throw error;
    }
  }

  async retrieve(query = {}) {
    if (!this.connected) {
      throw new Error('Not connected to TimescaleDB');
    }

    try {
      const conditions = [];
      const values = [];
      let paramCount = 1;

      if (query.sourceId) {
        conditions.push(`source_id = $${paramCount++}`);
        values.push(query.sourceId);
      }

      if (query.type) {
        conditions.push(`data_type = $${paramCount++}`);
        values.push(query.type);
      }

      if (query.startTime) {
        conditions.push(`time >= $${paramCount++}`);
        values.push(query.startTime);
      }

      if (query.endTime) {
        conditions.push(`time <= $${paramCount++}`);
        values.push(query.endTime);
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      const limit = Math.min(Math.max(parseInt(query.limit, 10) || 100, 1), 10000);
      const offset = Math.max(parseInt(query.offset, 10) || 0, 0);

      const sql = `
        SELECT id, time, source_id, data_type, value, metadata, created_at
        FROM ${this.table}
        ${whereClause}
        ORDER BY time DESC
        LIMIT ${limit} OFFSET ${offset}
      `;

      const result = await this.client.query(sql, values);

      return result.rows.map(row => ({
        id: row.id,
        timestamp: row.time,
        sourceId: row.source_id,
        type: row.data_type,
        value: row.value,
        metadata: row.metadata,
        createdAt: row.created_at
      }));
    } catch (error) {
      logger.error('Failed to retrieve data from TimescaleDB:', error);
      throw error;
    }
  }

  // Standard read interface (BaseStorageAdapter) — returns data points in the
  // same shape they were stored with: { id, sourceId, timestamp, data, ... }
  toDataPoint(row) {
    const value = row.value && typeof row.value === 'object' && !Array.isArray(row.value) ? row.value : { data: row.value };
    return {
      ...value,
      id: String(row.id),
      sourceId: value.sourceId || row.sourceId,
      timestamp: row.timestamp instanceof Date ? row.timestamp.toISOString() : row.timestamp
    };
  }

  async query(criteria = {}) {
    const rows = await this.retrieve({
      sourceId: criteria.sourceId,
      startTime: criteria.startTime,
      endTime: criteria.endTime,
      limit: criteria.limit || 100,
      offset: criteria.offset || 0
    });
    return rows.map(row => this.toDataPoint(row));
  }

  async getLatest(limit = 100) {
    return this.query({ limit });
  }

  async getBySource(sourceId, limit = 100) {
    return this.query({ sourceId, limit });
  }

  async getByTimeRange(startTime, endTime, limit = 10000) {
    return this.query({ startTime, endTime, limit });
  }

  async search(text, limit = 100) {
    if (!this.connected) {
      throw new Error('Not connected to TimescaleDB');
    }
    const result = await this.client.query(
      `SELECT id, time, source_id, value FROM ${this.table}
       WHERE source_id ILIKE $1 OR value::text ILIKE $1
       ORDER BY time DESC LIMIT $2`,
      [`%${String(text).replace(/[\\%_]/g, '\\$&')}%`, Math.min(Math.max(parseInt(limit, 10) || 100, 1), 1000)]
    );
    return result.rows.map(row => this.toDataPoint({ id: row.id, timestamp: row.time, sourceId: row.source_id, value: row.value }));
  }

  async getStats() {
    if (!this.connected) {
      return null;
    }

    try {
      // Get hypertable stats
      const hypertableStats = await this.client.query(`
        SELECT * FROM timescaledb_information.hypertables 
        WHERE hypertable_name = $1
      `, [this.table]);

      // Get chunk stats
      const chunkStats = await this.client.query(`
        SELECT 
          COUNT(*) as chunk_count,
          pg_size_pretty(SUM(total_bytes)) as total_size,
          pg_size_pretty(SUM(compressed_total_bytes)) as compressed_size
        FROM timescaledb_information.chunks
        WHERE hypertable_name = $1
      `, [this.table]);

      // Get data count and time range
      const dataStats = await this.client.query(`
        SELECT 
          COUNT(*) as total_records,
          MIN(time) as oldest_record,
          MAX(time) as newest_record,
          COUNT(DISTINCT source_id) as unique_sources
        FROM ${this.table}
      `);

      return {
        type: 'timescaledb',
        connected: this.connected,
        table: this.table,
        hypertable: hypertableStats.rows.length > 0,
        chunks: chunkStats.rows[0],
        data: dataStats.rows[0],
        compression: this.compressionEnabled,
        retentionPolicy: this.retentionPolicy
      };
    } catch (error) {
      logger.error('Failed to get TimescaleDB stats:', error);
      return { type: 'timescaledb', connected: this.connected, error: error.message };
    }
  }

  /**
   * TimescaleDB-specific: Get time-bucketed aggregates
   */
  async getAggregates(options = {}) {
    if (!this.connected) {
      throw new Error('Not connected to TimescaleDB');
    }

    const {
      sourceId,
      bucket = '1 hour',
      startTime,
      endTime,
      aggregateFunction = 'avg'
    } = options;

    try {
      const conditions = [];
      const values = [];
      let paramCount = 1;

      if (sourceId) {
        conditions.push(`source_id = $${paramCount++}`);
        values.push(sourceId);
      }

      if (startTime) {
        conditions.push(`time >= $${paramCount++}`);
        values.push(startTime);
      }

      if (endTime) {
        conditions.push(`time <= $${paramCount++}`);
        values.push(endTime);
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      const fn = ['avg', 'min', 'max', 'sum', 'count'].includes(String(aggregateFunction).toLowerCase())
        ? String(aggregateFunction).toLowerCase()
        : 'avg';
      values.push(bucket);

      const query = `
        SELECT 
          time_bucket($${paramCount}::interval, time) AS bucket,
          source_id,
          ${fn}((value->>'value')::float) AS aggregate_value,
          COUNT(*) AS count
        FROM ${this.table}
        ${whereClause}
        GROUP BY bucket, source_id
        ORDER BY bucket DESC
      `;

      const result = await this.client.query(query, values);
      return result.rows;
    } catch (error) {
      logger.error('Failed to get aggregates from TimescaleDB:', error);
      throw error;
    }
  }

  async clear() {
    if (!this.connected) {
      throw new Error('Not connected to TimescaleDB');
    }

    try {
      await this.client.query(`TRUNCATE TABLE ${this.table}`);
      logger.info(`Cleared all data from TimescaleDB table ${this.table}`);
      return { success: true };
    } catch (error) {
      logger.error('Failed to clear TimescaleDB data:', error);
      throw error;
    }
  }
}

module.exports = TimescaleDBAdapter;
