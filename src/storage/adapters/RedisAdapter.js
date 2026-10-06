const Redis = require('ioredis');
const BaseStorageAdapter = require('../BaseStorageAdapter');
const logger = require('../../utils/logger');

class RedisAdapter extends BaseStorageAdapter {
  constructor(config) {
    super(config);
    this.client = null;
    this.keyPrefix = config.keyPrefix || 'udc:';
    this.dataKey = `${this.keyPrefix}data`;
    this.indexKey = `${this.keyPrefix}index`;
    this.maxEntries = config.maxEntries || 10000;
    this.ttl = config.ttl || null; // Time to live in seconds
    this.validateConfig();
  }

  validateConfig() {
    super.validateConfig();
    if (!this.config.host && !this.config.url) {
      throw new Error('Redis adapter requires either "host" or "url" in configuration');
    }
  }

  async initialize() {
    await super.initialize();
    
    // Create Redis client
    const options = {
      port: this.config.port || 6379,
      password: this.config.password,
      db: this.config.database || 0,
      connectTimeout: this.config.connectTimeout || 10000,
      commandTimeout: this.config.commandTimeout || 5000,
      retryDelayOnFailover: 100,
      maxRetriesPerRequest: 3,
      ...this.config.options
    };

    if (this.config.url) {
      this.client = new Redis(this.config.url, options);
    } else {
      this.client = new Redis({
        host: this.config.host,
        ...options
      });
    }

    // Setup event handlers
    this.client.on('connect', () => {
      logger.debug('Redis client connected');
    });

    this.client.on('ready', () => {
      logger.debug('Redis client ready');
    });

    this.client.on('error', (error) => {
      logger.error('Redis client error:', error);
      this.onError(error);
    });

    this.client.on('close', () => {
      logger.debug('Redis client disconnected');
      this.onDisconnected();
    });

    logger.debug('Redis adapter initialized');
  }

  async connect() {
    try {
      if (!this.client) {
        await this.initialize();
      }
      // Test connection
      await this.client.ping();
      
      this.onConnected();
      return true;
      
    } catch (error) {
      logger.error('Failed to connect to Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.client) {
        await this.client.quit();
        this.client = null;
      }
      this.onDisconnected();
      return true;
    } catch (error) {
      logger.error('Error disconnecting from Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async store(data) {
    try {
      const record = this.createDataRecord(data);
      const recordKey = `${this.keyPrefix}record:${record.id}`;
      
      // Store the record
      const recordData = JSON.stringify(record);
      
      // Use pipeline for atomic operations
      const pipeline = this.client.pipeline();
      
      // Store the record with TTL if configured
      if (this.ttl) {
        pipeline.setex(recordKey, this.ttl, recordData);
      } else {
        pipeline.set(recordKey, recordData);
      }
      
      // Add to sorted set for time-based queries (score = timestamp)
      const timestamp = new Date(record.timestamp).getTime();
      pipeline.zadd(this.dataKey, timestamp, record.id);
      
      // Add to source index
      const sourceKey = `${this.keyPrefix}source:${record.sourceId}`;
      pipeline.zadd(sourceKey, timestamp, record.id);
      
      // Maintain max entries limit
      pipeline.zremrangebyrank(this.dataKey, 0, -(this.maxEntries + 1));
      pipeline.zremrangebyrank(sourceKey, 0, -(this.maxEntries + 1));
      
      await pipeline.exec();
      
      this.onWrite();
      return record.id;
      
    } catch (error) {
      logger.error('Error storing data in Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async query(criteria) {
    try {
      let recordIds = [];
      
      if (criteria.sourceId) {
        // Query by source
        const sourceKey = `${this.keyPrefix}source:${criteria.sourceId}`;
        recordIds = await this.client.zrevrange(sourceKey, 0, (criteria.limit || 100) - 1);
      } else if (criteria.startTime && criteria.endTime) {
        // Query by time range
        const start = new Date(criteria.startTime).getTime();
        const end = new Date(criteria.endTime).getTime();
        recordIds = await this.client.zrevrangebyscore(this.dataKey, end, start);
      } else {
        // Get latest records
        recordIds = await this.client.zrevrange(this.dataKey, 0, (criteria.limit || 100) - 1);
      }
      
      if (recordIds.length === 0) {
        return [];
      }
      
      // Get record data
      const recordKeys = recordIds.map(id => `${this.keyPrefix}record:${id}`);
      const recordData = await this.client.mget(...recordKeys);
      
      this.onRead();
      
      return recordData
        .filter(data => data !== null)
        .map(data => JSON.parse(data));

    } catch (error) {
      logger.error('Error querying Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async getLatest(limit = 100) {
    return this.query({ limit });
  }

  async getBySource(sourceId, limit = 100) {
    return this.query({ sourceId, limit });
  }

  async getByTimeRange(startTime, endTime) {
    return this.query({ startTime, endTime });
  }

  async search(query) {
    try {
      // Redis doesn't have full-text search built-in, so we'll do a simple pattern match
      // For more advanced search, consider using RediSearch module
      
      const allIds = await this.client.zrevrange(this.dataKey, 0, -1);
      const recordKeys = allIds.map(id => `${this.keyPrefix}record:${id}`);
      
      if (recordKeys.length === 0) {
        return [];
      }
      
      const recordData = await this.client.mget(...recordKeys);
      this.onRead();
      
      const results = [];
      const queryLower = query.toLowerCase();
      
      for (const data of recordData) {
        if (data !== null) {
          const record = JSON.parse(data);
          const recordStr = JSON.stringify(record).toLowerCase();
          
          if (recordStr.includes(queryLower)) {
            results.push(record);
          }
        }
      }
      
      return results.slice(0, 100); // Limit results

    } catch (error) {
      logger.error('Error searching Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async clear() {
    try {
      // Get all keys with our prefix
      const keys = await this.client.keys(`${this.keyPrefix}*`);
      
      if (keys.length === 0) {
        return 0;
      }
      
      // Delete all keys
      await this.client.del(...keys);
      
      logger.info(`Cleared ${keys.length} keys from Redis with prefix ${this.keyPrefix}`);
      return keys.length;
      
    } catch (error) {
      logger.error('Error clearing Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async getStats() {
    try {
      const baseStats = await super.getStats();
      
      const totalRecords = await this.client.zcard(this.dataKey);
      const info = await this.client.info('memory');
      const keyspaceInfo = await this.client.info('keyspace');
      
      // Parse memory info
      const memoryUsed = info.match(/used_memory:(\d+)/);
      const memoryUsedMB = memoryUsed ? 
        Math.round(parseInt(memoryUsed[1]) / 1024 / 1024 * 100) / 100 : 0;

      // Count keys with our prefix
      const prefixKeys = await this.client.eval(`
        return #redis.call('keys', ARGV[1])
      `, 0, `${this.keyPrefix}*`);

      return {
        ...baseStats,
        storage: {
          totalRecords: totalRecords,
          totalKeys: prefixKeys,
          memoryUsedMB: memoryUsedMB,
          maxEntries: this.maxEntries,
          ttl: this.ttl,
          keyPrefix: this.keyPrefix
        }
      };

    } catch (error) {
      logger.error('Error getting Redis stats:', error);
      const baseStats = await super.getStats();
      return { ...baseStats, storage: { error: error.message } };
    }
  }

  async healthCheck() {
    try {
      const pong = await this.client.ping();
      const info = await this.client.info('server');
      
      // Parse server info
      const version = info.match(/redis_version:([^\r\n]+)/);
      const uptime = info.match(/uptime_in_seconds:(\d+)/);
      
      const baseHealth = await super.healthCheck();
      
      return {
        ...baseHealth,
        details: {
          ping: pong,
          version: version ? version[1] : 'unknown',
          uptime: uptime ? parseInt(uptime[1]) : 0,
          database: this.config.database || 0,
          host: this.config.host,
          port: this.config.port || 6379
        }
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        type: this.type,
        lastCheck: new Date().toISOString(),
        error: error.message,
        details: {
          database: this.config.database || 0,
          host: this.config.host,
          port: this.config.port || 6379
        }
      };
    }
  }

  // Redis-specific methods
  async expire(key, seconds) {
    try {
      return await this.client.expire(key, seconds);
    } catch (error) {
      logger.error('Error setting expiration in Redis:', error);
      throw error;
    }
  }

  async getByPattern(pattern) {
    try {
      const keys = await this.client.keys(pattern);
      if (keys.length === 0) {
        return [];
      }
      
      const values = await this.client.mget(...keys);
      this.onRead();
      
      return values
        .filter(value => value !== null)
        .map(value => JSON.parse(value));
        
    } catch (error) {
      logger.error('Error getting data by pattern from Redis:', error);
      this.onError(error);
      throw error;
    }
  }

  async getSourceStats() {
    try {
      const sourceKeys = await this.client.keys(`${this.keyPrefix}source:*`);
      const stats = {};
      
      for (const key of sourceKeys) {
        const sourceId = key.replace(`${this.keyPrefix}source:`, '');
        const count = await this.client.zcard(key);
        stats[sourceId] = count;
      }
      
      return stats;
    } catch (error) {
      logger.error('Error getting source stats from Redis:', error);
      throw error;
    }
  }
}

module.exports = RedisAdapter;
