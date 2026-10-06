const fs = require('fs').promises;
const path = require('path');
const Joi = require('joi');
const logger = require('../utils/logger');

/**
 * StorageConfigManager - loads, validates and saves config/storage.json
 *
 * File format:
 * {
 *   "storage":      { "type": "timescaledb", "config": { ... } },   // active storage
 *   "alternatives": { "redis": { "type": "redis", "config": { ... } } }   // optional presets
 * }
 *
 * Supported types are the ones StorageFactory can instantiate: memory, redis, timescaledb.
 */

const TYPE_ALIASES = { timescale: 'timescaledb' };
const SUPPORTED_TYPES = ['memory', 'redis', 'timescaledb'];

const storageConfigSchema = Joi.object({
  storage: Joi.object({
    type: Joi.string().required(),
    config: Joi.object().default({})
  }).required(),
  alternatives: Joi.object().pattern(Joi.string(), Joi.object({
    type: Joi.string().required(),
    config: Joi.object().default({})
  }).unknown(true)).optional()
}).unknown(true);

const memoryConfigSchema = Joi.object({
  maxDataPoints: Joi.number().integer().min(100).default(10000)
}).rename('maxRecords', 'maxDataPoints', { ignoreUndefined: true, override: true }).unknown(true);

const redisConfigSchema = Joi.object({
  url: Joi.string().optional(),
  host: Joi.string().when('url', { is: Joi.exist(), then: Joi.optional(), otherwise: Joi.required() }),
  port: Joi.number().integer().min(1).max(65535).default(6379),
  database: Joi.number().integer().min(0).default(0),
  password: Joi.string().allow('').optional(),
  keyPrefix: Joi.string().default('udc:'),
  maxEntries: Joi.number().integer().min(100).default(10000),
  ttl: Joi.number().integer().min(60).optional(),
  connectTimeout: Joi.number().integer().min(1000).default(10000),
  commandTimeout: Joi.number().integer().min(1000).default(5000),
  options: Joi.object().default({})
}).rename('db', 'database', { ignoreUndefined: true, override: true }).unknown(true);

const timescaleConfigSchema = Joi.object({
  host: Joi.string().required(),
  port: Joi.number().integer().min(1).max(65535).default(5432),
  database: Joi.string().required(),
  username: Joi.string().required(),
  password: Joi.string().allow('').default(''),
  table: Joi.string().pattern(/^[A-Za-z_][A-Za-z0-9_]{0,62}$/).default('sensor_data'),
  hypertable: Joi.boolean().default(true),
  chunkTimeInterval: Joi.string().default('1 day'),
  compression: Joi.boolean().default(false),
  compressionAfter: Joi.string().default('7 days'),
  retentionPolicy: Joi.string().allow(null).optional(),
  options: Joi.object().default({})
}).rename('user', 'username', { ignoreUndefined: true, override: true }).unknown(true);

const SCHEMAS = {
  memory: memoryConfigSchema,
  redis: redisConfigSchema,
  timescaledb: timescaleConfigSchema
};

// Human-readable descriptions for the UI
const TYPE_INFO = {
  memory: {
    name: 'In-Memory Storage',
    description: 'Fast temporary buffer in memory, lost on restart',
    configSchema: {
      maxDataPoints: { type: 'number', default: 10000, description: 'Maximum number of data points kept' }
    }
  },
  redis: {
    name: 'Redis',
    description: 'Key-value store with TTL, survives restarts',
    configSchema: {
      host: { type: 'string', required: true, description: 'Redis host' },
      port: { type: 'number', default: 6379, description: 'Redis port' },
      password: { type: 'string', description: 'Redis password (optional)' },
      database: { type: 'number', default: 0, description: 'Database number' },
      keyPrefix: { type: 'string', default: 'udc:', description: 'Key prefix' },
      ttl: { type: 'number', description: 'TTL in seconds (optional)' },
      maxEntries: { type: 'number', default: 10000, description: 'Maximum entries kept' }
    }
  },
  timescaledb: {
    name: 'TimescaleDB',
    description: 'PostgreSQL time-series database, used for history browsing',
    configSchema: {
      host: { type: 'string', required: true, description: 'Database host' },
      port: { type: 'number', default: 5432, description: 'Database port' },
      database: { type: 'string', required: true, description: 'Database name' },
      username: { type: 'string', required: true, description: 'Database user' },
      password: { type: 'string', description: 'Database password' },
      table: { type: 'string', default: 'sensor_data', description: 'Hypertable name' },
      compression: { type: 'boolean', default: false, description: 'Compress old chunks' },
      compressionAfter: { type: 'string', default: '7 days', description: 'Compress chunks older than' },
      retentionPolicy: { type: 'string', description: 'Drop data older than (e.g. "90 days")' }
    }
  }
};

function normalizeType(type) {
  const t = String(type || '').toLowerCase();
  return TYPE_ALIASES[t] || t;
}

class StorageConfigManager {
  constructor() {
    this.configPath = path.join(process.cwd(), 'config');
    this.storageConfigFile = path.join(this.configPath, 'storage.json');
    this.storageConfig = null;
    this.alternatives = {};
    this.initialized = false;
  }

  async initialize() {
    try {
      await this.ensureConfigDirectory();
      await this.loadStorageConfig();
      this.initialized = true;
      logger.info('Storage configuration manager initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize storage configuration manager:', error);
      throw error;
    }
  }

  async ensureConfigDirectory() {
    await fs.mkdir(this.configPath, { recursive: true });
  }

  async loadStorageConfig() {
    let raw;
    try {
      raw = await fs.readFile(this.storageConfigFile, 'utf8');
    } catch (error) {
      if (error.code === 'ENOENT') {
        logger.info('Storage configuration file not found, creating default configuration');
        await this.createDefaultConfig();
        return;
      }
      throw error;
    }

    const parsed = JSON.parse(raw);
    const { error, value } = storageConfigSchema.validate(parsed);
    if (error) {
      throw new Error(`Invalid storage configuration: ${error.details[0].message}`);
    }

    const normalized = this.normalize(value.storage.type, value.storage.config);
    if (!normalized.valid) {
      throw new Error(`Invalid storage configuration: ${normalized.errors[0].message}`);
    }

    this.storageConfig = { type: normalized.type, config: normalized.config };
    this.alternatives = value.alternatives || {};
    logger.info(`Loaded storage configuration: ${this.storageConfig.type}`);
  }

  /**
   * Validate a type/config pair and apply defaults/renames.
   * @returns {{ valid: boolean, type?: string, config?: Object, errors?: Array }}
   */
  normalize(type, config = {}) {
    const key = normalizeType(type);
    const schema = SCHEMAS[key];
    if (!schema) {
      return {
        valid: false,
        errors: [{ field: 'type', message: `Unsupported storage type: ${type} (supported: ${SUPPORTED_TYPES.join(', ')})` }]
      };
    }

    const { error, value } = schema.validate(config || {}, { abortEarly: false });
    if (error) {
      return {
        valid: false,
        errors: error.details.map(detail => ({
          field: detail.path.join('.'),
          message: detail.message,
          value: detail.context?.value
        }))
      };
    }
    return { valid: true, type: key, config: value };
  }

  async createDefaultConfig() {
    this.storageConfig = { type: 'memory', config: { maxDataPoints: 10000 } };
    this.alternatives = {};
    await this.writeFile();
    logger.info('Created default storage configuration (memory)');
  }

  async writeFile() {
    const content = { storage: this.storageConfig };
    if (this.alternatives && Object.keys(this.alternatives).length) {
      content.alternatives = this.alternatives;
    }
    await fs.writeFile(this.storageConfigFile, JSON.stringify(content, null, 2), 'utf8');
  }

  getStorageConfig() {
    return this.storageConfig;
  }

  /**
   * Save a new active storage configuration (alternatives are preserved).
   */
  async updateStorageConfig(newConfig) {
    const normalized = this.normalize(newConfig?.type, newConfig?.config);
    if (!normalized.valid) {
      throw new Error(`Invalid storage configuration (validation): ${normalized.errors.map(e => e.message).join('; ')}`);
    }

    this.storageConfig = { type: normalized.type, config: normalized.config };
    // Keep the preset for this type in sync so switching back restores it
    this.alternatives = { ...this.alternatives, [normalized.type]: { ...this.storageConfig } };
    await this.writeFile();

    logger.info(`Updated storage configuration: ${this.storageConfig.type}`);
    return this.storageConfig;
  }

  async reloadConfig() {
    logger.info('Reloading storage configuration...');
    await this.loadStorageConfig();
    this.initialized = true;
    logger.info('Storage configuration reloaded successfully');
  }

  isInitialized() {
    return this.initialized;
  }

  getSupportedStorageTypes() {
    return [...SUPPORTED_TYPES];
  }

  getTypeInfo() {
    return SUPPORTED_TYPES.map(type => ({ type, ...TYPE_INFO[type] }));
  }

  getConfigSchema(storageType) {
    const schema = SCHEMAS[normalizeType(storageType)];
    if (!schema) {
      throw new Error(`Unsupported storage type: ${storageType}`);
    }
    return schema.describe();
  }

  async getConfig() {
    if (!this.initialized) {
      await this.initialize();
    }
    return { storage: this.storageConfig, alternatives: this.alternatives };
  }

  /**
   * Non-destructive connection test: connect, health check, disconnect.
   * (Never writes or clears data in the target storage.)
   */
  async testConnection(type, config) {
    const startTime = Date.now();
    const normalized = this.normalize(type, config);
    if (!normalized.valid) {
      return {
        success: false,
        message: `Invalid configuration: ${normalized.errors.map(e => e.message).join('; ')}`,
        responseTime: 0,
        details: { errors: normalized.errors }
      };
    }

    const StorageFactory = require('../storage/StorageFactory');
    let adapter = null;
    try {
      adapter = StorageFactory.create(normalized.type, normalized.config);
      await adapter.initialize();
      await adapter.connect();
      const health = await adapter.healthCheck();
      const responseTime = Date.now() - startTime;
      return {
        success: true,
        message: 'Connection test successful',
        responseTime,
        details: { canConnect: true, health }
      };
    } catch (error) {
      return {
        success: false,
        message: `Connection test failed: ${error.message}`,
        responseTime: Date.now() - startTime,
        details: { canConnect: false, error: error.message }
      };
    } finally {
      if (adapter) {
        await Promise.resolve(adapter.disconnect()).catch(() => {});
      }
    }
  }

  /**
   * Health of the storage the engine is actually using (live adapter, no new connection).
   */
  async getStorageHealth(engine) {
    const lastCheck = new Date().toISOString();
    const dataStore = engine?.dataStore;
    if (!dataStore) {
      return {
        type: this.storageConfig?.type || 'unknown',
        configuredType: this.storageConfig?.type || 'unknown',
        status: 'unavailable',
        connected: false,
        health: { responsive: false, error: 'Engine not available', lastCheck },
        statistics: null,
        lastCheck
      };
    }

    const info = dataStore.getStorageInfo();
    let health = null;
    let statistics = null;
    try {
      if (dataStore.useExternalStorage && dataStore.storageAdapter) {
        health = await dataStore.storageAdapter.healthCheck();
        statistics = await dataStore.storageAdapter.getStats();
      } else {
        statistics = dataStore.getStats();
      }
    } catch (error) {
      health = { status: 'unhealthy', error: error.message };
    }

    const healthy = info.status === 'connected' && (!health || health.status !== 'unhealthy');
    return {
      type: info.type,
      configuredType: info.configuredType,
      status: info.status === 'fallback' ? 'fallback' : healthy ? 'healthy' : 'unhealthy',
      connected: healthy,
      fallback: info.fallback,
      health: { responsive: healthy, ...(health || {}), lastCheck },
      statistics,
      lastCheck
    };
  }

  async validateConfig(type, config) {
    const normalized = this.normalize(type, config);
    if (!normalized.valid) {
      return { valid: false, errors: normalized.errors };
    }
    return { valid: true, type: normalized.type, config: normalized.config };
  }
}

module.exports = new StorageConfigManager();
module.exports.StorageConfigManager = StorageConfigManager;
