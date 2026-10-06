const logger = require('../utils/logger');

// Lazy loaders — avoids loading ioredis/pg at startup when only 'memory' is used
const storageLoaders = {
  'memory':      () => require('./adapters/MemoryStorageAdapter'),
  'redis':       () => require('./adapters/RedisAdapter'),
  'timescaledb': () => require('./adapters/TimescaleDBAdapter'),
  'timescale':   () => require('./adapters/TimescaleDBAdapter'),
};
const storageCache = {};

class StorageFactory {

  static create(type, config) {
    const key = type.toLowerCase();
    const loader = storageLoaders[key];

    if (!loader) {
      throw new Error(`Unsupported storage type: ${type}`);
    }

    try {
      if (!storageCache[key]) storageCache[key] = loader();
      const StorageAdapterClass = storageCache[key];
      // Adapters read their type from config.type (BaseStorageAdapter.validateConfig)
      const adapter = new StorageAdapterClass({ ...(config || {}), type: key === 'timescale' ? 'timescaledb' : key });
      logger.debug(`Created ${type} storage adapter`);
      return adapter;
    } catch (error) {
      logger.error(`Failed to create ${type} storage adapter:`, error);
      throw error;
    }
  }

  static getSupportedTypes() {
    return Object.keys(storageLoaders);
  }

  static registerStorageType(type, StorageAdapterClass) {
    storageLoaders[type.toLowerCase()] = () => StorageAdapterClass;
    logger.info(`Registered custom storage adapter: ${type}`);
  }

  static isTypeSupported(type) {
    return type.toLowerCase() in storageLoaders;
  }
}

module.exports = StorageFactory;
