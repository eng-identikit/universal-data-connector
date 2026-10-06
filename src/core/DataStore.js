const logger = require('../utils/logger');
const StorageFactory = require('../storage/StorageFactory');

// Node network errors (e.g. AggregateError ECONNREFUSED) can have an empty message
function describeError(error) {
  if (!error) return 'unknown error';
  if (error.message) return error.message;
  if (error.code) return error.code;
  if (Array.isArray(error.errors) && error.errors.length) return describeError(error.errors[0]);
  return String(error);
}

class DataStore {
  /**
   * @param {Object|null} storageConfig - { type, config }
   * @param {Object} options
   * @param {boolean} options.fallbackToMemory - if the external storage is unreachable at
   *   startup, keep running in memory and retry periodically (default true). When false,
   *   initialize() throws (used when switching storage at runtime).
   * @param {number} options.retryInterval - ms between reconnection attempts (default 30s)
   */
  constructor(storageConfig = null, options = {}) {
    // Legacy in-memory storage as fallback
    this.data = [];
    this.maxDataPoints = (storageConfig?.type === 'memory' && storageConfig.config?.maxDataPoints) ||
      parseInt(process.env.MAX_DATA_POINTS) || 10000;
    this.retentionDays = parseInt(process.env.DATA_RETENTION_DAYS) || 7;
    this.initialized = false;
    this.cleanupInterval = null;
    
    // Storage adapter configuration
    this.storageConfig = storageConfig || {
      type: 'memory',
      config: {}
    };
    this.storageAdapter = null;
    this.useExternalStorage = false;
    this.wantsExternalStorage = !!storageConfig && storageConfig.type !== 'memory';

    this.fallbackToMemory = options.fallbackToMemory !== false;
    this.retryInterval = options.retryInterval || parseInt(process.env.STORAGE_RETRY_INTERVAL) || 30000;
    this.retryTimer = null;
    this.fallback = null; // { reason, since } while running in memory instead of the configured storage
  }

  async initialize() {
    try {
      if (this.wantsExternalStorage) {
        try {
          await this.connectExternal();
          logger.info(`Data store initialized with ${this.storageConfig.type} storage adapter`);
        } catch (error) {
          if (!this.fallbackToMemory) throw error;
          this.fallback = { reason: describeError(error), since: new Date().toISOString() };
          logger.error(
            `${this.storageConfig.type} storage unavailable (${describeError(error)}): using in-memory storage, ` +
            `retrying every ${Math.round(this.retryInterval / 1000)}s`
          );
          this.startRetry();
        }
      } else {
        logger.info(`Data store initialized with in-memory storage (max ${this.maxDataPoints} data points, ${this.retentionDays} days retention)`);
      }
      
      // Start cleanup routine
      this.startCleanupRoutine();
      
      this.initialized = true;
    } catch (error) {
      logger.error('Failed to initialize data store:', error);
      throw error;
    }
  }

  async connectExternal() {
    const adapter = StorageFactory.create(this.storageConfig.type, this.storageConfig.config || {});
    try {
      await adapter.initialize();
      await adapter.connect();
    } catch (error) {
      await Promise.resolve(adapter.disconnect && adapter.disconnect()).catch(() => {});
      throw error;
    }
    this.storageAdapter = adapter;
    this.useExternalStorage = true;
  }

  startRetry() {
    if (this.retryTimer) return;
    this.retryTimer = setInterval(() => this.retryExternal(), this.retryInterval);
    if (this.retryTimer.unref) this.retryTimer.unref();
  }

  stopRetry() {
    if (this.retryTimer) {
      clearInterval(this.retryTimer);
      this.retryTimer = null;
    }
  }

  async retryExternal() {
    if (this.retrying || this.useExternalStorage) return;
    this.retrying = true;
    try {
      await this.connectExternal();
      this.stopRetry();
      const since = this.fallback?.since;
      this.fallback = null;
      logger.info(`${this.storageConfig.type} storage is now available, switched from in-memory storage`);

      // Move the data buffered in memory during the outage to the external storage (oldest first)
      const buffered = since ? this.data.filter(item => item.storedAt >= since).reverse() : [];
      let flushed = 0;
      for (const item of buffered) {
        try {
          await this.storageAdapter.store(item);
          flushed++;
        } catch (error) {
          logger.warn(`Could not flush buffered data point to ${this.storageConfig.type}: ${error.message}`);
          break;
        }
      }
      if (flushed) {
        logger.info(`Flushed ${flushed} buffered data points to ${this.storageConfig.type} storage`);
        this.data = this.data.filter(item => !(item.storedAt >= since));
      }
    } catch (error) {
      if (this.fallback) this.fallback.reason = describeError(error);
      logger.debug(`${this.storageConfig.type} storage still unavailable: ${error.message}`);
    } finally {
      this.retrying = false;
    }
  }

  getStorageInfo() {
    return {
      type: this.useExternalStorage ? this.storageConfig.type : 'memory',
      configuredType: this.storageConfig.type,
      status: this.fallback ? 'fallback' : 'connected',
      connected: this.useExternalStorage ? this.storageAdapter?.isConnected !== false : true,
      fallback: this.fallback,
      retryInterval: this.fallback ? this.retryInterval : undefined,
      bufferedDataPoints: this.data.length
    };
  }

  store(data) {
    try {
      // Store in external storage if configured
      if (this.useExternalStorage && this.storageAdapter) {
        return this.storeExternal(data);
      }
      
      // Legacy in-memory storage
      return this.storeMemory(data);
      
    } catch (error) {
      logger.error('Error storing data:', error);
      throw error;
    }
  }

  async storeExternal(data) {
    try {
      const id = await this.storageAdapter.store(data);
      
      logger.debug(`Stored data point from source '${data.sourceId}' in ${this.storageConfig.type} storage`, {
        dataId: id
      });

      return id;
    } catch (error) {
      logger.error(`Error storing data in ${this.storageConfig.type} storage:`, error);
      
      // Fallback to memory storage if external storage fails
      logger.warn('Falling back to in-memory storage');
      return this.storeMemory(data);
    }
  }

  storeMemory(data) {
    // Add unique ID and storage timestamp
    const dataPoint = {
      id: this.generateId(),
      storedAt: new Date().toISOString(),
      ...data
    };

    // Add to beginning of array (most recent first)
    this.data.unshift(dataPoint);

    // Enforce max data points limit
    if (this.data.length > this.maxDataPoints) {
      this.data = this.data.slice(0, this.maxDataPoints);
    }

    logger.debug(`Stored data point from source '${data.sourceId}' in memory storage`, {
      totalPoints: this.data.length,
      dataId: dataPoint.id
    });

    return dataPoint.id;
  }

  getLatest(limit = 100) {
    if (this.useExternalStorage && this.storageAdapter) {
      return this.storageAdapter.getLatest(limit).catch(error => {
        logger.error('Error getting latest data from external storage:', error);
        return this.data.slice(0, Math.min(limit, this.data.length));
      });
    }
    
    const actualLimit = Math.min(limit, this.data.length);
    return this.data.slice(0, actualLimit);
  }

  getBySource(sourceId, limit = 100) {
    if (this.useExternalStorage && this.storageAdapter) {
      return this.storageAdapter.getBySource(sourceId, limit).catch(error => {
        logger.error('Error getting data by source from external storage:', error);
        const sourceData = this.data.filter(item => item.sourceId === sourceId);
        return sourceData.slice(0, Math.min(limit, sourceData.length));
      });
    }
    
    const sourceData = this.data.filter(item => item.sourceId === sourceId);
    const actualLimit = Math.min(limit, sourceData.length);
    return sourceData.slice(0, actualLimit);
  }

  getById(id) {
    return this.data.find(item => item.id === id);
  }

  getByTimeRange(startTime, endTime) {
    if (this.useExternalStorage && this.storageAdapter) {
      return this.storageAdapter.getByTimeRange(startTime, endTime).catch(error => {
        logger.error('Error getting data by time range from external storage:', error);
        return this.getByTimeRangeMemory(startTime, endTime);
      });
    }
    
    return this.getByTimeRangeMemory(startTime, endTime);
  }

  getByTimeRangeMemory(startTime, endTime) {
    const start = new Date(startTime);
    const end = new Date(endTime);
    
    return this.data.filter(item => {
      const itemTime = new Date(item.timestamp);
      return itemTime >= start && itemTime <= end;
    });
  }

  getBySourceAndTimeRange(sourceId, startTime, endTime) {
    if (this.useExternalStorage && this.storageAdapter) {
      const adapter = this.storageAdapter;
      const request = typeof adapter.query === 'function'
        ? adapter.query({ sourceId, startTime, endTime, limit: 10000 })
        : adapter.getByTimeRange(startTime, endTime);
      return request
        .then(items => items.filter(item => item.sourceId === sourceId))
        .catch(error => {
          logger.error('Error getting data by source and time range from external storage:', error);
          return this.getBySourceAndTimeRangeMemory(sourceId, startTime, endTime);
        });
    }

    return this.getBySourceAndTimeRangeMemory(sourceId, startTime, endTime);
  }

  getBySourceAndTimeRangeMemory(sourceId, startTime, endTime) {
    const start = new Date(startTime);
    const end = new Date(endTime);
    
    return this.data.filter(item => {
      const itemTime = new Date(item.timestamp);
      return item.sourceId === sourceId && itemTime >= start && itemTime <= end;
    });
  }

  getStats() {
    const now = new Date();
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    const recentData = this.data.filter(item => 
      new Date(item.timestamp) >= oneHourAgo
    );

    const dailyData = this.data.filter(item => 
      new Date(item.timestamp) >= oneDayAgo
    );

    // Group by source
    const sourceStats = {};
    this.data.forEach(item => {
      if (!sourceStats[item.sourceId]) {
        sourceStats[item.sourceId] = {
          totalPoints: 0,
          recentPoints: 0,
          dailyPoints: 0,
          lastDataTime: null
        };
      }
      
      sourceStats[item.sourceId].totalPoints++;
      
      const itemTime = new Date(item.timestamp);
      if (itemTime >= oneHourAgo) {
        sourceStats[item.sourceId].recentPoints++;
      }
      if (itemTime >= oneDayAgo) {
        sourceStats[item.sourceId].dailyPoints++;
      }
      
      if (!sourceStats[item.sourceId].lastDataTime || 
          itemTime > new Date(sourceStats[item.sourceId].lastDataTime)) {
        sourceStats[item.sourceId].lastDataTime = item.timestamp;
      }
    });

    return {
      totalDataPoints: this.data.length,
      recentDataPoints: recentData.length,
      dailyDataPoints: dailyData.length,
      oldestDataTime: this.data.length > 0 ? this.data[this.data.length - 1].timestamp : null,
      newestDataTime: this.data.length > 0 ? this.data[0].timestamp : null,
      sourceStats,
      storageUtilization: (this.data.length / this.maxDataPoints) * 100
    };
  }

  search(query) {
    if (this.useExternalStorage && this.storageAdapter) {
      return this.storageAdapter.search(query).catch(error => {
        logger.error('Error searching in external storage:', error);
        return this.searchMemory(query);
      });
    }
    
    return this.searchMemory(query);
  }

  searchMemory(query) {
    const results = [];
    const queryLower = query.toLowerCase();

    for (const item of this.data) {
      // Search in source ID
      if (item.sourceId && item.sourceId.toLowerCase().includes(queryLower)) {
        results.push(item);
        continue;
      }

      // Search in data fields
      if (item.data && typeof item.data === 'object') {
        const dataStr = JSON.stringify(item.data).toLowerCase();
        if (dataStr.includes(queryLower)) {
          results.push(item);
          continue;
        }
      }

      // Search in metadata
      if (item.metadata && typeof item.metadata === 'object') {
        const metaStr = JSON.stringify(item.metadata).toLowerCase();
        if (metaStr.includes(queryLower)) {
          results.push(item);
          continue;
        }
      }
    }

    return results;
  }

  clear() {
    const clearedCount = this.data.length;
    this.data = [];
    logger.info(`Cleared ${clearedCount} data points from store`);
    return clearedCount;
  }

  clearBySource(sourceId) {
    const initialLength = this.data.length;
    this.data = this.data.filter(item => item.sourceId !== sourceId);
    const clearedCount = initialLength - this.data.length;
    logger.info(`Cleared ${clearedCount} data points from source '${sourceId}'`);
    return clearedCount;
  }

  startCleanupRoutine() {
    // Run cleanup every hour
    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, 60 * 60 * 1000);

    logger.info('Data store cleanup routine started (runs every hour)');
  }

  cleanup() {
    const initialLength = this.data.length;
    const cutoffTime = new Date();
    cutoffTime.setDate(cutoffTime.getDate() - this.retentionDays);

    this.data = this.data.filter(item => {
      const itemTime = new Date(item.timestamp);
      return itemTime >= cutoffTime;
    });

    const removedCount = initialLength - this.data.length;
    if (removedCount > 0) {
      logger.info(`Cleanup removed ${removedCount} expired data points (older than ${this.retentionDays} days)`);
    }

    return removedCount;
  }

  stopCleanupRoutine() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
      logger.info('Data store cleanup routine stopped');
    }
  }

  generateId() {
    return `dp_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  async shutdown() {
    this.stopCleanupRoutine();
    this.stopRetry();
    
    if (this.storageAdapter) {
      try {
        await this.storageAdapter.disconnect();
        logger.info('Storage adapter disconnected');
      } catch (error) {
        logger.error('Error disconnecting storage adapter:', error);
      }
    }
    
    logger.info('Data store shutdown completed');
  }

  // Export data methods
  exportData(format = 'json') {
    switch (format.toLowerCase()) {
      case 'json':
        return JSON.stringify(this.data, null, 2);
      
      case 'csv':
        return this.exportToCsv();
      
      default:
        throw new Error(`Unsupported export format: ${format}`);
    }
  }

  exportToCsv() {
    if (this.data.length === 0) {
      return '';
    }

    // Get all unique field names from data
    const fieldNames = new Set(['id', 'timestamp', 'sourceId', 'sourceType', 'storedAt']);
    this.data.forEach(item => {
      if (item.data && typeof item.data === 'object') {
        Object.keys(item.data).forEach(key => fieldNames.add(`data.${key}`));
      }
    });

    const headers = Array.from(fieldNames);
    const csvRows = [headers.join(',')];

    this.data.forEach(item => {
      const row = headers.map(header => {
        if (header.startsWith('data.')) {
          const dataKey = header.substring(5);
          return item.data && item.data[dataKey] !== undefined ? 
            JSON.stringify(item.data[dataKey]) : '';
        } else {
          return item[header] !== undefined ? JSON.stringify(item[header]) : '';
        }
      });
      csvRows.push(row.join(','));
    });

    return csvRows.join('\n');
  }
}

module.exports = DataStore;
