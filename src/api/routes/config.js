const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');
const configManager = require('../../config/ConfigManager');
const storageConfigManager = require('../../config/StorageConfigManager');
const { forCaller, restoreMasked } = require('../secrets');

/** Applies the saved sources to the running engine (start/stop/update connectors). */
async function applySources(req) {
  const server = req.app.get('serverInstance');
  if (server?.getEngine()) await server.reloadSourcesConfiguration();
}

/** Stored storage configuration of the given type (active or alternative), to restore masked secrets. */
async function storedStorageConfig(type) {
  const { storage, alternatives } = await storageConfigManager.getConfig();
  if (storage?.type === type) return storage.config;
  return alternatives?.[type]?.config;
}

// Get current configuration
router.get('/', (req, res) => {
  try {
    const sources = configManager.getSources();
    
    res.json({
      timestamp: new Date().toISOString(),
      configuration: {
        sources: {
          total: sources.length,
          enabled: sources.filter(s => s.enabled).length,
          disabled: sources.filter(s => !s.enabled).length,
          byType: sources.reduce((acc, source) => {
            acc[source.type] = (acc[source.type] || 0) + 1;
            return acc;
          }, {})
        },
        environment: {
          nodeEnv: process.env.NODE_ENV,
          logLevel: process.env.LOG_LEVEL,
          maxDataPoints: process.env.MAX_DATA_POINTS,
          dataRetentionDays: process.env.DATA_RETENTION_DAYS
        }
      }
    });

  } catch (error) {
    logger.error('Error getting configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve configuration'
    });
  }
});

// Get sources configuration
router.get('/sources', (req, res) => {
  try {
    const sources = configManager.getSources();

    res.json({
      timestamp: new Date().toISOString(),
      sources: forCaller(req, sources)
    });

  } catch (error) {
    logger.error('Error getting sources configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve sources configuration'
    });
  }
});

// Reload configuration
router.post('/reload', async (req, res) => {
  try {
    logger.info('Configuration reload requested via API');
    
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    // Reload configuration
    await engine.reloadConfiguration();
    
    logger.info('Configuration reloaded successfully via API');
    
    res.json({
      timestamp: new Date().toISOString(),
      action: 'reload',
      status: 'success',
      message: 'Configuration reloaded successfully'
    });

  } catch (error) {
    logger.error('Error reloading configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to reload configuration: ${error.message}`
    });
  }
});

// Update source configuration
router.put('/sources/:id', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const updates = req.body;
    
    if (!updates || Object.keys(updates).length === 0) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'No updates provided'
      });
    }

    // Update configuration and apply it to the running connectors
    const updatedSource = await configManager.updateSource(sourceId, updates);
    await applySources(req);
    
    logger.info(`Source configuration updated via API: ${sourceId}`);
    
    res.json({
      timestamp: new Date().toISOString(),
      action: 'update',
      sourceId,
      status: 'success',
      message: `Source '${sourceId}' configuration updated successfully`,
      source: forCaller(req, updatedSource)
    });

  } catch (error) {
    logger.error(`Error updating source configuration '${req.params.id}':`, error);
    
    if (error.message.includes('not found')) {
      res.status(404).json({
        error: 'Not Found',
        message: error.message
      });
    } else if (error.status === 400) {
      res.status(400).json({
        error: 'Bad Request',
        message: error.message
      });
    } else {
      res.status(500).json({
        error: 'Internal Server Error',
        message: `Failed to update source configuration: ${error.message}`
      });
    }
  }
});

// Add new source configuration
router.post('/sources', async (req, res) => {
  try {
    const sourceConfig = req.body;
    
    if (!sourceConfig || !sourceConfig.id) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Source configuration with ID is required'
      });
    }

    // Add source configuration and start it if enabled
    const addedSource = await configManager.addSource(sourceConfig);
    await applySources(req);
    
    logger.info(`New source configuration added via API: ${sourceConfig.id}`);
    
    res.status(201).json({
      timestamp: new Date().toISOString(),
      action: 'add',
      sourceId: sourceConfig.id,
      status: 'success',
      message: `Source '${sourceConfig.id}' configuration added successfully`,
      source: forCaller(req, addedSource)
    });

  } catch (error) {
    logger.error('Error adding source configuration:', error);
    
    if (error.message.includes('already exists')) {
      res.status(409).json({
        error: 'Conflict',
        message: error.message
      });
    } else if (error.status === 400) {
      res.status(400).json({
        error: 'Bad Request',
        message: error.message
      });
    } else {
      res.status(500).json({
        error: 'Internal Server Error',
        message: `Failed to add source configuration: ${error.message}`
      });
    }
  }
});

// Remove source configuration
router.delete('/sources/:id', async (req, res) => {
  try {
    const sourceId = req.params.id;

    // Remove source configuration, then stop and dispose its connector
    const removedSource = await configManager.removeSource(sourceId);
    await applySources(req);
    
    logger.info(`Source configuration removed via API: ${sourceId}`);
    
    res.json({
      timestamp: new Date().toISOString(),
      action: 'remove',
      sourceId,
      status: 'success',
      message: `Source '${sourceId}' configuration removed successfully`,
      removedSource: forCaller(req, removedSource)
    });

  } catch (error) {
    logger.error(`Error removing source configuration '${req.params.id}':`, error);
    
    if (error.message.includes('not found')) {
      res.status(404).json({
        error: 'Not Found',
        message: error.message
      });
    } else {
      res.status(500).json({
        error: 'Internal Server Error',
        message: `Failed to remove source configuration: ${error.message}`
      });
    }
  }
});

// Validate source configuration
router.post('/sources/validate', (req, res) => {
  try {
    const sourceConfig = req.body;
    
    if (!sourceConfig) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Source configuration is required'
      });
    }

    // Same rules as ConfigManager (all connector types, tenant, entityIdPrefix, ...)
    let value;
    try {
      [value] = configManager.validateSources([sourceConfig]);
    } catch (error) {
      if (error.status !== 400) throw error;
      return res.status(400).json({
        timestamp: new Date().toISOString(),
        valid: false,
        errors: [{ message: error.message }]
      });
    }

    res.json({
      timestamp: new Date().toISOString(),
      valid: true,
      message: 'Source configuration is valid',
      normalizedConfig: value
    });

  } catch (error) {
    logger.error('Error validating source configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to validate source configuration'
    });
  }
});

// === STORAGE CONFIGURATION ROUTES ===

// Get current storage configuration (saved) and the storage the engine is actually using
router.get('/storage', async (req, res) => {
  try {
    const storageConfig = await storageConfigManager.getConfig();
    const engine = req.app.get('serverInstance')?.getEngine();
    const runtime = engine?.dataStore ? engine.dataStore.getStorageInfo() : null;
    // UDC_RAW_STORAGE=memory makes the engine ignore the saved storage at startup
    const overridden = process.env.UDC_RAW_STORAGE === 'memory' && runtime
      && runtime.configuredType === 'memory' && storageConfig.storage?.type !== 'memory';

    res.json({
      timestamp: new Date().toISOString(),
      storage: {
        current: forCaller(req, storageConfig.storage),
        alternatives: forCaller(req, storageConfig.alternatives || {}),
        runtime,
        ...(overridden && {
          overriddenBy: 'UDC_RAW_STORAGE=memory',
          message: `The saved ${storageConfig.storage?.type} storage is not in use: UDC_RAW_STORAGE=memory keeps the raw data buffer in memory`
        })
      }
    });

  } catch (error) {
    logger.error('Error getting storage configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve storage configuration'
    });
  }
});

// Update storage configuration
router.put('/storage', async (req, res) => {
  try {
    const { storage } = req.body;
    
    if (!storage) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Storage configuration is required'
      });
    }

    const updatedConfig = await storageConfigManager.updateStorageConfig(
      { ...storage, config: restoreMasked(storage.config, await storedStorageConfig(storage.type)) }
    );
    
    logger.info('Storage configuration updated:', { newConfig: storage });
    
    res.json({
      timestamp: new Date().toISOString(),
      message: 'Storage configuration saved; use POST /api/v1/system/config/storage/reload to apply it',
      storage: forCaller(req, updatedConfig),
      restartRequired: false
    });

  } catch (error) {
    logger.error('Error updating storage configuration:', error);
    
    if (error.message.includes('validation')) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to update storage configuration'
    });
  }
});

// Get available storage types and their schemas
router.get('/storage/types', (req, res) => {
  try {
    const types = storageConfigManager.getTypeInfo();
    res.json({
      timestamp: new Date().toISOString(),
      types,
      storageTypes: Object.fromEntries(types.map(({ type, ...info }) => [type, info]))
    });

  } catch (error) {
    logger.error('Error getting storage types:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve storage types'
    });
  }
});

// Test storage connection
router.post('/storage/test', async (req, res) => {
  try {
    const { type, config } = req.body;
    
    if (!type || !config) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Storage type and config are required'
      });
    }

    const testResult = await storageConfigManager.testConnection(type, restoreMasked(config, await storedStorageConfig(type)));

    res.json({
      timestamp: new Date().toISOString(),
      test: {
        type,
        success: testResult.success,
        message: testResult.message,
        responseTime: testResult.responseTime,
        details: testResult.details
      }
    });

  } catch (error) {
    logger.error('Error testing storage connection:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to test storage connection',
      details: error.message
    });
  }
});

// Get storage health and statistics
router.get('/storage/health', async (req, res) => {
  try {
    const engine = req.app.get('serverInstance')?.getEngine();
    const healthInfo = await storageConfigManager.getStorageHealth(engine);

    res.json({
      timestamp: new Date().toISOString(),
      storage: healthInfo
    });

  } catch (error) {
    logger.error('Error getting storage health:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve storage health'
    });
  }
});

// Validate storage configuration
router.post('/storage/validate', async (req, res) => {
  try {
    const { type, config } = req.body;
    
    if (!type || !config) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Storage type and config are required'
      });
    }

    const validationResult = await storageConfigManager.validateConfig(type, config);
    
    if (!validationResult.valid) {
      return res.status(400).json({
        timestamp: new Date().toISOString(),
        valid: false,
        errors: validationResult.errors
      });
    }

    res.json({
      timestamp: new Date().toISOString(),
      valid: true,
      message: 'Storage configuration is valid',
      normalizedConfig: validationResult.config
    });

  } catch (error) {
    logger.error('Error validating storage configuration:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to validate storage configuration'
    });
  }
});

// === DYNAMIC CONFIGURATION ROUTES ===

// Reload sources configuration dynamically
router.post('/sources/reload', async (req, res) => {
  try {
    const { sources } = req.body;
    
    // Get server instance (needs to be passed from main app)
    const server = req.app.get('serverInstance');
    if (!server) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Server instance not available'
      });
    }
    
    // Reload configuration
    const result = await server.reloadSourcesConfiguration(sources);
    
    logger.info('Sources configuration reloaded via API', { 
      requestedBy: req.ip,
      sourcesCount: sources?.length || 'from-file'
    });
    
    res.json({
      timestamp: new Date().toISOString(),
      message: 'Sources configuration reloaded successfully',
      result,
      actions: {
        configurationUpdated: true,
        connectorsReinitialized: true,
        engineRestarted: false
      }
    });

  } catch (error) {
    logger.error('Error reloading sources configuration:', error);

    if (error.status === 400 || error.message.includes('validation')) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to reload sources configuration',
      details: error.message
    });
  }
});

// Update sources configuration with complete payload
router.post('/sources/configure', async (req, res) => {
  try {
    const { sources } = req.body;
    
    if (!sources || !Array.isArray(sources)) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Sources array is required in request body'
      });
    }
    
    // Validate each source configuration
    for (const source of sources) {
      if (!source.id || !source.type || !source.config) {
        return res.status(400).json({
          error: 'Validation Error',
          message: 'Each source must have id, type, and config properties',
          invalidSource: source
        });
      }
    }
    
    // Get server instance
    const server = req.app.get('serverInstance');
    if (!server) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Server instance not available'
      });
    }
    
    // Apply new configuration
    const result = await server.reloadSourcesConfiguration(sources);
    
    logger.info('Sources configuration updated via API', { 
      requestedBy: req.ip,
      sourcesCount: sources.length,
      sourceIds: sources.map(s => s.id)
    });
    
    res.json({
      timestamp: new Date().toISOString(),
      message: 'Sources configuration updated successfully',
      configuration: {
        totalSources: sources.length,
        enabledSources: sources.filter(s => s.enabled !== false).length,
        sourceTypes: sources.reduce((acc, source) => {
          acc[source.type] = (acc[source.type] || 0) + 1;
          return acc;
        }, {})
      },
      result,
      appliedImmediately: true
    });

  } catch (error) {
    logger.error('Error updating sources configuration:', error);

    if (error.status === 400 || error.message.includes('validation')) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to update sources configuration',
      details: error.message
    });
  }
});

// Reload storage configuration dynamically
router.post('/storage/reload', async (req, res) => {
  try {
    const { storage } = req.body;
    
    // Get server instance
    const server = req.app.get('serverInstance');
    if (!server) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Server instance not available'
      });
    }
    
    // Reload storage configuration (masked secrets keep their stored value)
    const result = await server.reloadStorageConfiguration(
      storage ? { ...storage, config: restoreMasked(storage.config, await storedStorageConfig(storage.type)) } : storage
    );
    
    logger.info('Storage configuration reloaded via API', { 
      requestedBy: req.ip,
      storageType: storage?.type || 'from-file'
    });
    
    res.json({
      timestamp: new Date().toISOString(),
      message: 'Storage configuration reloaded successfully',
      result,
      actions: {
        storageReconfigured: true,
        dataPreserved: true,
        engineRestarted: false
      }
    });

  } catch (error) {
    logger.error('Error reloading storage configuration:', error);
    
    if (error.message.includes('validation')) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to reload storage configuration',
      details: error.message
    });
  }
});

// Update storage configuration with complete payload
router.post('/storage/configure', async (req, res) => {
  try {
    const { type } = req.body;

    if (!type || !req.body.config) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Storage type and config are required'
      });
    }
    // Masked secrets sent back by the UI keep their stored value
    const config = restoreMasked(req.body.config, await storedStorageConfig(type));

    // Validate storage configuration
    const validationResult = await storageConfigManager.validateConfig(type, config);
    
    if (!validationResult.valid) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid storage configuration',
        errors: validationResult.errors
      });
    }
    
    // Test connection before applying
    const testResult = await storageConfigManager.testConnection(type, config);
    if (!testResult.success) {
      return res.status(400).json({
        error: 'Connection Test Failed',
        message: 'Cannot connect to specified storage',
        testResult
      });
    }
    
    // Get server instance
    const server = req.app.get('serverInstance');
    if (!server) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Server instance not available'
      });
    }
    
    // Apply new storage configuration (normalized: defaults + renamed fields)
    const storageConfig = { type: validationResult.type, config: validationResult.config };
    const result = await server.reloadStorageConfiguration(storageConfig);
    
    logger.info('Storage configuration updated via API', { 
      requestedBy: req.ip,
      storageType: type,
      testResponseTime: testResult.responseTime
    });
    
    res.json({
      timestamp: new Date().toISOString(),
      message: 'Storage configuration updated successfully',
      storage: {
        type: storageConfig.type,
        config: forCaller(req, storageConfig.config),
        connectionTest: {
          success: testResult.success,
          responseTime: testResult.responseTime
        }
      },
      result,
      appliedImmediately: true
    });

  } catch (error) {
    logger.error('Error updating storage configuration:', error);
    
    if (error.message.includes('validation')) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to update storage configuration',
      details: error.message
    });
  }
});

// Get current engine status and runtime information
router.get('/engine/status', (req, res) => {
  try {
    const server = req.app.get('serverInstance');
    if (!server) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Server instance not available'
      });
    }
    
    const engine = server.getEngine();
    if (!engine) {
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Engine not available'
      });
    }
    
    const engineStatus = engine.getStatus();
    const connectorStatus = engine.getConnectorStatus();
    
    res.json({
      timestamp: new Date().toISOString(),
      engine: {
        isRunning: engineStatus.isRunning,
        stats: engineStatus.stats,
        dataStore: engine.dataStore ? engine.dataStore.getStorageInfo() : null
      },
      connectors: connectorStatus,
      configuration: {
        canReloadDynamically: true,
        supportedOperations: [
          'sources/reload',
          'sources/configure', 
          'storage/reload',
          'storage/configure'
        ]
      }
    });

  } catch (error) {
    logger.error('Error getting engine status:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve engine status'
    });
  }
});

module.exports = router;
