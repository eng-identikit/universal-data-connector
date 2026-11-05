const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');

// Get overall system status
router.get('/', (req, res) => {
  try {
    const connector = global.connector;
    if (!connector) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const engine = connector.getEngine();
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not initialized'
      });
    }

    const status = engine.getStatus();
    
    res.json({
      timestamp: new Date().toISOString(),
      system: {
        status: status.isRunning ? 'running' : 'stopped',
        uptime: status.uptime,
        startTime: status.stats.startTime,
        nodeVersion: process.version,
        platform: process.platform,
        arch: process.arch,
        pid: process.pid
      },
      engine: {
        isRunning: status.isRunning,
        totalDataPoints: status.stats.totalDataPoints,
        totalErrors: status.stats.totalErrors,
        lastDataReceived: status.stats.lastDataReceived
      },
      connectors: Object.entries(status.connectors).map(([id, c]) => ({
        id,
        type: c.type,
        enabled: c.enabled,
        runtime: { status: c.status, lastActivity: c.lastActivity, stats: c.stats }
      })),
      memory: {
        used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
        external: Math.round(process.memoryUsage().external / 1024 / 1024),
        rss: Math.round(process.memoryUsage().rss / 1024 / 1024)
      }
    });

  } catch (error) {
    logger.error('Error getting system status:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve system status'
    });
  }
});

// Health check endpoint
router.get('/health', (req, res) => {
  try {
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    const health = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      checks: {
        engine: {
          status: engine ? 'up' : 'down',
          message: engine ? 'Engine is running' : 'Engine not available'
        },
        memory: {
          status: 'up',
          usage: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
          limit: Math.round(process.memoryUsage().heapTotal / 1024 / 1024)
        }
      }
    };

    // Check engine status
    if (engine) {
      const engineStatus = engine.getStatus();
      health.checks.connectors = {
        status: Object.keys(engineStatus.connectors).length > 0 ? 'up' : 'warning',
        count: Object.keys(engineStatus.connectors).length,
        connected: Object.values(engineStatus.connectors).filter(c => c.status === 'connected').length
      };
    }

    // Determine overall health
    const allChecksHealthy = Object.values(health.checks).every(check => 
      check.status === 'up' || check.status === 'warning'
    );
    
    if (!allChecksHealthy) {
      health.status = 'unhealthy';
      res.status(503);
    }

    res.json(health);

  } catch (error) {
    logger.error('Error in health check:', error);
    res.status(503).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: 'Health check failed',
      message: error.message
    });
  }
});

// Get detailed statistics
router.get('/stats', (req, res) => {
  try {
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const status = engine.getStatus();
    const dataStore = engine.dataStore;
    const dataStats = dataStore ? dataStore.getStats() : null;

    res.json({
      timestamp: new Date().toISOString(),
      uptime: status.uptime,
      engine: {
        totalDataPoints: status.stats.totalDataPoints,
        totalErrors: status.stats.totalErrors,
        startTime: status.stats.startTime,
        lastDataReceived: status.stats.lastDataReceived
      },
      connectors: Object.entries(status.connectors).map(([id, connector]) => ({
        id,
        type: connector.type,
        status: connector.status,
        lastActivity: connector.lastActivity,
        stats: connector.stats
      })),
      dataStore: dataStats,
      system: {
        memory: process.memoryUsage(),
        cpu: process.cpuUsage(),
        uptime: process.uptime(),
        version: process.version
      }
    });

  } catch (error) {
    logger.error('Error getting statistics:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve statistics'
    });
  }
});

module.exports = router;
