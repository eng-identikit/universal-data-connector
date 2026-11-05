const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');
const configManager = require('../../config/ConfigManager');
const { forCaller } = require('../secrets');
const { parseLimit } = require('../params');

// Get all sources
/**
 * Connector state by id. In a cluster each connector runs on one instance:
 * the ones running elsewhere are reported as their owner last saw them.
 */
async function clusterStatus(engine) {
  const local = engine.getConnectorStatus() || {};
  if (!engine.clusterView) return local;
  let remote;
  try {
    remote = await engine.clusterView.connectorRuntime();
  } catch (error) {
    logger.warn(`Cluster connector state unavailable: ${error.message}`);
    return local;
  }
  const merged = { ...local };
  for (const id of Object.keys(local)) {
    if (engine.canRun(id)) merged[id] = { ...local[id], instance: engine.clusterView.id };
    else if (remote.has(id)) merged[id] = { ...local[id], ...remote.get(id) };
  }
  return merged;
}

/** start/stop/restart of a connector that runs on another cluster instance. */
async function forward(res, engine, sourceId, op) {
  if (!engine.clusterView) {
    return res.status(409).json({ error: 'Conflict', message: `Source '${sourceId}' runs on another instance` });
  }
  try {
    const instance = await engine.clusterView.forwardConnectorOp(sourceId, op);
    return res.status(202).json({
      timestamp: new Date().toISOString(),
      sourceId,
      action: op,
      status: 'forwarded',
      instance,
      message: `Source '${sourceId}' runs on instance ${instance}: the ${op} was sent there`
    });
  } catch (error) {
    return res.status(error.status || 500).json({ error: error.status === 409 ? 'Conflict' : 'Internal Server Error', message: error.message });
  }
}

router.get('/', async (req, res) => {
  try {
    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const sources = configManager.getSources();
    const connectorStatus = await clusterStatus(engine);

    const sourcesWithStatus = sources.map(source => {
      const status = connectorStatus[source.id];
      return {
        ...source,
        runtime: {
          status: status?.status || 'unknown',
          lastActivity: status?.lastActivity || null,
          instance: status?.instance || null,
          stats: status?.stats || { dataPoints: 0, errors: 0, connections: 0 }
        }
      };
    });

    res.json({
      timestamp: new Date().toISOString(),
      total: sources.length,
      enabled: sources.filter(s => s.enabled).length,
      sources: forCaller(req, sourcesWithStatus)
    });

  } catch (error) {
    logger.error('Error getting sources:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve sources'
    });
  }
});

// Get specific source
router.get('/:id', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    let runtimeStatus = null;
    if (engine) {
      runtimeStatus = (await clusterStatus(engine))[sourceId] || null;
    }

    res.json({
      timestamp: new Date().toISOString(),
      source: {
        ...forCaller(req, source),
        runtime: runtimeStatus
      }
    });

  } catch (error) {
    logger.error('Error getting source:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve source'
    });
  }
});

// Get source status
router.get('/:id/status', (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const status = engine.getConnectorStatus(sourceId);

    if (!status) {
      return res.json({
        timestamp: new Date().toISOString(),
        sourceId,
        status: 'not_initialized',
        message: 'Source connector not initialized'
      });
    }

    res.json({
      timestamp: new Date().toISOString(),
      ...status
    });

  } catch (error) {
    logger.error('Error getting source status:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve source status'
    });
  }
});

// Start source
router.post('/:id/start', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    if (!engine.canRun(sourceId)) {
      return forward(res, engine, sourceId, 'start');
    }

    await engine.startConnector(sourceId);

    logger.info(`Source '${sourceId}' started via API`);

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      action: 'start',
      status: 'success',
      message: `Source '${sourceId}' started successfully`
    });

  } catch (error) {
    logger.error(`Error starting source '${req.params.id}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to start source: ${error.message}`
    });
  }
});

// Stop source
router.post('/:id/stop', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    if (!engine.canRun(sourceId)) {
      return forward(res, engine, sourceId, 'stop');
    }

    await engine.stopConnector(sourceId);

    logger.info(`Source '${sourceId}' stopped via API`);

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      action: 'stop',
      status: 'success',
      message: `Source '${sourceId}' stopped successfully`
    });

  } catch (error) {
    logger.error(`Error stopping source '${req.params.id}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to stop source: ${error.message}`
    });
  }
});

// Restart source
router.post('/:id/restart', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    if (!engine.canRun(sourceId)) {
      return forward(res, engine, sourceId, 'restart');
    }

    await engine.restartConnector(sourceId);

    logger.info(`Source '${sourceId}' restarted via API`);

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      action: 'restart',
      status: 'success',
      message: `Source '${sourceId}' restarted successfully`
    });

  } catch (error) {
    logger.error(`Error restarting source '${req.params.id}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to restart source: ${error.message}`
    });
  }
});

// Get source data
router.get('/:id/data', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const limit = parseLimit(req.query.limit);

    const source = configManager.getSourceById(sourceId);
    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const data = await engine.getDataBySource(sourceId, limit);

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      limit,
      total: data.length,
      data
    });

  } catch (error) {
    logger.error(`Error getting data for source '${req.params.id}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve source data'
    });
  }
});

// Get discovered nodes/topics/registers (auto-discovery results)
router.get('/:id/discovery', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    const connectorInstance = engine.connectors.get(sourceId)?.connector;
    if (!connectorInstance) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Connector '${sourceId}' not found or not initialized`
      });
    }

    // Check if connector has discovered data
    if (!connectorInstance.discoveredNodes && !connectorInstance.discoveredTopics && !connectorInstance.discoveredRegisters) {
      return res.status(404).json({
        error: 'No Discovery Data',
        message: 'Auto-discovery was not enabled or has not completed yet. Configure the source without nodes/topics/registers to enable auto-discovery.'
      });
    }

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      type: source.type,
      discovered: {
        nodes: connectorInstance.discoveredNodes || [],
        topics: connectorInstance.discoveredTopics || [],
        registers: connectorInstance.discoveredRegisters || []
      },
      count: {
        nodes: connectorInstance.discoveredNodes?.length || 0,
        topics: connectorInstance.discoveredTopics?.length || 0,
        registers: connectorInstance.discoveredRegisters?.length || 0
      },
      message: 'Use POST /api/v1/system/sources/:id/configure to select and activate discovered items'
    });

  } catch (error) {
    logger.error(`Error getting discovery data for source '${req.params.id}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve discovery data'
    });
  }
});

// Configure discovered nodes/topics/registers
router.post('/:id/configure', async (req, res) => {
  try {
    const sourceId = req.params.id;
    const { nodes, topics, registers } = req.body;

    const source = configManager.getSourceById(sourceId);

    if (!source) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Source '${sourceId}' not found`
      });
    }

    const connector = global.connector;
    const engine = connector?.getEngine();

    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    for (const [name, value] of Object.entries({ nodes, topics, registers })) {
      if (value !== undefined && !Array.isArray(value)) {
        return res.status(400).json({ error: 'Bad Request', message: `${name} must be an array` });
      }
    }

    // New configuration: saved (validated) first, the running source is untouched until then
    const config = { ...source.config };
    if (nodes) config.nodes = nodes;
    if (topics) config.topics = topics;
    if (registers) config.registers = registers;
    await configManager.updateSource(sourceId, { config });

    // Recreate the connector with the new configuration (a restart would reuse the old instance)
    await req.app.get('serverInstance').reloadSourcesConfiguration();

    logger.info(`Source '${sourceId}' configured with discovered items and restarted`);

    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      action: 'configure',
      status: 'success',
      message: `Source '${sourceId}' configured and restarted successfully`,
      configured: {
        nodes: nodes?.length || 0,
        topics: topics?.length || 0,
        registers: registers?.length || 0
      }
    });

  } catch (error) {
    logger.error(`Error configuring source '${req.params.id}':`, error);
    if (error.status === 400) {
      return res.status(400).json({ error: 'Bad Request', message: error.message });
    }
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to configure source: ${error.message}`
    });
  }
});

module.exports = router;
