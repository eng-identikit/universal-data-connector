const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');

/**
 * Mapping API Routes
 * 
 * Provides endpoints for accessing mapped data in various formats:
 * - Universal Data Model (JSON)
 * - TOON
 */

let engine = null;

function initialize(dataConnectorEngine) {
  engine = dataConnectorEngine;
  logger.info('Mapping API routes initialized');
}

/**
 * GET /api/mapping/entities
 * Get all mapped entities
 */
router.get('/entities', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const entities = engine.getAllMappedEntities();
    
    res.json({
      success: true,
      count: entities.length,
      entities: entities
    });
    
  } catch (error) {
    logger.error('Error getting mapped entities:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/entities/:id
 * Get a specific mapped entity by ID
 */
router.get('/entities/:id', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const entity = engine.getMappedEntity(req.params.id);
    
    if (!entity) {
      return res.status(404).json({
        success: false,
        error: `Entity '${req.params.id}' not found`
      });
    }

    res.json({
      success: true,
      entity: entity
    });
    
  } catch (error) {
    logger.error(`Error getting entity ${req.params.id}:`, error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/entities/type/:type
 * Get all entities of a specific type
 */
router.get('/entities/type/:type', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const entities = engine.getMappedEntitiesByType(req.params.type);
    
    res.json({
      success: true,
      type: req.params.type,
      count: entities.length,
      entities: entities
    });
    
  } catch (error) {
    logger.error(`Error getting entities of type ${req.params.type}:`, error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/export/json
 * Export mapped data to JSON format
 */
router.get('/export/json', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const options = {
      includeMetadata: req.query.includeMetadata !== 'false',
      includeRelationships: req.query.includeRelationships !== 'false'
    };

    const jsonData = engine.exportMappedDataToJSON(options);
    
    res.json({
      success: true,
      format: 'JSON',
      data: jsonData
    });
    
  } catch (error) {
    logger.error('Error exporting to JSON:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/export/toon
 * Export mapped data to TOON format
 */
router.get('/export/toon', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const options = {};
    const toonData = engine.exportMappedDataToTOON(options);
    
    res.json({
      success: true,
      format: 'TOON',
      data: toonData
    });
    
  } catch (error) {
    logger.error('Error exporting to TOON:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/statistics
 * Get mapping engine statistics
 */
router.get('/statistics', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    const stats = engine.getMappingStatistics();
    
    res.json({
      success: true,
      statistics: stats
    });
    
  } catch (error) {
    logger.error('Error getting mapping statistics:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * DELETE /api/mapping/entities
 * Clear all mapped data
 */
router.delete('/entities', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        error: 'Engine not initialized'
      });
    }

    engine.clearMappedData();
    
    res.json({
      success: true,
      message: 'All mapped data cleared'
    });
    
  } catch (error) {
    logger.error('Error clearing mapped data:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/mapping/health
 * Get mapping engine health status
 */
router.get('/health', (req, res) => {
  try {
    if (!engine) {
      return res.status(503).json({
        success: false,
        healthy: false,
        error: 'Engine not initialized'
      });
    }

    const mappingEngine = engine.getMappingEngine();
    const stats = mappingEngine.getStatistics();
    
    res.json({
      success: true,
      healthy: true,
      version: '1.0.0',
      statistics: {
        totalMappings: stats.totalMappings,
        successfulMappings: stats.successfulMappings,
        failedMappings: stats.failedMappings,
        devices: stats.dataModelStats.totalDevices,
        configuredDevices: stats.configuredDevices
      },
      registeredMappers: stats.mappersCount
    });
    
  } catch (error) {
    logger.error('Error getting mapping health:', error);
    res.status(500).json({
      success: false,
      healthy: false,
      error: error.message
    });
  }
});

module.exports = {
  router,
  initialize
};
