const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');

// Get latest data points
router.get('/latest', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const sourceId = req.query.source;
    
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    let data;
    if (sourceId) {
      data = await engine.getDataBySource(sourceId, limit);
    } else {
      data = await engine.getLatestData(null, limit);
    }
    
    res.json({
      timestamp: new Date().toISOString(),
      limit,
      sourceFilter: sourceId || null,
      total: data.length,
      data
    });

  } catch (error) {
    logger.error('Error getting latest data:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve latest data'
    });
  }
});

// Get data by source
router.get('/source/:sourceId', async (req, res) => {
  try {
    const sourceId = req.params.sourceId;
    const limit = parseInt(req.query.limit) || 100;
    const startTime = req.query.startTime;
    const endTime = req.query.endTime;
    
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    let data;
    if (startTime && endTime) {
      data = await engine.dataStore.getBySourceAndTimeRange(sourceId, startTime, endTime);
    } else {
      data = await engine.getDataBySource(sourceId, limit);
    }
    
    res.json({
      timestamp: new Date().toISOString(),
      sourceId,
      limit: startTime && endTime ? null : limit,
      timeRange: startTime && endTime ? { startTime, endTime } : null,
      total: data.length,
      data
    });

  } catch (error) {
    logger.error(`Error getting data for source '${req.params.sourceId}':`, error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve source data'
    });
  }
});

// Search data
router.get('/search', async (req, res) => {
  try {
    const query = req.query.q;
    const limit = parseInt(req.query.limit) || 100;
    
    if (!query) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Search query (q) parameter is required'
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

    const results = (await engine.dataStore.search(query)) || [];
    const limitedResults = results.slice(0, limit);
    
    res.json({
      timestamp: new Date().toISOString(),
      query,
      limit,
      total: results.length,
      returned: limitedResults.length,
      data: limitedResults
    });

  } catch (error) {
    logger.error('Error searching data:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to search data'
    });
  }
});

// Get data by time range
router.get('/range', async (req, res) => {
  try {
    const startTime = req.query.startTime;
    const endTime = req.query.endTime;
    const sourceId = req.query.source;
    
    if (!startTime || !endTime) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Both startTime and endTime parameters are required'
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

    let data;
    if (sourceId) {
      data = await engine.dataStore.getBySourceAndTimeRange(sourceId, startTime, endTime);
    } else {
      data = await engine.dataStore.getByTimeRange(startTime, endTime);
    }
    
    res.json({
      timestamp: new Date().toISOString(),
      timeRange: { startTime, endTime },
      sourceFilter: sourceId || null,
      total: data.length,
      data
    });

  } catch (error) {
    logger.error('Error getting data by time range:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve data by time range'
    });
  }
});

// Export data
router.get('/export', async (req, res) => {
  try {
    const format = req.query.format || 'json';
    const sourceId = req.query.source;
    const startTime = req.query.startTime;
    const endTime = req.query.endTime;
    const limit = parseInt(req.query.limit) || 10000;
    
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    let data;
    if (startTime && endTime) {
      if (sourceId) {
        data = await engine.dataStore.getBySourceAndTimeRange(sourceId, startTime, endTime);
      } else {
        data = await engine.dataStore.getByTimeRange(startTime, endTime);
      }
    } else {
      if (sourceId) {
        data = await engine.getDataBySource(sourceId, limit);
      } else {
        data = await engine.getLatestData(null, limit);
      }
    }

    // Create temporary data store for export
    const tempDataStore = {
      data: data,
      exportData: function(exportFormat) {
        switch (exportFormat.toLowerCase()) {
          case 'json':
            return JSON.stringify(this.data, null, 2);
          case 'csv':
            return this.exportToCsv();
          default:
            throw new Error(`Unsupported export format: ${exportFormat}`);
        }
      },
      exportToCsv: function() {
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
    };

    const exportedData = tempDataStore.exportData(format);
    
    // Set appropriate headers based on format
    switch (format.toLowerCase()) {
      case 'csv':
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="data-export-${Date.now()}.csv"`);
        break;
      case 'json':
      default:
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', `attachment; filename="data-export-${Date.now()}.json"`);
        break;
    }
    
    res.send(exportedData);

  } catch (error) {
    logger.error('Error exporting data:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: `Failed to export data: ${error.message}`
    });
  }
});

// Get data statistics
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

    const stats = engine.dataStore.getStats();
    
    res.json({
      timestamp: new Date().toISOString(),
      ...stats
    });

  } catch (error) {
    logger.error('Error getting data statistics:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve data statistics'
    });
  }
});

// Clear data
router.delete('/clear', (req, res) => {
  try {
    const sourceId = req.query.source;
    
    const connector = global.connector;
    const engine = connector?.getEngine();
    
    if (!engine) {
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Data connector engine not available'
      });
    }

    let clearedCount;
    if (sourceId) {
      clearedCount = engine.dataStore.clearBySource(sourceId);
    } else {
      clearedCount = engine.dataStore.clear();
    }
    
    logger.info(`Cleared ${clearedCount} data points via API${sourceId ? ` for source '${sourceId}'` : ''}`);
    
    res.json({
      timestamp: new Date().toISOString(),
      action: 'clear',
      sourceFilter: sourceId || null,
      clearedCount,
      message: `Cleared ${clearedCount} data points`
    });

  } catch (error) {
    logger.error('Error clearing data:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to clear data'
    });
  }
});

module.exports = router;
