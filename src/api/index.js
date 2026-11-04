const express = require('express');
const router = express.Router();

// Import route modules
const statusRoutes = require('./routes/status');
const sourcesRoutes = require('./routes/sources');
const dataRoutes = require('./routes/data');
const configRoutes = require('./routes/config');
const mappingRoutes = require('./routes/mapping');
const historyRoutes = require('./routes/history');

// Authentication is applied by the broker (systemGuard) where this router is mounted

// API routes
router.use('/status', statusRoutes);
router.use('/sources', sourcesRoutes);
router.use('/data', dataRoutes);
router.use('/config', configRoutes);
router.use('/mapping', mappingRoutes.router);
router.use('/history', historyRoutes);

// API documentation endpoint
router.get('/', (req, res) => {
  res.json({
    name: 'Universal Data Connector System API',
    version: '1.0.0',
    description: 'REST API for monitoring and controlling the Universal Data Connector',
    endpoints: {
      status: {
        'GET /api/v1/system/status': 'Get overall system status',
        'GET /api/v1/system/status/health': 'Get health check information'
      },
      sources: {
        'GET /api/v1/system/sources': 'Get all configured sources',
        'GET /api/v1/system/sources/:id': 'Get specific source information',
        'GET /api/v1/system/sources/:id/status': 'Get source status',
        'POST /api/v1/system/sources/:id/start': 'Start a source',
        'POST /api/v1/system/sources/:id/stop': 'Stop a source',
        'POST /api/v1/system/sources/:id/restart': 'Restart a source'
      },
      history: {
        'GET /api/v1/system/history/status': 'TimescaleDB availability and recorded time range',
        'GET /api/v1/system/history/sources': 'Sources with historical data',
        'GET /api/v1/system/history/measurements?source=': 'Measurements recorded for a source',
        'GET /api/v1/system/history/series?source=&measurements=&startTime=&endTime=&maxPoints=': 'Downsampled time series (avg/min/max/last)',
        'GET /api/v1/system/history/records?source=&startTime=&endTime=&limit=&offset=': 'Raw historical records'
      },
      data: {
        'GET /api/v1/system/data/latest': 'Get latest data points',
        'GET /api/v1/system/data/source/:id': 'Get data from specific source',
        'GET /api/v1/system/data/search': 'Search data points',
        'GET /api/v1/system/data/export': 'Export data in various formats'
      },
      config: {
        'GET /api/v1/system/config': 'Get current configuration',
        'POST /api/v1/system/config/reload': 'Reload configuration from file',
        'GET /api/v1/system/config/sources': 'Get sources configuration'
      },
      mapping: {
        'GET /api/v1/system/mapping/entities': 'Get all mapped entities',
        'GET /api/v1/system/mapping/entities/:id': 'Get specific mapped entity',
        'GET /api/v1/system/mapping/entities/type/:type': 'Get entities by type',
        'GET /api/v1/system/mapping/export/json': 'Export mapped data to JSON',
        'GET /api/v1/system/mapping/export/ngsi-ld': 'Export mapped data to NGSI-LD',
        'GET /api/v1/system/mapping/export/toon': 'Export mapped data to TOON',
        'GET /api/v1/system/mapping/statistics': 'Get mapping statistics',
        'GET /api/v1/system/mapping/health': 'Get mapping engine health',
        'DELETE /api/v1/system/mapping/entities': 'Clear all mapped data'
      }
    },
    authentication: 'Bearer token or X-API-Key (broker credentials); changes require a superadmin'
  });
});

module.exports = router;
