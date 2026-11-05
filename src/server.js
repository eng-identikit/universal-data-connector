const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

const logger = require('./utils/logger');
const DataConnectorEngine = require('./core/DataConnectorEngine');
const systemRoutes = require('./api');
const mappingRoutes = require('./api/routes/mapping');
const configManager = require('./config/ConfigManager');
const storageConfigManager = require('./config/StorageConfigManager');
const Broker = require('./broker/Broker');

/**
 * Universal Data Connector: connector engine + context broker.
 *
 *   /api/v1          broker API (entities, subscriptions, history, commands, admin)
 *   /api/v1/stream   authenticated realtime WebSocket
 *   /api/v1/system   connectors, mapping, storage and engine status
 */
class UniversalDataConnector {
  constructor(storageConfig = null) {
    this.app = express();
    // Client address from X-Forwarded-For when the request comes through a proxy on a
    // private network (the UI's nginx, an ingress); used by the login limiter and the access log
    this.app.set('trust proxy', process.env.BROKER_TRUST_PROXY || 'loopback, linklocal, uniquelocal');
    this.server = null;
    this.engine = null;
    this.port = process.env.PORT || 3000;
    this.storageConfig = storageConfig;
    this.broker = new Broker();
  }

  async initialize() {
    try {
      // Setup Express middleware
      this.setupMiddleware();

      // Broker: connects in background, /api/v1 answers 503 until ready
      this.broker.start();

      // Initialize configuration
      await configManager.initialize();

      // Storage from config/storage.json unless explicitly passed to the constructor.
      // If it is unreachable the data store runs in memory and retries in background.
      // UDC_RAW_STORAGE=memory skips storage.json (entity history is kept by the broker anyway).
      let storageConfig = this.storageConfig
        || (process.env.UDC_RAW_STORAGE === 'memory' ? { type: 'memory', config: {} } : null);
      if (!storageConfig) {
        try {
          await storageConfigManager.initialize();
          storageConfig = storageConfigManager.getStorageConfig();
        } catch (error) {
          logger.error(`Invalid config/storage.json, using in-memory storage: ${error.message}`);
        }
      }

      // Initialize data connector engine
      this.engine = new DataConnectorEngine(storageConfig);
      await this.engine.initialize();

      // Devices read by the connectors become broker entities
      this.broker.attachEngine(this.engine);
      this.engine.on('sourceStatusChanged', (sourceId, status) => {
        logger.info(`Source '${sourceId}' status: ${status}`);
      });

      // Initialize mapping routes with engine instance PRIMA di setup routes
      mappingRoutes.initialize(this.engine);

      // Setup routes DOPO l'inizializzazione
      this.setupRoutes();

      logger.info('Universal Data Connector initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Universal Data Connector:', error);
      throw error;
    }
  }

  setupMiddleware() {
    this.app.use(helmet());
    this.app.use(cors());
    this.app.use(express.json({ limit: process.env.BODY_LIMIT || '5mb' }));
    this.app.use(express.urlencoded({ extended: true }));

    // Request logging
    this.app.use((req, res, next) => {
      logger.debug(`${req.method} ${req.path}`, {
        ip: req.ip,
        userAgent: req.get('User-Agent')
      });
      next();
    });
  }

  setupRoutes() {
    // Make server instance available to routes
    this.app.set('serverInstance', this);

    // Health check endpoint
    this.app.get('/health', (req, res) => {
      res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        broker: this.broker.status()
      });
    });

    // Connector engine API (must be mounted before the broker router and its 404)
    this.app.use('/api/v1/system', ...this.broker.systemMiddleware(), systemRoutes);

    // Broker API
    this.app.use('/api/v1', this.broker.router);

    // 404 handler
    this.app.use('*', (req, res) => {
      res.status(404).json({
        error: 'Not Found',
        message: 'The requested endpoint does not exist'
      });
    });

    // Global error handler
    this.app.use((error, req, res, next) => {
      // Client errors raised by body parsing (invalid JSON, body too large, ...)
      if (error.expose && error.status >= 400 && error.status < 500) {
        return res.status(error.status).json({
          error: error.type === 'entity.parse.failed' ? 'BadRequest' : 'ClientError',
          message: error.type === 'entity.parse.failed' ? 'Request body is not valid JSON' : error.message
        });
      }
      logger.error('Unhandled error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
      });
    });
  }

  // Method to get engine instance for API access
  getEngine() {
    return this.engine;
  }

  // Dynamic configuration reload methods
  async reloadSourcesConfiguration(newSources = null) {
    if (!this.engine) {
      throw new Error('Engine not initialized');
    }
    return await this.engine.reloadSourcesConfiguration(newSources);
  }

  async reloadStorageConfiguration(newStorageConfig = null) {
    if (!this.engine) {
      throw new Error('Engine not initialized');
    }
    return await this.engine.reloadStorageConfiguration(newStorageConfig);
  }

  async start() {
    try {
      await this.initialize();

      this.server = this.app.listen(this.port, () => {
        logger.info(`Universal Data Connector listening on port ${this.port}`);
        logger.info(`  Broker API:  http://localhost:${this.port}/api/v1`);
        logger.info(`  Stream:      ws://localhost:${this.port}/api/v1/stream`);
        logger.info(`  System API:  http://localhost:${this.port}/api/v1/system`);
      });
      this.broker.attach(this.server);

      // Start the data connector engine
      await this.engine.start();

      return this.server;
    } catch (error) {
      logger.error('Failed to start Universal Data Connector:', error);
      throw error;
    }
  }

  async stop() {
    logger.info('Stopping Universal Data Connector...');

    try {
      if (this.engine) {
        await this.engine.stop();
      }

      if (this.server) {
        this.server.close();
      }

      await this.broker.stop();

      logger.info('Universal Data Connector stopped successfully');
    } catch (error) {
      logger.error('Error stopping Universal Data Connector:', error);
      throw error;
    }
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully');
  if (global.connector) {
    await global.connector.stop();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully');
  if (global.connector) {
    await global.connector.stop();
  }
  process.exit(0);
});

// Start the connector if this file is run directly
if (require.main === module) {
  const connector = new UniversalDataConnector();
  global.connector = connector;

  connector.start().catch((error) => {
    logger.error('Failed to start connector:', error);
    process.exit(1);
  });
}

module.exports = UniversalDataConnector;
