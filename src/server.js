const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const { createServer } = require('http');
const WebSocket = require('ws');
require('dotenv').config();

const logger = require('./utils/logger');
const DataConnectorEngine = require('./core/DataConnectorEngine');
const apiRoutes = require('./api');
const mappingRoutes = require('./api/routes/mapping');
const configManager = require('./config/ConfigManager');
const storageConfigManager = require('./config/StorageConfigManager');

class UniversalDataConnector {
  constructor(storageConfig = null) {
    this.app = express();
    this.server = null;
    this.wsServer = null;
    this.engine = null;
    this.dataCounter = 0;
    this.port = process.env.PORT || 3000;
    this.wsPort = process.env.WS_PORT || 3001;
    this.storageConfig = storageConfig;
  }

  async initialize() {
    try {
      // Setup Express middleware
      this.setupMiddleware();

      // Initialize configuration
      await configManager.initialize();

      // Storage from config/storage.json unless explicitly passed to the constructor.
      // If it is unreachable the data store runs in memory and retries in background.
      let storageConfig = this.storageConfig;
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

      // Initialize mapping routes with engine instance PRIMA di setup routes
      mappingRoutes.initialize(this.engine);

      // Setup routes DOPO l'inizializzazione
      this.setupRoutes();

      // Setup WebSocket server for real-time data
      this.setupWebSocket();

      logger.info('Universal Data Connector initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Universal Data Connector:', error);
      throw error;
    }
  }

  setupMiddleware() {
    this.app.use(helmet());
    this.app.use(cors());
    this.app.use(express.json());
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
        uptime: process.uptime()
      });
    });

    // API routes
    this.app.use('/api', apiRoutes);

    // 404 handler
    this.app.use('*', (req, res) => {
      res.status(404).json({
        error: 'Not Found',
        message: 'The requested endpoint does not exist'
      });
    });

    // Global error handler
    this.app.use((error, req, res, next) => {
      logger.error('Unhandled error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
      });
    });
  }

  setupWebSocket() {
    this.wsServer = new WebSocket.Server({ port: this.wsPort });

    this.wsServer.on('connection', (ws) => {
      logger.info('WebSocket client connected');

      ws.on('message', (message) => {
        try {
          const data = JSON.parse(message);
          this.handleWebSocketMessage(ws, data);
        } catch (error) {
          logger.error('Invalid WebSocket message:', error);
          ws.send(JSON.stringify({ error: 'Invalid message format' }));
        }
      });

      ws.on('close', () => {
        logger.info('WebSocket client disconnected');
      });

      // Send welcome message
      ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to Universal Data Connector',
        timestamp: new Date().toISOString()
      }));
    });

    // Subscribe to engine data events
    if (this.engine) {
      /* this.engine.on('data', (data) => {
        this.broadcastToWebSocketClients({
          type: 'data',
          payload: data,
          timestamp: new Date().toISOString()
        });
      }); */

      /* this.engine.on('sourceStatusChanged', (sourceId, status) => {
        this.broadcastToWebSocketClients({
          type: 'sourceStatus',
          sourceId,
          status,
          timestamp: new Date().toISOString()
        });
      }); */

      this.engine.on('data', (data) => {
        // 🎯 STAMPA FINALE DATI MAPPATI A VIDEO
        this.dataCounter++;
        console.log('\n' + '='.repeat(80));
        console.log(`📊 DATI MAPPATI #${this.dataCounter} - ${new Date().toISOString()}`);
        console.log('='.repeat(80));
        console.log(`🔹 Sorgente: ${data.sourceId}`);
        console.log(`🔹 Timestamp: ${data.timestamp}`);
        console.log(`\n📥 DATI ORIGINALI ( ${data.sourceId} ):`);
        console.log(JSON.stringify(data.originalData, null, 2));
        console.log('\n✅ DATI MAPPATI:');
        console.log(JSON.stringify(data.mappedData, null, 2));
        console.log('='.repeat(80) + '\n');

        this.broadcastToWebSocketClients({
          type: 'data',
          payload: data,
          timestamp: new Date().toISOString()
        });
      });

      this.engine.on('sourceStatusChanged', (sourceId, status) => {
        logger.info(`🔄 Cambio stato sorgente '${sourceId}': ${status}`);

        this.broadcastToWebSocketClients({
          type: 'sourceStatus',
          sourceId,
          status,
          timestamp: new Date().toISOString()
        });
      });
    }

    logger.info(`WebSocket server listening on port ${this.wsPort}`);
  }

  handleWebSocketMessage(ws, data) {
    switch (data.type) {
      case 'subscribe':
        // Handle subscription to specific data sources
        ws.subscriptions = ws.subscriptions || [];
        if (data.sourceId && !ws.subscriptions.includes(data.sourceId)) {
          ws.subscriptions.push(data.sourceId);
        }
        break;

      case 'unsubscribe':
        ws.subscriptions = ws.subscriptions || [];
        const index = ws.subscriptions.indexOf(data.sourceId);
        if (index > -1) {
          ws.subscriptions.splice(index, 1);
        }
        break;

      default:
        ws.send(JSON.stringify({ error: 'Unknown message type' }));
    }
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

  broadcastToWebSocketClients(message) {
    this.wsServer.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        // Check if client is subscribed to this source
        if (!message.sourceId || !client.subscriptions ||
          client.subscriptions.includes(message.sourceId)) {
          client.send(JSON.stringify(message));
        }
      }
    });
  }

  async start() {
    try {
      await this.initialize();

      this.server = this.app.listen(this.port, () => {
        console.log('\n' + '█'.repeat(80));
        console.log('  🚀 UNIVERSAL DATA CONNECTOR - AVVIATO');
        console.log('█'.repeat(80));
        logger.info(`Universal Data Connector API server listening on port ${this.port}`);
        logger.info('Available endpoints:');
        logger.info(`  Health Check: http://localhost:${this.port}/health`);
        logger.info(`  API Base: http://localhost:${this.port}/api`);
        logger.info(`  WebSocket: ws://localhost:${this.wsPort}`);
        console.log('█'.repeat(80));
      });
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

      if (this.wsServer) {
        this.wsServer.close();
      }

      if (this.server) {
        this.server.close();
      }

      logger.info('Universal Data Connector stopped successfully');
    } catch (error) {
      logger.error('Error stopping Universal Data Connector:', error);
      throw error;
    }
  }

  getEngine() {
    return this.engine;
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
