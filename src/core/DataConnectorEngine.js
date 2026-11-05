const EventEmitter = require('events');
const logger = require('../utils/logger');
const configManager = require('../config/ConfigManager');
const ConnectorFactory = require('../connectors/ConnectorFactory');
const DataProcessor = require('./DataProcessor');
const DataStore = require('./DataStore');
// Transports are loaded lazily inside initializeTransports() to avoid slow startup
const { MappingEngine } = require('../mappingTools');
const path = require('path');
const fs = require('fs').promises;

/**
 * DataConnectorEngine - Main engine orchestrating data collection and distribution
 * 
 * Nuove funzionalità:
 * - Discovery automatica dispositivi
 * - Mapping con configurazione da mapping.json
 * - Multi-transport: NATS, MQTT, HTTP Push
 * - Output in JSON o TOON format
 */
class DataConnectorEngine extends EventEmitter {
  constructor(storageConfig = null, mappingConfig = null) {
    super();
    this.connectors = new Map();
    this.dataProcessor = new DataProcessor();
    this.dataStore = new DataStore(storageConfig);
    
    // Initialize Mapping Engine
    this.mappingEngine = new MappingEngine(mappingConfig || {
      namespace: 'urn:ngsi-ld:industry50',
      mappingConfigPath: path.join(process.cwd(), 'config', 'mapping.json')
    });

    // Initialize Transports
    this.transports = {
      nats: null,
      mqtt: null,
      http: null
    };
    
    this.transportConfig = null;
    this.outputFormat = 'json'; // 'json' or 'toon'
    
    this.isRunning = false;
    this.stats = {
      totalDataPoints: 0,
      totalErrors: 0,
      startTime: null,
      lastDataReceived: null,
      transportStats: {}
    };
  }

  async initialize() {
    try {
      logger.info('Initializing Data Connector Engine v2.0...');

      // Initialize data processor
      await this.dataProcessor.initialize();

      // Initialize data store (buffer)
      await this.dataStore.initialize();

      // Load transport configuration from mapping.json
      await this.loadTransportConfig();

      // Initialize transports
      await this.initializeTransports();

      // Setup data processor event handlers
      this.dataProcessor.on('processed', (data) => {
        this.handleProcessedData(data);
      });

      this.dataProcessor.on('error', (error) => {
        logger.error('Data processor error:', error);
        this.stats.totalErrors++;
        this.emit('processingError', error);
      });

      // Load and initialize connectors
      await this.initializeConnectors();

      logger.info('Data Connector Engine v2.0 initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Data Connector Engine:', error);
      throw error;
    }
  }

  /**
   * Load transport configuration from mapping.json
   */
  async loadTransportConfig() {
    try {
      const mappingPath = path.join(process.cwd(), 'config', 'mapping.json');
      const data = await fs.readFile(mappingPath, 'utf8');
      const config = JSON.parse(data);
      
      this.transportConfig = config.transport || {};
      this.outputFormat = config.outputFormats?.json?.enabled ? 'json' : 
                         config.outputFormats?.toon?.enabled ? 'toon' : 'json';
      
      logger.info('Transport configuration loaded from mapping.json');
    } catch (error) {
      logger.warn('Could not load transport config, using defaults');
      this.transportConfig = {
        nats: { enabled: false },
        mqtt: { enabled: false },
        http: { enabled: false }
      };
    }
  }

  /**
   * Initialize transport layers
   */
  async initializeTransports() {
    // Initialize NATS Transport
    if (this.transportConfig.nats?.enabled) {
      try {
        const NatsTransport = require('../transport/NatsTransport');
        this.transports.nats = new NatsTransport(this.transportConfig.nats);
        await this.transports.nats.initialize();
        await this.transports.nats.connect();
        
        this.transports.nats.on('connected', () => {
          logger.info('NATS Transport connected');
        });

        this.transports.nats.on('error', (err) => {
          logger.warn('NATS Transport error:', err);
        });

        logger.info('NATS transport initialized');
      } catch (error) {
        logger.error('Failed to initialize NATS transport:', error);
      }
    }

    // Initialize MQTT Transport
    if (this.transportConfig.mqtt?.enabled) {
      try {
        const MqttTransport = require('../transport/MqttTransport');
        this.transports.mqtt = new MqttTransport({
          ...this.transportConfig.mqtt,
          format: this.transportConfig.mqtt.format || this.outputFormat
        });
        await this.transports.mqtt.initialize();
        await this.transports.mqtt.connect();
        
        logger.info('MQTT transport initialized');
      } catch (error) {
        logger.error('Failed to initialize MQTT transport:', error);
      }
    }

    // Initialize HTTP Push Transport
    if (this.transportConfig.http?.enabled) {
      try {
        const HttpPushTransport = require('../transport/HttpPushTransport');
        this.transports.http = new HttpPushTransport({
          ...this.transportConfig.http,
          format: this.transportConfig.http.format || this.outputFormat
        });
        await this.transports.http.initialize();
        await this.transports.http.connect();
        
        logger.info('HTTP Push transport initialized');
      } catch (error) {
        logger.error('Failed to initialize HTTP transport:', error);
      }
    }
  }

  async initializeConnectors() {
    const sources = configManager.getEnabledSources();

    logger.info(`Initializing ${sources.length} connectors...`);

    for (const sourceConfig of sources) {
      try {
        await this.createConnector(sourceConfig);
      } catch (error) {
        logger.error(`Failed to initialize connector '${sourceConfig.id}':`, error);
        // Continue with other connectors even if one fails
      }
    }
  }

  async createConnector(sourceConfig) {
    try {
      // Create connector instance
      const connector = ConnectorFactory.create(sourceConfig.type, sourceConfig);

      // Setup event handlers
      this.setupConnectorEventHandlers(connector, sourceConfig.id);

      // Initialize connector
      await connector.initialize();

      // Store connector
      this.connectors.set(sourceConfig.id, {
        connector,
        config: sourceConfig,
        status: 'initialized',
        lastActivity: new Date(),
        stats: {
          dataPoints: 0,
          errors: 0,
          connections: 0
        }
      });

      logger.info(`Connector '${sourceConfig.id}' (${sourceConfig.type}) initialized successfully`);
      this.emit('connectorInitialized', sourceConfig.id);

    } catch (error) {
      logger.error(`Failed to create connector '${sourceConfig.id}':`, error);
      throw error;
    }
  }

  // Dynamic configuration reload methods
  async reloadSourcesConfiguration(newSources = null) {
    try {
      logger.info('Reloading sources configuration...');

      // Save new sources configuration if provided (validated before it is written)
      if (newSources) {
        await configManager.replaceSources(newSources);
        logger.info('New sources configuration saved');
      } else {
        // (Re)load from file: validates and applies defaults
        await configManager.loadSourcesConfig();
        logger.info('Sources configuration reloaded from file');
      }

      // Get current enabled sources
      const enabledSources = configManager.getEnabledSources();
      const currentConnectorIds = Array.from(this.connectors.keys());
      const newConnectorIds = enabledSources.map(s => s.id);

      // Stop and remove connectors that are no longer configured
      for (const connectorId of currentConnectorIds) {
        if (!newConnectorIds.includes(connectorId)) {
          await this.removeConnector(connectorId);
        }
      }

      // Update or create connectors
      for (const sourceConfig of enabledSources) {
        const existingConnector = this.connectors.get(sourceConfig.id);

        if (existingConnector) {
          // Check if configuration changed
          const configChanged = JSON.stringify(existingConnector.config) !== JSON.stringify(sourceConfig);

          if (configChanged) {
            logger.info(`Updating connector '${sourceConfig.id}' due to configuration change`);
            await this.updateConnector(sourceConfig.id, sourceConfig);
          }
        } else {
          // Create new connector
          logger.info(`Creating new connector '${sourceConfig.id}'`);
          await this.createConnector(sourceConfig);

          // Auto-start if engine is running
          if (this.isRunning && this.canRun(sourceConfig.id)) {
            await this.startConnector(sourceConfig.id);
          }
        }
      }

      this.emit('sourcesConfigurationReloaded', {
        totalSources: enabledSources.length,
        activeConnectors: this.connectors.size
      });

      logger.info('Sources configuration reloaded successfully');
      return {
        success: true,
        totalSources: enabledSources.length,
        activeConnectors: this.connectors.size,
        message: 'Sources configuration reloaded successfully'
      };

    } catch (error) {
      logger.error('Failed to reload sources configuration:', error);
      this.emit('sourcesConfigurationError', error);
      throw error;
    }
  }

  async updateConnector(connectorId, newConfig) {
    try {
      const existingConnector = this.connectors.get(connectorId);
      if (!existingConnector) {
        throw new Error(`Connector '${connectorId}' not found`);
      }

      // Remove old connector (stops it if connected)
      await this.removeConnector(connectorId);

      // Create new connector with updated config
      await this.createConnector(newConfig);

      // Start it whenever the engine runs and the connector belongs to this instance,
      // also if the old one was offline or reconnecting (the new config may fix it)
      if (this.isRunning && this.canRun(connectorId)) {
        await this.startConnector(connectorId);
      }

      logger.info(`Connector '${connectorId}' updated successfully`);

    } catch (error) {
      logger.error(`Failed to update connector '${connectorId}':`, error);
      throw error;
    }
  }

  async removeConnector(connectorId) {
    try {
      const connectorInfo = this.connectors.get(connectorId);
      if (!connectorInfo) {
        logger.warn(`Connector '${connectorId}' not found for removal`);
        return;
      }

      // Stop connector if running
      if (connectorInfo.status === 'connected') {
        await this.stopConnector(connectorId);
      }

      // Cleanup connector
      if (connectorInfo.connector && typeof connectorInfo.connector.cleanup === 'function') {
        await connectorInfo.connector.cleanup();
      }

      // Remove from map
      this.connectors.delete(connectorId);

      logger.info(`Connector '${connectorId}' removed successfully`);
      this.emit('connectorRemoved', connectorId);

    } catch (error) {
      logger.error(`Failed to remove connector '${connectorId}':`, error);
      throw error;
    }
  }

  async reloadStorageConfiguration(newStorageConfig = null) {
    const StorageConfigManager = require('../config/StorageConfigManager');
    try {
      logger.info('Reloading storage configuration...');

      // Without an explicit configuration, reload the one saved in config/storage.json
      let storageConfig;
      if (newStorageConfig) {
        const normalized = StorageConfigManager.normalize(newStorageConfig.type, newStorageConfig.config);
        if (!normalized.valid) {
          throw new Error(`Invalid storage configuration (validation): ${normalized.errors.map(e => e.message).join('; ')}`);
        }
        storageConfig = { type: normalized.type, config: normalized.config };
      } else {
        await StorageConfigManager.reloadConfig();
        storageConfig = StorageConfigManager.getStorageConfig();
      }

      // Connect the new storage first: if it fails, the current one stays active
      const newDataStore = new DataStore(storageConfig, { fallbackToMemory: false });
      await newDataStore.initialize();

      // Persist only once the new storage is known to work
      if (newStorageConfig) {
        await StorageConfigManager.updateStorageConfig(storageConfig);
        logger.info('New storage configuration saved');
      }

      // Carry over the in-memory buffer (external storages already hold their data)
      const oldDataStore = this.dataStore;
      const buffered = oldDataStore && !oldDataStore.useExternalStorage ? oldDataStore.data.slice().reverse() : [];
      if (buffered.length > 0) {
        logger.info(`Restoring ${buffered.length} buffered data points to new storage`);
        for (const dataPoint of buffered) {
          await newDataStore.store(dataPoint);
        }
      }

      this.dataStore = newDataStore;
      if (oldDataStore) {
        await oldDataStore.shutdown();
      }

      this.emit('storageConfigurationReloaded', { storageType: storageConfig?.type || 'memory' });
      logger.info('Storage configuration reloaded successfully');
      return {
        success: true,
        storageType: storageConfig?.type || 'memory',
        restoredDataPoints: buffered.length,
        message: 'Storage configuration reloaded successfully'
      };

    } catch (error) {
      logger.error('Failed to reload storage configuration:', error);
      this.emit('storageConfigurationError', error);
      throw error;
    }
  }

  setupConnectorEventHandlers(connector, sourceId) {
    connector.on('data', (data) => {
      this.handleConnectorData(sourceId, data);
    });

    connector.on('connected', () => {
      this.updateConnectorStatus(sourceId, 'connected');
      logger.info(`Connector '${sourceId}' connected`);
    });

    connector.on('disconnected', () => {
      this.updateConnectorStatus(sourceId, 'disconnected');
      logger.warn(`Connector '${sourceId}' disconnected`);
    });

    connector.on('error', (error) => {
      this.handleConnectorError(sourceId, error);
    });

    connector.on('reconnecting', () => {
      this.updateConnectorStatus(sourceId, 'reconnecting');
      logger.info(`Connector '${sourceId}' reconnecting...`);
    });

    // Discovery Events for Auto-Mapping
    connector.on('nodesDiscovered', (discoveryData) => {
      logger.info(`Connector '${sourceId}' discovered nodes. Processing for mapping...`);
      this.emit('nodesDiscovered', discoveryData);
      // Here we could automatically register them if we had an auto-map policy
    });

    connector.on('registersDiscovered', (discoveryData) => {
      logger.info(`Connector '${sourceId}' discovered registers. Processing for mapping...`);
      this.emit('registersDiscovered', discoveryData);
    });
  }

  async handleIncomingData(sourceId, data) {
    try {
      // 📊 LOG 1: Dati grezzi ricevuti dalla sorgente (debug: i connettori fieldbus campionano ogni 100ms)
      logger.debug(`📥 DATI RICEVUTI da sorgente '${sourceId}': ${JSON.stringify(data)}`);

      // Map to Universal Data Model ({ id, type, measurements, metadata })
      const sourceConfig = this.connectors.get(sourceId)?.config;
      const sourceType = sourceConfig?.type || data.type || 'generic';
      const context = { sourceId, sourceType };

      // 🔥 AUTO-MAPPING: salva la configurazione del dispositivo la prima volta che viene visto
      if (sourceConfig?.autoMapping) {
        await this.mappingEngine.discoverDevice(data, sourceType, context);
      }

      const mappedData = await this.mappingEngine.mapData(data, sourceType, context);

      if (mappedData) {
        // 📊 LOG 2: Dati dopo il mapping
        logger.debug(`✅ DATI MAPPATI per sorgente '${sourceId}': ${mappedData.measurements?.length || 0} misure`);

        // Emit mapped data event
        this.emit('data', {
          sourceId,
          originalData: data,
          mappedData,
          timestamp: new Date().toISOString()
        });

        // Store data in configured storages
        if (this.dataStore) {
          await this.dataStore.store({
            sourceId: sourceId,
            timestamp: new Date().toISOString(),
            data: mappedData
          });
        }
      } else {
        logger.debug(`No mapping configured for source '${sourceId}', data not processed`);
      }

    } catch (error) {
      logger.error(`Error handling data from source '${sourceId}':`, error);
      this.emit('dataError', sourceId, error);
    }
  }

  handleConnectorData(sourceId, data) {
    try {
      const connectorInfo = this.connectors.get(sourceId);
      if (!connectorInfo) {
        logger.warn(`Received data from unknown connector: ${sourceId}`);
        return;
      }

      // Update connector stats
      connectorInfo.stats.dataPoints++;
      connectorInfo.lastActivity = new Date();

      // Update global stats
      this.stats.totalDataPoints++;
      this.stats.lastDataReceived = new Date();

      // 🔥 VERIFICA CHE MAPPING ENGINE SIA INIZIALIZZATO
      if (!this.mappingEngine) {
        logger.error('MappingEngine not initialized');
        return;
      }

      /* // Enrich data with metadata
      const enrichedData = {
        sourceId,
        sourceType: connectorInfo.config.type,
        timestamp: new Date().toISOString(),
        data: data,
        metadata: {
          sourceName: connectorInfo.config.name,
          sourceDescription: connectorInfo.config.description
        }
      }; */

      /* // Map data to Universal Data Model
      try {
        const mappingContext = {
          sourceType: connectorInfo.config.type,
          entityId: `${sourceId}_entity`,
          entityType: connectorInfo.config.entityType
        };

        this.mappingEngine.mapData(data, mappingContext);
        logger.debug(`Data from '${sourceId}' mapped to Universal Data Model`);
      } catch (mappingError) {
        logger.warn(`Failed to map data from '${sourceId}' to Universal Data Model:`, mappingError.message);
        // Continue processing even if mapping fails
      }

      // Send to data processor
      this.dataProcessor.process(enrichedData); */
      // 🔄 CHIAMA IL METODO CON LOGGING
      this.handleIncomingData(sourceId, data);

      /* // Emit raw data event
      this.emit('rawData', enrichedData); */

    } catch (error) {
      logger.error(`Error handling data from connector '${sourceId}':`, error);
      this.stats.totalErrors++;
    }
  }

  determineSubject(data) {
    // 1. Check for AAS specific metadata if mapped
    if (data.metadata && data.metadata.aas && data.metadata.aas.assetId && data.metadata.aas.submodelId) {
      return `aas.update.${data.metadata.aas.assetId}.${data.metadata.aas.submodelId}`;
    }

    // 2. Fallback to generic telemetry subject
    return `ingestor.telemetry.${data.sourceId}`;
  }

  /**
   * Handle processed data - map and distribute to transports
   */
  async handleProcessedData(processedData) {
    try {
      const context = {
        sourceId: processedData.sourceId,
        sourceType: processedData.sourceType || processedData.type,
        endpoint: processedData.endpoint,
        host: processedData.host,
        port: processedData.port,
        broker: processedData.broker,
        topic: processedData.topic
      };

      // Map data using MappingEngine
      const mappedDevice = await this.mappingEngine.mapData(
        processedData.data || processedData,
        processedData.sourceType || processedData.type || 'generic',
        context
      );

      if (!mappedDevice) {
        logger.warn('Mapping returned null, skipping transport');
        return;
      }

      // Update stats
      this.stats.totalDataPoints++;
      this.stats.lastDataReceived = new Date().toISOString();

      // Publish to all enabled transports
      await this.publishToTransports(mappedDevice);

      // Emit event
      this.emit('data', mappedDevice);

    } catch (error) {
      logger.error('Error handling processed data:', error);
      this.stats.totalErrors++;
      
      // Buffer on failure if needed
      try {
        await this.dataStore.store({
          ...processedData,
          _bufferedAt: new Date().toISOString(),
          _error: error.message
        });
      } catch (storeError) {
        logger.error('Failed to buffer data:', storeError);
      }
    }
  }

  /**
   * Publish device data to all enabled transports
   */
  async publishToTransports(deviceData) {
    const results = {
      nats: false,
      mqtt: false,
      http: false
    };

    // Publish to NATS
    if (this.transports.nats && this.transports.nats.isConnected()) {
      try {
        const subject = this.transportConfig.nats.subject || 'udc.data';
        await this.transports.nats.publish(subject, deviceData);
        results.nats = true;
        logger.debug(`Published to NATS: ${deviceData.id}`);
      } catch (error) {
        logger.error('Failed to publish to NATS:', error);
      }
    }

    // Publish to MQTT
    if (this.transports.mqtt && this.transports.mqtt.isConnected()) {
      try {
        await this.transports.mqtt.publish(deviceData);
        results.mqtt = true;
        logger.debug(`Published to MQTT: ${deviceData.id}`);
      } catch (error) {
        logger.error('Failed to publish to MQTT:', error);
      }
    }

    // Publish to HTTP Push
    if (this.transports.http && this.transports.http.isConnected()) {
      try {
        await this.transports.http.publish(deviceData);
        results.http = true;
        logger.debug(`Published to HTTP: ${deviceData.id}`);
      } catch (error) {
        logger.error('Failed to publish to HTTP:', error);
      }
    }

    // Update transport stats
    this.stats.transportStats = results;

    return results;
  }

  handleConnectorError(sourceId, error) {
    const connectorInfo = this.connectors.get(sourceId);
    if (connectorInfo) {
      connectorInfo.stats.errors++;
    }

    this.stats.totalErrors++;

    logger.error(`Connector '${sourceId}' error:`, error);
    this.emit('connectorError', sourceId, error);
  }

  updateConnectorStatus(sourceId, status) {
    const connectorInfo = this.connectors.get(sourceId);
    if (connectorInfo) {
      const oldStatus = connectorInfo.status;
      connectorInfo.status = status;
      connectorInfo.lastActivity = new Date();

      if (status === 'connected') {
        connectorInfo.stats.connections++;
      }

      if (oldStatus !== status) {
        this.emit('sourceStatusChanged', sourceId, status);
      }
    }
  }

  /**
   * Runtime status of every connector, or of one connector (null if unknown)
   * @param {string} [sourceId]
   */
  getConnectorStatus(sourceId) {
    const status = {};

    for (const [connectorId, connectorInfo] of this.connectors.entries()) {
      if (sourceId !== undefined && connectorId !== sourceId) continue;
      status[connectorId] = {
        status: connectorInfo.status,
        type: connectorInfo.config.type,
        enabled: connectorInfo.config.enabled,
        lastActivity: connectorInfo.lastActivity,
        stats: {
          dataPoints: connectorInfo.stats.dataPoints,
          errors: connectorInfo.stats.errors,
          connections: connectorInfo.stats.connections
        }
      };
    }

    return sourceId !== undefined ? status[sourceId] || null : status;
  }

  getStatus() {
    const uptime = this.stats.startTime ? Date.now() - this.stats.startTime.getTime() : 0;
    return {
      isRunning: this.isRunning,
      natsConnected: this.transports.nats ? this.transports.nats.isConnected() : false,
      uptime,
      connectors: this.getConnectorStatus(),
      stats: {
        totalDataPoints: this.stats.totalDataPoints,
        totalErrors: this.stats.totalErrors,
        startTime: this.stats.startTime,
        lastDataReceived: this.stats.lastDataReceived,
        connectorCount: this.connectors.size,
        uptime
      }
    };
  }


  /**
 * Get latest data from all sources or specific source
 * @param {string} sourceId - Optional source ID filter
 * @param {number} limit - Maximum number of records
 * @returns {Array} Array of data records
 */
  /**
   * Latest data points (newest first), from the active storage
   * @param {string|null} sourceId - Optional source filter
   * @param {number} limit - Maximum number of records
   * @returns {Promise<Array>} Array of data records
   */
  async getLatestData(sourceId = null, limit = 100) {
    try {
      if (!this.dataStore) {
        logger.warn('DataStore not available');
        return [];
      }
      const data = sourceId
        ? await this.dataStore.getBySource(sourceId, limit)
        : await this.dataStore.getLatest(limit);
      return Array.isArray(data) ? data : [];
    } catch (error) {
      logger.error('Error getting latest data:', error);
      return [];
    }
  }

  /**
   * Get data by source ID
   * @param {string} sourceId - Source ID
   * @param {number} limit - Maximum number of records
   * @returns {Promise<Array>} Array of data records
   */
  getDataBySource(sourceId, limit = 100) {
    return this.getLatestData(sourceId, limit);
  }

  // Configuration management helpers
  getEnabledSources() {
    return configManager.getEnabledSources();
  }

  getAllSources() {
    return configManager.getSources();
  }

  getStorageInfo() {
    if (this.dataStore && typeof this.dataStore.getStorageInfo === 'function') {
      return this.dataStore.getStorageInfo();
    }

    return {
      type: 'unknown',
      status: 'unknown'
    };
  }

  async start() {
    if (this.isRunning) {
      logger.warn('Data Connector Engine is already running');
      return;
    }

    try {
      logger.info('Starting Data Connector Engine...');

      this.stats.startTime = new Date();
      this.isRunning = true;

      // Transports are already connected during initializeTransports()
      // Log active transports
      const activeTransports = Object.entries(this.transports)
        .filter(([, t]) => t !== null)
        .map(([name]) => name);
      if (activeTransports.length > 0) {
        logger.info(`Active transports: ${activeTransports.join(', ')}`);
      }

      // Start all connectors this instance may run (in a cluster each runs on one instance)
      const startPromises = Array.from(this.connectors.values()).filter(c => this.canRun(c.config.id)).map(async (connectorInfo) => {
        try {
          await connectorInfo.connector.start();
          logger.info(`Started connector '${connectorInfo.config.id}'`);
          logger.info('🎯 MONITORING ATTIVO - In attesa di dati dal server OPC UA...');
        } catch (error) {
          logger.error(`Failed to start connector '${connectorInfo.config.id}':`, error);
        }
      });

      await Promise.allSettled(startPromises);

      logger.info('Data Connector Engine started successfully');
      this.emit('started');

    } catch (error) {
      logger.error('Failed to start Data Connector Engine:', error);
      this.isRunning = false;
      throw error;
    }
  }

  async stop() {
    if (!this.isRunning) {
      logger.warn('Data Connector Engine is not running');
      return;
    }

    try {
      logger.info('Stopping Data Connector Engine...');

      this.isRunning = false;

      // Stop all connectors
      const stopPromises = Array.from(this.connectors.values()).map(async (connectorInfo) => {
        try {
          await connectorInfo.connector.stop();
          logger.info(`Stopped connector '${connectorInfo.config.id}'`);
        } catch (error) {
          logger.error(`Failed to stop connector '${connectorInfo.config.id}':`, error);
        }
      });

      await Promise.allSettled(stopPromises);

      // Close output transports
      for (const transport of Object.values(this.transports)) {
        if (transport && typeof transport.close === 'function') {
          await transport.close().catch(error => logger.warn(`Failed to close transport: ${error.message}`));
        }
      }

      // Close storage (flushes connections, stops retry timers)
      if (this.dataStore) {
        await this.dataStore.shutdown();
      }

      logger.info('Data Connector Engine stopped successfully');
      this.emit('stopped');

    } catch (error) {
      logger.error('Failed to stop Data Connector Engine:', error);
      throw error;
    }
  }

  /**
   * Cluster gate: set by the broker's cluster coordinator so that each connector
   * runs on exactly one instance. Without a gate every connector runs here.
   */
  setConnectorGate(gate) {
    this.connectorGate = gate;
  }

  canRun(sourceId) {
    return !this.connectorGate || this.connectorGate(sourceId);
  }

  /** True when the connector exists and is meant to run on this instance. */
  isConnectorActive(sourceId) {
    return this.connectors.has(sourceId) && this.canRun(sourceId);
  }

  async startConnector(sourceId) {
    const connectorInfo = this.connectors.get(sourceId);
    if (!connectorInfo) {
      throw new Error(`Connector '${sourceId}' not found`);
    }
    if (!this.canRun(sourceId)) {
      throw new Error(`Connector '${sourceId}' runs on another cluster instance`);
    }

    try {
      await connectorInfo.connector.start();
      logger.info(`Started connector '${sourceId}'`);
      return true;
    } catch (error) {
      logger.error(`Failed to start connector '${sourceId}':`, error);
      throw error;
    }
  }

  async stopConnector(sourceId) {
    const connectorInfo = this.connectors.get(sourceId);
    if (!connectorInfo) {
      throw new Error(`Connector '${sourceId}' not found`);
    }

    try {
      await connectorInfo.connector.stop();
      this.updateConnectorStatus(sourceId, 'disconnected');
      logger.info(`Stopped connector '${sourceId}'`);
      return true;
    } catch (error) {
      logger.error(`Failed to stop connector '${sourceId}':`, error);
      throw error;
    }
  }

  async restartConnector(sourceId) {
    await this.stopConnector(sourceId);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
    await this.startConnector(sourceId);
  }

  // Mapping Engine Methods
  getMappingEngine() {
    return this.mappingEngine;
  }

  exportMappedDataToJSON(options = {}) {
    return this.mappingEngine.exportData('json', options);
  }

  exportMappedDataToTOON(options = {}) {
    return this.mappingEngine.exportToTOON(options);
  }

  getMappedEntity(entityId) {
    return this.mappingEngine.getEntity(entityId);
  }

  getMappedEntitiesByType(type) {
    return this.mappingEngine.getEntitiesByType(type);
  }

  getAllMappedEntities() {
    return this.mappingEngine.getAllEntities();
  }

  getMappingStatistics() {
    return this.mappingEngine.getStatistics();
  }

  clearMappedData() {
    // Runtime data only: saved device mappings must survive (they are written back to mapping.json)
    this.mappingEngine.clearData();
    logger.info('Mapped data cleared');
  }

  async reloadConfiguration() {
    logger.info('Reloading configuration...');

    try {
      const wasRunning = this.isRunning;

      // Stop and dispose the current connectors; transports and storage stay up
      for (const connectorId of Array.from(this.connectors.keys())) {
        await this.removeConnector(connectorId);
      }

      // Reload config
      await configManager.reloadConfig();

      // Reinitialize connectors
      await this.initializeConnectors();

      // Restart the connectors this instance runs
      if (wasRunning) {
        for (const connectorInfo of this.connectors.values()) {
          if (!this.canRun(connectorInfo.config.id)) continue;
          await this.startConnector(connectorInfo.config.id)
            .catch(error => logger.error(`Failed to start connector '${connectorInfo.config.id}': ${error.message}`));
        }
      }

      logger.info('Configuration reloaded successfully');
      this.emit('configurationReloaded');

    } catch (error) {
      logger.error('Failed to reload configuration:', error);
      throw error;
    }
  }
}



module.exports = DataConnectorEngine;
