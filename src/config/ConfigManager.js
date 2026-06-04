const fs = require('fs').promises;
const path = require('path');
const Joi = require('joi');
const logger = require('../utils/logger');

// Schema per validazione configurazione sources
const sourceConfigSchema = Joi.object({
  sources: Joi.array().items(
    Joi.object({
      id: Joi.string().required(),
      type: Joi.string().valid('opcua', 'mqtt', 'http', 'modbus', 'i3x').required(),
      enabled: Joi.boolean().default(true),
      name: Joi.string().optional(),
      description: Joi.string().optional(),
      config: Joi.object().required(),
      tags: Joi.array().items(
        Joi.object({
          name: Joi.string().required(),
          address: Joi.string().required(),
          dataType: Joi.string().valid('int16', 'uint16', 'int32', 'uint32', 'float', 'double', 'boolean', 'string').required(),
          writable: Joi.boolean().default(false),
          description: Joi.string().optional(),
          scale: Joi.number().optional(),
          offset: Joi.number().optional(),
          unit: Joi.string().optional()
        })
      ).optional(),
      pollInterval: Joi.number().integer().min(100).optional(),
      retryConfig: Joi.object({
        enabled: Joi.boolean().default(true),
        maxRetries: Joi.number().integer().min(0).default(3),
        retryDelay: Joi.number().integer().min(100).default(5000)
      }).default(),
      dataProcessing: Joi.object({
        enabled: Joi.boolean().default(true),
        transforms: Joi.array().items(Joi.string()).default([]),
        validation: Joi.object().optional()
      }).default(),
      autoMapping: Joi.boolean().default(false)
    })
  ).required()
});

class ConfigManager {
  constructor() {
    this.configPath = path.join(process.cwd(), 'config');
    this.sourcesConfigFile = path.join(this.configPath, 'sources.json');
    this.sources = [];
    this.initialized = false;
  }

  async initialize() {
    try {
      // Create config directory if it doesn't exist
      await this.ensureConfigDirectory();

      // Load or create default sources configuration
      await this.loadSourcesConfig();

      this.initialized = true;
      logger.info('Configuration manager initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize configuration manager:', error);
      throw error;
    }
  }

  async ensureConfigDirectory() {
    try {
      await fs.access(this.configPath);
    } catch (error) {
      await fs.mkdir(this.configPath, { recursive: true });
      logger.info('Created config directory');
    }
  }

  async loadSourcesConfig() {
    try {
      // Check if sources config file exists
      await fs.access(this.sourcesConfigFile);

      // Read and parse the configuration
      const configData = await fs.readFile(this.sourcesConfigFile, 'utf8');
      const config = JSON.parse(configData);

      // Validate configuration
      const { error, value } = sourceConfigSchema.validate(config);
      if (error) {
        throw new Error(`Invalid configuration: ${error.details[0].message}`);
      }

      this.sources = value.sources;
      logger.info(`Loaded ${this.sources.length} source configurations`);

    } catch (error) {
      if (error.code === 'ENOENT') {
        // File doesn't exist, create default configuration
        logger.info('Sources configuration file not found, creating default configuration');
        await this.createDefaultConfig();
      } else {
        throw error;
      }
    }
  }

  async createDefaultConfig() {
    const defaultConfig = {
      sources: [
        {
          id: "example-opcua",
          type: "opcua",
          enabled: false,
          name: "Example OPC UA Server",
          description: "Example OPC UA connection configuration",
          config: {
            endpoint: "opc.tcp://localhost:4840",
            securityPolicy: "None",
            securityMode: "None",
            nodes: [
              "ns=1;s=Temperature",
              "ns=1;s=Pressure",
              "ns=1;s=Flow"
            ],
            subscriptionOptions: {
              requestedPublishingInterval: 1000,
              requestedLifetimeCount: 60,
              requestedMaxKeepAliveCount: 10,
              maxNotificationsPerPublish: 10,
              publishingEnabled: true,
              priority: 10
            }
          }
        },
        {
          id: "example-mqtt",
          type: "mqtt",
          enabled: false,
          name: "Example MQTT Broker",
          description: "Example MQTT broker connection configuration",
          config: {
            broker: "mqtt://localhost:1883",
            clientId: "universal-data-connector",
            username: null,
            password: null,
            topics: [
              "sensors/+/temperature",
              "sensors/+/humidity",
              "machines/+/status"
            ],
            qos: 1,
            options: {
              keepalive: 60,
              connectTimeout: 30000,
              reconnectPeriod: 1000,
              clean: true
            }
          }
        },
        {
          id: "example-http",
          type: "http",
          enabled: false,
          name: "Example HTTP API",
          description: "Example HTTP REST API polling configuration",
          config: {
            url: "http://localhost:8080/api/sensors",
            method: "GET",
            headers: {
              "Content-Type": "application/json",
              "User-Agent": "Universal-Data-Connector/1.0"
            },
            authentication: {
              type: "none"
            },
            polling: {
              enabled: true,
              interval: 30000
            },
            timeout: 10000
          }
        }
      ]
    };

    await fs.writeFile(
      this.sourcesConfigFile,
      JSON.stringify(defaultConfig, null, 2),
      'utf8'
    );

    this.sources = defaultConfig.sources;
    logger.info('Created default sources configuration');
  }

  getSources() {
    return this.sources;
  }

  getSourceById(id) {
    return this.sources.find(source => source.id === id);
  }

  getEnabledSources() {
    return this.sources.filter(source => source.enabled);
  }

  getSourcesByType(type) {
    return this.sources.filter(source => source.type === type);
  }

  async updateSource(id, updates) {
    const sourceIndex = this.sources.findIndex(source => source.id === id);
    if (sourceIndex === -1) {
      throw new Error(`Source with id '${id}' not found`);
    }

    // Merge updates with existing source
    const updatedSource = { ...this.sources[sourceIndex], ...updates };

    // Validate updated source
    const { error } = sourceConfigSchema.validate({ sources: [updatedSource] });
    if (error) {
      throw new Error(`Invalid source configuration: ${error.details[0].message}`);
    }

    this.sources[sourceIndex] = updatedSource;

    // Save to file
    await this.saveSourcesConfig();

    logger.info(`Updated source configuration: ${id}`);
    return updatedSource;
  }

  async addSource(sourceConfig) {
    // Check if source with same id already exists
    if (this.getSourceById(sourceConfig.id)) {
      throw new Error(`Source with id '${sourceConfig.id}' already exists`);
    }

    // Validate source configuration
    const { error } = sourceConfigSchema.validate({ sources: [sourceConfig] });
    if (error) {
      throw new Error(`Invalid source configuration: ${error.details[0].message}`);
    }

    this.sources.push(sourceConfig);

    // Save to file
    await this.saveSourcesConfig();

    logger.info(`Added new source configuration: ${sourceConfig.id}`);
    return sourceConfig;
  }

  async removeSource(id) {
    const sourceIndex = this.sources.findIndex(source => source.id === id);
    if (sourceIndex === -1) {
      throw new Error(`Source with id '${id}' not found`);
    }

    const removedSource = this.sources.splice(sourceIndex, 1)[0];

    // Save to file
    await this.saveSourcesConfig();

    logger.info(`Removed source configuration: ${id}`);
    return removedSource;
  }

  async saveSourcesConfig() {
    const config = { sources: this.sources };
    await fs.writeFile(
      this.sourcesConfigFile,
      JSON.stringify(config, null, 2),
      'utf8'
    );
  }

  async reloadConfig() {
    logger.info('Reloading configuration...');
    await this.loadSourcesConfig();
    logger.info('Configuration reloaded successfully');
  }

  isInitialized() {
    return this.initialized;
  }
}

// Singleton instance
const configManager = new ConfigManager();

module.exports = configManager;
