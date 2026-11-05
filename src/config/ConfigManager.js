const fs = require('fs').promises;
const path = require('path');
const Joi = require('joi');
const logger = require('../utils/logger');
const ConnectorFactory = require('../connectors/ConnectorFactory');
const { restoreMasked } = require('../api/secrets');

// Schema per validazione configurazione sources
const sourceConfigSchema = Joi.object({
  sources: Joi.array().items(
    Joi.object({
      id: Joi.string().required(),
      type: Joi.string().valid(...ConnectorFactory.getSupportedTypes()).required(),
      enabled: Joi.boolean().default(true),
      name: Joi.string().optional(),
      description: Joi.string().optional(),
      // Broker tenant receiving this source's devices as entities
      tenant: Joi.string().pattern(/^[a-z0-9][a-z0-9_-]{0,62}$/).default('default'),
      // Optional prefix for the entity ids of this source's devices
      entityIdPrefix: Joi.string().max(64).allow('').optional(),
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

/** Configuration error caused by the input (the API answers 400). */
function invalid(message) {
  const error = new Error(message);
  error.status = 400;
  return error;
}

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

      this.sources = this.validateSources(config.sources);
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

  /**
   * Validates a complete list of sources (schema, unique ids) and returns it
   * with defaults applied. Throws a 400 error when invalid.
   */
  validateSources(sources) {
    const { error, value } = sourceConfigSchema.validate({ sources });
    if (error) {
      throw invalid(`Invalid configuration (validation): ${error.details[0].message}`);
    }
    const seen = new Set();
    for (const source of value.sources) {
      if (seen.has(source.id)) throw invalid(`Invalid configuration (validation): duplicate source id '${source.id}'`);
      seen.add(source.id);
    }
    return value.sources;
  }

  /**
   * Replaces all sources: validated first, so an invalid list never reaches the file.
   * Masked secrets coming back from the UI keep their stored value.
   */
  async replaceSources(sources) {
    if (!Array.isArray(sources)) throw invalid('Invalid configuration (validation): sources must be an array');
    const validated = this.validateSources(restoreMasked(sources, this.sources));
    await this.saveSourcesConfig(validated);
    this.sources = validated;
    return validated;
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

    if (updates.id !== undefined && updates.id !== id) {
      throw invalid('Invalid source configuration: the id cannot be changed');
    }

    // Merge updates with existing source; masked secrets keep their stored value
    const current = this.sources[sourceIndex];
    const updatedSource = { ...current, ...restoreMasked(updates, current) };

    // Validate updated source
    const { error } = sourceConfigSchema.validate({ sources: [updatedSource] });
    if (error) {
      throw invalid(`Invalid source configuration: ${error.details[0].message}`);
    }

    // Save to file before changing the in-memory list
    const sources = this.sources.slice();
    sources[sourceIndex] = updatedSource;
    await this.saveSourcesConfig(sources);
    this.sources = sources;

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
      throw invalid(`Invalid source configuration: ${error.details[0].message}`);
    }

    // Save to file before changing the in-memory list
    const sources = [...this.sources, sourceConfig];
    await this.saveSourcesConfig(sources);
    this.sources = sources;

    logger.info(`Added new source configuration: ${sourceConfig.id}`);
    return sourceConfig;
  }

  async removeSource(id) {
    const sourceIndex = this.sources.findIndex(source => source.id === id);
    if (sourceIndex === -1) {
      throw new Error(`Source with id '${id}' not found`);
    }

    const removedSource = this.sources[sourceIndex];

    // Save to file before changing the in-memory list
    const sources = this.sources.filter((_, i) => i !== sourceIndex);
    await this.saveSourcesConfig(sources);
    this.sources = sources;

    logger.info(`Removed source configuration: ${id}`);
    return removedSource;
  }

  /**
   * Writes sources.json atomically (temporary file + rename): the config
   * directory is shared by every instance, a reader never sees a partial file.
   */
  async saveSourcesConfig(sources = this.sources) {
    const tmp = `${this.sourcesConfigFile}.${process.pid}.tmp`;
    await fs.writeFile(tmp, JSON.stringify({ sources }, null, 2), 'utf8');
    await fs.rename(tmp, this.sourcesConfigFile);
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
