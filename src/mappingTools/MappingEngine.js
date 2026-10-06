const logger = require('../utils/logger');
const UniversalDataModel = require('./UniversalDataModel');
const fs = require('fs').promises;
const path = require('path');

// Import all mappers
const {
  OPCUAMapper,
  ModbusMapper,
  AASMapper,
  MQTTMapper,
  GenericMapper
} = require('./mappers');

/**
 * MappingEngine - Core engine for data mapping and discovery
 * 
 * Gestisce:
 * - Discovery automatica dei dispositivi
 * - Salvataggio configurazione in mapping.json
 * - Mapping dati usando la configurazione
 * - Output in formato JSON o TOON
 */
class MappingEngine {
  constructor(config = {}) {
    this.config = config;
    this.mappingConfigPath = config.mappingConfigPath || 
                              path.join(process.cwd(), 'config', 'mapping.json');
    
    this.dataModel = new UniversalDataModel({
      source: 'universal-data-connector',
      namespace: config.namespace || 'urn:ngsi-ld:industry50'
    });

    this.mappers = new Map();
    this.mappingConfigs = new Map(); // Configurazioni di mapping per deviceId
    this.discoveryMode = config.discoveryMode !== false; // Discovery abilitata di default
    
    this.mappingStats = {
      totalMappings: 0,
      successfulMappings: 0,
      failedMappings: 0,
      discoveredDevices: 0,
      lastMappingTime: null
    };

    // Register default mappers
    this.registerDefaultMappers();
    
    // Load existing mapping configuration
    this.loadMappingConfig().catch(err => {
      logger.warn('Could not load mapping configuration:', err.message);
    });
    
    logger.info('Mapping Engine initialized with discovery support');
  }

  /**
   * Register default mappers for known protocols
   * @private
   */
  registerDefaultMappers() {
    this.registerMapper('opcua', new OPCUAMapper());
    this.registerMapper('modbus', new ModbusMapper());
    this.registerMapper('aas', new AASMapper());
    this.registerMapper('asset-administration-shell', new AASMapper());
    this.registerMapper('mqtt', new MQTTMapper());

    // Register generic mapper as fallback
    this.registerMapper('generic', new GenericMapper({ sourceType: 'generic' }));
    this.registerMapper('http', new GenericMapper({ sourceType: 'http' }));
    this.registerMapper('s7', new GenericMapper({ sourceType: 's7' }));
    this.registerMapper('bacnet', new GenericMapper({ sourceType: 'bacnet' }));
    this.registerMapper('fins', new GenericMapper({ sourceType: 'fins' }));
    this.registerMapper('melsec', new GenericMapper({ sourceType: 'melsec' }));
    this.registerMapper('cip', new GenericMapper({ sourceType: 'cip' }));
    this.registerMapper('serial', new GenericMapper({ sourceType: 'serial' }));

    logger.debug(`Registered ${this.mappers.size} default mappers`);
  }

  /**
   * Register a custom mapper for a specific source type
   * @param {string} sourceType - Source type identifier
   * @param {BaseMapper} mapper - Mapper instance
   */
  registerMapper(sourceType, mapper) {
    if (!mapper || typeof mapper.map !== 'function') {
      throw new Error('Mapper must implement map() method');
    }

    this.mappers.set(sourceType.toLowerCase(), mapper);
    logger.debug(`Mapper registered for source type: ${sourceType}`);
  }

  /**
   * Load mapping configuration from file
   */
  async loadMappingConfig() {
    try {
      const data = await fs.readFile(this.mappingConfigPath, 'utf8');
      const config = JSON.parse(data);
      
      if (config.devices && Array.isArray(config.devices)) {
        for (const deviceConfig of config.devices) {
          this.mappingConfigs.set(deviceConfig.id, deviceConfig);
        }
        logger.info(`Loaded ${config.devices.length} device configurations from ${this.mappingConfigPath}`);
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        logger.error('Error loading mapping configuration:', error);
      }
    }
  }

  /**
   * Save mapping configuration to file
   */
  async saveMappingConfig() {
    try {
      const devices = Array.from(this.mappingConfigs.values());
      
      const config = {
        version: '2.0.0',
        updated: new Date().toISOString(),
        discoveryMode: this.discoveryMode,
        devices: devices
      };

      // Ensure directory exists
      const dir = path.dirname(this.mappingConfigPath);
      await fs.mkdir(dir, { recursive: true });

      await fs.writeFile(
        this.mappingConfigPath, 
        JSON.stringify(config, null, 2), 
        'utf8'
      );
      
      logger.info(`Saved ${devices.length} device configurations to ${this.mappingConfigPath}`);
      return true;
    } catch (error) {
      logger.error('Error saving mapping configuration:', error);
      return false;
    }
  }

  /**
   * Process and map source data
   * @param {Object} sourceData - Raw data from source
   * @param {string} sourceType - Type of source (opcua, modbus, mqtt, etc.)
   * @param {Object} context - Additional context
   * @returns {Object} Mapped device data
   */
  async mapData(sourceData, sourceType, context = {}) {
    try {
      const mapper = this.getMapper(sourceType);
      
      if (!mapper) {
        throw new Error(`No mapper found for source type: ${sourceType}`);
      }

      const deviceId = mapper.extractDeviceId(sourceData, context);
      const mappingConfig = this.mappingConfigs.get(deviceId);

      let mappedData;

      if (mappingConfig && !this.discoveryMode) {
        // Use existing configuration
        mappedData = mapper.mapWithConfig(sourceData, mappingConfig, context);
        logger.debug(`Mapped data for device ${deviceId} using configuration`);
      } else {
        // Discovery mode or no config: auto-map
        mappedData = mapper.map(sourceData, context);
        
        // If discovery mode, save the discovered structure
        if (this.discoveryMode && !mappingConfig) {
          const discoveryConfig = mapper.discover(sourceData, context);
          this.mappingConfigs.set(deviceId, discoveryConfig);
          this.mappingStats.discoveredDevices++;
          
          // Auto-save after discovery
          await this.saveMappingConfig();
          
          logger.info(`Discovered and saved configuration for device ${deviceId}`);
        }
      }

      // Add to data model
      if (mappedData) {
        this.dataModel.addDevice(mappedData);
        this.mappingStats.successfulMappings++;
        this.mappingStats.totalMappings++;
        this.mappingStats.lastMappingTime = new Date().toISOString();
      }

      return mappedData;

    } catch (error) {
      this.mappingStats.failedMappings++;
      this.mappingStats.totalMappings++;
      logger.error(`Mapping error for ${sourceType}:`, error);
      throw error;
    }
  }

  /**
   * Get mapper for source type
   * @param {string} sourceType - Source type
   * @returns {BaseMapper} Mapper instance
   */
  getMapper(sourceType) {
    return this.mappers.get(sourceType.toLowerCase()) || 
           this.mappers.get('generic');
  }

  /**
   * Enable/disable discovery mode
   * @param {boolean} enabled - Discovery mode status
   */
  setDiscoveryMode(enabled) {
    this.discoveryMode = enabled;
    logger.info(`Discovery mode ${enabled ? 'enabled' : 'disabled'}`);
  }

  /**
   * Discover and persist the mapping configuration of a device the first time
   * it is seen (per-source "autoMapping"), without enabling global discovery mode.
   * @returns {Promise<Object|null>} The new configuration, or null if one already existed
   */
  async discoverDevice(sourceData, sourceType, context = {}) {
    const mapper = this.getMapper(sourceType);
    if (!mapper) return null;

    const deviceId = mapper.extractDeviceId(sourceData, context);
    if (this.mappingConfigs.has(deviceId)) return null;

    this.pendingDiscoveries = this.pendingDiscoveries || new Set();
    if (this.pendingDiscoveries.has(deviceId)) return null;
    this.pendingDiscoveries.add(deviceId);

    try {
      const discoveryConfig = mapper.discover(sourceData, context);
      this.mappingConfigs.set(deviceId, discoveryConfig);
      this.mappingStats.discoveredDevices++;
      await this.saveMappingConfig();
      logger.info(`Auto-mapping: discovered device ${deviceId} with ${discoveryConfig.measurements.length} measurements`);
      return discoveryConfig;
    } finally {
      this.pendingDiscoveries.delete(deviceId);
    }
  }

  /**
   * Get device configuration
   * @param {string} deviceId - Device ID
   * @returns {Object|null} Device configuration
   */
  getDeviceConfig(deviceId) {
    return this.mappingConfigs.get(deviceId) || null;
  }

  /**
   * Update device configuration
   * @param {string} deviceId - Device ID
   * @param {Object} config - Updated configuration
   */
  async updateDeviceConfig(deviceId, config) {
    this.mappingConfigs.set(deviceId, config);
    await this.saveMappingConfig();
    logger.info(`Updated configuration for device ${deviceId}`);
  }

  /**
   * Export data in specified format
   * @param {string} format - 'json' or 'toon'
   * @param {Object} options - Export options
   * @returns {Object|Array} Exported data
   */
  exportData(format = 'json', options = {}) {
    if (format === 'toon') {
      return this.dataModel.toTOON(options);
    } else {
      return this.dataModel.toJSON(options);
    }
  }

  /**
   * Get all discovered devices
   * @returns {Array} Array of discovered device configurations
   */
  getDiscoveredDevices() {
    return Array.from(this.mappingConfigs.values());
  }

  /**
   * Get all mapped entities (devices) from the data model
   * @returns {Array} Array of all devices/entities
   */
  getAllEntities() {
    return this.dataModel.getAllDevices();
  }

  /**
   * Get a single entity by ID
   * @param {string} entityId - Entity/device ID
   * @returns {Object|null} Device data or null
   */
  getEntity(entityId) {
    return this.dataModel.getDevice(entityId);
  }

  /**
   * Get all entities of a specific type
   * @param {string} type - Entity type
   * @returns {Array} Array of matching devices
   */
  getEntitiesByType(type) {
    return this.dataModel.getDevicesByType(type);
  }

  /**
   * Export data in NGSI-LD JSON format
   * @param {Object} options - Export options
   * @returns {Object} JSON representation
   */
  exportToNGSILD(options = {}) {
    return this.exportData('json', options);
  }

  /**
   * Export data in TOON format
   * @param {Object} options - Export options
   * @returns {Object} TOON representation
   */
  exportToTOON(options = {}) {
    return this.exportData('toon', options);
  }

  /**
   * Clear all data and configurations
   */
  clearAll() {
    this.dataModel.clear();
    this.mappingConfigs.clear();
    logger.debug('All data and configurations cleared');
  }

  /**
   * Clear all data
   */
  clearData() {
    this.dataModel.clear();
    logger.debug('Data model cleared');
  }

  /**
   * Get mapping statistics
   * @returns {Object} Statistics
   */
  getStatistics() {
    return {
      ...this.mappingStats,
      dataModelStats: this.dataModel.getStats(),
      mappersCount: this.mappers.size,
      configuredDevices: this.mappingConfigs.size
    };
  }
}

module.exports = MappingEngine;

