const logger = require('../utils/logger');

/**
 * UniversalDataModel - Universal Data Model for Industry 5.0
 * 
 * Nuova struttura dati unificata:
 * {
 *   id: "device-unique-id",           // ID univoco del dispositivo
 *   type: "device-type",              // Tipologia dispositivo (es. "PLC", "Sensor", "Gateway")
 *   measurements: [                   // Array di misure
 *     {
 *       id: "measurement-id",         // ID della misura (es. "temperature", "pressure")
 *       type: "data-type",            // Tipo di dato (float, int, bool, string)
 *       value: <actual-value>         // Valore effettivo
 *     }
 *   ],
 *   metadata: {                       // Metadati da bus di ingresso
 *     timestamp: "2026-02-13T...",    // Timestamp della lettura
 *     source: "opcua",                // Tipo di sorgente
 *     quality: "GOOD",                // Qualità del dato (opzionale)
 *     ... altri metadati specifici
 *   }
 * }
 * 
 * Esportabile in JSON o TOON format
 */
class UniversalDataModel {
  constructor(options = {}) {
    this.version = '2.0.0';
    this.devices = new Map(); // Map di dispositivi nel nuovo formato
    this.metadata = {
      created: new Date().toISOString(),
      updated: new Date().toISOString(),
      source: options.source || 'universal-data-connector',
      namespace: options.namespace || 'urn:ngsi-ld:default'
    };
  }

  /**
   * Add or update a device in the data model
   * @param {Object} device - Device data in new unified format
   * @returns {string} Device ID
   */
  addDevice(device) {
    if (!device.id) {
      throw new Error('Device must have an id');
    }
    
    if (!device.type) {
      throw new Error('Device must have a type');
    }

    // Validate measurements array
    if (!Array.isArray(device.measurements)) {
      device.measurements = [];
    }

    // Validate each measurement
    device.measurements.forEach((measurement, idx) => {
      if (!measurement.id) {
        throw new Error(`Measurement at index ${idx} must have an id`);
      }
      if (measurement.type === undefined) {
        measurement.type = this.inferType(measurement.value);
      }
    });

    const deviceData = {
      id: device.id,
      type: device.type,
      measurements: device.measurements,
      metadata: {
        timestamp: new Date().toISOString(),
        source: device.metadata?.source || this.metadata.source,
        ...device.metadata
      }
    };

    this.devices.set(device.id, deviceData);
    this.metadata.updated = new Date().toISOString();
    
    logger.debug(`Device added/updated: ${device.id} (type: ${device.type}) with ${device.measurements.length} measurements`);
    return device.id;
  }

  /**
   * Infer data type from value
   * @param {*} value - Value to analyze
   * @returns {string} Data type
   */
  inferType(value) {
    if (value === null || value === undefined) return 'unknown';
    if (typeof value === 'boolean') return 'bool';
    if (typeof value === 'string') return 'string';
    if (Number.isInteger(value)) return 'int';
    if (typeof value === 'number') return 'float';
    if (typeof value === 'object') return 'object';
    return 'unknown';
  }

  /**
   * Get a device by ID
   * @param {string} id - Device ID
   * @returns {Object|null} Device data or null if not found
   */
  getDevice(id) {
    return this.devices.get(id) || null;
  }

  /**
   * Get all devices of a specific type
   * @param {string} type - Device type
   * @returns {Array} Array of devices
   */
  getDevicesByType(type) {
    const devices = [];
    for (const [id, device] of this.devices.entries()) {
      if (device.type === type) {
        devices.push(device);
      }
    }
    return devices;
  }

  /**
   * Get all devices
   * @returns {Array} Array of all devices
   */
  getAllDevices() {
    return Array.from(this.devices.values());
  }

  /**
   * Remove a device
   * @param {string} id - Device ID
   * @returns {boolean} True if device was removed
   */
  removeDevice(id) {
    const removed = this.devices.delete(id);
    if (removed) {
      this.metadata.updated = new Date().toISOString();
      logger.debug(`Device removed: ${id}`);
    }
    return removed;
  }

  /**
   * Update specific measurements for a device
   * @param {string} deviceId - Device ID
   * @param {Array} measurements - Array of measurements to update
   * @returns {boolean} Success status
   */
  updateMeasurements(deviceId, measurements) {
    const device = this.devices.get(deviceId);
    if (!device) {
      logger.warn(`Device ${deviceId} not found for measurement update`);
      return false;
    }

    measurements.forEach(newMeasurement => {
      const existingIdx = device.measurements.findIndex(m => m.id === newMeasurement.id);
      if (existingIdx >= 0) {
        // Update existing measurement
        device.measurements[existingIdx] = {
          ...device.measurements[existingIdx],
          ...newMeasurement,
          type: newMeasurement.type || this.inferType(newMeasurement.value)
        };
      } else {
        // Add new measurement
        device.measurements.push({
          ...newMeasurement,
          type: newMeasurement.type || this.inferType(newMeasurement.value)
        });
      }
    });

    device.metadata.timestamp = new Date().toISOString();
    this.metadata.updated = new Date().toISOString();
    
    return true;
  }

  /**
   * Export the data model to JSON format
   * @param {Object} options - Export options
   * @returns {Object|Array} JSON representation
   */
  toJSON(options = {}) {
    const includeMetadata = options.includeMetadata !== false;
    const singleDevice = options.deviceId;

    if (singleDevice) {
      const device = this.devices.get(singleDevice);
      return device || null;
    }

    const devices = Array.from(this.devices.values());

    if (includeMetadata) {
      return {
        version: this.version,
        metadata: this.metadata,
        devices: devices
      };
    }

    return devices;
  }

  /**
   * Export the data model to TOON format
   * TOON (Token-Oriented Object Notation) - formato compatto ottimizzato per LLM
   * @param {Object} options - Export options
   * @returns {Object} TOON representation
   */
  toTOON(options = {}) {
    const devices = Array.from(this.devices.values());
    
    return {
      format: 'TOON',
      version: '1.0.0',
      timestamp: new Date().toISOString(),
      source: this.metadata.source,
      devices: devices.map(device => ({
        id: device.id,
        type: device.type,
        ts: device.metadata.timestamp,
        m: device.measurements.map(measurement => ({
          i: measurement.id,
          t: measurement.type,
          v: measurement.value
        })),
        meta: Object.keys(device.metadata)
          .filter(key => key !== 'timestamp')
          .reduce((acc, key) => ({ ...acc, [key]: device.metadata[key] }), {})
      }))
    };
  }

  /**
   * Export single device to simplified JSON
   * @param {string} deviceId - Device ID
   * @returns {Object} Device in simplified format
   */
  exportDevice(deviceId) {
    const device = this.devices.get(deviceId);
    if (!device) {
      return null;
    }

    return {
      id: device.id,
      type: device.type,
      measurements: device.measurements,
      metadata: device.metadata
    };
  }

  /**
   * Import data from JSON format
   * @param {Object|Array} json - JSON data (single device or array)
   */
  fromJSON(json) {
    if (Array.isArray(json)) {
      // Array of devices
      json.forEach(device => this.addDevice(device));
    } else if (json.devices) {
      // Object with devices array
      if (json.metadata) {
        this.metadata = { ...this.metadata, ...json.metadata };
      }
      json.devices.forEach(device => this.addDevice(device));
    } else if (json.id && json.type) {
      // Single device
      this.addDevice(json);
    }
  }

  /**
   * Clear all devices
   */
  clear() {
    this.devices.clear();
    this.metadata.updated = new Date().toISOString();
    logger.debug('All devices cleared from data model');
  }

  /**
   * Get statistics about stored devices
   * @returns {Object} Statistics
   */
  getStats() {
    let totalMeasurements = 0;
    const deviceTypes = new Map();

    for (const device of this.devices.values()) {
      totalMeasurements += device.measurements.length;
      deviceTypes.set(device.type, (deviceTypes.get(device.type) || 0) + 1);
    }

    return {
      totalDevices: this.devices.size,
      totalMeasurements,
      deviceTypes: Object.fromEntries(deviceTypes),
      created: this.metadata.created,
      updated: this.metadata.updated
    };
  }
}

module.exports = UniversalDataModel;

