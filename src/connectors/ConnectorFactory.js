const logger = require('../utils/logger');

// Lazy-load map: connectors are required only when first used.
// This dramatically reduces startup time since heavy libraries like
// node-opcua, modbus-serial, nodes7, serialport, bacstack etc.
// are NOT loaded until the connector type is actually instantiated.
const connectorLoaders = {
  'opcua':                    () => require('./protocols/OpcUaConnector'),
  'mqtt':                     () => require('./protocols/MqttConnector'),
  'http':                     () => require('./protocols/HttpConnector'),
  'modbus':                   () => require('./protocols/ModbusConnector'),
  'ethercat':                 () => require('./protocols/EtherCATConnector'),
  's7':                       () => require('./protocols/S7Connector'),
  'siemens-s7':               () => require('./protocols/S7Connector'),
  'bacnet':                   () => require('./protocols/BACnetConnector'),
  'profinet':                 () => require('./protocols/ProfinetConnector'),
  'fins':                     () => require('./protocols/FinsTcpConnector'),
  'fins-tcp':                 () => require('./protocols/FinsTcpConnector'),
  'omron-fins':               () => require('./protocols/FinsTcpConnector'),
  'melsec':                   () => require('./protocols/MelsecConnector'),
  'mitsubishi':               () => require('./protocols/MelsecConnector'),
  'cip':                      () => require('./protocols/CIPConnector'),
  'ethernet-ip':              () => require('./protocols/CIPConnector'),
  'rockwell':                 () => require('./protocols/CIPConnector'),
  'serial':                   () => require('./protocols/SerialConnector'),
  'rs232':                    () => require('./protocols/SerialConnector'),
  'rs485':                    () => require('./protocols/SerialConnector'),
  'aas':                      () => require('./protocols/AASConnector'),
  'asset-administration-shell': () => require('./protocols/AASConnector'),
  'i3x':                      () => require('./protocols/I3XConnector'),
};

// Cache already-loaded classes to avoid repeated require() calls
const connectorCache = {};

class ConnectorFactory {
  static create(type, config) {
    const loader = connectorLoaders[type];

    if (!loader) {
      throw new Error(`Unsupported connector type: ${type}`);
    }

    // Load and cache the class on first use
    if (!connectorCache[type]) {
      connectorCache[type] = loader();
    }
    const ConnectorClass = connectorCache[type];

    try {
      const connector = new ConnectorClass(config);
      logger.debug(`Created ${type} connector for source '${config.id}'`);
      return connector;
    } catch (error) {
      logger.error(`Failed to create ${type} connector:`, error);
      throw error;
    }
  }

  static getSupportedTypes() {
    return Object.keys(connectorLoaders);
  }

  static registerConnectorType(type, ConnectorClass) {
    connectorLoaders[type] = () => ConnectorClass;
    logger.info(`Registered custom connector type: ${type}`);
  }

  static isTypeSupported(type) {
    return type in connectorLoaders;
  }
}

module.exports = ConnectorFactory;
