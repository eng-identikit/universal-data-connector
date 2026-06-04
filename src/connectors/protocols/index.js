/**
 * Protocol Connectors Index
 * 
 * Exports all available protocol connector implementations.
 */

// IT/IoT Protocols
const OpcUaConnector = require('./OpcUaConnector');
const MqttConnector = require('./MqttConnector');
const HttpConnector = require('./HttpConnector');

// Industrial PLC Protocols
const ModbusConnector = require('./ModbusConnector');
const S7Connector = require('./S7Connector');
const EtherCATConnector = require('./EtherCATConnector');
const ProfinetConnector = require('./ProfinetConnector');
const FinsTcpConnector = require('./FinsTcpConnector');
const MelsecConnector = require('./MelsecConnector');
const CIPConnector = require('./CIPConnector');

// Building Automation
const BACnetConnector = require('./BACnetConnector');

// Serial Communication
const SerialConnector = require('./SerialConnector');

// Industry 4.0/5.0
const AASConnector = require('./AASConnector');
const I3XConnector = require('./I3XConnector');

module.exports = {
  // IT/IoT
  OpcUaConnector,
  MqttConnector,
  HttpConnector,

  // Industrial PLC
  ModbusConnector,
  S7Connector,
  EtherCATConnector,
  ProfinetConnector,
  FinsTcpConnector,
  MelsecConnector,
  CIPConnector,

  // Building Automation
  BACnetConnector,

  // Serial
  SerialConnector,

  // Industry 4.0/5.0
  AASConnector,
  I3XConnector
};
