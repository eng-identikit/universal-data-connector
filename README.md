# Universal Data Connector v2.0 - New Architecture

🇮🇹 [Versione italiana](./README.it.md)

## Overview

The Universal Data Connector project has been completely restructured to provide a simpler, more flexible and more powerful system for collecting and distributing data from industrial sources.

## 🎯 Key Features

### 1. **Unified Data Format**

All data sources are mapped into a single standardized format:

```json
{
  "id": "device-unique-id",
  "type": "device-type",
  "measurements": [
    {
      "id": "measurement-id",
      "type": "float|int|bool|string",
      "value": <actual-value>
    }
  ],
  "metadata": {
    "timestamp": "2026-02-13T10:00:00.000Z",
    "source": "opcua|modbus|mqtt|http",
    "quality": "GOOD",
    "...": "other protocol-specific metadata"
  }
}
```

**Practical example:**
```json
{
  "id": "opcua_cartif_server",
  "type": "OPC_UA_Server",
  "measurements": [
    {
      "id": "temperature",
      "type": "float",
      "value": 23.5
    },
    {
      "id": "pressure",
      "type": "float",
      "value": 1.013
    },
    {
      "id": "motor_status",
      "type": "bool",
      "value": true
    }
  ],
  "metadata": {
    "timestamp": "2026-02-13T10:15:30.000Z",
    "source": "opcua",
    "endpoint": "opc.tcp://127.0.0.1:4840",
    "quality": "GOOD"
  }
}
```

### 2. **Automatic Discovery**

The system can automatically discover the structure of devices and save it to `config/mapping.json`:

- **First read**: The mapper analyzes the received data and generates the configuration
- **Automatic saving**: The structure is saved to `mapping.json`
- **Customization**: The user can edit the file to:
  - Rename measurements
  - Add transformations (scale, offset, formula)
  - Specify units of measurement
  - Disable unneeded measurements

**Process:**
1. Enable discovery mode in `mapping.json`: `"discoveryMode": true`
2. Start UDC and connect to the sources
3. The system automatically detects the devices and creates the configurations
4. Edit `mapping.json` to customize the mapping rules
5. Restart with `"discoveryMode": false` to use the configuration

### 3. **Multi-Transport**

Support for three transport layers (configurable in `mapping.json`):

#### **NATS** (Fast messaging)
```json
"transport": {
  "nats": {
    "enabled": true,
    "servers": "nats://localhost:4222",
    "subject": "udc.data"
  }
}
```

#### **MQTT** (IoT standard)
```json
"transport": {
  "mqtt": {
    "enabled": true,
    "broker": "mqtt://localhost:1883",
    "baseTopic": "udc/data",
    "format": "json",
    "qos": 1
  }
}
```

#### **HTTP Push** (REST API)
```json
"transport": {
  "http": {
    "enabled": true,
    "endpoint": "http://localhost:8080/api/data",
    "method": "POST",
    "format": "json",
    "batchSize": 10
  }
}
```

### 4. **Output Formats**

#### **JSON** (Standard, human-readable)
```json
{
  "id": "device-001",
  "type": "Sensor",
  "measurements": [
    {"id": "temp", "type": "float", "value": 23.5}
  ],
  "metadata": {
    "timestamp": "2026-02-13T10:00:00.000Z",
    "source": "opcua"
  }
}
```

#### **TOON** (Time-Oriented Object Notation - Compact)
```json
{
  "format": "TOON",
  "version": "1.0.0",
  "timestamp": "2026-02-13T10:00:00.000Z",
  "devices": [
    {
      "i": "device-001",
      "t": "Sensor",
      "ts": "2026-02-13T10:00:00.000Z",
      "m": [
        {"i": "temp", "t": "float", "v": 23.5}
      ],
      "meta": {"source": "opcua"}
    }
  ]
}
```

## 📁 Main File Structure

```
universal-data-connector/
├── config/
│   ├── mapping.json          # Device and transport configuration
│   ├── sources.json          # Data source configuration
│   └── storage.json          # Storage configuration (optional)
├── src/
│   ├── mappingTools/
│   │   ├── MappingEngine.js      # Mapping and discovery management
│   │   ├── UniversalDataModel.js # Unified data model
│   │   ├── BaseMapper.js         # Base for all mappers
│   │   └── mappers/
│   │       ├── OPCUAMapper.js    # OPC UA mapper
│   │       ├── ModbusMapper.js   # Modbus mapper
│   │       ├── MQTTMapper.js     # MQTT mapper
│   │       └── GenericMapper.js  # Generic mapper
│   ├── transport/
│   │   ├── NatsTransport.js      # NATS transport
│   │   ├── MqttTransport.js      # MQTT transport
│   │   └── HttpPushTransport.js  # HTTP Push transport
│   └── core/
│       └── DataConnectorEngine.js # Main engine
```

## 🔧 mapping.json Configuration

### Full Structure

```json
{
  "version": "2.0.0",
  "updated": "2026-02-13T10:00:00.000Z",
  "discoveryMode": true,
  
  "devices": [
    {
      "id": "device-unique-id",
      "type": "Device_Type",
      "sourceType": "opcua|modbus|mqtt|http",
      "discovered": "2026-02-13T09:00:00.000Z",
      "enabled": true,
      
      "measurements": [
        {
          "id": "measurement-id",
          "name": "Human Readable Name",
          "type": "float|int|bool|string",
          "unit": "°C|bar|rpm|%|...",
          "description": "Description of the measurement",
          "sourcePath": "path.to.source.value",
          
          "transform": {
            "type": "scale|offset|round|formula|map",
            "factor": 0.1,
            "offset": 0,
            "decimals": 2,
            "formula": "(x * 0.1) + 32"
          }
        }
      ],
      
      "metadata": {
        "endpoint": "...",
        "...": "protocol-specific metadata"
      }
    }
  ],
  
  "outputFormats": {
    "json": {
      "enabled": true,
      "includeMetadata": true
    },
    "toon": {
      "enabled": true,
      "compact": true
    }
  },
  
  "transport": {
    "nats": { "enabled": true, "subject": "udc.data" },
    "mqtt": { "enabled": false },
    "http": { "enabled": false }
  }
}
```

## 🚀 Complete Workflow

### 1. **Discovery Mode - Initial Configuration**

```bash
# 1. Enable discovery in mapping.json
"discoveryMode": true

# 2. Start UDC
npm start

# 3. The system automatically discovers the devices
# and generates the configuration in mapping.json
```

### 2. **Customization**

Edit `config/mapping.json` to:
- Rename measurements
- Add transformations
- Specify units of measurement
- Disable unneeded measurements

### 3. **Production**

```bash
# Disable discovery
"discoveryMode": false

# Restart UDC
npm start

# The system uses the customized configuration
```

## 🔄 Transformation Examples

### Scale and Offset
```json
{
  "transform": {
    "type": "scale",
    "factor": 0.1,
    "offset": -273.15
  }
}
```
Result: `(value * 0.1) - 273.15`

### Rounding
```json
{
  "transform": {
    "type": "round",
    "decimals": 2
  }
}
```

### Custom Formula
```json
{
  "transform": {
    "type": "formula",
    "formula": "(x * 1.8) + 32"
  }
}
```
Example: Celsius → Fahrenheit conversion

### Value Mapping
```json
{
  "transform": {
    "type": "map",
    "mapping": {
      "0": "OFF",
      "1": "ON",
      "2": "ERROR"
    }
  }
}
```

## 📊 Supported Protocols

- **OPC UA** - OPCUAMapper with support for nodeId, data types, quality
- **Modbus** - ModbusMapper for holding, input, coil, discrete registers
- **MQTT** - MQTTMapper with automatic JSON parsing
- **HTTP** - GenericMapper for REST APIs
- **Others** - GenericMapper for any protocol

## 🎯 Benefits of the New Architecture

1. **Simplicity**: A single unified data format
2. **Flexibility**: Automatic discovery + manual customization
3. **Scalability**: Multi-transport for different use cases
4. **Maintainability**: Centralized configuration in mapping.json
5. **Extensibility**: Easy to add new mappers and transports

## 📝 Migration Notes

If you are migrating from the previous version:

1. The old entities/attributes format has been replaced by devices/measurements
2. Relationships have been removed for simplicity
3. NGSI-LD export has been simplified
4. Mapping configuration now lives in mapping.json instead of being spread across files

## 🛠️ Programmatic API

```javascript
// Direct access to the MappingEngine
const { MappingEngine } = require('./src/mappingTools');

const engine = new MappingEngine({
  namespace: 'urn:ngsi-ld:industry50',
  mappingConfigPath: './config/mapping.json'
});

// Map data
const device = await engine.mapData(sourceData, 'opcua', context);

// Export as JSON
const jsonData = engine.exportData('json');

// Export as TOON
const toonData = engine.exportData('toon');

// Get discovered devices
const devices = engine.getDiscoveredDevices();

// Get statistics
const stats = engine.getStatistics();
```

## 📖 Further Resources

- [API Documentation](./docs/API.md)
- [Configuration Guide](./docs/Configuration.md)
- [Mapping Guide](./docs/Mapping.md)
- [Transport Guide](./docs/Transport.md)

---

**Universal Data Connector v2.0** - Industry 5.0 Ready 🚀
