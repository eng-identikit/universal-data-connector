# Universal Data Connector v2.0 - New Architecture

🇮🇹 [Versione italiana](./README.it.md)

## Overview

The Universal Data Connector project has been completely restructured to provide a simpler, more flexible and more powerful system for collecting and distributing data from industrial sources.

## 🧭 Context broker

UDC is also a standalone **context broker**:
- devices read by the connectors become **entities** (current state, relationships, location, attribute history);
- producers can write entities through the API;
- **subscriptions** notify external systems over HTTP, MQTT, NATS or WebSocket, with filters, deadband, throttling and a dead-letter queue;
- several instances can run as a **cluster**.

```bash
cp .env.example .env          # set BROKER_JWT_SECRET and BROKER_ADMIN_PASSWORD
docker compose up -d --build  # UI on http://localhost:8080, API on /api/v1
```

Full guide: [Context broker](./docs/Broker.md) (architecture, API, subscriptions, cluster, Helm, configuration, performance).

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
    {"id": "temp", "type": "float", "value": 23.5},
    {"id": "pressure", "type": "float", "value": 1.2}
  ],
  "metadata": {
    "timestamp": "2026-02-13T10:00:00.000Z",
    "source": "opcua"
  }
}
```

#### **TOON** (Token-Oriented Object Notation - Compact)
Indentation-based format designed to use fewer tokens than JSON when data is passed to LLMs. Arrays of uniform objects become tables: field names are declared once in the header (`[N]{fields}`) and each row holds only the values.

```toon
id: device-001
type: Sensor
measurements[2]{id,type,value}:
  temp,float,23.5
  pressure,float,1.2
metadata:
  timestamp: "2026-02-13T10:00:00.000Z"
  source: opcua
```

### 5. **Storage and History**

Mapped data is saved to the storage configured in `config/storage.json`: **Memory**, **Redis** or **TimescaleDB**.

- **Fallback**: if the storage is not reachable at startup, the server starts anyway and keeps the data in memory. It retries the connection every 30 s (`STORAGE_RETRY_INTERVAL`) and moves the buffered data to the storage once it is back.
- **Hot switch**: the storage can be changed at runtime (`POST /api/config/storage/configure`). The new storage is connected before the old one is replaced, so a failed switch leaves the current one running.
- **History**: the attribute history of the entities is kept by the broker in TimescaleDB (`GET /api/v1/entities/:id/history/...`, see the [broker guide](./docs/Broker.md)).

Details: [Storage Guide](./docs/Storage.md).

### 6. **Real-time Stream (WebSocket)**

Entity changes are pushed over an authenticated WebSocket on the API port (`/api/v1/stream?access_token=…&tenant=…`). Clients subscribe to entity filters or to websocket-channel subscriptions:

```json
{ "type": "subscribe", "id": "live", "entities": { "types": ["Press"], "idPattern": "plant1:*" } }
{ "type": "entity", "sub": "live", "op": "update", "entity": { "id": "plant1:press07", "type": "Press", "attrs": { ... } } }
```

The [Web UI](https://github.com/Engineering-Research-and-Development/universal-data-connector-ui) uses this stream for its real-time charts. Protocol details are in the [broker guide](./docs/Broker.md#realtime-stream).

## 📁 Main File Structure

```
universal-data-connector/
├── config/
│   ├── mapping.json          # Device and transport configuration
│   ├── sources.json          # Data source configuration
│   └── storage.json          # Storage configuration (optional)
├── src/
│   ├── server.js             # REST API and WebSocket stream (port 3000)
│   ├── api/routes/           # System routes (status, sources, data, config, mapping)
│   ├── connectors/protocols/ # One connector per protocol (OPC UA, Modbus, PROFINET, EtherCAT, ...)
│   ├── storage/
│   │   └── StorageFactory.js     # Memory, Redis, TimescaleDB adapters
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

| Area | Connectors |
|------|------------|
| IT / IoT | **OPC UA**, **MQTT**, **HTTP** (REST polling), **DDS** (RTPS: RTI Connext, Fast DDS, Cyclone DDS, ROS 2) |
| Industrial PLC | **Modbus** TCP/RTU, **Siemens S7**, **PROFINET** (IO Devices' process image via the S7 IO Controller), **EtherCAT** (TwinCAT master over ADS: PDOs, slave states, CoE SDO), **FINS** (Omron), **MELSEC** (Mitsubishi), **CIP / EtherNet/IP** |
| Building automation | **BACnet** |
| Serial | **Serial** (RS-232/RS-485) |
| Industry 4.0 / 5.0 | **AAS** (Asset Administration Shell), **i3X** |

PROFINET, EtherCAT and DDS also have a `simulation` mode that generates random values without hardware.

Dedicated mappers exist for OPC UA (nodeId, data types, quality), Modbus (holding/input/coil/discrete registers), MQTT (automatic JSON parsing) and AAS. All other protocols use the GenericMapper. See the [Industrial Connectors guide](./docs/IndustrialConnectors.md).

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

- [Context broker](./docs/Broker.md): entities, subscriptions, history, commands, cluster, deployment
- [API Documentation](./docs/API.md) (connector engine endpoints, now under `/api/v1/system`)
- [Configuration Guide](./docs/Configuration.md)
- [Dynamic Configuration](./docs/DynamicConfiguration.md)
- [Mapping Guide](./docs/Mapping.md)
- [Auto Discovery](./docs/AutoDiscovery.md)
- [Industrial Connectors](./docs/IndustrialConnectors.md)
- [Storage Guide](./docs/Storage.md)
- [Web UI](https://github.com/Engineering-Research-and-Development/universal-data-connector-ui): dashboard, real-time charts, history

---

**Universal Data Connector v2.0** - Industry 5.0 Ready 🚀
