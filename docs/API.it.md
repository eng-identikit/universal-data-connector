# Documentazione API

**[🇬🇧 English](API.md)** | **[🇮🇹 Italiano](API.it.md)**

---

## URL Base
```
http://localhost:3000/api
```

## Authentication
If configured, API key is required in the `x-api-key` header or `apiKey` query parameter.

## Endpoints

### Status Endpoints

#### GET /api/status
Get overall system status including engine state, connectors, and system metrics.

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "system": {
    "status": "running",
    "uptime": 3600000,
    "startTime": "2025-07-03T09:30:00.000Z",
    "nodeVersion": "v18.17.0",
    "platform": "win32",
    "arch": "x64",
    "pid": 12345
  },
  "engine": {
    "isRunning": true,
    "totalDataPoints": 15240,
    "totalErrors": 2,
    "lastDataReceived": "2025-07-03T10:29:55.123Z"
  },
  "connectors": {
    "plc-001": {
      "status": "connected",
      "type": "opcua",
      "lastActivity": "2025-07-03T10:29:55.123Z",
      "stats": {
        "dataPoints": 8450,
        "errors": 0,
        "connections": 1
      }
    }
  }
}
```

#### GET /api/status/health
Health check endpoint for monitoring systems.

#### GET /api/status/stats
Detailed statistics about the system and data processing.

### Sources Endpoints

#### GET /api/sources
Get all configured sources with their runtime status.

#### GET /api/sources/:id
Get specific source configuration and status.

#### GET /api/sources/:id/status
Get runtime status of a specific source.

#### POST /api/sources/:id/start
Start a specific source connector.

#### POST /api/sources/:id/stop
Stop a specific source connector.

#### POST /api/sources/:id/restart
Restart a specific source connector.

#### GET /api/sources/:id/data
Get latest data points from a specific source.

**Query Parameters:**
- `limit` (optional): Number of data points to return (default: 100)

#### GET /api/sources/:id/discovery
Ottieni gli elementi auto-scoperti (nodi, topic o registri) da un connettore source.

**Requisiti:**
- La source deve essere configurata in modalità auto-discovery (array nodes/topics/registers vuoto)
- La source deve essere connessa e il processo di discovery completato

**Esempi di Response:**

Connettore OPC UA:
```json
{
  "sourceId": "plc-001",
  "protocol": "OPC-UA",
  "discoveredNodes": [
    {
      "nodeId": "ns=2;s=Temperature",
      "browseName": "Temperature",
      "displayName": "Sensore Temperatura",
      "nodeClass": "Variable",
      "dataType": "Double"
    },
    {
      "nodeId": "ns=2;s=Pressure",
      "browseName": "Pressure",
      "displayName": "Sensore Pressione",
      "nodeClass": "Variable",
      "dataType": "Double"
    }
  ]
}
```

Connettore MQTT:
```json
{
  "sourceId": "iot-sensors",
  "protocol": "MQTT",
  "discoveredTopics": [
    "sensors/temp/room1",
    "sensors/humidity/room1",
    "machines/status/line1"
  ]
}
```

Connettore Modbus:
```json
{
  "sourceId": "modbus-plc",
  "protocol": "Modbus",
  "discoveredRegisters": [
    {
      "address": 0,
      "type": "HoldingRegister",
      "name": "HR_0",
      "value": 234,
      "functionCode": 3
    },
    {
      "address": 1,
      "type": "HoldingRegister",
      "name": "HR_1",
      "value": 567,
      "functionCode": 3
    },
    {
      "address": 0,
      "type": "InputRegister",
      "name": "IR_0",
      "value": 123,
      "functionCode": 4
    }
  ]
}
```

#### POST /api/sources/:id/configure
Configura gli elementi scoperti e attiva il monitoraggio per un connettore source.

**Request Body:**

OPC UA:
```json
{
  "nodes": [
    "ns=2;s=Temperature",
    "ns=2;s=Pressure"
  ]
}
```

MQTT:
```json
{
  "topics": [
    "sensors/+/temperature",
    "machines/+/status"
  ]
}
```

Modbus:
```json
{
  "registers": [
    {
      "address": 0,
      "type": "HoldingRegister",
      "name": "Temperature",
      "scale": 0.1
    },
    {
      "address": 10,
      "type": "InputRegister",
      "name": "Pressure"
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "message": "Configurazione aggiornata e connettore riavviato",
  "sourceId": "plc-001"
}
```

**Workflow:**
1. Configura la source con array nodes/topics/registers vuoto
2. Avvia il connettore source
3. Attendi che la discovery sia completata (verifica nei log o nello status)
4. Chiama GET /api/sources/:id/discovery per visualizzare gli elementi scoperti
5. Seleziona gli elementi desiderati e chiama POST /api/sources/:id/configure
6. Il connettore si riavvia con la nuova configurazione e inizia il monitoraggio

### Data Endpoints

#### GET /api/data/latest
Get latest data points from all sources.

**Query Parameters:**
- `limit` (optional): Number of data points to return (default: 100)
- `source` (optional): Filter by source ID

#### GET /api/data/source/:sourceId
Get data points from a specific source.

**Query Parameters:**
- `limit` (optional): Number of data points to return (default: 100)
- `startTime` (optional): ISO timestamp for range start
- `endTime` (optional): ISO timestamp for range end

#### GET /api/data/search
Search data points by content.

**Query Parameters:**
- `q` (required): Search query
- `limit` (optional): Number of results to return (default: 100)

#### GET /api/data/range
Get data points within a time range.

**Query Parameters:**
- `startTime` (required): ISO timestamp for range start
- `endTime` (required): ISO timestamp for range end
- `source` (optional): Filter by source ID

#### GET /api/data/export
Export data in various formats.

**Query Parameters:**
- `format` (optional): Export format ('json' or 'csv', default: 'json')
- `source` (optional): Filter by source ID
- `startTime` (optional): ISO timestamp for range start
- `endTime` (optional): ISO timestamp for range end
- `limit` (optional): Number of data points (default: 10000)

#### GET /api/data/stats
Get data storage statistics.

#### DELETE /api/data/clear
Clear stored data.

**Query Parameters:**
- `source` (optional): Clear data from specific source only

### History Endpoints (TimescaleDB)

Read-only access to the historical data recorded in TimescaleDB, used by the **History** page of the UI.
The connection is taken from the active storage when it is TimescaleDB, otherwise from `config/storage.json`
(`storage` or `alternatives.timescaledb`), so history stays browsable even when the engine is currently
writing elsewhere. All times are ISO 8601.

#### GET /api/history/status
TimescaleDB availability, whether it is currently recording, and the recorded time range.

#### GET /api/history/sources
Sources with recorded data (`sourceId`, `records`, `first`, `last`).
Optional `startTime`, `endTime`.

#### GET /api/history/measurements
Measurements recorded for a source (`id`, `type`, `device`, `numeric`).

**Query Parameters:** `source` (required), `startTime`, `endTime` (default: last 24h)

#### GET /api/history/series
Downsampled time series using `time_bucket`: per bucket `avg`, `min`, `max`, `last`, `samples`.
Booleans are plotted as 0/1. The bucket is chosen automatically from the range and `maxPoints`.

**Query Parameters:**
- `source` (required)
- `measurements` (required): comma-separated measurement ids (max 20)
- `startTime`, `endTime` (default: last hour)
- `maxPoints` (optional, default 500, max 5000)
- `bucket` (optional): one of `1 second` … `30 days`

#### GET /api/history/records
Raw records, newest first.

**Query Parameters:** `source` (optional), `startTime`, `endTime` (default: last hour), `limit` (max 1000), `offset`, `order` (`asc`|`desc`). The response includes `hasMore`.

### Configuration Endpoints

#### GET /api/config
Get current configuration overview.

#### GET /api/config/sources
Get all source configurations.

#### POST /api/config/reload
Reload configuration from files.

#### PUT /api/config/sources/:id
Update a source configuration.

**Request Body:**
```json
{
  "enabled": true,
  "config": {
    "polling": {
      "interval": 5000
    }
  }
}
```

#### POST /api/config/sources
Add a new source configuration.

**Request Body:**
```json
{
  "id": "new-source",
  "type": "mqtt",
  "enabled": true,
  "name": "New MQTT Source",
  "config": {
    "broker": "mqtt://localhost:1883",
    "topics": ["test/+"]
  }
}
```

#### DELETE /api/config/sources/:id
Remove a source configuration.

#### POST /api/config/sources/validate
Validate a source configuration without saving.

**Request Body:**
```json
{
  "id": "test-source",
  "type": "opcua",
  "config": {
    "endpoint": "opc.tcp://localhost:4840",
    "nodes": ["ns=1;s=Temperature"]
  }
}
```

### Storage Configuration

#### GET /api/config/storage
Get current storage configuration.

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "storage": {
    "current": {
      "type": "postgresql",
      "config": {
        "host": "localhost",
        "port": 5432,
        "database": "universal_data_connector",
        "username": "postgres",
        "table": "sensor_data"
      }
    },
    "alternatives": {
      "mongodb": {
        "type": "mongodb",
        "config": {
          "uri": "mongodb://localhost:27017",
          "database": "universal_data_connector",
          "collection": "sensor_data"
        }
      }
    }
  }
}
```

#### PUT /api/config/storage
Update storage configuration.

**Request Body:**
```json
{
  "storage": {
    "type": "postgresql",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "universal_data_connector",
      "username": "postgres",
      "password": "password",
      "table": "sensor_data"
    }
  }
}
```

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "message": "Storage configuration updated successfully",
  "storage": {
    "type": "postgresql",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "universal_data_connector",
      "username": "postgres",
      "table": "sensor_data"
    }
  },
  "restartRequired": true
}
```

#### GET /api/config/storage/types
Get available storage types and their configuration schemas.

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "storageTypes": {
    "memory": {
      "name": "In-Memory Storage",
      "description": "Fast temporary storage in memory",
      "configSchema": {
        "maxRecords": {
          "type": "number",
          "default": 10000,
          "description": "Maximum number of records to keep"
        },
        "ttl": {
          "type": "number",
          "default": 3600000,
          "description": "Time to live in milliseconds"
        }
      }
    },
    "postgresql": {
      "name": "PostgreSQL Database",
      "description": "Relational database storage with PostgreSQL",
      "configSchema": {
        "host": {
          "type": "string",
          "required": true,
          "description": "Database host"
        },
        "port": {
          "type": "number",
          "default": 5432,
          "description": "Database port"
        }
      }
    }
  }
}
```

#### POST /api/config/storage/test
Test storage connection with provided configuration.

**Request Body:**
```json
{
  "type": "postgresql",
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "test_db",
    "username": "postgres",
    "password": "password"
  }
}
```

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "test": {
    "type": "postgresql",
    "success": true,
    "message": "Connection test successful",
    "responseTime": 245,
    "details": {
      "canConnect": true,
      "canStore": true,
      "canRetrieve": true,
      "canClear": true
    }
  }
}
```

#### GET /api/config/storage/health
Get storage health and statistics.

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "storage": {
    "type": "postgresql",
    "status": "healthy",
    "health": {
      "responsive": true,
      "responseTime": 120,
      "lastCheck": "2025-07-03T10:30:00.000Z"
    },
    "statistics": {
      "totalRecords": 15240,
      "totalSize": "2.5MB",
      "oldestRecord": "2025-07-03T08:00:00.000Z",
      "newestRecord": "2025-07-03T10:29:55.123Z",
      "sourceCount": 3
    },
    "lastCheck": "2025-07-03T10:30:00.000Z"
  }
}
```

#### POST /api/config/storage/validate
Validate storage configuration.

**Request Body:**
```json
{
  "type": "mongodb",
  "config": {
    "uri": "mongodb://localhost:27017",
    "database": "test_db",
    "collection": "sensor_data"
  }
}
```

**Response (Success):**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "valid": true,
  "message": "Storage configuration is valid",
  "normalizedConfig": {
    "uri": "mongodb://localhost:27017",
    "database": "test_db",
    "collection": "sensor_data",
    "options": {
      "maxPoolSize": 10,
      "serverSelectionTimeoutMS": 5000
    }
  }
}
```

**Response (Error):**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "valid": false,
  "errors": [
    {
      "field": "uri",
      "message": "URI is required for MongoDB configuration",
      "value": null
    }
  ]
}
```

### Dynamic Configuration

#### POST /api/config/sources/configure
Configure sources dynamically with complete payload.

**Request Body:**
```json
{
  "sources": [
    {
      "id": "plc-line1",
      "type": "opcua",
      "name": "PLC Line 1",
      "enabled": true,
      "config": {
        "endpoint": "opc.tcp://192.168.1.100:4840",
        "nodes": ["ns=2;s=Temperature", "ns=2;s=Pressure"],
        "interval": 1000
      }
    }
  ]
}
```

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "message": "Sources configuration updated successfully",
  "configuration": {
    "totalSources": 1,
    "enabledSources": 1,
    "sourceTypes": {
      "opcua": 1
    }
  },
  "result": {
    "success": true,
    "totalSources": 1,
    "activeConnectors": 1
  },
  "appliedImmediately": true
}
```

#### POST /api/config/sources/reload
Reload sources configuration from file or with new data.

**Request Body (optional):**
```json
{
  "sources": [
    // Optional: new sources array
  ]
}
```

#### POST /api/config/storage/configure
Configure storage dynamically with connection testing.

**Request Body:**
```json
{
  "type": "postgresql",
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "universal_data_connector",
    "username": "postgres",
    "password": "password",
    "table": "sensor_data"
  }
}
```

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "message": "Storage configuration updated successfully",
  "storage": {
    "type": "postgresql",
    "connectionTest": {
      "success": true,
      "responseTime": 145
    }
  },
  "result": {
    "success": true,
    "storageType": "postgresql",
    "restoredDataPoints": 1250
  },
  "appliedImmediately": true
}
```

#### POST /api/config/storage/reload
Reload storage configuration from file or with new data.

#### GET /api/config/engine/status
Get current engine status and dynamic configuration capabilities.

**Response:**
```json
{
  "timestamp": "2025-07-03T10:30:00.000Z",
  "engine": {
    "isRunning": true,
    "stats": {
      "totalDataPoints": 15240,
      "totalErrors": 2,
      "connectorCount": 3,
      "uptime": 3600000
    },
    "dataStore": {
      "type": "postgresql",
      "status": "connected"
    }
  },
  "connectors": {
    "plc-line1": {
      "status": "connected",
      "type": "opcua",
      "enabled": true,
      "stats": {
        "dataPoints": 8450,
        "errors": 0
      }
    }
  },
  "configuration": {
    "canReloadDynamically": true,
    "supportedOperations": [
      "sources/reload",
      "sources/configure", 
      "storage/reload",
      "storage/configure"
    ]
  }
}
```

### Source Configuration

// ...existing source configuration endpoints...
