# Documentazione Mapping Tools

**[🇬🇧 English](Mapping.md)** | **[🇮🇹 Italiano](Mapping.it.md)**

---

## Panoramica

Il modulo **Mapping Tools** fornisce un modello dati unificato e un layer di trasformazione per l'Universal Data Connector. It maps data from various industrial protocols (OPC-UA, Modbus, AAS, MQTT, etc.) into a common Universal Data Model that can be exported in multiple formats including JSON, NGSI-LD, and TOON.

## Architecture

### Components

```
mappingTools/
├── UniversalDataModel.js    # Core unified data model
├── MappingEngine.js          # Main mapping orchestration engine
├── BaseMapper.js             # Abstract base class for all mappers
├── mappers/                  # Protocol-specific mappers
│   ├── OPCUAMapper.js
│   ├── ModbusMapper.js
│   ├── AASMapper.js
│   ├── MQTTMapper.js
│   └── GenericMapper.js
└── index.js                  # Module exports
```

### Integration

The Mapping Engine is integrated into the DataConnectorEngine and automatically processes all incoming data:

```javascript
const engine = new DataConnectorEngine();
await engine.initialize();

// Mapping happens automatically on data arrival
// Access mapped data through engine methods
const entities = engine.getAllMappedEntities();
const jsonExport = engine.exportMappedDataToJSON();
```

## Universal Data Model

The Universal Data Model represents entities and their relationships in a standardized format.

### Entity Structure

```javascript
{
  id: "OPC:Device:PLC1",
  type: "Device",
  name: "PLC 1",
  properties: {
    manufacturer: "Siemens",
    model: "S7-1500",
    status: "Running"
  },
  metadata: {
    source: "opcua-server",
    protocol: "OPC-UA",
    timestamp: "2024-01-15T10:30:00Z",
    quality: "Good"
  }
}
```

### Relationship Structure

```javascript
{
  id: "rel_001",
  type: "parentOf",
  source: "OPC:Device:PLC1",
  target: "OPC:Sensor:Temp1",
  properties: {
    description: "PLC1 contains Temp1"
  }
}
```

## Mappers

### Available Mappers

#### 1. OPCUAMapper
Maps OPC-UA nodes to the Universal Data Model.

**Mapping Rules:**
- `Object` nodes → `Device` entities
- `Variable` nodes → `Sensor` entities
- `Method` nodes → `Actuator` entities
- Quality codes → standardized quality values

**Example:**
```javascript
// Input (OPC-UA)
{
  nodeId: "ns=2;s=Temperature",
  displayName: "Temperature Sensor",
  value: 23.5,
  dataType: "Double",
  quality: "Good",
  timestamp: "2024-01-15T10:30:00Z"
}

// Output (Universal Data Model)
{
  id: "OPC:Temperature",
  type: "Sensor",
  name: "Temperature Sensor",
  properties: {
    value: 23.5,
    dataType: "Double",
    nodeId: "ns=2;s=Temperature"
  },
  metadata: {
    source: "opcua-source",
    protocol: "OPC-UA",
    quality: "GOOD",
    timestamp: "2024-01-15T10:30:00Z"
  }
}
```

#### 2. ModbusMapper
Maps Modbus registers to the Universal Data Model.

**Mapping Rules:**
- `HoldingRegister` / `InputRegister` → `Sensor` entities
- `Coil` → `Actuator` entities
- `DiscreteInput` → `Sensor` entities

**Example:**
```javascript
// Input (Modbus)
{
  address: 40001,
  type: "HoldingRegister",
  value: 235,
  unit: 0,
  timestamp: "2024-01-15T10:30:00Z"
}

// Output (Universal Data Model)
{
  id: "MB:HR:40001",
  type: "Sensor",
  name: "Register 40001",
  properties: {
    value: 235,
    address: 40001,
    registerType: "HoldingRegister",
    unit: 0
  },
  metadata: {
    source: "modbus-device",
    protocol: "Modbus",
    timestamp: "2024-01-15T10:30:00Z"
  }
}
```

#### 3. AASMapper
Maps Asset Administration Shell submodels to the Universal Data Model.

**Mapping Rules:**
- `AssetAdministrationShell` → `Asset` entities
- `Submodel` → `Component` entities
- `Property` → properties in parent entity
- `Operation` → `Capability` entities
- `File` → `Document` entities

**Example:**
```javascript
// Input (AAS)
{
  idShort: "Motor_001",
  identification: { id: "http://example.com/ids/motor/001" },
  submodels: [{
    idShort: "TechnicalData",
    submodelElements: [{
      idShort: "PowerRating",
      valueType: "double",
      value: "15.0"
    }]
  }]
}

// Output (Universal Data Model)
{
  id: "AAS:Motor_001",
  type: "Asset",
  name: "Motor_001",
  properties: {
    identification: "http://example.com/ids/motor/001",
    PowerRating: 15.0
  },
  metadata: {
    source: "aas-server",
    protocol: "AAS",
    submodel: "TechnicalData"
  }
}
```

#### 4. MQTTMapper
Maps MQTT messages to the Universal Data Model.

**Mapping Rules:**
- Topic-based entity creation
- Payload parsing (JSON, plain text)
- Dynamic type detection

**Example:**
```javascript
// Input (MQTT)
{
  topic: "sensors/temp/room1",
  payload: { "value": 22.5, "unit": "°C" },
  timestamp: "2024-01-15T10:30:00Z"
}

// Output (Universal Data Model)
{
  id: "MQTT:sensors:temp:room1",
  type: "Sensor",
  name: "room1",
  properties: {
    value: 22.5,
    unit: "°C",
    topic: "sensors/temp/room1"
  },
  metadata: {
    source: "mqtt-broker",
    protocol: "MQTT",
    timestamp: "2024-01-15T10:30:00Z"
  }
}
```

#### 5. GenericMapper
Fallback mapper for unknown or custom data formats.

**Features:**
- Preserves original data structure
- Creates generic entities
- Adds source metadata

## Export Formats

### 1. JSON Export

Standard JSON representation of the Universal Data Model.

```javascript
const jsonData = engine.exportMappedDataToJSON({
  includeMetadata: true,
  includeRelationships: true
});
```

**Output:**
```json
{
  "entities": [
    {
      "id": "OPC:Device:PLC1",
      "type": "Device",
      "name": "PLC 1",
      "properties": { ... },
      "metadata": { ... }
    }
  ],
  "relationships": [
    {
      "id": "rel_001",
      "type": "parentOf",
      "source": "OPC:Device:PLC1",
      "target": "OPC:Sensor:Temp1"
    }
  ],
  "metadata": {
    "exportedAt": "2024-01-15T10:30:00Z",
    "totalEntities": 1,
    "totalRelationships": 1
  }
}
```

### 2. NGSI-LD Export

FIWARE NGSI-LD compliant format for IoT platforms.

```javascript
const ngsiLdData = engine.exportMappedDataToNGSILD({
  context: "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
});
```

**Output:**
```json
[
  {
    "id": "urn:ngsi-ld:Device:OPC:Device:PLC1",
    "type": "Device",
    "name": {
      "type": "Property",
      "value": "PLC 1"
    },
    "manufacturer": {
      "type": "Property",
      "value": "Siemens"
    },
    "status": {
      "type": "Property",
      "value": "Running",
      "observedAt": "2024-01-15T10:30:00Z"
    },
    "@context": [
      "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
  }
]
```

### 3. TOON Export

TOON (Token-Oriented Object Notation) - formato di rappresentazione compatto, ottimizzato per LLM.

```javascript
const toonData = engine.exportMappedDataToTOON({
  // Options to be defined
});
```

**Status:** Format specification pending definition.

## API Endpoints

### Get All Mapped Entities
```http
GET /api/mapping/entities
```

**Response:**
```json
{
  "success": true,
  "count": 10,
  "entities": [ ... ]
}
```

### Get Specific Entity
```http
GET /api/mapping/entities/:id
```

**Response:**
```json
{
  "success": true,
  "entity": { ... }
}
```

### Get Entities by Type
```http
GET /api/mapping/entities/type/:type
```

**Response:**
```json
{
  "success": true,
  "type": "Sensor",
  "count": 5,
  "entities": [ ... ]
}
```

### Export to JSON
```http
GET /api/mapping/export/json?includeMetadata=true&includeRelationships=true
```

**Response:**
```json
{
  "success": true,
  "format": "JSON",
  "data": { ... }
}
```

### Export to NGSI-LD
```http
GET /api/mapping/export/ngsi-ld?context=https://...
```

**Response:**
```json
{
  "success": true,
  "format": "NGSI-LD",
  "count": 10,
  "entities": [ ... ]
}
```

### Export to TOON
```http
GET /api/mapping/export/toon
```

**Response:**
```json
{
  "success": true,
  "format": "TOON",
  "data": { ... }
}
```

### Get Mapping Statistics
```http
GET /api/mapping/statistics
```

**Response:**
```json
{
  "success": true,
  "statistics": {
    "totalMappings": 150,
    "successfulMappings": 148,
    "failedMappings": 2,
    "dataModel": {
      "totalEntities": 45,
      "totalRelationships": 23
    },
    "byProtocol": {
      "OPC-UA": 30,
      "Modbus": 10,
      "MQTT": 5
    }
  }
}
```

### Get Mapping Health
```http
GET /api/mapping/health
```

**Response:**
```json
{
  "success": true,
  "healthy": true,
  "version": "1.0.0",
  "statistics": { ... },
  "registeredMappers": ["opcua", "modbus", "aas", "mqtt", "generic"]
}
```

### Clear Mapped Data
```http
DELETE /api/mapping/entities
```

**Response:**
```json
{
  "success": true,
  "message": "All mapped data cleared"
}
```

## Configuration

Configuration file: `config/mapping.example.json`

### Mapper Rules

Define custom mapping rules for each protocol:

```json
{
  "mapping": {
    "mapperRules": {
      "opcua": {
        "entityPrefix": "OPC:",
        "typeMapping": {
          "Object": "Device",
          "Variable": "Sensor"
        },
        "propertyMapping": {
          "Value": "value",
          "Quality": "quality"
        }
      }
    }
  }
}
```

### Relationship Rules

Auto-detect relationships between entities:

```json
{
  "relationships": {
    "autoDetect": true,
    "rules": [
      {
        "type": "parentOf",
        "sourcePattern": "^OPC:.*Device.*$",
        "targetPattern": "^OPC:.*Sensor.*$"
      }
    ]
  }
}
```

### Export Configuration

Configure export format options:

```json
{
  "export": {
    "json": {
      "pretty": true,
      "includeMetadata": true
    },
    "ngsiLd": {
      "context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    }
  }
}
```

## Programmatic Usage

### Using the Mapping Engine Directly

```javascript
const { MappingEngine } = require('./src/mappingTools');

const mappingEngine = new MappingEngine();

// Map data from various sources
mappingEngine.mapData({
  nodeId: "ns=2;s=Temperature",
  displayName: "Temp Sensor",
  value: 23.5
}, {
  sourceId: 'opcua-server',
  protocol: 'OPC-UA'
});

// Get mapped entities
const entities = mappingEngine.getAllEntities();

// Export to different formats
const jsonExport = mappingEngine.exportToJSON();
const ngsiLdExport = mappingEngine.exportToNGSILD();
```

### Creating Custom Mappers

```javascript
const BaseMapper = require('./src/mappingTools/BaseMapper');

class CustomMapper extends BaseMapper {
  constructor() {
    super('custom', ['myProtocol']);
  }

  map(data, context = {}) {
    return this.createEntity({
      id: `CUSTOM:${data.id}`,
      type: 'CustomDevice',
      name: data.name,
      properties: data.properties,
      metadata: {
        source: context.sourceId,
        protocol: 'Custom'
      }
    });
  }
}

// Register with mapping engine
mappingEngine.registerMapper(new CustomMapper());
```

## Best Practices

1. **Entity IDs**: Use prefixes to identify protocol origin (e.g., `OPC:`, `MB:`, `AAS:`)
2. **Metadata**: Always include source, protocol, and timestamp in metadata
3. **Type Consistency**: Use consistent entity types across mappers
4. **Relationships**: Define clear relationship rules for entity hierarchies
5. **Error Handling**: Mappers should handle errors gracefully and log issues
6. **Performance**: Use batch mapping for large datasets
7. **Configuration**: Externalize mapping rules in configuration files

## Troubleshooting

### No Data Being Mapped

Check that:
- MappingEngine is initialized in DataConnectorEngine
- Connector data contains valid fields
- Appropriate mapper is registered for the protocol

### Export Failures

- Verify entity structure matches expected format
- Check that all required properties are present
- Validate export format options

### Custom Mapper Not Working

- Ensure mapper extends BaseMapper
- Register mapper with MappingEngine
- Check that canHandle() returns true for your data

## Future Enhancements

- [ ] Define TOON format specification
- [ ] Add semantic validation of mapped entities
- [ ] Implement entity deduplication
- [ ] Add query language for filtered exports
- [ ] Support for streaming exports
- [ ] Integration with knowledge graphs
- [ ] Machine learning-based mapping suggestions
