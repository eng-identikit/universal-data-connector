# Universal Data Connector v2.0 - Nuova Architettura

🇬🇧 [English version](./README.md)

## Panoramica

Il progetto Universal Data Connector è stato completamente ristrutturato per fornire un sistema più semplice, flessibile e potente di raccolta e distribuzione dati da sorgenti industriali.

## 🧭 Context broker

L'UDC è anche un **context broker** standalone:
- i dispositivi letti dai connettori diventano **entità** (stato corrente, relazioni, posizione, storico degli attributi);
- i producer possono scrivere entità tramite l'API;
- le **subscription** notificano i sistemi esterni via HTTP, MQTT, NATS o WebSocket, con filtri, deadband, throttling e dead-letter queue, e con un **transformer** JSONata che dà al messaggio il formato voluto dal destinatario;
- un endpoint **MCP** permette a un assistente AI (Claude o altri client MCP) di leggere entità, relazioni e storico, in sola lettura;
- più istanze possono girare in **cluster**.

```bash
cp .env.example .env          # imposta BROKER_JWT_SECRET e BROKER_ADMIN_PASSWORD
docker compose up -d --build  # UI orchestratore su http://localhost:8080, API UDC su http://localhost:3000/api/v1
```

Guida completa: [Context broker](./docs/Broker.it.md) (architettura, API, subscription, cluster, Helm, configurazione, prestazioni).

## 🎯 Caratteristiche Principali

### 1. **Formato Dati Unificato**

Tutte le sorgenti dati vengono mappate in un unico formato standardizzato:

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
    "...": "altri metadati specifici del protocollo"
  }
}
```

**Esempio pratico:**
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

### 2. **Discovery Automatica**

Il sistema può scoprire automaticamente la struttura dei dispositivi e salvarla in `config/mapping.json`:

- **Prima lettura**: Il mapper analizza i dati ricevuti e genera la configurazione
- **Salvataggio automatico**: La struttura viene salvata in `mapping.json`
- **Personalizzazione**: L'utente può modificare il file per:
  - Rinominare measurements
  - Aggiungere trasformazioni (scale, offset, formula)
  - Specificare unità di misura
  - Disabilitare measurements non necessari

**Processo:**
1. Abilita discovery mode in `mapping.json`: `"discoveryMode": true`
2. Avvia UDC e connettiti alle sorgenti
3. Il sistema rileva automaticamente i dispositivi e crea le configurazioni
4. Modifica `mapping.json` per personalizzare le regole di mapping
5. Riavvia con `"discoveryMode": false` per usare la configurazione

### 3. **Multi-Transport**

Supporto per tre layer di trasporto (configurabili in `mapping.json`):

#### **NATS** (Messaging veloce)
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

### 4. **Formati di Output**

#### **JSON** (Standard, leggibile)
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

#### **TOON** (Token-Oriented Object Notation - Compatto)
Formato basato sull'indentazione, pensato per usare meno token del JSON quando i dati vengono passati a un LLM. Gli array di oggetti uniformi diventano tabelle: i nomi dei campi sono dichiarati una sola volta nell'intestazione (`[N]{campi}`) e ogni riga contiene solo i valori.

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

### 5. **Storage e Storico**

I dati mappati vengono salvati nello storage configurato in `config/storage.json`: **Memory**, **Redis** o **TimescaleDB**.

- **Fallback**: se all'avvio lo storage non è raggiungibile, il server parte comunque e tiene i dati in memoria. Riprova a connettersi ogni 30 s (`STORAGE_RETRY_INTERVAL`) e, quando lo storage torna disponibile, ci trasferisce i dati accumulati.
- **Cambio a caldo**: lo storage si può cambiare a runtime (`POST /api/config/storage/configure`). Il nuovo storage viene connesso prima di sostituire il vecchio, quindi se il cambio fallisce resta attivo quello attuale.
- **Storico**: lo storico degli attributi delle entità è tenuto dal broker in TimescaleDB (`GET /api/v1/entities/:id/history/...`, vedi la [guida al broker](./docs/Broker.it.md)).

Dettagli: [Guida Storage](./docs/Storage.it.md).

### 6. **Stream in Tempo Reale (WebSocket)**

Le modifiche alle entità vengono inviate su un WebSocket autenticato, sulla porta dell'API (`/api/v1/stream?access_token=…&tenant=…`). I client si iscrivono a filtri sulle entità o a subscription con canale websocket:

```json
{ "type": "subscribe", "id": "live", "entities": { "types": ["Press"], "idPattern": "plant1:*" } }
{ "type": "entity", "sub": "live", "op": "update", "entity": { "id": "plant1:press07", "type": "Press", "attrs": { ... } } }
```

La [Web UI](https://github.com/Engineering-Research-and-Development/universal-data-connector-ui/blob/main/README.it.md) usa questo stream per i grafici in tempo reale. I dettagli del protocollo sono nella [guida al broker](./docs/Broker.it.md#stream-in-tempo-reale).

## 📁 Struttura File Principali

```
universal-data-connector/
├── config/
│   ├── mapping.json          # Configurazione dispositivi e transport
│   ├── sources.json          # Configurazione sorgenti dati
│   └── storage.json          # Configurazione storage (opzionale)
├── src/
│   ├── server.js             # REST API e stream WebSocket (porta 3000)
│   ├── api/routes/           # Route di sistema (status, sources, data, config, mapping)
│   ├── connectors/protocols/ # Un connettore per protocollo (OPC UA, Modbus, PROFINET, EtherCAT, ...)
│   ├── storage/
│   │   └── StorageFactory.js     # Adapter Memory, Redis, TimescaleDB
│   ├── mappingTools/
│   │   ├── MappingEngine.js      # Gestione mapping e discovery
│   │   ├── UniversalDataModel.js # Modello dati unificato
│   │   ├── BaseMapper.js         # Base per tutti i mapper
│   │   └── mappers/
│   │       ├── OPCUAMapper.js    # Mapper OPC UA
│   │       ├── ModbusMapper.js   # Mapper Modbus
│   │       ├── MQTTMapper.js     # Mapper MQTT
│   │       └── GenericMapper.js  # Mapper generico
│   ├── transport/
│   │   ├── NatsTransport.js      # Transport NATS
│   │   ├── MqttTransport.js      # Transport MQTT
│   │   └── HttpPushTransport.js  # Transport HTTP Push
│   └── core/
│       └── DataConnectorEngine.js # Engine principale
```

## 🔧 Configurazione mapping.json

### Struttura Completa

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
          "description": "Descrizione della misura",
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
        "...": "metadati specifici protocollo"
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

## 🚀 Workflow Completo

### 1. **Discovery Mode - Prima Configurazione**

```bash
# 1. Abilita discovery in mapping.json
"discoveryMode": true

# 2. Avvia UDC
npm start

# 3. Il sistema scopre automaticamente i dispositivi
# e genera la configurazione in mapping.json
```

### 2. **Personalizzazione**

Modifica `config/mapping.json` per:
- Rinominare measurements
- Aggiungere trasformazioni
- Specificare unità di misura
- Disabilitare measurements non necessari

### 3. **Produzione**

```bash
# Disabilita discovery
"discoveryMode": false

# Riavvia UDC
npm start

# Il sistema usa la configurazione personalizzata
```

## 🔄 Esempi di Trasformazioni

### Scale e Offset
```json
{
  "transform": {
    "type": "scale",
    "factor": 0.1,
    "offset": -273.15
  }
}
```
Risultato: `(value * 0.1) - 273.15`

### Arrotondamento
```json
{
  "transform": {
    "type": "round",
    "decimals": 2
  }
}
```

### Formula Custom
```json
{
  "transform": {
    "type": "formula",
    "formula": "(x * 1.8) + 32"
  }
}
```
Esempio: Conversione Celsius → Fahrenheit

### Mapping Valori
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

## 📊 Protocolli Supportati

| Area | Connettori |
|------|------------|
| IT / IoT | **OPC UA**, **MQTT**, **HTTP** (polling REST), **DDS** (RTPS: RTI Connext, Fast DDS, Cyclone DDS, ROS 2) |
| PLC industriali | **Modbus** TCP/RTU, **Siemens S7**, **PROFINET** (immagine di processo degli IO Device tramite l'IO Controller S7), **EtherCAT** (master TwinCAT via ADS: PDO, stati degli slave, CoE SDO), **FINS** (Omron), **MELSEC** (Mitsubishi), **CIP / EtherNet/IP** |
| Building automation | **BACnet** |
| Seriale | **Serial** (RS-232/RS-485) |
| Industria 4.0 / 5.0 | **AAS** (Asset Administration Shell), **i3X** |

PROFINET, EtherCAT e DDS hanno anche una modalità `simulation` che genera valori casuali senza hardware.

Esistono mapper dedicati per OPC UA (nodeId, tipi di dato, quality), Modbus (registri holding/input/coil/discrete), MQTT (parsing JSON automatico) e AAS. Tutti gli altri protocolli usano il GenericMapper. Vedi la [guida ai Connettori Industriali](./docs/IndustrialConnectors.it.md).

## 🎯 Vantaggi della Nuova Architettura

1. **Semplicità**: Un unico formato dati unificato
2. **Flessibilità**: Discovery automatica + personalizzazione manuale
3. **Scalabilità**: Multi-transport per diversi use cases
4. **Manutenibilità**: Configurazione centralizzata in mapping.json
5. **Estensibilità**: Facile aggiungere nuovi mapper e transport

## 📝 Note di Migrazione

Se stai migrando dalla versione precedente:

1. Il vecchio formato entities/attributes è stato sostituito da devices/measurements
2. Le relazioni (relationships) sono state rimosse per semplicità
3. NGSI-LD export è stato semplificato
4. Mapping configuration è ora in mapping.json invece di essere distribuita

## 🛠️ API Programmatica

```javascript
// Accesso diretto al MappingEngine
const { MappingEngine } = require('./src/mappingTools');

const engine = new MappingEngine({
  namespace: 'urn:ngsi-ld:industry50',
  mappingConfigPath: './config/mapping.json'
});

// Map data
const device = await engine.mapData(sourceData, 'opcua', context);

// Export in JSON
const jsonData = engine.exportData('json');

// Export in TOON
const toonData = engine.exportData('toon');

// Get discovered devices
const devices = engine.getDiscoveredDevices();

// Get statistics
const stats = engine.getStatistics();
```

## 📖 Ulteriori Risorse

- [Context broker](./docs/Broker.it.md): entità, subscription, storico, comandi, cluster, deploy
- [Documentazione API](./docs/API.it.md) (endpoint del motore dei connettori, ora sotto `/api/v1/system`)
- [Guida alla Configurazione](./docs/Configuration.it.md)
- [Configurazione Dinamica](./docs/DynamicConfiguration.it.md)
- [Guida al Mapping](./docs/Mapping.it.md)
- [Auto Discovery](./docs/AutoDiscovery.it.md)
- [Connettori Industriali](./docs/IndustrialConnectors.it.md)
- [Guida Storage](./docs/Storage.it.md)
- [Web UI](https://github.com/Engineering-Research-and-Development/universal-data-connector-ui/blob/main/README.it.md): dashboard, grafici in tempo reale, storico

---

**Universal Data Connector v2.0** - Industry 5.0 Ready 🚀
