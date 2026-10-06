# Storage Configuration Guide

**[🇬🇧 English](Storage.md)** | **[🇮🇹 Italiano](Storage.it.md)**

---

The Universal Data Connector supports multiple storage types to persist data collected from various sources. This guide explains how to configure and use the available storage backends.

## Tipi di Storage Supportati

### 1. Memory Storage (Predefinito)
Storage temporaneo in memoria per test e sviluppo.

```json
{
  "storage": {
    "type": "memory",
    "config": {
      "maxRecords": 10000,
      "ttl": 3600000
    }
  }
}
```

**Parametri:**
- `maxRecords`: Numero massimo di record da mantenere in memoria
- `ttl`: Time-to-live in millisecondi per i record

### 2. PostgreSQL
Database relazionale per storage persistente ad alte prestazioni.

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
      "schema": "public",
      "table": "sensor_data",
      "pool": {
        "min": 2,
        "max": 10,
        "acquireTimeout": 30000,
        "createTimeoutMillis": 30000,
        "destroyTimeoutMillis": 5000,
        "idleTimeoutMillis": 30000
      }
    }
  }
}
```

**Setup Database:**
```sql
CREATE DATABASE universal_data_connector;

CREATE TABLE sensor_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_sensor_data_source_timestamp ON sensor_data(source_id, timestamp);
CREATE INDEX idx_sensor_data_timestamp ON sensor_data(timestamp);
```

### 3. MariaDB/MySQL
Database relazionale compatibile con MySQL per ambienti misti.

```json
{
  "storage": {
    "type": "mariadb",
    "config": {
      "host": "localhost",
      "port": 3306,
      "database": "universal_data_connector",
      "username": "root",
      "password": "password",
      "table": "sensor_data",
      "pool": {
        "min": 2,
        "max": 10,
        "acquireTimeout": 30000
      }
    }
  }
}
```

**Setup Database:**
```sql
CREATE DATABASE universal_data_connector;
USE universal_data_connector;

CREATE TABLE sensor_data (
    id VARCHAR(36) PRIMARY KEY DEFAULT (UUID()),
    source_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_source_timestamp (source_id, timestamp),
    INDEX idx_timestamp (timestamp)
);
```

### 4. MongoDB
Database NoSQL per dati semi-strutturati e flessibilità di schema.

```json
{
  "storage": {
    "type": "mongodb",
    "config": {
      "uri": "mongodb://localhost:27017",
      "database": "universal_data_connector",
      "collection": "sensor_data",
      "options": {
        "maxPoolSize": 10,
        "serverSelectionTimeoutMS": 5000,
        "socketTimeoutMS": 45000
      }
    }
  }
}
```

**Setup Collection:**
```javascript
// Connetti a MongoDB
use universal_data_connector

// Crea indici per performance
db.sensor_data.createIndex({ "sourceId": 1, "timestamp": -1 })
db.sensor_data.createIndex({ "timestamp": -1 })
db.sensor_data.createIndex({ "sourceId": 1 })
```

### 5. Redis
Storage in memoria ad alte prestazioni con persistenza opzionale.

```json
{
  "storage": {
    "type": "redis",
    "config": {
      "host": "localhost",
      "port": 6379,
      "password": "",
      "db": 0,
      "keyPrefix": "udc:",
      "ttl": 3600,
      "maxMemory": "256mb"
    }
  }
}
```

**Configurazione Redis:**
```
# redis.conf
maxmemory 256mb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
```

## Gestione della Configurazione

### File di Configurazione
La configurazione dello storage è definita nel file `config/storage.json`:

```json
{
  "storage": {
    "type": "postgresql",
    "config": {
      // configurazione specifica
    }
  },
  "alternatives": {
    "mongodb": {
      "type": "mongodb",
      "config": {
        // configurazione alternativa
      }
    }
  }
}
```

### API REST per Storage

#### Ottenere la configurazione corrente
```http
GET /api/config/storage
```

#### Aggiornare la configurazione
```http
PUT /api/config/storage
Content-Type: application/json

{
  "storage": {
    "type": "postgresql",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "my_db",
      "username": "user",
      "password": "pass"
    }
  }
}
```

#### Testare una configurazione
```http
POST /api/config/storage/test
Content-Type: application/json

{
  "type": "postgresql",
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "test_db",
    "username": "user",
    "password": "pass"
  }
}
```

#### Validare una configurazione
```http
POST /api/config/storage/validate
Content-Type: application/json

{
  "type": "mongodb",
  "config": {
    "uri": "mongodb://localhost:27017",
    "database": "test_db"
  }
}
```

#### Verificare lo stato dello storage
```http
GET /api/config/storage/health
```

#### Ottenere i tipi di storage disponibili
```http
GET /api/config/storage/types
```

## Avvio, fallback e cambio a caldo

- **Avvio:** il server legge `config/storage.json` (`storage`) e si connette allo storage configurato. I tipi supportati sono `memory`, `redis` e `timescaledb` (alias `timescale`). Alcuni campi vengono normalizzati: `user` → `username`, `db` → `database`, `maxRecords` → `maxDataPoints`.
- **Fallback:** se lo storage non è raggiungibile, il server **parte comunque** e tiene i dati in memoria. Riprova a connettersi ogni 30 s (variabile `STORAGE_RETRY_INTERVAL`, in ms). Quando lo storage torna disponibile, ci trasferisce i dati accumulati. Lo stato è visibile nella pagina Archiviazione e in `GET /api/config/storage` (`runtime`).
- **Cambio a caldo** (`POST /api/config/storage/configure` o pulsante *Applica e Ricarica*): il nuovo storage viene validato e connesso **prima** di sostituire quello attuale. Se fallisce, resta attivo il precedente e `storage.json` non viene modificato. Le `alternatives` presenti nel file vengono mantenute.
- **Test connessione** (`POST /api/config/storage/test`): si connette, esegue un health check e si disconnette, **senza scrivere né cancellare dati**.

## Migrazione tra Storage

### 1. Backup dei dati correnti
```bash
# Esporta dati via API
curl -X GET "http://localhost:3000/api/data?limit=10000" > backup.json
```

### 2. Cambiare configurazione storage
```bash
# Aggiorna configurazione via API
curl -X PUT "http://localhost:3000/api/config/storage" \
  -H "Content-Type: application/json" \
  -d @new-storage-config.json
```

### 3. Riavviare il servizio
```bash
npm restart
```

### 4. Importare dati (se necessario)
```bash
# Implementare script di importazione personalizzato
node scripts/import-data.js backup.json
```

## Best Practices

### Performance
1. **PostgreSQL/MariaDB**: Usa indici appropriati su `source_id` e `timestamp`
2. **MongoDB**: Configura indici compound per query frequenti
3. **Redis**: Configura TTL appropriato per evitare out-of-memory
4. **Pool Connections**: Tunia la dimensione del pool basandoti sul carico

### Sicurezza
1. **Credenziali**: Usa variabili d'ambiente per password sensibili
2. **SSL/TLS**: Abilita connessioni sicure per database remoti
3. **Firewall**: Limita l'accesso ai database solo dai server necessari
4. **Backup**: Implementa strategie di backup regolari

### Monitoraggio
1. **Health Checks**: Usa l'endpoint `/api/config/storage/health`
2. **Metriche**: Monitora performance e utilizzo risorse
3. **Logging**: Abilita logging dettagliato per debug
4. **Alerting**: Configura alert per problemi di connessione

## Risoluzione Problemi

### Errori di Connessione
```javascript
// Verifica configurazione
const testResult = await storageManager.testConnection(type, config);
console.log(testResult);
```

### Performance Lenta
1. Verifica indici del database
2. Ottimizza query e batch size
3. Monitora utilizzo CPU/memoria
4. Considera sharding per volumi elevati

### Errori di Memoria (Redis)
1. Configura `maxmemory-policy`
2. Riduci TTL dei dati
3. Implementa cleanup automatico
4. Monitora uso memoria

## Esempi di Utilizzo

### Configurazione per Produzione (PostgreSQL)
```json
{
  "storage": {
    "type": "postgresql",
    "config": {
      "host": "prod-db.company.com",
      "port": 5432,
      "database": "udc_production",
      "username": "udc_user",
      "password": "${DB_PASSWORD}",
      "schema": "sensor_data",
      "table": "measurements",
      "pool": {
        "min": 5,
        "max": 25,
        "acquireTimeout": 60000,
        "idleTimeout": 30000
      },
      "ssl": {
        "rejectUnauthorized": true,
        "ca": "path/to/ca-cert.pem"
      }
    }
  }
}
```

### Configurazione per Sviluppo (Memory)
```json
{
  "storage": {
    "type": "memory",
    "config": {
      "maxRecords": 1000,
      "ttl": 600000
    }
  }
}
```

### Configurazione Ibrida (Redis + MongoDB)
```json
{
  "storage": {
    "type": "redis",
    "config": {
      "host": "cache.company.com",
      "port": 6379,
      "ttl": 300,
      "keyPrefix": "udc:hot:"
    }
  },
  "alternatives": {
    "mongodb_archive": {
      "type": "mongodb",
      "config": {
        "uri": "mongodb://archive.company.com:27017",
        "database": "udc_archive",
        "collection": "historical_data"
      }
    }
  }
}
```
