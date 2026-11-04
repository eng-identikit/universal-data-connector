# UDC Context Broker

L'Universal Data Connector è un **context broker standalone**: legge i dispositivi industriali tramite i connettori, ne mantiene lo stato come **entità**, notifica i sistemi esterni con le **subscription** e conserva lo **storico** di ogni attributo. Non implementa NGSI, ed è una scelta voluta.

- [Architettura](#architettura)
- [Avvio](#avvio)
- [Modello entità](#modello-entità)
- [API](#api)
- [Subscription](#subscription)
- [Stream in tempo reale](#stream-in-tempo-reale)
- [Connettori, comandi e storico](#connettori-comandi-e-storico)
- [Query geografiche](#query-geografiche)
- [Sicurezza e tenant](#sicurezza-e-tenant)
- [Cluster](#cluster)
- [Configurazione](#configurazione)
- [Prestazioni](#prestazioni)
- [Test](#test)
- [Limiti noti e scelte](#limiti-noti-e-scelte)

## Architettura

```
 Connettori (OPC UA, Modbus, MQTT, S7, ...) ─► ConnectorBridge ─┐
 POST /ingest ──────────────────────────────────────────────────┼─► coda UDC_INGEST (JetStream, 16 partizioni)
                                                                │        │ batch fusi per partizione
 API /entities (scritture sincrone) ─────────────┐              │        ▼
                                                 ▼              │   EntityService ──► PostgreSQL: entità, relazioni (+ PostGIS)
                                            EntityService ──────┘        │            TimescaleDB: storico attributi
                                                 │                       │
                                                 └──── ChangeBus (NATS, a pacchetti) ◄───┘
                                                          │
                       ┌──────────────────────────────────┼─────────────────────────────┐
                       ▼                                  ▼                             ▼
              SubscriptionEngine                      StreamHub                 (ogni istanza)
              (partizioni possedute)          WebSocket /api/v1/stream
                       │
                       ▼
          coda UDC_NOTIFY (JetStream, at-least-once) ─► DeliveryWorker ─► HTTP / MQTT / NATS / WebSocket
                                                              └─ DLQ dopo i tentativi
```

- **PostgreSQL** è la fonte di verità per entità, relazioni, subscription, utenti, tenant, comandi e storico (hypertable TimescaleDB).
- **NATS JetStream** fornisce le due code durevoli: ingest e notifiche. Le modifiche alle entità arrivano a tutte le istanze su NATS core, a pacchetti.
- Senza `NATS_URL` il broker gira in **istanza singola** con code in memoria.

### Servizi opzionali: NATS e MQTT

Solo PostgreSQL è obbligatorio. NATS e Mosquitto si attivano da configurazione:

| Servizio | Cosa offre | Senza |
|---|---|---|
| NATS JetStream | Code durevoli (at-least-once anche dopo un riavvio), modalità cluster, predefinito del canale `nats` | Una sola istanza; code in memoria, quindi le notifiche non ancora consegnate si perdono al riavvio; le subscription `nats` devono indicare `notify.url` |
| MQTT (Mosquitto) | Broker predefinito del canale `mqtt` | Le subscription `mqtt` devono indicare `notify.url` |

Le subscription HTTP e WebSocket non richiedono nessuno dei due. Una subscription che conta su un broker predefinito non configurato viene rifiutata con 400, e il wizard della UI chiede l'URL.

- **Compose**: `COMPOSE_PROFILES` in `.env` avvia i servizi inclusi (`nats`, `mqtt`, entrambi o nessuno). `COMPOSE_NATS_URL` e `COMPOSE_MQTT_URL` dicono al container del backend cosa usare (`nats://nats:4222`, `mqtt://mqtt:1883`, un server esterno, oppure vuoto). La modalità cluster richiede NATS.
- **Helm**: `nats.enabled=false` toglie NATS (richiede `backend.replicas=1`); `mqtt.bundled=true` aggiunge un Mosquitto, oppure `mqtt.url` punta a uno esterno.
- **Fuori dai container**: `NATS_URL` e `BROKER_MQTT_URL`.

## Avvio

| Modalità | Comando |
|---|---|
| Sviluppo, istanza singola | `cp .env.example .env` (imposta `BROKER_JWT_SECRET` e `BROKER_ADMIN_PASSWORD`), poi `docker compose up -d --build` |
| Cluster con Compose | `docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d --build` (3 repliche, `UDC_REPLICAS` per cambiarle) |
| Kubernetes | `helm install udc deploy/helm/udc` (vedi `values.yaml`: database e NATS inclusi oppure esterni, NATS e MQTT opzionali) |

La UI è su **http://localhost:8080**: nginx inoltra `/api` e lo stream WebSocket al backend. L'API è anche su `:3000` in istanza singola. Il primo avvio crea il tenant `default` e il superadmin `BROKER_ADMIN_USER`; se `BROKER_ADMIN_PASSWORD` manca, la password viene generata e scritta nel log una volta sola.

## Modello entità

```json
{
  "id": "plant1:line3:press07",
  "type": "Press",
  "version": 1842,
  "createdAt": "2026-11-04T10:00:00.000Z",
  "updatedAt": "2026-11-04T10:05:00.000Z",
  "location": { "type": "Point", "coordinates": [9.19, 45.46] },
  "attrs": {
    "temperature": { "type": "float", "value": 82.4, "unit": "°C", "ts": "...", "quality": "good", "source": "opcua-plc1", "meta": {} },
    "partOf":      { "type": "relationship", "target": "plant1:line3" },
    "setpoint":    { "type": "float", "value": 80, "writable": true,
                     "command": { "connector": "opcua-plc1", "method": "writeNode", "args": ["ns=2;s=SP", "$value", "Double"] } }
  }
}
```

**Identificatori**
- `id`: `[A-Za-z0-9_\-:.]`, al massimo 256 caratteri, univoco nel tenant.
- `type`: inizia con una lettera, poi `[A-Za-z0-9_]`, al massimo 64 caratteri.
- Nomi degli attributi: `[A-Za-z_][A-Za-z0-9_]*`, al massimo 500 per entità.

**Tipi di attributo**
- Sono disponibili `float`, `int`, `bool`, `string`, `datetime`, `json` e `relationship`.
- Se il tipo non è indicato viene inferito dal valore, e ogni numero diventa `float`. In questo modo un sensore che invia `25` e poi `25.5` non viene rifiutato. `int` si ottiene solo dichiarandolo.

**Timestamp e versione**
- `ts` cambia solo quando cambia il valore, oppure quando il producer lo invia esplicitamente.
- `version` aumenta a ogni scrittura. `If-Match: <version>` su PUT, PATCH e DELETE abilita il controllo ottimistico: se la versione non corrisponde la risposta è `409`.

**Aggiornamenti**
- `PATCH` fonde gli attributi uno per uno: un valore `null` rimuove l'attributo, e un attributo parziale (per esempio solo `value`) conserva `unit`, `meta` e gli altri campi.
- `PUT` sostituisce l'intera entità.

**Geometrie**
- `location` è una geometria GeoJSON: Point, MultiPoint, LineString, MultiLineString, Polygon o MultiPolygon. Le coordinate sono `[lon, lat]` e i poligoni devono essere chiusi.

## API

Tutte le route sono sotto `/api/v1`.

**Autenticazione**
- `Authorization: Bearer <jwt>` per gli utenti, oppure `X-API-Key: <key>` per le macchine.

**Tenant**
- Si sceglie con `X-Tenant`. Si può omettere se l'utente ha un solo tenant; per le API key vale sempre il tenant della chiave.

**Errori**
- Formato: `{ error, message, details? }`.

| Area | Endpoint |
|---|---|
| Auth | `POST /auth/login`, `POST /auth/refresh`, `GET /auth/me`, `POST /auth/password` |
| Entità | `GET /entities` (`type`, `idPattern` con `*`, `related`+`depth`+`direction`, `bbox`, `near`+`maxDistance`, `georel`+`geometry`, `located`, `attrs`, `orderBy`, `limit`/`offset`; header `X-Total-Count`), `POST /entities`, `GET`/`PUT`/`PATCH`/`DELETE /entities/:id`, `PUT`/`PATCH`/`DELETE /entities/:id/attrs/:attr`, `GET /entities/:id/graph`, `GET /types` |
| Scrittura in blocco | `POST /entities/batch` (sincrona e atomica: `upsert`, `create`, `update`, `delete`, al massimo 1000 elementi), `POST /ingest` (asincrona, al massimo 5000 per richiesta, risponde `202`), `GET /ingest/stats` |
| Storico | `GET /entities/:id/history?attrs&from&to&maxPoints` (avg/min/max/last per intervallo), `GET /entities/:id/history/attributes`, `GET /entities/:id/history/:attr/raw` |
| Comandi | `POST /entities/:id/attrs/:attr/command {value}` (`?wait=ms`), `GET /commands?entityId&status`, `GET /commands/:id` |
| Subscription | `GET`/`POST /subscriptions`, `GET`/`PATCH`/`DELETE /subscriptions/:id`, `POST /:id/pause`, `POST /:id/resume`, `POST /:id/test`, `DELETE /:id/stats`, `GET /:id/dlq`, `POST /:id/dlq/replay`, `DELETE /:id/dlq`, `POST /subscriptions/preview`, `POST /expressions/validate` |
| Amministrazione | `/users` (superadmin), `/tenants`, `/tenants/:id/members`, `/apikeys`, `GET /cluster` (superadmin) |
| Sistema | `/system/*`: connettori, mapping, configurazione storage, stato del motore. Lettura per tutti gli autenticati, modifiche solo per il superadmin. Ha sostituito la vecchia `/api`. |
| Stream | `ws://host/api/v1/stream?access_token=…&tenant=…` |

## Subscription

Una subscription definisce quali entità osservare, quando notificare, con quale filtro e dove inviare la notifica:

```json
{
  "name": "Presse calde",
  "select":   { "types": ["Press"], "idPattern": "plant1:*", "ids": [], "related": { "id": "plant1:line3", "depth": 2, "direction": "in" }, "bbox": [9.1, 45.4, 9.3, 45.5] },
  "trigger":  { "on": ["create", "update", "delete"], "attrs": ["temperature"], "deadband": { "temperature": 0.5 } },
  "filter":   "temperature > 80 && status == 'RUN'",
  "filterMode": "edge",
  "throttleMs": 5000,
  "periodicMs": null,
  "notify":   { "channel": "http", "url": "https://...", "headers": { "Authorization": "Bearer ..." }, "attrs": ["temperature"], "format": "full", "perEntity": false },
  "delivery": { "maxRetries": 10, "backoffMs": 1000 },
  "expiresAt": null
}
```

**Selezione (`select`)**
- I criteri sono in AND; un elenco vuoto vale "tutti".
- `related` usa le relazioni in modo transitivo e viene ricalcolato quando le relazioni cambiano.

**Trigger**
- Notifica solo se cambia uno degli `attrs` indicati; se l'elenco è vuoto vale qualsiasi attributo.
- La `deadband` ignora le variazioni numeriche entro ±x rispetto all'**ultimo valore notificato**.

**Filtro e modalità**
- `level`: notifica ogni cambiamento mentre la condizione è vera.
- `edge`: notifica solo quando la condizione passa da falsa a vera (allarmi).

**Throttling e invio periodico**
- `throttleMs`: al massimo una notifica per entità in quella finestra; alla fine della finestra viene inviato l'ultimo stato.
- `periodicMs`: invia lo stato corrente di tutte le entità selezionate, a blocchi da 500.

**Canali**

| Canale | Parametri | Note |
|---|---|---|
| `http` | `url`, `method`, `headers`, `timeoutMs` | Successo solo con risposta 2xx; i redirect non vengono seguiti |
| `mqtt` | `url` (predefinito `BROKER_MQTT_URL`), `topic`, `qos`, `retain`, credenziali | |
| `nats` | `subject`, `url` (predefinito: il NATS del broker) | |
| `websocket` | nessuno | Ai client dello stream iscritti; best-effort |

- `topic` e `subject` accettano `{tenant}`, `{subscription}`, `{entityType}` e `{entityId}`.
- `perEntity: true` invia un messaggio per ogni entità.
- `format: "values"` invia `{ id, type, attr: valore }` invece dell'entità completa.

**Notifica**

`{ id, subscriptionId, subscriptionName, tenant, reason: create|update|delete|periodic|test, timestamp, entities: [...] }`

**Consegna**
- La garanzia è **at-least-once**: retry con backoff esponenziale (`backoffMs × 2^n`, al massimo 5 minuti, con jitter).
- Dopo `maxRetries` la notifica passa nella **DLQ**, che si può ispezionare, reinviare o svuotare dall'API e dalla UI.
- L'ordine fra notifiche non è garantito.

**Altro**
- Chi ha solo il ruolo viewer vede mascherati i segreti negli header (Authorization, token, key, password).

### Linguaggio dei filtri

| Elemento | Sintassi |
|---|---|
| Valore attributo | `temperature` (per le relazioni: l'id del target) |
| Campi attributo | `temperature.unit`, `.quality`, `.ts`, `.source`, `.type`, `.target`, `.meta.chiave` |
| Stato precedente | `prev.temperature`, `prev.temperature.quality` (`null` alla creazione) |
| Entità | `@id`, `@type`, `@version` |
| Operatori | `== != > >= < <=`, `&& \|\| !` (anche `and or not`), `+ - * / %`, `in [..]`, `contains`, `matches 'regex'` |
| Funzioni | `abs round(x, cifre) floor ceil min max len lower upper has('attr') startsWith endsWith now() age(ts)` |

Esempi:
- `abs(temperature - prev.temperature) > 5`
- `@type in ['Press','Lathe'] && temperature.quality != 'good'`
- `age(heartbeat.ts) > 300`

La valutazione non lancia mai errori: tipi incompatibili rendono falso il confronto, e l'aritmetica su valori non numerici produce `null`.

## Stream in tempo reale

```
ws://host/api/v1/stream?access_token=<jwt>&tenant=<id>      (oppure api_key=<key>)
→ { "type": "subscribe", "id": "live", "entities": { "types": ["Press"], "idPattern": "plant1:*" } }
→ { "type": "subscribe", "id": "n1", "subscription": "<uuid di una subscription websocket>" }
← { "type": "welcome" } · { "type": "entity", "sub": "live", "op": "update", "entity": {...} } · { "type": "notification", "sub": "n1", ... }
```

- Le iscrizioni vanno inviate dopo `welcome`.
- Alla scadenza del token il server chiude con codice `4001`; la UI rinnova il token e si riconnette.
- I client lenti oltre 5 MB di buffer perdono messaggi e non bloccano il server.

## Connettori, comandi e storico

**Da dispositivo a entità**
- Ogni dispositivo mappato da una sorgente diventa un'entità del tenant `tenant` della sorgente (predefinito `default`).
- L'id è `entityIdPrefix` + id del dispositivo, con i caratteri non ammessi sostituiti da `_`.
- I measurement diventano attributi, con `ts` e `quality` presi dai metadata, `source` = id del connettore e `unit` da `mapping.json`.
- Le scritture passano dall'ingest: coalescing per entità ogni 200 ms (`BROKER_INGEST_FLUSH_MS`), coda durevole, batch fusi. Le entità che non cambiano non generano scritture né eventi.

**Comandi**
- L'attributo deve avere `writable: true` e un `command { connector, method, args }`. `method` deve essere un metodo di scrittura del connettore (`writeNode`, `writeRegister`, `writeVariable`, `writeTag`…) e `"$value"` negli args viene sostituito dal valore inviato.
- Nel cluster risponde l'istanza che esegue quel connettore.
- Ciclo di vita: `pending`, `sent`, poi `succeeded` oppure `failed`. Il timeout è `BROKER_COMMAND_TIMEOUT_MS`.
- Il valore dell'attributo si aggiorna alla lettura successiva del dispositivo.

**Storico**
- Viene salvata una riga per ogni nuovo `ts` di un attributo, nella stessa scrittura dell'entità.
- `BROKER_HISTORY_RETENTION` (per esempio `90 days`) applica una retention policy TimescaleDB, `BROKER_HISTORY=off` disattiva lo storico.
- Lo storico resta anche dopo la cancellazione dell'entità (audit) e viene rimosso insieme al tenant.

## Query geografiche

Richiedono PostGIS, che è incluso nell'immagine `timescale/timescaledb-ha`. Senza PostGIS i parametri geo rispondono `501`.

| Parametro | Esempio |
|---|---|
| `bbox=minLon,minLat,maxLon,maxLat` | `bbox=9.1,45.4,9.3,45.5` |
| `near=lon,lat&maxDistance=m` | Ordina per distanza e aggiunge `distanceM` |
| `georel=within\|intersects\|disjoint\|contains&geometry=<GeoJSON>` | Il GeoJSON va codificato nell'URL |
| `located=true` | Solo entità con una posizione |

Le subscription accettano `select.bbox`.

## Sicurezza e tenant

| Ruolo | Permessi nel tenant |
|---|---|
| viewer | Lettura di entità, storico, subscription (segreti mascherati), comandi |
| operator | viewer + scrittura di entità, ingest, comandi, subscription |
| admin | operator + membri e API key del tenant |
| superadmin | Tutti i tenant, utenti, cluster, modifiche alla configurazione dei connettori (`/system`) |

**Password**
- Salvate con scrypt.
- Dopo 5 tentativi falliti in 15 minuti il login si blocca per quella coppia utente/IP.

**Token**
- Access token da 15 minuti, refresh token da 7 giorni.
- Cambiare la password o disabilitare l'utente revoca tutti i token emessi.

**API key**
- Salvate come hash SHA-256 e mostrate una sola volta.
- Hanno un ruolo, una scadenza e un elenco opzionale di `allowedTypes`, che limita sia la lettura sia la scrittura.

## Cluster

Con `BROKER_CLUSTER=true` (e NATS) più istanze condividono il lavoro attraverso **lease** in PostgreSQL. I lease durano 3 tick e vengono rinnovati ogni tick (`BROKER_CLUSTER_TICK_MS`, predefinito 5 s).

| Risorsa | Distribuzione |
|---|---|
| `sub:0..31` | Partizioni delle subscription (hash dell'id). Chi le possiede valuta i cambiamenti, gestisce throttle e invio periodico. |
| `ingest:0..15` | Partizioni dell'ingest (hash dell'id dell'entità). Una sola istanza le consuma, in ordine. |
| `connector:<id>` | Ogni connettore gira su una sola istanza. Anche i comandi verso quel connettore li esegue lei. |
| Code delle notifiche | Consumer JetStream condiviso fra tutte le istanze, nessun lease |
| Stream WebSocket | Su ogni istanza |

**Bilanciamento**
- Ogni istanza tiene una quota equa di risorse: `ceil(risorse / istanze vive)`.

**Ribilanciamento**
- Quando un'istanza entra o esce in modo pulito, le partizioni delle subscription vengono cedute in "draining": chi le cede continua a valutarle finché un'altra istanza non le ha prese. Il risultato è al più qualche duplicato, mai un buco, come verificato dal test del cluster.
- I connettori invece passano di mano direttamente, perché non devono mai girare su due istanze insieme.

**Crash di un'istanza**
- Le sue risorse passano alle altre entro circa 15 s.

**Configurazione dei connettori**
- `sources.json` e `mapping.json` sono condivisi tramite il database (`system_config`). Ogni modifica fatta da `/system` e ogni salvataggio della discovery si propaga alle altre istanze. `BROKER_CONFIG_SYNC=false` lo disattiva.

**Monitoraggio**
- La pagina Amministrazione → Cluster mostra istanze, partizioni, connettori, ritardo dell'event loop (sopra 200 ms l'istanza è satura), tempo di apply dell'ingest e backlog delle code.

## Configurazione

| Variabile | Predefinito | Descrizione |
|---|---|---|
| `BROKER_DB_URL` oppure `BROKER_DB_HOST`/`PORT`/`NAME`/`USER`/`PASSWORD` | `localhost:5432/udc_broker` | PostgreSQL (con TimescaleDB e PostGIS consigliati) |
| `BROKER_DB_POOL_SIZE` | 20 | Connessioni per istanza |
| `BROKER_JWT_SECRET` | casuale | **Obbligatoria in produzione e nei cluster**: deve essere uguale su tutte le istanze |
| `BROKER_ADMIN_USER` / `BROKER_ADMIN_PASSWORD` | `admin` / generata | Superadmin creato al primo avvio |
| `BROKER_ACCESS_TOKEN_TTL` / `BROKER_REFRESH_TOKEN_TTL` | `15m` / `7d` | |
| `NATS_URL` | — | Abilita JetStream (code durevoli) e il cluster |
| `BROKER_CLUSTER` | `false` | Distribuzione del lavoro fra istanze |
| `BROKER_CLUSTER_TICK_MS` | 5000 | Rinnovo dei lease |
| `BROKER_CONFIG_SYNC` | `true` (nel cluster) | Condivisione di `sources.json` e `mapping.json` |
| `BROKER_INGEST_FLUSH_MS` | 200 | Coalescing degli update dei connettori |
| `BROKER_INGEST_PARTITIONS` | 16 | Partizioni dell'ingest (uguale su tutte le istanze) |
| `BROKER_HISTORY` / `BROKER_HISTORY_RETENTION` | `on` / — | Storico degli attributi e sua retention |
| `BROKER_MQTT_URL` | — | Broker MQTT predefinito per le notifiche |
| `BROKER_COMMAND_TIMEOUT_MS` | 10000 | Timeout dei comandi |
| `UDC_RAW_STORAGE` | — | `memory` ignora `storage.json`, cioè il buffer dei dati grezzi dei connettori |
| `PORT`, `BODY_LIMIT`, `LOG_LEVEL` | 3000, `5mb`, `info` | |

## Prestazioni

Il test di carico è `tests/load/load-test.js` (vedi l'intestazione del file):
- semina N entità su 100 tipi;
- mantiene un tasso di update costante tramite `/ingest`, con M subscription selettive (circa il 10% degli update di ogni tipo genera una notifica HTTP);
- misura backlog, latenza di scrittura end-to-end e latenza delle notifiche.

Risultati misurati il 2026-11-04 su un portatile Windows con Docker Desktop. Le 8 vCPU erano condivise da PostgreSQL, NATS, le repliche del broker, nginx e il generatore stesso. Il target era 100k entità, 10k update/s e 100 subscription.

| Configurazione | Inviati | Applicati in modo sostenuto | Note |
|---|---|---|---|
| Seed di 100k entità | — | 12–25k entità/s | |
| 3 repliche, prima delle ottimizzazioni | 9.6k/s | 5.4k/s | Backlog crescente (attese sui lock di riga, un messaggio NATS per entità) |
| 3 repliche, versione finale | 8.4k/s | Backlog stabile durante il carico | Scrittura p95 3.1 s; istanze sature (event loop p99 fino a 500 ms) |
| 5 repliche, versione finale | 8.4k/s | **8.2k/s, backlog svuotato in 1 s** | Il generatore è diventato il limite |
| 5 repliche, 2 generatori | 9.1k/s | 7.9k/s | Backlog svuotato in 9 s; tutta la macchina satura |
| Solo PostgreSQL, 16 worker paralleli | — | 22k/s | Il database ha margine |

- La latenza delle notifiche va da circa 0,12 s (p50, sistema non saturo) a 1–2 s (p50, sistema saturo).
- **I 10k/s sostenuti non sono stati dimostrati su questa macchina.** Il limite è la CPU dell'host condiviso, non il database e non l'architettura: con 5 repliche il broker applicava tutto ciò che il generatore riusciva a inviare.
- Su nodi dedicati ci si aspetta che il throughput cresca con le repliche, fino al limite del database (circa 22k/s misurati), ma va verificato sull'hardware di destinazione.

## Test

```
docker compose up -d db nats mqtt
NATS_URL=nats://localhost:4222 TEST_MQTT_URL=mqtt://localhost:1883 npm run test:broker
```

Le suite sono modello, linguaggio dei filtri, API, subscription, dati (ingest, storico, comandi, conflitti), geo e cluster (due istanze nello stesso processo). Senza `NATS_URL` usano le code in memoria.

Prima dei test vanno fermate altre istanze del broker sullo stesso database e NATS, per esempio il backend del compose: entrerebbero nella divisione del lavoro e falserebbero i risultati.

## Limiti noti e scelte

**Garanzie di consegna**
- Il fan-out delle modifiche usa NATS core, quindi è at-most-once. La garanzia at-least-once vale dalla coda delle notifiche in poi.
- Se un'istanza va in crash fra il commit e la pubblicazione, o durante i ~15 s di failover delle partizioni di subscription dopo un crash, le notifiche di quelle modifiche possono andare perse. Un ribilanciamento ordinato invece non crea buchi.
- Il canale `websocket` raggiunge solo i client collegati.

**Ordine**
- L'ordine delle notifiche non è garantito.
- Un batch di ingest che fallisce sul database e viene ritentato può essere superato da batch più recenti della stessa partizione.

**Sicurezza**
- I webhook possono puntare a indirizzi interni, perché nelle reti industriali è spesso necessario. Se serve, limitarli con policy di rete (egress).

**Differenze rispetto al progetto iniziale**
- I comandi hanno un endpoint esplicito (`/command`) invece di scattare con il PATCH di un attributo scrivibile: così il valore memorizzato resta quello letto dal dispositivo.
- JetStream non è usato come log delle modifiche: per il fan-out basta NATS core, e i dati restano durevoli in PostgreSQL e nelle due code.
- I numeri vengono inferiti come `float`.

**Codice preesistente corretto lungo il percorso**
- Lo schema di `sources.json` accettava solo 6 tipi di connettore.
- Il reload delle sorgenti chiamava un metodo inesistente.
- Il mapper MQTT ignorava il payload emesso dal connettore MQTT.
- Un errore Redis senza listener mandava in crash il processo.
- Salvare un connettore dalla UI non lo avviava.
