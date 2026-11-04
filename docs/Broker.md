# UDC Context Broker

The Universal Data Connector is a **standalone context broker**. It reads industrial devices through its connectors, keeps their state as **entities**, notifies external systems through **subscriptions** and stores the **history** of every attribute. It does not implement NGSI, by design.

- [Architecture](#architecture)
- [Running](#running)
- [Entity model](#entity-model)
- [API](#api)
- [Subscriptions](#subscriptions)
- [Realtime stream](#realtime-stream)
- [Connectors, commands and history](#connectors-commands-and-history)
- [Geo queries](#geo-queries)
- [Security and tenants](#security-and-tenants)
- [Cluster](#cluster)
- [Configuration](#configuration)
- [Performance](#performance)
- [Tests](#tests)
- [Known limits and choices](#known-limits-and-choices)

## Architecture

```
 Connectors (OPC UA, Modbus, MQTT, S7, ...) ─► ConnectorBridge ─┐
 POST /ingest ──────────────────────────────────────────────────┼─► UDC_INGEST queue (JetStream, 16 partitions)
                                                                │        │ batches merged per partition
 API /entities (synchronous writes) ─────────────┐              │        ▼
                                                 ▼              │   EntityService ──► PostgreSQL: entities, relationships (+ PostGIS)
                                            EntityService ──────┘        │            TimescaleDB: attribute history
                                                 │                       │
                                                 └──── ChangeBus (NATS, batched) ◄───────┘
                                                          │
                       ┌──────────────────────────────────┼─────────────────────────────┐
                       ▼                                  ▼                             ▼
              SubscriptionEngine                      StreamHub                 (every instance)
              (owned partitions)              WebSocket /api/v1/stream
                       │
                       ▼
          UDC_NOTIFY queue (JetStream, at-least-once) ─► DeliveryWorker ─► HTTP / MQTT / NATS / WebSocket
                                                              └─ DLQ after retries
```

- **PostgreSQL** is the source of truth for entities, relationships, subscriptions, users, tenants, commands and history (a TimescaleDB hypertable).
- **NATS JetStream** provides the two durable queues: ingest and notifications. Entity changes reach every instance over core NATS, in batches.
- Without `NATS_URL` the broker runs as a **single instance** with in-memory queues.

### Optional services: NATS and MQTT

Only PostgreSQL is mandatory. NATS and Mosquitto are switched on by configuration:

| Service | What it gives | Without it |
|---|---|---|
| NATS JetStream | Durable queues (at-least-once across restarts), cluster mode, default for the `nats` channel | One instance only; queues in memory, so notifications not yet delivered are lost on restart; `nats` subscriptions must set `notify.url` |
| MQTT (Mosquitto) | Default broker for the `mqtt` channel | `mqtt` subscriptions must set `notify.url` |

HTTP and WebSocket subscriptions need neither. A subscription that relies on a default broker that is not configured is rejected with 400, and the UI wizard asks for the URL.

- **Compose**: `COMPOSE_PROFILES` in `.env` starts the bundled services (`nats`, `mqtt`, both or none). `COMPOSE_NATS_URL` and `COMPOSE_MQTT_URL` tell the backend container what to use (`nats://nats:4222`, `mqtt://mqtt:1883`, an external server, or empty). Cluster mode needs NATS.
- **Helm**: `nats.enabled=false` removes NATS (requires `backend.replicas=1`); `mqtt.bundled=true` adds a Mosquitto, or `mqtt.url` points to an external one.
- **Outside containers**: `NATS_URL` and `BROKER_MQTT_URL`.

## Running

| Mode | Command |
|---|---|
| Development, single instance | `cp .env.example .env` (set `BROKER_JWT_SECRET` and `BROKER_ADMIN_PASSWORD`), then `docker compose up -d --build` |
| Cluster with Compose | `docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d --build` (3 replicas; `UDC_REPLICAS` changes the count) |
| Kubernetes | `helm install udc deploy/helm/udc` (see `values.yaml`: database and NATS bundled or external, NATS and MQTT optional) |

The UI is at **http://localhost:8080**: nginx forwards `/api` and the WebSocket stream to the backend. In single-instance mode the API is also on `:3000`. The first start creates the `default` tenant and the superadmin `BROKER_ADMIN_USER`; if `BROKER_ADMIN_PASSWORD` is missing, a password is generated and logged once.

## Entity model

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

**Identifiers**
- `id`: `[A-Za-z0-9_\-:.]`, at most 256 characters, unique within the tenant.
- `type`: starts with a letter, then `[A-Za-z0-9_]`, at most 64 characters.
- Attribute names: `[A-Za-z_][A-Za-z0-9_]*`, at most 500 per entity.

**Attribute types**
- The types are `float`, `int`, `bool`, `string`, `datetime`, `json` and `relationship`.
- When no type is given it is inferred from the value, and every number becomes `float`, so a sensor sending `25` and then `25.5` is not rejected. `int` must be declared explicitly.

**Timestamps and version**
- `ts` changes only when the value changes, or when the producer sends it explicitly.
- `version` increases on every write. `If-Match: <version>` on PUT, PATCH and DELETE enables optimistic concurrency: a mismatch returns `409`.

**Updates**
- `PATCH` merges attributes one by one: a `null` value removes the attribute, and a partial attribute (for example only `value`) keeps `unit`, `meta` and the other fields.
- `PUT` replaces the whole entity.

**Geometries**
- `location` is a GeoJSON geometry: Point, MultiPoint, LineString, MultiLineString, Polygon or MultiPolygon. Coordinates are `[lon, lat]` and polygons must be closed.

## API

All routes are under `/api/v1`.

**Authentication**
- `Authorization: Bearer <jwt>` for users, or `X-API-Key: <key>` for machines.

**Tenant**
- Chosen with `X-Tenant`. It can be omitted when the user has a single tenant; API keys always use the key's tenant.

**Errors**
- Format: `{ error, message, details? }`.

| Area | Endpoints |
|---|---|
| Auth | `POST /auth/login`, `POST /auth/refresh`, `GET /auth/me`, `POST /auth/password` |
| Entities | `GET /entities` (`type`, `idPattern` with `*`, `related`+`depth`+`direction`, `bbox`, `near`+`maxDistance`, `georel`+`geometry`, `located`, `attrs`, `orderBy`, `limit`/`offset`; `X-Total-Count` header), `POST /entities`, `GET`/`PUT`/`PATCH`/`DELETE /entities/:id`, `PUT`/`PATCH`/`DELETE /entities/:id/attrs/:attr`, `GET /entities/:id/graph`, `GET /types` |
| Bulk writes | `POST /entities/batch` (synchronous and atomic: `upsert`, `create`, `update`, `delete`, at most 1000 items), `POST /ingest` (asynchronous, at most 5000 per request, answers `202`), `GET /ingest/stats` |
| History | `GET /entities/:id/history?attrs&from&to&maxPoints` (avg/min/max/last per bucket), `GET /entities/:id/history/attributes`, `GET /entities/:id/history/:attr/raw` |
| Commands | `POST /entities/:id/attrs/:attr/command {value}` (`?wait=ms`), `GET /commands?entityId&status`, `GET /commands/:id` |
| Subscriptions | `GET`/`POST /subscriptions`, `GET`/`PATCH`/`DELETE /subscriptions/:id`, `POST /:id/pause`, `POST /:id/resume`, `POST /:id/test`, `DELETE /:id/stats`, `GET /:id/dlq`, `POST /:id/dlq/replay`, `DELETE /:id/dlq`, `POST /subscriptions/preview`, `POST /expressions/validate` |
| Administration | `/users` (superadmin), `/tenants`, `/tenants/:id/members`, `/apikeys`, `GET /cluster` (superadmin) |
| System | `/system/*`: connectors, mapping, storage configuration, engine status. Any authenticated caller can read; only a superadmin can change anything. Replaces the former `/api`. |
| Stream | `ws://host/api/v1/stream?access_token=…&tenant=…` |

## Subscriptions

A subscription defines which entities to watch, when to notify, which filter to apply and where to send the notification:

```json
{
  "name": "Hot presses",
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

**Selection (`select`)**
- Criteria are ANDed; an empty list means "all".
- `related` follows relationships transitively and is refreshed when relationships change.

**Trigger**
- A notification fires only when one of the listed `attrs` changes; an empty list means any attribute.
- `deadband` ignores numeric changes within ±x of the **last notified value**.

**Filter and mode**
- `level`: notify every change while the condition holds.
- `edge`: notify only when the condition turns from false to true (alarms).

**Throttling and periodic sends**
- `throttleMs`: at most one notification per entity in that window; the latest state is sent when the window ends.
- `periodicMs`: sends the current state of all selected entities, in chunks of 500.

**Channels**

| Channel | Parameters | Notes |
|---|---|---|
| `http` | `url`, `method`, `headers`, `timeoutMs` | Success only on a 2xx response; redirects are not followed |
| `mqtt` | `url` (default `BROKER_MQTT_URL`), `topic`, `qos`, `retain`, credentials | |
| `nats` | `subject`, `url` (default: the broker's NATS) | |
| `websocket` | none | Reaches subscribed stream clients; best-effort |

- `topic` and `subject` accept `{tenant}`, `{subscription}`, `{entityType}` and `{entityId}`.
- `perEntity: true` sends one message per entity.
- `format: "values"` sends `{ id, type, attr: value }` instead of the full entity.

**Notification**

`{ id, subscriptionId, subscriptionName, tenant, reason: create|update|delete|periodic|test, timestamp, entities: [...] }`

**Delivery**
- The guarantee is **at-least-once**: retries with exponential backoff (`backoffMs × 2^n`, capped at 5 minutes, with jitter).
- After `maxRetries` the notification goes to the **DLQ**, which can be inspected, replayed or purged from the API and the UI.
- Ordering between notifications is not guaranteed.

**Other**
- Users with only the viewer role see secret headers masked (Authorization, token, key, password).

### Filter language

| Element | Syntax |
|---|---|
| Attribute value | `temperature` (for relationships: the target id) |
| Attribute fields | `temperature.unit`, `.quality`, `.ts`, `.source`, `.type`, `.target`, `.meta.key` |
| Previous state | `prev.temperature`, `prev.temperature.quality` (`null` on create) |
| Entity | `@id`, `@type`, `@version` |
| Operators | `== != > >= < <=`, `&& \|\| !` (also `and or not`), `+ - * / %`, `in [..]`, `contains`, `matches 'regex'` |
| Functions | `abs round(x, digits) floor ceil min max len lower upper has('attr') startsWith endsWith now() age(ts)` |

Examples:
- `abs(temperature - prev.temperature) > 5`
- `@type in ['Press','Lathe'] && temperature.quality != 'good'`
- `age(heartbeat.ts) > 300`

Evaluation never throws: mismatched types make a comparison false, and arithmetic on non-numbers yields `null`.

## Realtime stream

```
ws://host/api/v1/stream?access_token=<jwt>&tenant=<id>      (or api_key=<key>)
→ { "type": "subscribe", "id": "live", "entities": { "types": ["Press"], "idPattern": "plant1:*" } }
→ { "type": "subscribe", "id": "n1", "subscription": "<uuid of a websocket subscription>" }
← { "type": "welcome" } · { "type": "entity", "sub": "live", "op": "update", "entity": {...} } · { "type": "notification", "sub": "n1", ... }
```

- Send subscriptions after `welcome`.
- When the token expires the server closes the connection with code `4001`; the UI refreshes the token and reconnects.
- Clients that fall behind by more than 5 MB of buffer lose messages; they never block the server.

## Connectors, commands and history

**From device to entity**
- Every device mapped by a source becomes an entity of the source's `tenant` (default `default`).
- The id is `entityIdPrefix` + device id, with invalid characters replaced by `_`.
- Measurements become attributes, with `ts` and `quality` taken from the metadata, `source` set to the connector id and `unit` taken from `mapping.json`.
- Writes go through ingest: coalescing per entity every 200 ms (`BROKER_INGEST_FLUSH_MS`), a durable queue and merged batches. Entities that do not change cause no write and no event.

**Commands**
- The attribute needs `writable: true` and a `command { connector, method, args }`. `method` must be a connector write method (`writeNode`, `writeRegister`, `writeVariable`, `writeTag`…), and `"$value"` in the args is replaced by the value sent.
- In a cluster, the instance running that connector answers.
- Lifecycle: `pending`, `sent`, then `succeeded` or `failed`. The timeout is `BROKER_COMMAND_TIMEOUT_MS`.
- The attribute value updates on the next device read.

**History**
- One row is stored for every new attribute `ts`, in the same write as the entity.
- `BROKER_HISTORY_RETENTION` (for example `90 days`) adds a TimescaleDB retention policy; `BROKER_HISTORY=off` disables history.
- History is kept after an entity is deleted (audit) and removed together with its tenant.

## Geo queries

These need PostGIS, which the `timescale/timescaledb-ha` image includes. Without PostGIS, geo parameters answer `501`.

| Parameter | Example |
|---|---|
| `bbox=minLon,minLat,maxLon,maxLat` | `bbox=9.1,45.4,9.3,45.5` |
| `near=lon,lat&maxDistance=m` | Orders by distance and adds `distanceM` |
| `georel=within\|intersects\|disjoint\|contains&geometry=<GeoJSON>` | The GeoJSON must be URL-encoded |
| `located=true` | Only entities with a location |

Subscriptions accept `select.bbox`.

## Security and tenants

| Role | Permissions in the tenant |
|---|---|
| viewer | Read entities, history, subscriptions (secrets masked), commands |
| operator | viewer + write entities, ingest, commands, subscriptions |
| admin | operator + tenant members and API keys |
| superadmin | All tenants, users, cluster, connector configuration changes (`/system`) |

**Passwords**
- Stored with scrypt.
- After 5 failed attempts within 15 minutes, login is blocked for that user/IP pair.

**Tokens**
- 15-minute access tokens and 7-day refresh tokens.
- Changing the password or disabling the user revokes every issued token.

**API keys**
- Stored as SHA-256 hashes and shown only once.
- Each key has a role, an expiry and an optional `allowedTypes` list, which restricts both reads and writes.

## Cluster

With `BROKER_CLUSTER=true` (and NATS), several instances share the work through **leases** in PostgreSQL. A lease lasts 3 ticks and is renewed every tick (`BROKER_CLUSTER_TICK_MS`, default 5 s).

| Resource | How it is distributed |
|---|---|
| `sub:0..31` | Subscription partitions (hash of the id). The owner evaluates changes and handles throttling and periodic sends. |
| `ingest:0..15` | Ingest partitions (hash of the entity id). A single instance consumes each one, in order. |
| `connector:<id>` | Each connector runs on exactly one instance, which also executes its commands. |
| Notification queue | JetStream consumer shared by all instances; no lease |
| WebSocket stream | On every instance |

**Balancing**
- Each instance holds a fair share of the resources: `ceil(resources / live instances)`.

**Rebalancing**
- When an instance joins or leaves cleanly, subscription partitions are handed over in "draining" mode: the instance giving them up keeps evaluating them until another one has taken them. The result is at most a few duplicates, never a gap, as the cluster test verifies.
- Connectors are handed over directly, because they must never run on two instances at once.

**Instance crash**
- Its resources move to the other instances within about 15 s.

**Connector configuration**
- `sources.json` and `mapping.json` are shared through the database (`system_config`). Every change made through `/system`, and every auto-discovery save, propagates to the other instances. `BROKER_CONFIG_SYNC=false` turns this off.

**Monitoring**
- Administration → Cluster shows instances, partitions, connectors, event-loop delay (above 200 ms an instance is saturated), ingest apply time and queue backlogs.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `BROKER_DB_URL` or `BROKER_DB_HOST`/`PORT`/`NAME`/`USER`/`PASSWORD` | `localhost:5432/udc_broker` | PostgreSQL (TimescaleDB and PostGIS recommended) |
| `BROKER_DB_POOL_SIZE` | 20 | Connections per instance |
| `BROKER_JWT_SECRET` | random | **Required in production and in clusters**: must be the same on every instance |
| `BROKER_ADMIN_USER` / `BROKER_ADMIN_PASSWORD` | `admin` / generated | Superadmin created on first start |
| `BROKER_ACCESS_TOKEN_TTL` / `BROKER_REFRESH_TOKEN_TTL` | `15m` / `7d` | |
| `NATS_URL` | — | Enables JetStream (durable queues) and clustering |
| `BROKER_CLUSTER` | `false` | Splits the work between instances |
| `BROKER_CLUSTER_TICK_MS` | 5000 | Lease renewal interval |
| `BROKER_CONFIG_SYNC` | `true` (in a cluster) | Shares `sources.json` and `mapping.json` |
| `BROKER_INGEST_FLUSH_MS` | 200 | Coalescing window for connector updates |
| `BROKER_INGEST_PARTITIONS` | 16 | Ingest partitions (same value on every instance) |
| `BROKER_HISTORY` / `BROKER_HISTORY_RETENTION` | `on` / — | Attribute history and its retention |
| `BROKER_MQTT_URL` | — | Default MQTT broker for notifications |
| `BROKER_COMMAND_TIMEOUT_MS` | 10000 | Command timeout |
| `UDC_RAW_STORAGE` | — | `memory` ignores `storage.json`, the raw connector data buffer |
| `PORT`, `BODY_LIMIT`, `LOG_LEVEL` | 3000, `5mb`, `info` | |

## Performance

The load test is `tests/load/load-test.js` (see the file header). It:
- seeds N entities over 100 types;
- keeps a steady update rate through `/ingest`, with M selective subscriptions (about 10% of each type's updates produce an HTTP notification);
- measures backlog, end-to-end write latency and notification latency.

Results measured on 2026-11-04 on a Windows laptop with Docker Desktop. Its 8 vCPUs were shared by PostgreSQL, NATS, the broker replicas, nginx and the load generator itself. The target was 100k entities, 10k updates/s and 100 subscriptions.

| Setup | Sent | Sustained apply | Notes |
|---|---|---|---|
| Seeding 100k entities | — | 12–25k entities/s | |
| 3 replicas, before optimization | 9.6k/s | 5.4k/s | Growing backlog (row-lock waits, one NATS message per entity) |
| 3 replicas, final | 8.4k/s | Backlog stable during the load | Write p95 3.1 s; instances saturated (event loop p99 up to 500 ms) |
| 5 replicas, final | 8.4k/s | **8.2k/s, backlog drained in 1 s** | The generator became the limit |
| 5 replicas, 2 generators | 9.1k/s | 7.9k/s | Backlog drained in 9 s; the whole machine saturated |
| PostgreSQL alone, 16 parallel workers | — | 22k/s | The database has headroom |

- Notification latency ranges from about 0.12 s (p50, unsaturated) to 1–2 s (p50, saturated).
- **10k/s sustained was not demonstrated on this machine.** The limit was the CPU of the shared host, not the database and not the architecture: with 5 replicas the broker applied everything the generator could send.
- On dedicated nodes, throughput is expected to grow with the number of replicas up to the database limit (about 22k/s measured), but this still has to be verified on the target hardware.

## Tests

```
docker compose up -d db nats mqtt
NATS_URL=nats://localhost:4222 TEST_MQTT_URL=mqtt://localhost:1883 npm run test:broker
```

The suites cover the model, the filter language, the API, subscriptions, data (ingest, history, commands, conflicts), geo and the cluster (two instances in one process). Without `NATS_URL` they use the in-memory queues.

Stop any other broker instance that uses the same database and NATS before running the tests, for example the compose backend: it would join the work split and skew the results.

## Known limits and choices

**Delivery guarantees**
- Change fan-out uses core NATS, so it is at-most-once. The at-least-once guarantee applies from the notification queue onward.
- If an instance crashes between commit and publish, or during the ~15 s failover of subscription partitions after a crash, notifications for those changes can be lost. A graceful rebalance leaves no gap.
- The `websocket` channel only reaches connected clients.

**Ordering**
- Notification order is not guaranteed.
- An ingest batch that fails at the database and is retried can be overtaken by newer batches of the same partition.

**Security**
- Webhooks may target internal addresses, which industrial networks often need. Restrict them with network (egress) policies where required.

**Changes from the initial design**
- Commands have an explicit endpoint (`/command`) instead of firing on a PATCH of a writable attribute, so the stored value stays the one read from the device.
- JetStream is not used as a change log: core NATS is enough for fan-out, and data stays durable in PostgreSQL and in the two queues.
- Numbers are inferred as `float`.

**Existing code fixed along the way**
- The `sources.json` schema accepted only 6 connector types.
- Reloading sources called a method that did not exist.
- The MQTT mapper ignored the payload emitted by the MQTT connector.
- A Redis error with no listener crashed the process.
- Saving a connector from the UI did not start it.
