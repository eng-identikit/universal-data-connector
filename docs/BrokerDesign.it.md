# UDC Broker — Documento di design

Obiettivo: trasformare l'UDC in un **context broker standalone** che sostituisce Orion. Non è previsto alcun livello di compatibilità NGSI.

## Decisioni

| Tema | Decisione |
|---|---|
| Device e entità | Ogni device mappato dai connettori diventa automaticamente un'entità. Anche i client esterni possono creare e aggiornare entità via API (producer). |
| Metadata | Ogni attributo ha campi standard (`value`, `type`, `unit`, `ts`, `quality`, `source`) più metadata liberi (`meta`). |
| Relazioni | Attributi di tipo `relationship`, navigabili con query ricorsive (`related`, `depth`, `direction`). |
| Subscription | Selezione delle entità, on-change con deadband, filtri sui valori (linguaggio di espressioni proprietario), throttling oppure invio periodico. |
| Canali di notifica | HTTP webhook, MQTT, WebSocket, NATS. |
| Consegna | At-least-once su NATS JetStream, con retry, backoff e dead-letter queue. |
| Sicurezza | Utenti con JWT e ruoli `admin`/`operator`/`viewer` per tenant, superadmin globale, API key per i client macchina con limitazione per type. |
| Tenancy | Multi-tenant: ogni entità, subscription e API key appartiene a un tenant. |
| Extra (v1) | Comandi verso i device, geo-query (PostGIS), storico per entità (TimescaleDB), clustering. |
| Scala target | ~100k entità, ~10k update/s, centinaia di subscription. |
| API esistente | Sostituita completamente dalla nuova `/api/v2`. |
| Deploy | Docker Compose, più Helm per K8s (fase 5). |

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
    "setpoint":    { "type": "float", "value": 80, "writable": true, "command": { "connector": "opcua-plc1", "path": "ns=2;s=SP" } }
  }
}
```

- `id`: `[A-Za-z0-9_\-:.]`, massimo 256 caratteri, univoco nel tenant.
- `type`: inizia con una lettera, poi `[A-Za-z0-9_]`, massimo 64 caratteri.
- Tipi di attributo: `float`, `int`, `bool`, `string`, `datetime`, `json`, `relationship`. Se manca, il tipo viene ricavato dal valore.
- `version` aumenta di 1 a ogni scrittura. `PATCH`, `PUT` e `DELETE` accettano `If-Match: <version>` per l'optimistic locking; se la versione non corrisponde rispondono `409`.
- `PATCH` fa il merge attributo per attributo: un attributo passato viene fuso con quello esistente, `null` lo rimuove. Se il valore cambia e `ts` non è indicato, `ts` viene impostato all'ora attuale.

## Storage

PostgreSQL (immagine `timescale/timescaledb-ha`, che include TimescaleDB e PostGIS).

- `tenants`, `users`, `user_tenants` (ruolo per tenant), `api_keys` (salvate come hash SHA-256, con `allowed_types` opzionale)
- `entities` (`tenant`, `id`) con `attrs` in JSONB e indice GIN
- `entity_relationships` (`tenant`, `source_id`, `attr`, `target_id`): proiezione degli attributi `relationship` per le query ricorsive. Un `target` che non esiste ancora è ammesso.
- `schema_migrations`: le migrazioni vengono applicate all'avvio sotto un advisory lock, quindi sono sicure anche con più istanze.

## Architettura (stato finale)

```
 Connettori ─┐                         ┌─► HTTP / MQTT / NATS / WS
 API write ──┼─► Entity Service ─► NATS JetStream ─► Subscription Engine ─► Delivery workers
             │          │             (log update)      (matching in memoria)    (retry, DLQ)
             ▼          ▼
        cache in    PostgreSQL ── entità, subscription, utenti, tenant
        memoria     TimescaleDB ── storico attributi
```

Nella fase 1 l'Entity Service scrive direttamente su PostgreSQL e notifica le modifiche con un evento interno (`entityChanged`). Cache in memoria e log JetStream arrivano con la fase 2.

## API v2

Autenticazione: `Authorization: Bearer <jwt>` oppure `X-API-Key: <key>`. Il tenant si sceglie con l'header `X-Tenant`. Con le API key il tenant è sempre quello della chiave.

| Ruolo | Permessi |
|---|---|
| viewer | lettura entità |
| operator | viewer + scrittura entità |
| admin | operator + membri del tenant e API key |
| superadmin | tutto, compresi tenant e utenti globali |

```
POST   /api/v2/auth/login | /auth/refresh       GET /auth/me     POST /auth/password
GET    /api/v2/users          POST /users        PATCH|DELETE /users/:id                  (superadmin)
GET    /api/v2/tenants        POST /tenants      PATCH|DELETE /tenants/:id                (superadmin per scrivere)
GET    /api/v2/tenants/:id/members               PUT|DELETE /tenants/:id/members/:userId  (admin)
GET    /api/v2/apikeys        POST /apikeys      DELETE /apikeys/:id                      (admin)
GET    /api/v2/types
GET    /api/v2/entities       ?type&idPattern&related&depth&direction&attrs&orderBy&limit&offset
POST   /api/v2/entities       POST /entities/batch {mode: upsert|create|update|delete}
GET|PUT|PATCH|DELETE /api/v2/entities/:id
PUT|PATCH|DELETE     /api/v2/entities/:id/attrs/:attr
GET    /api/v2/entities/:id/graph   ?direction=in|out|both&depth
```

## Fasi

1. **Fondamenta:** schema, Entity Service (CRUD, versioning, relazioni), autenticazione e tenant, API entità, Entity Explorer e Admin nella UI, Docker Compose.
2. **Subscription:** linguaggio di espressioni, matching, JetStream con at-least-once e DLQ, i quattro canali di notifica, cache in memoria, WebSocket autenticato, UI.
3. **Connettori:** device → entità, comandi verso i device, storico per entità, rimozione della vecchia API `/api`.
4. **Geo:** PostGIS, filtri geo, mappa nella UI.
5. **Cluster:** lease per connettori e subscription, Helm, test di carico.
