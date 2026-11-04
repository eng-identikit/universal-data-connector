const EventEmitter = require('events');
const { withTransaction } = require('../db/pool');
const { BrokerError, badRequest, forbidden, notFound, conflict } = require('../errors');
const model = require('../model/entityModel');

const MAX_LIMIT = 1000;
const MAX_DEPTH = 10;
const MAX_BATCH = 1000;
const ORDER_COLUMNS = { id: 'id', type: 'type', createdAt: 'created_at', updatedAt: 'updated_at' };
// Explicit list: the PostGIS geom column is internal (derived from location)
const COLUMNS = 'tenant, id, type, attrs, location, version, created_at, updated_at';
const MAX_DISTANCE_M = 20000000;
const GEOREL = { within: 'ST_Within', intersects: 'ST_Intersects', disjoint: 'ST_Disjoint', contains: 'ST_Contains' };

/**
 * Entity store on PostgreSQL.
 *
 * Every method takes a context { tenant, allowedTypes } resolved by the auth
 * middleware; allowedTypes (from API keys) limits both reads and writes.
 *
 * Emits 'entityChanged' { tenant, op: create|update|delete, entity, previous }
 * after each committed write.
 */
class EntityService extends EventEmitter {
  constructor(pool, { history = null } = {}) {
    super();
    this.pool = pool;
    this.history = history;
    this.geoEnabled = false;
  }

  /** Geo queries need the PostGIS geom column (migration 004 skips it without PostGIS). */
  async detectGeo() {
    const { rows } = await this.pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_name = 'entities' AND column_name = 'geom'"
    );
    this.geoEnabled = rows.length > 0;
    return this.geoEnabled;
  }

  /**
   * Geo filters for list():
   *   bbox=minLon,minLat,maxLon,maxLat
   *   near=lon,lat&maxDistance=<m>        (results gain distanceM, ordered by distance)
   *   georel=within|intersects|disjoint|contains&geometry=<GeoJSON>
   *   located=true                        only entities with a location
   */
  _geoFilter(query, add) {
    const where = [];
    let distance = null;
    if (query.located === 'true' || query.located === true) where.push('location IS NOT NULL');
    if (!query.bbox && !query.near && !query.georel) return { where, distance };
    if (!this.geoEnabled) throw new BrokerError(501, 'NotImplemented', 'Geo queries need PostGIS on the broker database');

    if (query.bbox) {
      const b = String(query.bbox).split(',').map(Number);
      if (b.length !== 4 || b.some(n => !Number.isFinite(n)) || b[0] > b[2] || b[1] > b[3]) {
        throw badRequest('bbox must be minLon,minLat,maxLon,maxLat');
      }
      where.push(`ST_Intersects(geom, ST_MakeEnvelope(${add(b[0])}, ${add(b[1])}, ${add(b[2])}, ${add(b[3])}, 4326))`);
    }
    if (query.near) {
      const p = String(query.near).split(',').map(Number);
      if (p.length !== 2 || p.some(n => !Number.isFinite(n)) || Math.abs(p[0]) > 180 || Math.abs(p[1]) > 90) {
        throw badRequest('near must be lon,lat');
      }
      const max = query.maxDistance === undefined ? 1000 : Number(query.maxDistance);
      if (!Number.isFinite(max) || max <= 0 || max > MAX_DISTANCE_M) throw badRequest(`maxDistance must be between 0 and ${MAX_DISTANCE_M} meters`);
      const point = `ST_SetSRID(ST_MakePoint(${add(p[0])}, ${add(p[1])}), 4326)::geography`;
      where.push(`ST_DWithin(geom::geography, ${point}, ${add(max)})`);
      distance = `ST_Distance(geom::geography, ${point})`;
    }
    if (query.georel) {
      const fn = GEOREL[query.georel];
      if (!fn) throw badRequest(`georel must be one of ${Object.keys(GEOREL).join(', ')}`);
      let geometry;
      try {
        geometry = typeof query.geometry === 'string' ? JSON.parse(query.geometry) : query.geometry;
      } catch {
        throw badRequest('geometry must be a GeoJSON geometry (JSON)');
      }
      geometry = model.normalizeGeometry(geometry, 'geometry');
      where.push(`${fn}(geom, ST_SetSRID(ST_GeomFromGeoJSON(${add(JSON.stringify(geometry))}), 4326))`);
    }
    return { where, distance };
  }

  // ─── Reads ────────────────────────────────────────────────────────────────

  async list(ctx, query = {}) {
    const params = [ctx.tenant];
    const where = ['tenant = $1'];
    const add = (value) => { params.push(value); return `$${params.length}`; };

    const types = splitList(query.type);
    if (types.length) {
      types.forEach(t => model.assertEntityType(t));
      where.push(`type = ANY(${add(types)})`);
    }
    if (ctx.allowedTypes) where.push(`type = ANY(${add(ctx.allowedTypes)})`);

    if (query.idPattern) where.push(`id LIKE ${add(globToLike(query.idPattern))} ESCAPE '\\'`);

    let cte = '';
    if (query.related) {
      model.assertEntityId(query.related, 'related');
      const depth = parseIntParam(query.depth, 1, 1, MAX_DEPTH, 'depth');
      const direction = query.direction || 'in';
      if (!['in', 'out'].includes(direction)) throw badRequest("direction must be 'in' or 'out'");
      // 'in': entities whose relationships lead (transitively) to `related`.
      // 'out': entities that `related` points to.
      const [from, to] = direction === 'in' ? ['source_id', 'target_id'] : ['target_id', 'source_id'];
      const rel = add(query.related);
      const max = add(depth);
      cte = `WITH RECURSIVE reach(id, depth) AS (
          SELECT ${from}, 1 FROM entity_relationships WHERE tenant = $1 AND ${to} = ${rel}
          UNION
          SELECT er.${from}, reach.depth + 1 FROM entity_relationships er
            JOIN reach ON er.${to} = reach.id
           WHERE er.tenant = $1 AND reach.depth < ${max}
        ) `;
      where.push('id IN (SELECT id FROM reach)');
    }

    const geo = this._geoFilter(query, add);
    where.push(...geo.where);

    const limit = parseIntParam(query.limit, 100, 1, MAX_LIMIT, 'limit');
    const offset = parseIntParam(query.offset, 0, 0, Number.MAX_SAFE_INTEGER, 'offset');
    let orderBy = parseOrderBy(query.orderBy);
    if (geo.distance && !query.orderBy) orderBy = 'distance_m ASC, id ASC';
    const select = geo.distance ? `${COLUMNS}, ${geo.distance} AS distance_m` : COLUMNS;
    const whereSql = where.join(' AND ');

    const [rows, count] = await Promise.all([
      this.pool.query(
        `${cte}SELECT ${select} FROM entities WHERE ${whereSql} ORDER BY ${orderBy} LIMIT ${add(limit)} OFFSET ${add(offset)}`,
        params
      ),
      this.pool.query(`${cte}SELECT count(*)::int AS total FROM entities WHERE ${whereSql}`, params.slice(0, params.length - 2))
    ]);

    const attrs = splitList(query.attrs);
    return {
      entities: rows.rows.map(r => project(toEntity(r), attrs)),
      total: count.rows[0].total,
      limit,
      offset
    };
  }

  async get(ctx, id, { attrs } = {}) {
    const { rows } = await this.pool.query(`SELECT ${COLUMNS} FROM entities WHERE tenant = $1 AND id = $2`, [ctx.tenant, id]);
    if (!rows.length || !typeAllowed(ctx, rows[0].type)) throw notFound(`Entity '${id}' not found`);
    return project(toEntity(rows[0]), splitList(attrs));
  }

  /** Ids reachable from `id` through relationships (same semantics as list ?related). */
  async reachableIds(tenant, id, depth = 1, direction = 'in') {
    const [from, to] = direction === 'in' ? ['source_id', 'target_id'] : ['target_id', 'source_id'];
    const { rows } = await this.pool.query(
      `WITH RECURSIVE reach(id, depth) AS (
          SELECT ${from}, 1 FROM entity_relationships WHERE tenant = $1 AND ${to} = $2
          UNION
          SELECT er.${from}, reach.depth + 1 FROM entity_relationships er
            JOIN reach ON er.${to} = reach.id
           WHERE er.tenant = $1 AND reach.depth < $3
        ) SELECT DISTINCT id FROM reach`,
      [tenant, id, Math.min(Math.max(depth, 1), MAX_DEPTH)]
    );
    return rows.map(r => r.id);
  }

  async types(ctx) {
    const params = [ctx.tenant];
    let filter = '';
    if (ctx.allowedTypes) { params.push(ctx.allowedTypes); filter = 'AND type = ANY($2)'; }
    const { rows } = await this.pool.query(
      `SELECT type, count(*)::int AS count FROM entities WHERE tenant = $1 ${filter} GROUP BY type ORDER BY type`,
      params
    );
    return rows;
  }

  /**
   * Relationship graph around an entity, for navigation in the UI.
   * Returns { root, nodes: [{id, type|null}], edges: [{source, attr, target}] };
   * type is null for targets that do not exist (dangling relationships).
   */
  async graph(ctx, id, { direction = 'both', depth } = {}) {
    if (!['in', 'out', 'both'].includes(direction)) throw badRequest("direction must be 'in', 'out' or 'both'");
    const maxDepth = parseIntParam(depth, 2, 1, 5, 'depth');
    await this.get(ctx, id);

    const edges = new Map();
    const visited = new Set([id]);
    let frontier = [id];
    for (let level = 0; level < maxDepth && frontier.length; level++) {
      const conditions = [];
      if (direction !== 'in') conditions.push('source_id = ANY($2)');
      if (direction !== 'out') conditions.push('target_id = ANY($2)');
      const { rows } = await this.pool.query(
        `SELECT source_id, attr, target_id FROM entity_relationships WHERE tenant = $1 AND (${conditions.join(' OR ')})`,
        [ctx.tenant, frontier]
      );
      const next = [];
      for (const r of rows) {
        edges.set(`${r.source_id}|${r.attr}`, { source: r.source_id, attr: r.attr, target: r.target_id });
        for (const n of [r.source_id, r.target_id]) {
          if (!visited.has(n)) { visited.add(n); next.push(n); }
        }
      }
      frontier = next;
    }

    const { rows } = await this.pool.query(
      'SELECT id, type FROM entities WHERE tenant = $1 AND id = ANY($2)',
      [ctx.tenant, [...visited]]
    );
    const typeOf = new Map(rows.map(r => [r.id, r.type]));
    // Entities hidden by allowedTypes are dropped together with their edges
    const visible = (n) => !typeOf.has(n) || typeAllowed(ctx, typeOf.get(n));
    return {
      root: id,
      nodes: [...visited].filter(visible).map(n => ({ id: n, type: typeOf.get(n) || null })),
      edges: [...edges.values()].filter(e => visible(e.source) && visible(e.target))
    };
  }

  // ─── Writes ───────────────────────────────────────────────────────────────

  async create(ctx, input) {
    const entity = model.normalizeEntity(input, nowIso());
    checkWritable(ctx, entity.type);
    const created = await withTransaction(this.pool, client => this._insert(client, ctx.tenant, entity));
    this._emit(ctx.tenant, 'create', created, null);
    return created;
  }

  async replace(ctx, id, input, expectedVersion) {
    return this._update(ctx, id, expectedVersion, (existing, now) => {
      const entity = model.normalizeEntity({ ...input, id }, now);
      if (entity.type !== existing.type) throw badRequest("'type' cannot be changed");
      return entity;
    });
  }

  async patch(ctx, id, input, expectedVersion) {
    return this._update(ctx, id, expectedVersion, (existing, now) => model.applyPatch(existing, input, now));
  }

  async setAttr(ctx, id, name, input, { merge }, expectedVersion) {
    if (input === null || input === undefined) throw badRequest('Attribute body is required');
    return this._update(ctx, id, expectedVersion, (existing, now) => {
      const attrs = { ...existing.attrs };
      attrs[name] = model.normalizeAttr(name, input, merge ? existing.attrs[name] : null, now);
      return { ...existing, attrs };
    });
  }

  async deleteAttr(ctx, id, name, expectedVersion) {
    return this._update(ctx, id, expectedVersion, (existing) => {
      if (!(name in existing.attrs)) throw notFound(`Attribute '${name}' not found on entity '${id}'`);
      const attrs = { ...existing.attrs };
      delete attrs[name];
      return { ...existing, attrs };
    });
  }

  async remove(ctx, id, expectedVersion) {
    const previous = await withTransaction(this.pool, async client => {
      const existing = await this._lock(client, ctx, id);
      checkVersion(existing, expectedVersion);
      await client.query('DELETE FROM entities WHERE tenant = $1 AND id = $2', [ctx.tenant, id]);
      return existing;
    });
    this._emit(ctx.tenant, 'delete', null, previous);
  }

  /**
   * Atomic batch: either every operation is applied or none.
   * mode: create | update (merge, entity must exist) | upsert (merge or create) | delete
   */
  async batch(ctx, mode, items) {
    if (!['create', 'update', 'upsert', 'delete'].includes(mode)) {
      throw badRequest("mode must be one of create, update, upsert, delete");
    }
    if (!Array.isArray(items) || !items.length) throw badRequest("'entities' must be a non-empty array");
    if (items.length > MAX_BATCH) throw badRequest(`Batch too large (max ${MAX_BATCH})`);

    const ids = new Set();
    for (const item of items) {
      if (!item || typeof item !== 'object') throw badRequest('Each batch item must be an object');
      model.assertEntityId(item.id);
      if (ids.has(item.id)) throw badRequest(`Duplicate id '${item.id}' in batch`);
      ids.add(item.id);
    }
    // Consistent lock order prevents deadlocks between concurrent batches
    const sorted = [...items].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    const now = nowIso();

    const events = await withTransaction(this.pool, async client => {
      const out = [];
      for (const item of sorted) {
        if (mode === 'delete') {
          const existing = await this._lock(client, ctx, item.id);
          await client.query('DELETE FROM entities WHERE tenant = $1 AND id = $2', [ctx.tenant, item.id]);
          out.push(['delete', null, existing]);
          continue;
        }
        const existing = mode === 'create' ? null : await this._lock(client, ctx, item.id, { required: mode === 'update' });
        if (existing) {
          const next = model.applyPatch(existing, item, now);
          out.push(['update', await this._write(client, ctx.tenant, existing, next), existing]);
        } else {
          const entity = model.normalizeEntity(item, now);
          checkWritable(ctx, entity.type);
          out.push(['create', await this._insert(client, ctx.tenant, entity), null]);
        }
      }
      return out;
    });

    events.forEach(([op, entity, previous]) => this._emit(ctx.tenant, op, entity, previous));
    return {
      mode,
      count: events.length,
      results: events.map(([op, entity, previous]) => ({ id: (entity || previous).id, op, version: entity?.version ?? null }))
    };
  }

  // ─── Internals ────────────────────────────────────────────────────────────

  async _update(ctx, id, expectedVersion, mutate) {
    const [entity, previous] = await withTransaction(this.pool, async client => {
      const existing = await this._lock(client, ctx, id);
      checkVersion(existing, expectedVersion);
      const next = mutate(existing, nowIso());
      return [await this._write(client, ctx.tenant, existing, next), existing];
    });
    this._emit(ctx.tenant, 'update', entity, previous);
    return entity;
  }

  async _lock(client, ctx, id, { required = true } = {}) {
    const { rows } = await client.query(`SELECT ${COLUMNS} FROM entities WHERE tenant = $1 AND id = $2 FOR UPDATE`, [ctx.tenant, id]);
    if (!rows.length || !typeAllowed(ctx, rows[0].type)) {
      if (required) throw notFound(`Entity '${id}' not found`);
      return null;
    }
    checkWritable(ctx, rows[0].type);
    return toEntity(rows[0]);
  }

  async _insert(client, tenant, entity) {
    const { rows } = await client.query(
      `INSERT INTO entities (tenant, id, type, attrs, location) VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (tenant, id) DO NOTHING RETURNING ${COLUMNS}`,
      [tenant, entity.id, entity.type, entity.attrs, entity.location]
    );
    if (!rows.length) throw conflict(`Entity '${entity.id}' already exists`);
    await this._syncRelationships(client, tenant, entity.id, [], model.extractRelationships(entity.attrs));
    const created = toEntity(rows[0]);
    if (this.history) await this.history.insert(client, tenant, this.history.rows(tenant, null, created));
    return created;
  }

  async _write(client, tenant, existing, next) {
    const { rows } = await client.query(
      `UPDATE entities SET attrs = $3, location = $4, version = version + 1, updated_at = now()
        WHERE tenant = $1 AND id = $2 RETURNING ${COLUMNS}`,
      [tenant, existing.id, next.attrs, next.location ?? null]
    );
    await this._syncRelationships(
      client, tenant, existing.id,
      model.extractRelationships(existing.attrs), model.extractRelationships(next.attrs)
    );
    const updated = toEntity(rows[0]);
    if (this.history) await this.history.insert(client, tenant, this.history.rows(tenant, existing, updated));
    return updated;
  }

  /**
   * High-throughput upsert used by connectors and /ingest: one transaction,
   * two statements for the whole batch. Attributes are merged like PATCH;
   * entities whose state would not change are skipped (no version bump, no event).
   * Invalid items are reported in `rejected` instead of failing the batch.
   *
   * updates: [{ id, type, attrs, location? }]  (type is used only on create)
   */
  async ingestBatch(tenant, updates, { allowedTypes = null } = {}) {
    // Last update wins when the same entity appears twice in a batch
    const byId = new Map();
    for (const u of updates) {
      const prev = byId.get(u.id);
      byId.set(u.id, prev ? { ...prev, ...u, attrs: { ...prev.attrs, ...u.attrs } } : u);
    }
    const ids = [...byId.keys()].sort();
    const now = nowIso();
    const rejected = [];

    const events = await withTransaction(this.pool, async client => {
      const { rows } = await client.query(
        `SELECT ${COLUMNS} FROM entities WHERE tenant = $1 AND id = ANY($2) ORDER BY id FOR UPDATE`, [tenant, ids]
      );
      const existing = new Map(rows.map(r => [r.id, toEntity(r)]));

      const writes = [];
      for (const id of ids) {
        const u = byId.get(id);
        const before = existing.get(id) || null;
        try {
          const type = before ? before.type : u.type;
          if (allowedTypes && !allowedTypes.includes(type)) throw forbidden(`type '${type}' not allowed`);
          const next = before
            ? model.applyPatch(before, { attrs: u.attrs, ...(u.location !== undefined && { location: u.location }) }, now)
            : model.normalizeEntity({ id, type: u.type, attrs: u.attrs, location: u.location }, now);
          if (before && JSON.stringify(next.attrs) === JSON.stringify(before.attrs)
            && JSON.stringify(next.location ?? null) === JSON.stringify(before.location ?? null)) continue;
          writes.push({ before, next: { ...next, type } });
        } catch (error) {
          rejected.push({ id, error: error.message });
        }
      }
      if (!writes.length) return [];

      const { rows: saved } = await client.query(
        `INSERT INTO entities AS e (tenant, id, type, attrs, location)
         SELECT $1, i, t, a, l FROM unnest($2::text[], $3::text[], $4::jsonb[], $5::jsonb[]) AS x(i, t, a, l)
         ON CONFLICT (tenant, id) DO UPDATE
           SET attrs = EXCLUDED.attrs, location = EXCLUDED.location, version = e.version + 1, updated_at = now()
         RETURNING ${COLUMNS}`,
        [tenant, writes.map(w => w.next.id), writes.map(w => w.next.type),
          writes.map(w => JSON.stringify(w.next.attrs)), writes.map(w => (w.next.location ? JSON.stringify(w.next.location) : null))]
      );
      const savedById = new Map(saved.map(r => [r.id, toEntity(r)]));

      const historyRows = [];
      const out = [];
      for (const w of writes) {
        const entity = savedById.get(w.next.id);
        const relsBefore = w.before ? model.extractRelationships(w.before.attrs) : [];
        await this._syncRelationships(client, tenant, entity.id, relsBefore, model.extractRelationships(entity.attrs));
        if (this.history) historyRows.push(...this.history.rows(tenant, w.before, entity));
        out.push([w.before ? 'update' : 'create', entity, w.before]);
      }
      if (this.history) await this.history.insert(client, tenant, historyRows);
      return out;
    });

    events.forEach(([op, entity, previous]) => this._emit(tenant, op, entity, previous));
    return { written: events.length, skipped: ids.length - events.length - rejected.length, rejected };
  }

  async _syncRelationships(client, tenant, id, before, after) {
    const key = (rels) => JSON.stringify([...rels].sort((a, b) => (a.attr < b.attr ? -1 : 1)));
    if (key(before) === key(after)) return;
    await client.query('DELETE FROM entity_relationships WHERE tenant = $1 AND source_id = $2', [tenant, id]);
    if (!after.length) return;
    await client.query(
      `INSERT INTO entity_relationships (tenant, source_id, attr, target_id)
       SELECT $1, $2, a, t FROM unnest($3::text[], $4::text[]) AS x(a, t)`,
      [tenant, id, after.map(r => r.attr), after.map(r => r.target)]
    );
  }

  _emit(tenant, op, entity, previous) {
    this.emit('entityChanged', { tenant, op, entity, previous });
  }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

const nowIso = () => new Date().toISOString();

function toEntity(row) {
  const entity = {
    id: row.id,
    type: row.type,
    version: Number(row.version),
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    attrs: row.attrs
  };
  if (row.location) entity.location = row.location;
  if (row.distance_m !== undefined && row.distance_m !== null) entity.distanceM = Math.round(row.distance_m * 10) / 10;
  return entity;
}

function project(entity, attrs) {
  if (!attrs.length) return entity;
  const picked = {};
  for (const name of attrs) if (name in entity.attrs) picked[name] = entity.attrs[name];
  return { ...entity, attrs: picked };
}

function typeAllowed(ctx, type) {
  return !ctx.allowedTypes || ctx.allowedTypes.includes(type);
}

function checkWritable(ctx, type) {
  if (!typeAllowed(ctx, type)) throw forbidden(`This credential cannot write entities of type '${type}'`);
}

function checkVersion(entity, expectedVersion) {
  if (expectedVersion !== undefined && entity.version !== expectedVersion) {
    throw conflict(`Version mismatch on '${entity.id}'`, { expected: expectedVersion, actual: entity.version });
  }
}

function splitList(value) {
  if (!value) return [];
  return String(value).split(',').map(s => s.trim()).filter(Boolean);
}

function parseIntParam(value, fallback, min, max, name) {
  if (value === undefined || value === '') return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) throw badRequest(`${name} must be an integer between ${min} and ${max}`);
  return n;
}

function parseOrderBy(value) {
  if (!value) return 'id ASC';
  const columns = splitList(value).map(field => {
    const desc = field.startsWith('-');
    const column = ORDER_COLUMNS[desc ? field.slice(1) : field];
    if (!column) throw badRequest(`orderBy: unknown field '${field}'`, { allowed: Object.keys(ORDER_COLUMNS) });
    return `${column} ${desc ? 'DESC' : 'ASC'}`;
  });
  // id as final tie-breaker keeps offset pagination stable
  if (!columns.some(c => c.startsWith('id '))) columns.push('id ASC');
  return columns.join(', ');
}

// Glob with '*' wildcard → SQL LIKE pattern with '\' escape
function globToLike(glob) {
  return String(glob).replace(/[\\%_]/g, c => `\\${c}`).replace(/\*/g, '%');
}

module.exports = EntityService;
module.exports.globToLike = globToLike;
