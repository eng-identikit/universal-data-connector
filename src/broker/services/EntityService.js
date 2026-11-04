const EventEmitter = require('events');
const { withTransaction } = require('../db/pool');
const { badRequest, forbidden, notFound, conflict } = require('../errors');
const model = require('../model/entityModel');

const MAX_LIMIT = 1000;
const MAX_DEPTH = 10;
const MAX_BATCH = 1000;
const ORDER_COLUMNS = { id: 'id', type: 'type', createdAt: 'created_at', updatedAt: 'updated_at' };

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
  constructor(pool) {
    super();
    this.pool = pool;
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

    const limit = parseIntParam(query.limit, 100, 1, MAX_LIMIT, 'limit');
    const offset = parseIntParam(query.offset, 0, 0, Number.MAX_SAFE_INTEGER, 'offset');
    const orderBy = parseOrderBy(query.orderBy);
    const whereSql = where.join(' AND ');

    const [rows, count] = await Promise.all([
      this.pool.query(
        `${cte}SELECT * FROM entities WHERE ${whereSql} ORDER BY ${orderBy} LIMIT ${add(limit)} OFFSET ${add(offset)}`,
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
    const { rows } = await this.pool.query('SELECT * FROM entities WHERE tenant = $1 AND id = $2', [ctx.tenant, id]);
    if (!rows.length || !typeAllowed(ctx, rows[0].type)) throw notFound(`Entity '${id}' not found`);
    return project(toEntity(rows[0]), splitList(attrs));
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
    const { rows } = await client.query('SELECT * FROM entities WHERE tenant = $1 AND id = $2 FOR UPDATE', [ctx.tenant, id]);
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
       ON CONFLICT (tenant, id) DO NOTHING RETURNING *`,
      [tenant, entity.id, entity.type, entity.attrs, entity.location]
    );
    if (!rows.length) throw conflict(`Entity '${entity.id}' already exists`);
    await this._syncRelationships(client, tenant, entity.id, [], model.extractRelationships(entity.attrs));
    return toEntity(rows[0]);
  }

  async _write(client, tenant, existing, next) {
    const { rows } = await client.query(
      `UPDATE entities SET attrs = $3, location = $4, version = version + 1, updated_at = now()
        WHERE tenant = $1 AND id = $2 RETURNING *`,
      [tenant, existing.id, next.attrs, next.location ?? null]
    );
    await this._syncRelationships(
      client, tenant, existing.id,
      model.extractRelationships(existing.attrs), model.extractRelationships(next.attrs)
    );
    return toEntity(rows[0]);
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
