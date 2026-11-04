const { badRequest } = require('../errors');

/**
 * Entity model: validation, normalization and merge rules.
 * Pure functions, no I/O: EntityService persists what these return.
 */

const ENTITY_ID_RE = /^[A-Za-z0-9_\-:.]{1,256}$/;
const ENTITY_TYPE_RE = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const ATTR_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const ATTR_TYPES = ['float', 'int', 'bool', 'string', 'datetime', 'json', 'relationship'];
const QUALITIES = ['good', 'bad', 'uncertain'];
const ATTR_KEYS = new Set(['type', 'value', 'target', 'unit', 'ts', 'quality', 'source', 'meta', 'writable', 'command']);
const GEO_TYPES = ['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'];
const MAX_ATTRS = 500;

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const isIsoDate = (v) => typeof v === 'string' && !Number.isNaN(Date.parse(v));

function assertEntityId(id, field = 'id') {
  if (typeof id !== 'string' || !ENTITY_ID_RE.test(id)) {
    throw badRequest(`Invalid ${field}: expected 1-256 chars of [A-Za-z0-9_-:.]`, { field, value: id });
  }
}

function assertEntityType(type) {
  if (typeof type !== 'string' || !ENTITY_TYPE_RE.test(type)) {
    throw badRequest('Invalid type: must start with a letter, then [A-Za-z0-9_], max 64 chars', { field: 'type', value: type });
  }
}

function assertAttrName(name) {
  if (!ATTR_NAME_RE.test(name)) {
    throw badRequest(`Invalid attribute name '${name}': must match ${ATTR_NAME_RE}`, { field: `attrs.${name}` });
  }
}

// Numbers are inferred as float: a sensor sending 25 then 25.5 must not be rejected.
// 'int' is only used when declared explicitly.
function inferType(value) {
  if (typeof value === 'boolean') return 'bool';
  if (typeof value === 'number') return 'float';
  if (typeof value === 'string') return 'string';
  return 'json';
}

function checkValue(name, type, value) {
  if (value === undefined || value === null) return;
  const ok = {
    float: () => typeof value === 'number' && Number.isFinite(value),
    int: () => Number.isInteger(value),
    bool: () => typeof value === 'boolean',
    string: () => typeof value === 'string',
    datetime: () => isIsoDate(value),
    json: () => true
  }[type]();
  if (!ok) throw badRequest(`Attribute '${name}': value does not match type '${type}'`, { field: `attrs.${name}.value`, value });
}

/**
 * Validates one attribute and merges it over `existing` (pass null for a fresh attribute).
 * A primitive input is shorthand for { value }.
 */
function normalizeAttr(name, input, existing, now) {
  assertAttrName(name);
  const patch = isPlainObject(input) ? input : { value: input };

  for (const key of Object.keys(patch)) {
    if (!ATTR_KEYS.has(key)) throw badRequest(`Attribute '${name}': unknown field '${key}'`, { field: `attrs.${name}.${key}` });
  }

  const attr = { ...(existing || {}), ...patch };
  if (!patch.type && !existing?.type) {
    attr.type = patch.target !== undefined ? 'relationship' : inferType(attr.value);
  }
  if (!ATTR_TYPES.includes(attr.type)) {
    throw badRequest(`Attribute '${name}': unknown type '${attr.type}'`, { field: `attrs.${name}.type`, allowed: ATTR_TYPES });
  }

  if (attr.type === 'relationship') {
    assertEntityId(attr.target, `attrs.${name}.target`);
    delete attr.value;
    delete attr.unit;
  } else {
    if (patch.target !== undefined) throw badRequest(`Attribute '${name}': 'target' is only valid for relationships`);
    delete attr.target;
    checkValue(name, attr.type, attr.value);
  }

  if (attr.quality !== undefined && !QUALITIES.includes(attr.quality)) {
    throw badRequest(`Attribute '${name}': quality must be one of ${QUALITIES.join(', ')}`);
  }
  for (const key of ['unit', 'source']) {
    if (attr[key] !== undefined && typeof attr[key] !== 'string') throw badRequest(`Attribute '${name}': '${key}' must be a string`);
  }
  if (attr.meta !== undefined && !isPlainObject(attr.meta)) throw badRequest(`Attribute '${name}': 'meta' must be an object`);
  if (attr.command !== undefined && !isPlainObject(attr.command)) throw badRequest(`Attribute '${name}': 'command' must be an object`);
  if (attr.writable !== undefined && typeof attr.writable !== 'boolean') throw badRequest(`Attribute '${name}': 'writable' must be a boolean`);

  if (patch.ts !== undefined) {
    if (!isIsoDate(patch.ts)) throw badRequest(`Attribute '${name}': 'ts' must be an ISO-8601 date`);
    attr.ts = new Date(patch.ts).toISOString();
  } else {
    const changed = attr.type === 'relationship'
      ? existing?.target !== attr.target
      : 'value' in patch && JSON.stringify(existing?.value) !== JSON.stringify(attr.value);
    if (changed || !attr.ts) attr.ts = now;
  }

  return attr;
}

function normalizeLocation(location) {
  if (location === null || location === undefined) return null;
  if (!isPlainObject(location) || !GEO_TYPES.includes(location.type) || !Array.isArray(location.coordinates)) {
    throw badRequest(`Invalid location: expected a GeoJSON geometry (${GEO_TYPES.join(', ')})`, { field: 'location' });
  }
  return { type: location.type, coordinates: location.coordinates };
}

function normalizeAttrs(attrsInput, existingAttrs, now, { merge }) {
  if (attrsInput === undefined) return merge ? { ...existingAttrs } : {};
  if (!isPlainObject(attrsInput)) throw badRequest("'attrs' must be an object", { field: 'attrs' });

  const result = merge ? { ...existingAttrs } : {};
  for (const [name, input] of Object.entries(attrsInput)) {
    if (input === null) {
      if (!merge) throw badRequest(`Attribute '${name}': null is only allowed in a partial update`);
      delete result[name];
      continue;
    }
    result[name] = normalizeAttr(name, input, merge ? existingAttrs[name] : null, now);
  }
  if (Object.keys(result).length > MAX_ATTRS) throw badRequest(`Too many attributes (max ${MAX_ATTRS})`);
  return result;
}

const ENTITY_KEYS = new Set(['id', 'type', 'attrs', 'location', 'version', 'createdAt', 'updatedAt']);

function checkEntityKeys(input) {
  if (!isPlainObject(input)) throw badRequest('Entity must be a JSON object');
  for (const key of Object.keys(input)) {
    if (!ENTITY_KEYS.has(key)) throw badRequest(`Unknown entity field '${key}' (attributes go under 'attrs')`, { field: key });
  }
}

/** Full entity for create/replace. */
function normalizeEntity(input, now) {
  checkEntityKeys(input);
  assertEntityId(input.id);
  assertEntityType(input.type);
  return {
    id: input.id,
    type: input.type,
    attrs: normalizeAttrs(input.attrs, {}, now, { merge: false }),
    location: normalizeLocation(input.location)
  };
}

/** Partial update: attributes are merged one by one, null removes. */
function applyPatch(existing, input, now) {
  checkEntityKeys(input);
  if (input.id !== undefined && input.id !== existing.id) throw badRequest("'id' cannot be changed");
  if (input.type !== undefined && input.type !== existing.type) throw badRequest("'type' cannot be changed");
  return {
    ...existing,
    attrs: normalizeAttrs(input.attrs, existing.attrs, now, { merge: true }),
    location: input.location === undefined ? existing.location : normalizeLocation(input.location)
  };
}

function extractRelationships(attrs) {
  return Object.entries(attrs)
    .filter(([, a]) => a.type === 'relationship')
    .map(([attr, a]) => ({ attr, target: a.target }));
}

module.exports = {
  ATTR_TYPES,
  assertEntityId,
  assertEntityType,
  assertAttrName,
  normalizeAttr,
  normalizeEntity,
  applyPatch,
  extractRelationships
};
