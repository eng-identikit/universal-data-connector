const { generateApiKey, hashApiKey } = require('../auth/tokens');
const { assertEntityType } = require('../model/entityModel');
const logger = require('../../utils/logger');
const { badRequest, notFound } = require('../errors');
const { ROLES } = require('./TenantService');

// last_used_at is written at most once per minute per key to avoid a write on every request
const LAST_USED_RESOLUTION_MS = 60 * 1000;

class ApiKeyService {
  constructor(pool) {
    this.pool = pool;
    this.lastUsedWrites = new Map();
  }

  async list(tenant) {
    const { rows } = await this.pool.query(
      `SELECT k.*, u.username AS created_by_username FROM api_keys k
         LEFT JOIN users u ON u.id = k.created_by
        WHERE k.tenant = $1 ORDER BY k.created_at DESC`,
      [tenant]
    );
    return rows.map(toApiKey);
  }

  /** Returns the key record plus the plaintext key, which is shown only once. */
  async create(tenant, { name, role, allowedTypes, expiresAt }, createdBy) {
    if (typeof name !== 'string' || !name.trim()) throw badRequest('name is required');
    if (!ROLES.includes(role)) throw badRequest(`role must be one of ${ROLES.join(', ')}`);
    let types = null;
    if (allowedTypes !== undefined && allowedTypes !== null) {
      if (!Array.isArray(allowedTypes)) throw badRequest('allowedTypes must be an array of entity types');
      allowedTypes.forEach(assertEntityType);
      types = allowedTypes.length ? allowedTypes : null;
    }
    let expires = null;
    if (expiresAt) {
      expires = new Date(expiresAt);
      if (Number.isNaN(expires.getTime()) || expires <= new Date()) throw badRequest('expiresAt must be a future date');
    }

    const { key, prefix, hash } = generateApiKey();
    const { rows } = await this.pool.query(
      `INSERT INTO api_keys (tenant, name, key_prefix, key_hash, role, allowed_types, created_by, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
      [tenant, name.trim(), prefix, hash, role, types, createdBy || null, expires]
    );
    return { ...toApiKey(rows[0]), key };
  }

  // ─── Service keys: no tenant, superadmin powers (orchestrator access) ──────

  async listService() {
    const { rows } = await this.pool.query(
      `SELECT k.*, u.username AS created_by_username FROM api_keys k
         LEFT JOIN users u ON u.id = k.created_by
        WHERE k.scope = 'service' ORDER BY k.created_at DESC`
    );
    return rows.map(toApiKey);
  }

  async createService({ name, expiresAt }, createdBy) {
    if (typeof name !== 'string' || !name.trim()) throw badRequest('name is required');
    let expires = null;
    if (expiresAt) {
      expires = new Date(expiresAt);
      if (Number.isNaN(expires.getTime()) || expires <= new Date()) throw badRequest('expiresAt must be a future date');
    }
    const { key, prefix, hash } = generateApiKey();
    const { rows } = await this.pool.query(
      `INSERT INTO api_keys (tenant, scope, name, key_prefix, key_hash, role, created_by, expires_at)
       VALUES (NULL, 'service', $1, $2, $3, 'admin', $4, $5) RETURNING *`,
      [name.trim(), prefix, hash, createdBy || null, expires]
    );
    return { ...toApiKey(rows[0]), key };
  }

  async revokeService(id) {
    const { rowCount } = await this.pool.query(
      "UPDATE api_keys SET revoked_at = now() WHERE scope = 'service' AND id = $1 AND revoked_at IS NULL", [id]
    );
    if (!rowCount) throw notFound(`Active service key '${id}' not found`);
  }

  /**
   * BROKER_SERVICE_KEY: a service key provisioned by the deployment (e.g. Compose
   * wiring the orchestrator to this instance). Registered once, by hash.
   */
  async ensureBootstrapServiceKey(key = process.env.BROKER_SERVICE_KEY) {
    if (!key) return;
    if (!/^udc_[A-Za-z0-9_-]{20,}$/.test(key)) {
      logger.warn('BROKER_SERVICE_KEY ignored: it must start with udc_ followed by at least 20 letters, digits, _ or -');
      return;
    }
    const prefix = key.slice(4, 12);
    await this.pool.query(
      `INSERT INTO api_keys (tenant, scope, name, key_prefix, key_hash, role)
       VALUES (NULL, 'service', 'bootstrap (BROKER_SERVICE_KEY)', $1, $2, 'admin')
       ON CONFLICT (key_hash) DO NOTHING`,
      [prefix, hashApiKey(key)]
    );
  }

  async revoke(tenant, id) {
    const { rowCount } = await this.pool.query(
      'UPDATE api_keys SET revoked_at = now() WHERE tenant = $1 AND id = $2 AND revoked_at IS NULL', [tenant, id]
    );
    if (!rowCount) throw notFound(`Active API key '${id}' not found`);
  }

  /** Valid (not revoked, not expired) key record for a plaintext key, or null. */
  async resolve(key) {
    const { rows } = await this.pool.query(
      `SELECT * FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())`,
      [hashApiKey(key)]
    );
    if (!rows.length) return null;
    this._touch(rows[0].id);
    return rows[0];
  }

  _touch(id) {
    const now = Date.now();
    if (now - (this.lastUsedWrites.get(id) || 0) < LAST_USED_RESOLUTION_MS) return;
    this.lastUsedWrites.set(id, now);
    this.pool.query('UPDATE api_keys SET last_used_at = now() WHERE id = $1', [id]).catch(() => {});
  }
}

function toApiKey(row) {
  return {
    id: row.id,
    tenant: row.tenant,
    scope: row.scope,
    name: row.name,
    prefix: `udc_${row.key_prefix}_`,
    role: row.role,
    allowedTypes: row.allowed_types,
    createdBy: row.created_by_username ?? null,
    createdAt: row.created_at.toISOString(),
    expiresAt: row.expires_at ? row.expires_at.toISOString() : null,
    revokedAt: row.revoked_at ? row.revoked_at.toISOString() : null,
    lastUsedAt: row.last_used_at ? row.last_used_at.toISOString() : null
  };
}

module.exports = ApiKeyService;
