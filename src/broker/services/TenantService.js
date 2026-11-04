const { badRequest, notFound, conflict } = require('../errors');

const TENANT_ID_RE = /^[a-z0-9][a-z0-9_-]{0,62}$/;
const ROLES = ['admin', 'operator', 'viewer'];

class TenantService {
  constructor(pool) {
    this.pool = pool;
  }

  /** All tenants, or only the given ids. Includes entity and member counts for the UI. */
  async list(ids = null) {
    const { rows } = await this.pool.query(
      `SELECT t.*,
              (SELECT count(*)::int FROM entities e WHERE e.tenant = t.id) AS entity_count,
              (SELECT count(*)::int FROM user_tenants ut WHERE ut.tenant = t.id) AS member_count
         FROM tenants t
        WHERE $1::text[] IS NULL OR t.id = ANY($1)
        ORDER BY t.id`,
      [ids]
    );
    return rows.map(toTenant);
  }

  async exists(id) {
    const { rows } = await this.pool.query('SELECT 1 FROM tenants WHERE id = $1', [id]);
    return rows.length > 0;
  }

  async create({ id, name, description }) {
    if (typeof id !== 'string' || !TENANT_ID_RE.test(id)) {
      throw badRequest('Tenant id must be 1-63 chars of [a-z0-9_-], starting with a letter or digit');
    }
    const { rows } = await this.pool.query(
      `INSERT INTO tenants (id, name, description) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING RETURNING *`,
      [id, name || id, description || null]
    );
    if (!rows.length) throw conflict(`Tenant '${id}' already exists`);
    return toTenant(rows[0]);
  }

  async update(id, { name, description }) {
    const { rows } = await this.pool.query(
      `UPDATE tenants SET name = COALESCE($2, name), description = COALESCE($3, description), updated_at = now()
        WHERE id = $1 RETURNING *`,
      [id, name ?? null, description ?? null]
    );
    if (!rows.length) throw notFound(`Tenant '${id}' not found`);
    return toTenant(rows[0]);
  }

  /** Deletes the tenant and, by cascade, its entities, memberships and API keys. */
  async remove(id) {
    if (id === 'default') throw badRequest("The 'default' tenant cannot be deleted");
    const { rowCount } = await this.pool.query('DELETE FROM tenants WHERE id = $1', [id]);
    if (!rowCount) throw notFound(`Tenant '${id}' not found`);
    // History has no foreign key (hypertable): remove it explicitly
    await this.pool.query('DELETE FROM attribute_history WHERE tenant = $1', [id]);
  }

  async members(tenant) {
    const { rows } = await this.pool.query(
      `SELECT u.id, u.username, u.display_name, u.disabled, ut.role
         FROM user_tenants ut JOIN users u ON u.id = ut.user_id
        WHERE ut.tenant = $1 ORDER BY u.username`,
      [tenant]
    );
    return rows.map(r => ({ userId: r.id, username: r.username, displayName: r.display_name, disabled: r.disabled, role: r.role }));
  }

  /** Adds or updates a member, identified by { userId } or { username }. */
  async setMember(tenant, { userId, username }, role) {
    if (!ROLES.includes(role)) throw badRequest(`role must be one of ${ROLES.join(', ')}`);
    if (!(await this.exists(tenant))) throw notFound(`Tenant '${tenant}' not found`);
    const user = userId
      ? await this.pool.query('SELECT id FROM users WHERE id = $1', [userId])
      : await this.pool.query('SELECT id FROM users WHERE username = $1', [username]);
    if (!user.rows.length) throw notFound(`User '${userId || username}' not found`);
    userId = user.rows[0].id;
    await this.pool.query(
      `INSERT INTO user_tenants (user_id, tenant, role) VALUES ($1, $2, $3)
       ON CONFLICT (user_id, tenant) DO UPDATE SET role = EXCLUDED.role`,
      [userId, tenant, role]
    );
    return { tenant, userId, role };
  }

  async removeMember(tenant, userId) {
    const { rowCount } = await this.pool.query(
      'DELETE FROM user_tenants WHERE tenant = $1 AND user_id = $2', [tenant, userId]
    );
    if (!rowCount) throw notFound('Membership not found');
  }
}

function toTenant(row) {
  const tenant = {
    id: row.id,
    name: row.name,
    description: row.description,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString()
  };
  if (row.entity_count !== undefined) {
    tenant.entityCount = row.entity_count;
    tenant.memberCount = row.member_count;
  }
  return tenant;
}

module.exports = TenantService;
module.exports.ROLES = ROLES;
