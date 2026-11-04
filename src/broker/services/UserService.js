const crypto = require('crypto');
const { withTransaction } = require('../db/pool');
const { hashPassword, verifyPassword } = require('../auth/passwords');
const { badRequest, notFound, conflict, unauthorized } = require('../errors');
const logger = require('../../utils/logger');

const USERNAME_RE = /^[A-Za-z0-9_.@-]{3,64}$/;
const MIN_PASSWORD_LENGTH = 8;
// Computed once so unknown usernames take as long as wrong passwords
const DUMMY_HASH_PROMISE = hashPassword(crypto.randomBytes(16).toString('hex'));

function checkPassword(password) {
  if (typeof password !== 'string' || password.length < MIN_PASSWORD_LENGTH) {
    throw badRequest(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`);
  }
}

class UserService {
  constructor(pool) {
    this.pool = pool;
  }

  /**
   * First start: creates the 'default' tenant and a superadmin.
   * The password comes from BROKER_ADMIN_PASSWORD or is generated and logged once.
   */
  async bootstrap() {
    await this.pool.query(
      `INSERT INTO tenants (id, name) VALUES ('default', 'Default') ON CONFLICT (id) DO NOTHING`
    );
    const { rows } = await this.pool.query('SELECT count(*)::int AS n FROM users');
    if (rows[0].n > 0) return;

    const username = process.env.BROKER_ADMIN_USER || 'admin';
    let password = process.env.BROKER_ADMIN_PASSWORD;
    if (!password) {
      password = crypto.randomBytes(12).toString('base64url');
      logger.warn(`Broker: created superadmin '${username}' with generated password: ${password} (change it after login)`);
    } else {
      logger.info(`Broker: created superadmin '${username}' from BROKER_ADMIN_PASSWORD`);
    }
    const user = await this.create({ username, password, displayName: 'Administrator', isSuperadmin: true });
    await this.pool.query(
      `INSERT INTO user_tenants (user_id, tenant, role) VALUES ($1, 'default', 'admin')`, [user.id]
    );
  }

  async authenticate(username, password) {
    const { rows } = await this.pool.query('SELECT * FROM users WHERE username = $1', [username]);
    const user = rows[0];
    const valid = await verifyPassword(String(password || ''), user ? user.password_hash : await DUMMY_HASH_PROMISE);
    if (!user || !valid || user.disabled) throw unauthorized('Invalid username or password');
    return user;
  }

  /** Raw row (with token_version) for token validation; null if missing or disabled. */
  async findActive(id) {
    const { rows } = await this.pool.query('SELECT * FROM users WHERE id = $1 AND NOT disabled', [id]);
    return rows[0] || null;
  }

  async memberships(userId) {
    const { rows } = await this.pool.query(
      `SELECT ut.tenant, ut.role, t.name FROM user_tenants ut JOIN tenants t ON t.id = ut.tenant
        WHERE ut.user_id = $1 ORDER BY ut.tenant`,
      [userId]
    );
    return rows.map(r => ({ tenant: r.tenant, name: r.name, role: r.role }));
  }

  async list() {
    const { rows } = await this.pool.query(
      `SELECT u.*, COALESCE(json_agg(json_build_object('tenant', ut.tenant, 'role', ut.role))
                FILTER (WHERE ut.tenant IS NOT NULL), '[]') AS tenants
         FROM users u LEFT JOIN user_tenants ut ON ut.user_id = u.id
        GROUP BY u.id ORDER BY u.username`
    );
    return rows.map(toUser);
  }

  async get(id) {
    const { rows } = await this.pool.query('SELECT * FROM users WHERE id = $1', [id]);
    if (!rows.length) throw notFound(`User '${id}' not found`);
    return { ...toUser(rows[0]), tenants: await this.memberships(id) };
  }

  async create({ username, password, displayName, isSuperadmin = false }) {
    if (typeof username !== 'string' || !USERNAME_RE.test(username)) {
      throw badRequest('Username must be 3-64 chars of [A-Za-z0-9_.@-]');
    }
    checkPassword(password);
    const { rows } = await this.pool.query(
      `INSERT INTO users (username, password_hash, display_name, is_superadmin) VALUES ($1, $2, $3, $4)
       ON CONFLICT (username) DO NOTHING RETURNING *`,
      [username, await hashPassword(password), displayName || null, !!isSuperadmin]
    );
    if (!rows.length) throw conflict(`Username '${username}' already exists`);
    return toUser(rows[0]);
  }

  async update(id, { displayName, password, disabled, isSuperadmin }) {
    const sets = [];
    const params = [id];
    const set = (sql, value) => { params.push(value); sets.push(`${sql} = $${params.length}`); };

    if (displayName !== undefined) set('display_name', displayName || null);
    if (isSuperadmin !== undefined) set('is_superadmin', !!isSuperadmin);
    if (disabled !== undefined) set('disabled', !!disabled);
    if (password !== undefined) {
      checkPassword(password);
      set('password_hash', await hashPassword(password));
    }
    if (!sets.length) throw badRequest('Nothing to update');
    // Password change or disabling revokes all issued tokens
    if (password !== undefined || disabled) sets.push('token_version = token_version + 1');

    return withTransaction(this.pool, async client => {
      const { rows } = await client.query(
        `UPDATE users SET ${sets.join(', ')}, updated_at = now() WHERE id = $1 RETURNING *`, params
      );
      if (!rows.length) throw notFound(`User '${id}' not found`);
      await this._assertSuperadminLeft(client);
      return toUser(rows[0]);
    });
  }

  async changeOwnPassword(id, currentPassword, newPassword) {
    const { rows } = await this.pool.query('SELECT * FROM users WHERE id = $1', [id]);
    if (!rows.length || !(await verifyPassword(String(currentPassword || ''), rows[0].password_hash))) {
      throw badRequest('Current password is wrong');
    }
    return this.update(id, { password: newPassword });
  }

  async remove(id) {
    await withTransaction(this.pool, async client => {
      const { rowCount } = await client.query('DELETE FROM users WHERE id = $1', [id]);
      if (!rowCount) throw notFound(`User '${id}' not found`);
      await this._assertSuperadminLeft(client);
    });
  }

  async _assertSuperadminLeft(client) {
    const { rows } = await client.query('SELECT count(*)::int AS n FROM users WHERE is_superadmin AND NOT disabled');
    if (rows[0].n === 0) throw conflict('At least one active superadmin must remain');
  }
}

function toUser(row) {
  const user = {
    id: row.id,
    username: row.username,
    displayName: row.display_name,
    isSuperadmin: row.is_superadmin,
    disabled: row.disabled,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString()
  };
  if (row.tenants) user.tenants = row.tenants;
  return user;
}

module.exports = UserService;
