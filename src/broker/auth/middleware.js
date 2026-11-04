const { verifyToken } = require('./tokens');
const { unauthorized, forbidden, badRequest, notFound } = require('../errors');

const ROLE_RANK = { viewer: 1, operator: 2, admin: 3 };

/**
 * Resolves the caller from `Authorization: Bearer <jwt>` or `X-API-Key`
 * and sets req.auth:
 *   user:   { kind: 'user', user, isSuperadmin, memberships: Map<tenant, role> }
 *   apikey: { kind: 'apikey', apiKey, tenant, role, allowedTypes }
 */
function authenticate({ users, apiKeys }) {
  return async (req, res, next) => {
    try {
      const header = req.get('authorization') || '';
      const apiKey = req.get('x-api-key');

      if (header.startsWith('Bearer ')) {
        const claims = verifyToken(header.slice(7), 'access');
        const user = claims && await users.findActive(claims.sub);
        if (!user || user.token_version !== claims.tv) throw unauthorized('Invalid or expired token');
        const memberships = await users.memberships(user.id);
        req.auth = {
          kind: 'user',
          user,
          isSuperadmin: user.is_superadmin,
          memberships: new Map(memberships.map(m => [m.tenant, m.role]))
        };
      } else if (apiKey) {
        const key = await apiKeys.resolve(apiKey);
        if (!key) throw unauthorized('Invalid, expired or revoked API key');
        req.auth = { kind: 'apikey', apiKey: key, tenant: key.tenant, role: key.role, allowedTypes: key.allowed_types };
      } else {
        throw unauthorized();
      }
      next();
    } catch (error) {
      next(error);
    }
  };
}

/**
 * Resolves the target tenant (X-Tenant header) and checks the caller has at
 * least `minRole` there. Sets req.ctx = { tenant, role, allowedTypes } for the services.
 * A user with a single tenant may omit the header; API keys are bound to their tenant.
 */
function requireTenantRole(minRole, { tenants }) {
  return async (req, res, next) => {
    try {
      const auth = req.auth;
      const requested = req.params.tenant || req.get('x-tenant');
      let tenant;
      let role;

      if (auth.kind === 'apikey') {
        if (requested && requested !== auth.tenant) throw forbidden('API key is bound to another tenant');
        tenant = auth.tenant;
        role = auth.role;
      } else {
        tenant = requested || (auth.memberships.size === 1 ? [...auth.memberships.keys()][0] : null);
        if (!tenant) throw badRequest('X-Tenant header is required');
        role = auth.memberships.get(tenant);
        if (auth.isSuperadmin) {
          if (!role && !(await tenants.exists(tenant))) throw notFound(`Tenant '${tenant}' not found`);
          role = 'admin';
        }
        if (!role) throw forbidden(`No access to tenant '${tenant}'`);
      }

      if (ROLE_RANK[role] < ROLE_RANK[minRole]) throw forbidden(`Role '${minRole}' required on tenant '${tenant}'`);
      req.ctx = { tenant, role, allowedTypes: auth.allowedTypes || null };
      next();
    } catch (error) {
      next(error);
    }
  };
}

function requireSuperadmin(req, res, next) {
  if (req.auth?.kind === 'user' && req.auth.isSuperadmin) return next();
  next(forbidden('Superadmin required'));
}

module.exports = { authenticate, requireTenantRole, requireSuperadmin, ROLE_RANK };
