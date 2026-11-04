const { verifyToken } = require('./tokens');
const { unauthorized, forbidden, badRequest, notFound } = require('../errors');

const ROLE_RANK = { viewer: 1, operator: 2, admin: 3 };

/**
 * Resolves the caller from a bearer token or an API key:
 *   user:   { kind: 'user', user, isSuperadmin, memberships: Map<tenant, role>, tokenExp }
 *   apikey: { kind: 'apikey', apiKey, tenant, role, allowedTypes }
 * Throws 401 when neither is valid.
 */
async function resolveCaller({ users, apiKeys }, { token, apiKey }) {
  if (token) {
    const claims = verifyToken(token, 'access');
    const user = claims && await users.findActive(claims.sub);
    if (!user || user.token_version !== claims.tv) throw unauthorized('Invalid or expired token');
    const memberships = await users.memberships(user.id);
    return {
      kind: 'user',
      user,
      isSuperadmin: user.is_superadmin,
      memberships: new Map(memberships.map(m => [m.tenant, m.role])),
      tokenExp: claims.exp
    };
  }
  if (apiKey) {
    const key = await apiKeys.resolve(apiKey);
    if (!key) throw unauthorized('Invalid, expired or revoked API key');
    return { kind: 'apikey', apiKey: key, tenant: key.tenant, role: key.role, allowedTypes: key.allowed_types };
  }
  throw unauthorized();
}

/**
 * Target tenant and role check. A user with a single tenant may omit the
 * tenant; API keys are bound to their tenant. Returns { tenant, role, allowedTypes }.
 */
async function resolveTenant({ tenants }, auth, requested, minRole) {
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
  return { tenant, role, allowedTypes: auth.allowedTypes || null };
}

/** Express middleware: `Authorization: Bearer <jwt>` or `X-API-Key`, sets req.auth. */
function authenticate(services) {
  return async (req, res, next) => {
    try {
      const header = req.get('authorization') || '';
      req.auth = await resolveCaller(services, {
        token: header.startsWith('Bearer ') ? header.slice(7) : null,
        apiKey: req.get('x-api-key')
      });
      next();
    } catch (error) {
      next(error);
    }
  };
}

/** Express middleware: tenant from the :tenant param or X-Tenant, sets req.ctx. */
function requireTenantRole(minRole, services) {
  return async (req, res, next) => {
    try {
      req.ctx = await resolveTenant(services, req.auth, req.params.tenant || req.get('x-tenant'), minRole);
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

module.exports = { authenticate, requireTenantRole, requireSuperadmin, resolveCaller, resolveTenant, ROLE_RANK };
