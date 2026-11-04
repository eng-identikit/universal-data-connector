const express = require('express');
const { asyncHandler } = require('./helpers');
const { issueTokens, verifyToken } = require('../auth/tokens');
const { badRequest, unauthorized, tooManyRequests } = require('../errors');
const LoginLimiter = require('../auth/loginLimiter');

/** /auth: public login and refresh; me/password need an authenticated user. */
module.exports = function authRoutes({ services, authenticate }) {
  const router = express.Router();
  const limiter = new LoginLimiter();

  const session = async (user) => ({
    ...issueTokens(user),
    user: await describeUser(services, user)
  });

  router.post('/login', asyncHandler(async (req, res) => {
    const { username, password } = req.body || {};
    if (typeof username !== 'string' || typeof password !== 'string') throw badRequest('username and password are required');

    const wait = limiter.blockedFor(username, req.ip);
    if (wait) throw tooManyRequests(`Too many failed attempts, retry in ${Math.ceil(wait / 60000)} min`);
    try {
      const user = await services.users.authenticate(username, password);
      limiter.succeed(username, req.ip);
      res.json(await session(user));
    } catch (error) {
      if (error.status === 401) limiter.fail(username, req.ip);
      throw error;
    }
  }));

  router.post('/refresh', asyncHandler(async (req, res) => {
    const claims = verifyToken(req.body?.refreshToken, 'refresh');
    const user = claims && await services.users.findActive(claims.sub);
    if (!user || user.token_version !== claims.tv) throw unauthorized('Invalid or expired refresh token');
    res.json(await session(user));
  }));

  router.get('/me', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind === 'apikey') {
      const k = req.auth.apiKey;
      return res.json({ kind: 'apikey', name: k.name, tenant: k.tenant, role: k.role, allowedTypes: k.allowed_types });
    }
    res.json({ kind: 'user', ...(await describeUser(services, req.auth.user)) });
  }));

  router.post('/password', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind !== 'user') throw badRequest('Only users have a password');
    const { currentPassword, newPassword } = req.body || {};
    const user = await services.users.changeOwnPassword(req.auth.user.id, currentPassword, newPassword);
    // Old tokens are revoked by the password change: hand out fresh ones
    res.json(await session(await services.users.findActive(user.id)));
  }));

  return router;
};

/** User profile with the tenants it can act on (all tenants, as admin, for a superadmin). */
async function describeUser(services, user) {
  const memberships = await services.users.memberships(user.id);
  let tenants = memberships;
  if (user.is_superadmin) {
    const roles = new Map(memberships.map(m => [m.tenant, m.role]));
    tenants = (await services.tenants.list()).map(t => ({ tenant: t.id, name: t.name, role: roles.get(t.id) || 'admin' }));
  }
  return {
    id: user.id,
    username: user.username,
    displayName: user.display_name,
    isSuperadmin: user.is_superadmin,
    tenants
  };
}
