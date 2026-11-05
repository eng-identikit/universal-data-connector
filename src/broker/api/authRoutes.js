const express = require('express');
const { asyncHandler } = require('./helpers');
const { issueTokens, refreshExpiry, verifyToken } = require('../auth/tokens');
const { badRequest, unauthorized, notFound, tooManyRequests } = require('../errors');
const LoginLimiter = require('../auth/loginLimiter');

/**
 * /auth: public login and refresh; me, password, logout and own sessions need
 * an authenticated user. Every login opens a session (see SessionStore).
 */
module.exports = function authRoutes({ services, authenticate, broker }) {
  const router = express.Router();
  const { sessions, authEvents } = services;
  const limiter = new LoginLimiter(sessions.redis || null);
  const client = (req) => ({ ip: req.ip, userAgent: (req.get('user-agent') || '').slice(0, 300) || null });

  const respond = async (user, session) => ({
    ...issueTokens(user, session),
    sessionId: session.id,
    user: await describeUser(services, user)
  });

  router.post('/login', asyncHandler(async (req, res) => {
    const { username, password } = req.body || {};
    if (typeof username !== 'string' || typeof password !== 'string') throw badRequest('username and password are required');
    const from = client(req);

    const wait = await limiter.attempt(username, req.ip);
    if (wait) {
      authEvents.record({ kind: 'login_blocked', username, ...from });
      throw tooManyRequests(`Too many failed attempts, retry in ${Math.ceil(wait / 60000)} min`);
    }
    let user;
    try {
      user = await services.users.authenticate(username, password);
    } catch (error) {
      if (error.status === 401) authEvents.record({ kind: 'login_failed', username, ...from });
      throw error;
    }
    await limiter.succeed(username, req.ip);
    const session = await sessions.create({ userId: user.id, ...from, expiresAt: refreshExpiry() });
    authEvents.record({ kind: 'login', userId: user.id, username: user.username, sessionId: session.id, ...from });
    res.json(await respond(user, session));
  }));

  router.post('/refresh', asyncHandler(async (req, res) => {
    const claims = verifyToken(req.body?.refreshToken, 'refresh');
    const user = claims && claims.sid && await services.users.findActive(claims.sub);
    if (!user || user.token_version !== claims.tv) throw unauthorized('Invalid or expired refresh token');
    const rotated = await sessions.rotate(claims.sid, claims.jti, refreshExpiry());
    if (rotated?.reuse) {
      // A refresh token is single use: a second use means it was copied
      await revokeSession(claims.sid, { reason: 'refresh token reused' });
      authEvents.record({ kind: 'refresh_reuse', userId: user.id, username: user.username, sessionId: claims.sid, ...client(req) });
    }
    if (!rotated?.jti) throw unauthorized('Invalid or expired refresh token');
    res.json(await respond(user, { id: claims.sid, jti: rotated.jti }));
  }));

  router.post('/logout', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind === 'user') {
      await revokeSession(req.auth.sessionId);
      authEvents.record({ kind: 'logout', userId: req.auth.user.id, username: req.auth.user.username, sessionId: req.auth.sessionId, ...client(req) });
    }
    res.status(204).end();
  }));

  router.get('/me', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind === 'apikey') {
      const k = req.auth.apiKey;
      return res.json({ kind: 'apikey', name: k.name, tenant: k.tenant, role: k.role, allowedTypes: k.allowed_types });
    }
    res.json({ kind: 'user', ...(await describeUser(services, req.auth.user)), sessionId: req.auth.sessionId });
  }));

  router.get('/sessions', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind !== 'user') throw badRequest('Only users have sessions');
    const list = await sessions.list({ userId: req.auth.user.id });
    res.json({ sessions: list.map(s => ({ ...s, current: s.id === req.auth.sessionId })) });
  }));

  router.delete('/sessions/:sid', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind !== 'user') throw badRequest('Only users have sessions');
    const session = await sessions.get(req.params.sid);
    if (!session || session.userId !== req.auth.user.id) throw notFound('Session not found');
    await revokeSession(session.id);
    authEvents.record({
      kind: 'session_revoked', userId: req.auth.user.id, username: req.auth.user.username, sessionId: session.id, actor: req.auth.user.username, ...client(req)
    });
    res.status(204).end();
  }));

  router.post('/password', authenticate, asyncHandler(async (req, res) => {
    if (req.auth.kind !== 'user') throw badRequest('Only users have a password');
    const { currentPassword, newPassword } = req.body || {};
    const user = await services.users.changeOwnPassword(req.auth.user.id, currentPassword, newPassword);
    // The password change revokes every token and session: open a fresh one for this client
    await revokeUserSessions(user.id);
    const fresh = await services.users.findActive(user.id);
    const session = await sessions.create({ userId: user.id, ...client(req), expiresAt: refreshExpiry() });
    res.json(await respond(fresh, session));
  }));

  const revokeSession = (sid, opts) => broker.revokeSession(sid, opts);
  const revokeUserSessions = (userId) => broker.revokeUserSessions(userId);

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

module.exports.describeUser = describeUser;
