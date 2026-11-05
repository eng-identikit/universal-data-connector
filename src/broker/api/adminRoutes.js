const express = require('express');
const { asyncHandler } = require('./helpers');
const { requireSuperadmin } = require('../auth/middleware');
const { badRequest, forbidden, notFound } = require('../errors');
const { ROLES } = require('../services/TenantService');
const { parseIntParam } = require('../services/EntityService');

// Managing members and API keys needs a user: an API key must not mint other
// keys or grant roles (it could escape its own allowedTypes restriction)
function userOnly(req, res, next) {
  next(req.auth.kind === 'user' ? undefined : forbidden('This operation needs a user, not an API key'));
}

/** Users, sessions and access log (superadmin), tenants, tenant members and API keys. Mounted after authenticate. */
module.exports = function adminRoutes({ services, tenantRole, broker }) {
  const router = express.Router();
  const { users, tenants, apiKeys, sessions, authEvents } = services;
  const actorOf = (req) => ({ actor: req.auth.user.username, ip: req.ip });

  // ─── Users ──────────────────────────────────────────────────────────────
  router.get('/users', requireSuperadmin, asyncHandler(async (req, res) => {
    res.json({ users: await users.list() });
  }));

  router.post('/users', requireSuperadmin, asyncHandler(async (req, res) => {
    const { username, password, displayName, isSuperadmin, tenants: memberships } = req.body || {};
    // Checked before creating the user, so a bad membership does not leave a half-created user
    for (const m of Array.isArray(memberships) ? memberships : []) {
      if (!ROLES.includes(m?.role)) throw badRequest(`role must be one of ${ROLES.join(', ')}`);
      if (!(await tenants.exists(m.tenant))) throw notFound(`Tenant '${m.tenant}' not found`);
    }
    const user = await users.create({ username, password, displayName, isSuperadmin });
    for (const m of Array.isArray(memberships) ? memberships : []) {
      await tenants.setMember(m.tenant, { userId: user.id }, m.role);
    }
    res.status(201).json(await users.get(user.id));
  }));

  router.get('/users/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    res.json(await users.get(req.params.id));
  }));

  router.patch('/users/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    const body = req.body || {};
    const user = await users.update(req.params.id, body);
    // A new password or disabling ends every session of the user
    if (body.password !== undefined || body.disabled) {
      await broker.revokeUserSessions(req.params.id);
      authEvents.record({ kind: 'user_sessions_revoked', userId: user.id, username: user.username, detail: body.disabled ? 'user disabled' : 'password changed', ...actorOf(req) });
    }
    res.json(await users.get(req.params.id));
  }));

  router.delete('/users/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    if (req.params.id === req.auth.user.id) throw badRequest('You cannot delete yourself');
    await users.remove(req.params.id);
    await broker.revokeUserSessions(req.params.id);
    res.status(204).end();
  }));

  // ─── Sessions and access log (superadmin) ───────────────────────────────
  router.get('/sessions', requireSuperadmin, asyncHandler(async (req, res) => {
    const list = await sessions.list({ userId: req.query.userId || undefined });
    const names = new Map((await users.list()).map(u => [u.id, u]));
    res.json({
      store: sessions.kind,
      sessions: list.map(s => ({
        ...s,
        username: names.get(s.userId)?.username ?? null,
        displayName: names.get(s.userId)?.displayName ?? null,
        current: s.id === req.auth.sessionId
      }))
    });
  }));

  router.delete('/sessions/:sid', requireSuperadmin, asyncHandler(async (req, res) => {
    const session = await sessions.get(req.params.sid);
    if (!session) throw notFound('Session not found');
    await broker.revokeSession(session.id);
    const user = await users.get(session.userId).catch(() => null);
    authEvents.record({ kind: 'session_revoked', userId: session.userId, username: user?.username, sessionId: session.id, ...actorOf(req) });
    res.status(204).end();
  }));

  router.delete('/users/:id/sessions', requireSuperadmin, asyncHandler(async (req, res) => {
    const user = await users.get(req.params.id);
    const revoked = await broker.revokeUserSessions(user.id);
    authEvents.record({ kind: 'user_sessions_revoked', userId: user.id, username: user.username, detail: `${revoked.length} sessions`, ...actorOf(req) });
    res.json({ revoked: revoked.length });
  }));

  router.get('/auth-events', requireSuperadmin, asyncHandler(async (req, res) => {
    const { userId, kind, from, to } = req.query;
    for (const [name, value] of [['from', from], ['to', to]]) {
      if (value && Number.isNaN(Date.parse(value))) throw badRequest(`${name} must be an ISO date`);
    }
    if (kind && !authEvents.constructor.KINDS.includes(kind)) throw badRequest(`kind must be one of ${authEvents.constructor.KINDS.join(', ')}`);
    res.json(await authEvents.list({
      userId: userId || undefined,
      kind: kind || undefined,
      from: from || undefined,
      to: to || undefined,
      limit: parseIntParam(req.query.limit, 100, 1, 1000, 'limit'),
      offset: parseIntParam(req.query.offset, 0, 0, 1000000, 'offset')
    }));
  }));

  // ─── Tenants ────────────────────────────────────────────────────────────
  router.get('/tenants', asyncHandler(async (req, res) => {
    const auth = req.auth;
    const ids = auth.kind === 'apikey' ? [auth.tenant] : auth.isSuperadmin ? null : [...auth.memberships.keys()];
    res.json({ tenants: await tenants.list(ids) });
  }));

  router.post('/tenants', requireSuperadmin, asyncHandler(async (req, res) => {
    res.status(201).json(await tenants.create(req.body || {}));
  }));

  router.patch('/tenants/:tenant', requireSuperadmin, asyncHandler(async (req, res) => {
    res.json(await tenants.update(req.params.tenant, req.body || {}));
  }));

  router.delete('/tenants/:tenant', requireSuperadmin, asyncHandler(async (req, res) => {
    await tenants.remove(req.params.tenant);
    res.status(204).end();
  }));

  // ─── Members (tenant admin) ─────────────────────────────────────────────
  router.get('/tenants/:tenant/members', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    res.json({ members: await tenants.members(req.ctx.tenant) });
  }));

  router.post('/tenants/:tenant/members', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    const { username, role } = req.body || {};
    if (typeof username !== 'string') throw badRequest('username is required');
    res.status(201).json(await tenants.setMember(req.ctx.tenant, { username }, role));
  }));

  router.put('/tenants/:tenant/members/:userId', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    res.json(await tenants.setMember(req.ctx.tenant, { userId: req.params.userId }, req.body?.role));
  }));

  router.delete('/tenants/:tenant/members/:userId', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    await tenants.removeMember(req.ctx.tenant, req.params.userId);
    res.status(204).end();
  }));

  // ─── API keys (tenant admin, tenant from X-Tenant) ──────────────────────
  router.get('/apikeys', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    res.json({ apiKeys: await apiKeys.list(req.ctx.tenant) });
  }));

  router.post('/apikeys', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    const createdBy = req.auth.kind === 'user' ? req.auth.user.id : null;
    res.status(201).json(await apiKeys.create(req.ctx.tenant, req.body || {}, createdBy));
  }));

  router.delete('/apikeys/:id', tenantRole('admin'), userOnly, asyncHandler(async (req, res) => {
    await apiKeys.revoke(req.ctx.tenant, req.params.id);
    res.status(204).end();
  }));

  return router;
};
