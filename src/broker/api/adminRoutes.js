const express = require('express');
const { asyncHandler } = require('./helpers');
const { requireSuperadmin } = require('../auth/middleware');
const { badRequest } = require('../errors');

/** Users (superadmin), tenants, tenant members and API keys. Mounted after authenticate. */
module.exports = function adminRoutes({ services, tenantRole }) {
  const router = express.Router();
  const { users, tenants, apiKeys } = services;

  // ─── Users ──────────────────────────────────────────────────────────────
  router.get('/users', requireSuperadmin, asyncHandler(async (req, res) => {
    res.json({ users: await users.list() });
  }));

  router.post('/users', requireSuperadmin, asyncHandler(async (req, res) => {
    const { username, password, displayName, isSuperadmin, tenants: memberships } = req.body || {};
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
    await users.update(req.params.id, req.body || {});
    res.json(await users.get(req.params.id));
  }));

  router.delete('/users/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    if (req.params.id === req.auth.user.id) throw badRequest('You cannot delete yourself');
    await users.remove(req.params.id);
    res.status(204).end();
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
  router.get('/tenants/:tenant/members', tenantRole('admin'), asyncHandler(async (req, res) => {
    res.json({ members: await tenants.members(req.ctx.tenant) });
  }));

  router.post('/tenants/:tenant/members', tenantRole('admin'), asyncHandler(async (req, res) => {
    const { username, role } = req.body || {};
    if (typeof username !== 'string') throw badRequest('username is required');
    res.status(201).json(await tenants.setMember(req.ctx.tenant, { username }, role));
  }));

  router.put('/tenants/:tenant/members/:userId', tenantRole('admin'), asyncHandler(async (req, res) => {
    res.json(await tenants.setMember(req.ctx.tenant, { userId: req.params.userId }, req.body?.role));
  }));

  router.delete('/tenants/:tenant/members/:userId', tenantRole('admin'), asyncHandler(async (req, res) => {
    await tenants.removeMember(req.ctx.tenant, req.params.userId);
    res.status(204).end();
  }));

  // ─── API keys (tenant admin, tenant from X-Tenant) ──────────────────────
  router.get('/apikeys', tenantRole('admin'), asyncHandler(async (req, res) => {
    res.json({ apiKeys: await apiKeys.list(req.ctx.tenant) });
  }));

  router.post('/apikeys', tenantRole('admin'), asyncHandler(async (req, res) => {
    const createdBy = req.auth.kind === 'user' ? req.auth.user.id : null;
    res.status(201).json(await apiKeys.create(req.ctx.tenant, req.body || {}, createdBy));
  }));

  router.delete('/apikeys/:id', tenantRole('admin'), asyncHandler(async (req, res) => {
    await apiKeys.revoke(req.ctx.tenant, req.params.id);
    res.status(204).end();
  }));

  return router;
};
