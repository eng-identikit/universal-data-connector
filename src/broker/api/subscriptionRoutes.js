const crypto = require('crypto');
const express = require('express');
const { asyncHandler } = require('./helpers');
const { compile, ExpressionError, FUNCTIONS } = require('../expr/expression');
const { validateCreate } = require('../subscriptions/subscriptionSchema');
const { pickEditable } = require('../subscriptions/SubscriptionService');
const { badRequest } = require('../errors');
const { ROLE_RANK } = require('../auth/middleware');

const SECRET_HEADER_RE = /authorization|token|secret|key|password/i;

/**
 * /subscriptions: CRUD, pause/resume, test, preview, statistics and DLQ.
 * Viewers can read (secrets masked), operators can write.
 */
module.exports = function subscriptionRoutes({ services, tenantRole, engine, worker }) {
  const router = express.Router();
  const subs = services.subscriptions;
  const read = tenantRole('viewer');
  const write = tenantRole('operator');

  // Viewers see webhook auth headers and MQTT passwords masked
  const present = (req, sub) => {
    if (ROLE_RANK[req.ctx.role] >= ROLE_RANK.operator) return sub;
    const notify = { ...sub.notify };
    if (notify.headers) {
      notify.headers = Object.fromEntries(Object.entries(notify.headers).map(([k, v]) => [k, SECRET_HEADER_RE.test(k) ? '••••' : v]));
    }
    if (notify.password) notify.password = '••••';
    return { ...sub, notify };
  };

  router.post('/expressions/validate', read, (req, res) => {
    const { expression } = req.body || {};
    try {
      const compiled = compile(expression);
      res.json({ valid: true, attrs: compiled.attrs });
    } catch (error) {
      if (!(error instanceof ExpressionError)) throw error;
      res.json({ valid: false, error: error.message, position: error.position ?? null });
    }
  });

  router.get('/expressions/functions', read, (req, res) => res.json({ functions: FUNCTIONS }));

  router.get('/subscriptions', read, asyncHandler(async (req, res) => {
    res.json({ subscriptions: (await subs.list(req.ctx.tenant)).map(s => present(req, s)) });
  }));

  router.post('/subscriptions', write, asyncHandler(async (req, res) => {
    const createdBy = req.auth.kind === 'user' ? req.auth.user.username : `apikey:${req.auth.apiKey.name}`;
    res.status(201).json(await subs.create(req.ctx.tenant, req.body, createdBy));
  }));

  // Entities that would currently match a (draft) subscription, for the wizard
  router.post('/subscriptions/preview', read, asyncHandler(async (req, res) => {
    const draft = validateCreate({ name: 'preview', notify: { channel: 'websocket' }, ...req.body });
    const entities = await engine.matchingEntities({ ...draft, tenant: req.ctx.tenant }, 20);
    res.json({ entities, count: entities.length });
  }));

  router.get('/subscriptions/:id', read, asyncHandler(async (req, res) => {
    res.json(present(req, await subs.get(req.ctx.tenant, req.params.id)));
  }));

  router.patch('/subscriptions/:id', write, asyncHandler(async (req, res) => {
    res.json(await subs.update(req.ctx.tenant, req.params.id, req.body || {}));
  }));

  router.delete('/subscriptions/:id', write, asyncHandler(async (req, res) => {
    await subs.remove(req.ctx.tenant, req.params.id);
    res.status(204).end();
  }));

  router.post('/subscriptions/:id/pause', write, asyncHandler(async (req, res) => {
    res.json(await subs.update(req.ctx.tenant, req.params.id, { enabled: false }));
  }));

  router.post('/subscriptions/:id/resume', write, asyncHandler(async (req, res) => {
    res.json(await subs.update(req.ctx.tenant, req.params.id, { enabled: true }));
  }));

  router.delete('/subscriptions/:id/stats', write, asyncHandler(async (req, res) => {
    await subs.resetStats(req.ctx.tenant, req.params.id);
    res.status(204).end();
  }));

  /**
   * Sends one notification right now, bypassing queue, filter and throttle.
   * Body: { entityId? } — uses that entity, else the first matching one, else a sample.
   */
  router.post('/subscriptions/:id/test', write, asyncHandler(async (req, res) => {
    const sub = await subs.get(req.ctx.tenant, req.params.id);
    let entity;
    if (req.body?.entityId) entity = await services.entities.get(req.ctx, req.body.entityId);
    else [entity] = await engine.matchingEntities({ ...pickEditable(sub), id: sub.id, tenant: sub.tenant }, 1);
    entity = entity || {
      id: 'test:sample', type: 'Sample', version: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      attrs: { value: { type: 'float', value: 42, ts: new Date().toISOString() } }
    };
    const notification = {
      id: crypto.randomUUID(), subscriptionId: sub.id, subscriptionName: sub.name, tenant: sub.tenant,
      reason: 'test', timestamp: new Date().toISOString(), entities: [entity]
    };
    res.json({ ...(await worker.test({ ...sub, tenant: sub.tenant }, notification)), entityId: entity.id });
  }));

  // ─── Dead-letter queue ────────────────────────────────────────────────
  router.get('/subscriptions/:id/dlq', read, asyncHandler(async (req, res) => {
    res.json({ items: await subs.listDead(req.ctx.tenant, req.params.id, req.query) });
  }));

  /** Re-enqueues DLQ notifications (all, or body.ids) with a fresh retry budget. */
  router.post('/subscriptions/:id/dlq/replay', write, asyncHandler(async (req, res) => {
    const ids = req.body?.ids;
    if (ids !== undefined && !Array.isArray(ids)) throw badRequest('ids must be an array');
    const notifications = await subs.takeDead(req.ctx.tenant, req.params.id, ids);
    for (const n of notifications) {
      const replay = { ...n, id: crypto.randomUUID(), replayOf: n.id };
      await engine.queue.publish(`udc.notify.${req.ctx.tenant.replace(/[.\s*>]/g, '_')}.${req.params.id}`, replay, { id: replay.id });
    }
    res.json({ replayed: notifications.length });
  }));

  router.delete('/subscriptions/:id/dlq', write, asyncHandler(async (req, res) => {
    const ids = Array.isArray(req.body?.ids) ? req.body.ids : undefined;
    res.json({ purged: (await subs.takeDead(req.ctx.tenant, req.params.id, ids)).length });
  }));

  return router;
};
