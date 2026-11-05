const express = require('express');
const { asyncHandler } = require('./helpers');
const { requireSuperadmin } = require('../auth/middleware');
const { badRequest, conflict } = require('../errors');
const { TransformError } = require('../transform/transformer');

const MAX_PREVIEW_ENTITIES = 5;

/**
 * Output transformer library (config/transformers.json): anyone authenticated
 * can read it and preview, a superadmin changes it.
 */
module.exports = function transformerRoutes({ services, tenantRole, broker }) {
  const router = express.Router();
  const { transformers } = services;

  router.get('/transformers', asyncHandler(async (req, res) => {
    res.json({ transformers: transformers.list() });
  }));

  router.get('/transformers/:id', asyncHandler(async (req, res) => {
    res.json(transformers.get(req.params.id));
  }));

  router.put('/transformers/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    const saved = await transformers.put(req.params.id, req.body || {});
    broker.configChanged('transformers');
    res.json(saved);
  }));

  router.delete('/transformers/:id', requireSuperadmin, asyncHandler(async (req, res) => {
    transformers.get(req.params.id);
    const { rows } = await broker.pool.query(
      `SELECT id, tenant, name FROM subscriptions WHERE notify->>'transformer' = $1 ORDER BY tenant, name`, [req.params.id]
    );
    if (rows.length) {
      throw conflict(`Transformer '${req.params.id}' is used by ${rows.length} subscription(s)`, { subscriptions: rows });
    }
    await transformers.remove(req.params.id);
    broker.configChanged('transformers');
    res.status(204).end();
  }));

  /**
   * Runs a transformer on real entities of the caller's tenant.
   * Body: { expression | transformer, entityIds?, types?, notify?: { attrs, format, perEntity } }
   * Without entityIds the first entities of `types` (or of any type) are used.
   */
  router.post('/transformers/preview', tenantRole('viewer'), asyncHandler(async (req, res) => {
    const { expression, transformer, entityIds, types, notify = {} } = req.body || {};
    if (!expression && !transformer) throw badRequest('expression or transformer is required');

    let entities;
    if (Array.isArray(entityIds) && entityIds.length) {
      entities = [];
      for (const id of entityIds.slice(0, MAX_PREVIEW_ENTITIES)) entities.push(await services.entities.get(req.ctx, String(id)));
    } else {
      const query = { limit: String(Math.min(Number(notify.perEntity ? 2 : 1) || 1, MAX_PREVIEW_ENTITIES)) };
      if (Array.isArray(types) && types.length) query.type = types.join(',');
      entities = (await services.entities.list(req.ctx, query)).entities;
    }
    if (!entities.length) throw badRequest('No entities to preview with: create one or pick other types');

    const sub = {
      id: 'preview',
      name: 'preview',
      tenant: req.ctx.tenant,
      notify: {
        channel: 'http',
        attrs: Array.isArray(notify.attrs) ? notify.attrs : [],
        format: notify.format === 'values' ? 'values' : 'full',
        perEntity: !!notify.perEntity,
        ...(expression ? { transform: { expression: String(expression) } } : { transformer: String(transformer) })
      }
    };
    const notification = {
      id: 'preview', subscriptionId: 'preview', subscriptionName: 'preview', tenant: req.ctx.tenant,
      reason: 'update', timestamp: new Date().toISOString(), entities
    };
    try {
      const messages = await broker.channels.render(sub, notification);
      res.json({
        input: { ...notification, entity: entities[0] },
        messages: messages.map(m => ({ contentType: m.contentType, payload: m.payload, entityId: m.entity?.id ?? null }))
      });
    } catch (error) {
      if (error instanceof TransformError || /not found/.test(error.message)) throw badRequest(error.message);
      throw error;
    }
  }));

  return router;
};
