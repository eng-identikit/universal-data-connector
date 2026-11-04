const express = require('express');
const { asyncHandler } = require('./helpers');
const { badRequest } = require('../errors');
const model = require('../model/entityModel');

const MAX_INGEST = 5000;

/** Entity history, device commands and the high-throughput /ingest endpoint. */
module.exports = function dataRoutes({ services, tenantRole, broker }) {
  const router = express.Router();
  const read = tenantRole('viewer');
  const write = tenantRole('operator');
  const caller = (req) => (req.auth.kind === 'user' ? req.auth.user.username : `apikey:${req.auth.apiKey.name}`);

  // ─── History ──────────────────────────────────────────────────────────
  router.get('/entities/:id/history', read, asyncHandler(async (req, res) => {
    await services.entities.get(req.ctx, req.params.id);
    res.json(await services.history.series(req.ctx.tenant, req.params.id, req.query));
  }));

  router.get('/entities/:id/history/attributes', read, asyncHandler(async (req, res) => {
    await services.entities.get(req.ctx, req.params.id);
    res.json({ attributes: await services.history.attributes(req.ctx.tenant, req.params.id) });
  }));

  router.get('/entities/:id/history/:attr/raw', read, asyncHandler(async (req, res) => {
    await services.entities.get(req.ctx, req.params.id);
    res.json({ samples: await services.history.raw(req.ctx.tenant, req.params.id, req.params.attr, req.query) });
  }));

  // ─── Commands ─────────────────────────────────────────────────────────
  router.post('/entities/:id/attrs/:attr/command', write, asyncHandler(async (req, res) => {
    if (!req.body || !('value' in req.body)) throw badRequest("Body must be { value }");
    const command = await services.commands.create(req.ctx, req.params.id, req.params.attr, req.body.value, {
      createdBy: caller(req),
      waitMs: req.query.wait !== undefined ? Number(req.query.wait) || 0 : 5000
    });
    res.status(command.status === 'succeeded' || command.status === 'failed' ? 200 : 202).json(command);
  }));

  router.get('/commands', read, asyncHandler(async (req, res) => {
    res.json({ commands: await services.commands.list(req.ctx, req.query) });
  }));

  router.get('/commands/:id', read, asyncHandler(async (req, res) => {
    res.json(await services.commands.get(req.ctx, req.params.id));
  }));

  // ─── Ingest ───────────────────────────────────────────────────────────
  /**
   * Asynchronous bulk upsert for producers: { entities: [{ id, type, attrs, location? }] }.
   * Answers 202 once the batch is queued; invalid items are skipped when applied
   * (see /ingest/stats). Use /entities/batch for synchronous, atomic writes.
   */
  router.post('/ingest', write, asyncHandler(async (req, res) => {
    const items = req.body?.entities;
    if (!Array.isArray(items) || !items.length) throw badRequest("'entities' must be a non-empty array");
    if (items.length > MAX_INGEST) throw badRequest(`Too many entities (max ${MAX_INGEST} per request)`);
    for (const item of items) {
      if (!item || typeof item !== 'object') throw badRequest('Each item must be an object');
      model.assertEntityId(item.id);
      model.assertEntityType(item.type);
      if (item.attrs !== undefined && (typeof item.attrs !== 'object' || Array.isArray(item.attrs))) {
        throw badRequest(`'attrs' of '${item.id}' must be an object`);
      }
      if (req.ctx.allowedTypes && !req.ctx.allowedTypes.includes(item.type)) {
        throw badRequest(`This credential cannot write entities of type '${item.type}'`);
      }
    }
    const updates = items.map(({ id, type, attrs, location }) => ({ id, type, attrs: attrs || {}, ...(location !== undefined && { location }) }));
    await broker.ingest.enqueue(req.ctx.tenant, updates, { allowedTypes: req.ctx.allowedTypes });
    res.status(202).json({ accepted: updates.length });
  }));

  router.get('/ingest/stats', read, (req, res) => res.json(broker.ingest.stats));

  return router;
};
