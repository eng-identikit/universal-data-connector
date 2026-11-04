const express = require('express');
const { asyncHandler, expectedVersion } = require('./helpers');

/** /entities and /types. Mounted after authenticate; tenant from X-Tenant. */
module.exports = function entityRoutes({ services, tenantRole }) {
  const router = express.Router();
  const entities = services.entities;
  const read = tenantRole('viewer');
  const write = tenantRole('operator');

  const sendEntity = (res, entity, status = 200) => {
    res.status(status).set('ETag', `"${entity.version}"`).json(entity);
  };

  router.get('/types', read, asyncHandler(async (req, res) => {
    res.json({ types: await entities.types(req.ctx) });
  }));

  router.get('/entities', read, asyncHandler(async (req, res) => {
    const result = await entities.list(req.ctx, req.query);
    res.set('X-Total-Count', String(result.total)).json(result);
  }));

  router.post('/entities', write, asyncHandler(async (req, res) => {
    const entity = await entities.create(req.ctx, req.body);
    res.location(`${req.baseUrl}/entities/${encodeURIComponent(entity.id)}`);
    sendEntity(res, entity, 201);
  }));

  router.post('/entities/batch', write, asyncHandler(async (req, res) => {
    const { mode = 'upsert', entities: items } = req.body || {};
    res.json(await entities.batch(req.ctx, mode, items));
  }));

  router.get('/entities/:id', read, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.get(req.ctx, req.params.id, { attrs: req.query.attrs }));
  }));

  router.get('/entities/:id/graph', read, asyncHandler(async (req, res) => {
    res.json(await entities.graph(req.ctx, req.params.id, req.query));
  }));

  router.put('/entities/:id', write, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.replace(req.ctx, req.params.id, req.body, expectedVersion(req)));
  }));

  router.patch('/entities/:id', write, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.patch(req.ctx, req.params.id, req.body, expectedVersion(req)));
  }));

  router.delete('/entities/:id', write, asyncHandler(async (req, res) => {
    await entities.remove(req.ctx, req.params.id, expectedVersion(req));
    res.status(204).end();
  }));

  router.put('/entities/:id/attrs/:attr', write, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.setAttr(req.ctx, req.params.id, req.params.attr, req.body, { merge: false }, expectedVersion(req)));
  }));

  router.patch('/entities/:id/attrs/:attr', write, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.setAttr(req.ctx, req.params.id, req.params.attr, req.body, { merge: true }, expectedVersion(req)));
  }));

  router.delete('/entities/:id/attrs/:attr', write, asyncHandler(async (req, res) => {
    sendEntity(res, await entities.deleteAttr(req.ctx, req.params.id, req.params.attr, expectedVersion(req)));
  }));

  return router;
};
