#!/usr/bin/env node

/**
 * Broker - geo query integration tests (PostGIS, docker compose up -d db)
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const express = require('express');
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `geo-${RUN}`;
const PASSWORD = 'test-password-123';
let base;
let token;
let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log(`  ✓ ${name}`);
  } catch (error) {
    failed++;
    console.log(`  ✗ ${name}\n    Error: ${error.message}`);
  }
}

async function call(method, path, body) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}`, 'X-Tenant': TENANT },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

const ok = async (promise, status = 200) => {
  const res = await promise;
  assert.strictEqual(res.status, status, `expected ${status}, got ${res.status}: ${JSON.stringify(res.body)}`);
  return res.body;
};

const point = (lon, lat) => ({ type: 'Point', coordinates: [lon, lat] });
const ids = (r) => r.entities.map(e => e.id);

async function main() {
  const broker = new Broker();
  const app = express();
  app.use(express.json());
  app.use('/api/v1', broker.router);
  const server = app.listen(0);
  base = `http://127.0.0.1:${server.address().port}/api/v1`;
  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}`);
    process.exit(1);
  }
  const admin = await broker.services.users.create({ username: `geoadmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  token = (await fetch(`${base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json())).accessToken;
  await ok(call('POST', '/tenants', { id: TENANT }), 201);

  console.log(`\n=== BROKER GEO TESTS (PostGIS: ${broker.services.entities.geoEnabled}) ===`);

  // Milan duomo, Milan central station (~2.1 km), Turin (~126 km), no location
  await ok(call('POST', '/entities/batch', {
    mode: 'create',
    entities: [
      { id: 'duomo', type: 'Site', location: point(9.1916, 45.4642) },
      { id: 'centrale', type: 'Site', location: point(9.2042, 45.4855) },
      { id: 'torino', type: 'Site', location: point(7.6869, 45.0703) },
      { id: 'nowhere', type: 'Site' },
      { id: 'area', type: 'Zone', location: { type: 'Polygon', coordinates: [[[9.18, 45.45], [9.21, 45.45], [9.21, 45.47], [9.18, 45.47], [9.18, 45.45]]] } }
    ]
  }));

  await test('rejects malformed geometries', async () => {
    assert.strictEqual((await call('POST', '/entities', { id: 'bad1', type: 'Site', location: point(200, 10) })).status, 400);
    const open = { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1]]] };
    assert.strictEqual((await call('POST', '/entities', { id: 'bad2', type: 'Site', location: open })).status, 400);
    assert.strictEqual((await call('GET', '/entities?bbox=1,2,3')).status, 400);
    assert.strictEqual((await call('GET', '/entities?near=9,45&maxDistance=-1')).status, 400);
  });

  await test('bbox selects entities inside the box', async () => {
    const r = await ok(call('GET', '/entities?bbox=9.1,45.4,9.3,45.5&type=Site'));
    assert.deepStrictEqual(ids(r).sort(), ['centrale', 'duomo']);
  });

  await test('near orders by distance and reports it', async () => {
    const r = await ok(call('GET', '/entities?near=9.1916,45.4642&maxDistance=5000&type=Site'));
    assert.deepStrictEqual(ids(r), ['duomo', 'centrale']);
    assert.strictEqual(r.entities[0].distanceM, 0);
    assert.ok(r.entities[1].distanceM > 2000 && r.entities[1].distanceM < 2800, `distance ${r.entities[1].distanceM}`);
    const far = await ok(call('GET', '/entities?near=9.1916,45.4642&maxDistance=200000&type=Site'));
    assert.deepStrictEqual(ids(far), ['duomo', 'centrale', 'torino']);
  });

  await test('georel within / intersects with a GeoJSON geometry', async () => {
    const polygon = encodeURIComponent(JSON.stringify({ type: 'Polygon', coordinates: [[[9.18, 45.45], [9.20, 45.45], [9.20, 45.47], [9.18, 45.47], [9.18, 45.45]]] }));
    const within = await ok(call('GET', `/entities?georel=within&geometry=${polygon}`));
    assert.deepStrictEqual(ids(within), ['duomo']);
    const inter = await ok(call('GET', `/entities?georel=intersects&geometry=${polygon}`));
    assert.deepStrictEqual(ids(inter).sort(), ['area', 'duomo']);
  });

  await test('located filter and location updates move entities', async () => {
    const located = await ok(call('GET', '/entities?located=true&type=Site'));
    assert.ok(!ids(located).includes('nowhere'));
    await ok(call('PATCH', '/entities/nowhere', { location: point(9.19, 45.465) }));
    const r = await ok(call('GET', '/entities?bbox=9.1,45.4,9.3,45.5&type=Site'));
    assert.ok(ids(r).includes('nowhere'));
    await ok(call('PATCH', '/entities/nowhere', { location: null }));
    const after = await ok(call('GET', '/entities?bbox=9.1,45.4,9.3,45.5&type=Site'));
    assert.ok(!ids(after).includes('nowhere'));
  });

  await test('subscription bbox selector and preview', async () => {
    const p = await ok(call('POST', '/subscriptions/preview', { select: { types: ['Site'], bbox: [9.1, 45.4, 9.3, 45.5] } }));
    assert.deepStrictEqual(ids(p).sort(), ['centrale', 'duomo']);
  });

  await ok(call('DELETE', `/tenants/${TENANT}`), 204);
  await broker.pool.query('DELETE FROM users WHERE id = $1', [admin.id]);
  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  await broker.stop();
  server.close();
  process.exit(failed ? 1 : 0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
