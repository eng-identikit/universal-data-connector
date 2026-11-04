#!/usr/bin/env node

/**
 * Broker - /api/v2 integration tests.
 * Requires PostgreSQL configured through BROKER_DB_* (e.g. `docker compose up -d db`).
 * Mounts the broker in-process on a random port and works in a throw-away tenant.
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const express = require('express');
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `test-${RUN}`;
const PASSWORD = 'test-password-123';
let base;
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

/** Minimal client: call(method, path, { token, apiKey, tenant, body, ifMatch }) → { status, body, headers } */
async function call(method, path, { token, apiKey, tenant, body, ifMatch } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (apiKey) headers['X-API-Key'] = apiKey;
  if (tenant) headers['X-Tenant'] = tenant;
  if (ifMatch !== undefined) headers['If-Match'] = String(ifMatch);
  const res = await fetch(`${base}${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null, headers: res.headers };
}

const expectStatus = (res, status) => {
  assert.strictEqual(res.status, status, `expected ${status}, got ${res.status}: ${JSON.stringify(res.body)}`);
  return res.body;
};

async function login(username, password = PASSWORD) {
  return expectStatus(await call('POST', '/auth/login', { body: { username, password } }), 200);
}

async function main() {
  const broker = new Broker();
  const app = express();
  app.use(express.json());
  app.use('/api/v2', broker.router);
  const server = app.listen(0);
  base = `http://127.0.0.1:${server.address().port}/api/v2`;

  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}. Is PostgreSQL running (docker compose up -d db)?`);
    await broker.stop();
    server.close();
    process.exit(1);
  }

  const users = broker.services.users;
  const admin = await users.create({ username: `admin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  const createdUsers = [admin.id];
  let adminToken;
  let operatorToken;
  let viewerToken;
  const t = () => ({ token: adminToken, tenant: TENANT });

  console.log('\n=== BROKER API TESTS ===');

  await test('rejects unauthenticated requests and bad credentials', async () => {
    expectStatus(await call('GET', '/entities'), 401);
    expectStatus(await call('POST', '/auth/login', { body: { username: admin.username, password: 'wrong-password' } }), 401);
  });

  await test('superadmin logs in and creates a tenant', async () => {
    const session = await login(admin.username);
    adminToken = session.accessToken;
    assert.ok(session.refreshToken);
    expectStatus(await call('POST', '/tenants', { token: adminToken, body: { id: TENANT, name: 'Test tenant' } }), 201);
    expectStatus(await call('POST', '/tenants', { token: adminToken, body: { id: TENANT } }), 409);
  });

  await test('creates users with roles and lists members', async () => {
    for (const role of ['operator', 'viewer']) {
      const user = expectStatus(await call('POST', '/users', {
        token: adminToken,
        body: { username: `${role}-${RUN}`, password: PASSWORD, tenants: [{ tenant: TENANT, role }] }
      }), 201);
      createdUsers.push(user.id);
    }
    operatorToken = (await login(`operator-${RUN}`)).accessToken;
    viewerToken = (await login(`viewer-${RUN}`)).accessToken;
    const { members } = expectStatus(await call('GET', `/tenants/${TENANT}/members`, { token: adminToken }), 200);
    assert.deepStrictEqual(members.map(m => m.role).sort(), ['operator', 'viewer']);
  });

  await test('creates entities with relationships', async () => {
    const plant = expectStatus(await call('POST', '/entities', { ...t(), body: { id: 'plant1', type: 'Plant' } }), 201);
    assert.strictEqual(plant.version, 1);
    expectStatus(await call('POST', '/entities', { ...t(), body: { id: 'plant1:line3', type: 'Line', attrs: { partOf: { target: 'plant1' } } } }), 201);
    expectStatus(await call('POST', '/entities', {
      ...t(),
      body: {
        id: 'plant1:line3:press07', type: 'Press',
        attrs: { temperature: { value: 82.4, unit: '°C' }, status: 'RUN', partOf: { target: 'plant1:line3' } }
      }
    }), 201);
    expectStatus(await call('POST', '/entities', { ...t(), body: { id: 'plant1', type: 'Plant' } }), 409);
  });

  await test('operator writes, viewer only reads', async () => {
    expectStatus(await call('POST', '/entities', {
      token: operatorToken, tenant: TENANT,
      body: { id: 'plant1:line3:press08', type: 'Press', attrs: { temperature: 70.1, partOf: { target: 'plant1:line3' } } }
    }), 201);
    // Single-tenant user: X-Tenant may be omitted
    expectStatus(await call('GET', '/entities/plant1', { token: viewerToken }), 200);
    expectStatus(await call('PATCH', '/entities/plant1', { token: viewerToken, body: { attrs: { x: 1 } } }), 403);
    expectStatus(await call('GET', '/entities', { token: viewerToken, tenant: 'default' }), 403);
  });

  await test('patch merges attributes and enforces If-Match', async () => {
    const before = expectStatus(await call('GET', '/entities/plant1:line3:press07', t()), 200);
    const after = expectStatus(await call('PATCH', '/entities/plant1:line3:press07', {
      ...t(), ifMatch: before.version, body: { attrs: { temperature: { value: 85 } } }
    }), 200);
    assert.strictEqual(after.version, before.version + 1);
    assert.strictEqual(after.attrs.temperature.unit, '°C');
    assert.strictEqual(after.attrs.temperature.value, 85);
    const stale = await call('PATCH', '/entities/plant1:line3:press07', { ...t(), ifMatch: before.version, body: { attrs: { status: 'STOP' } } });
    expectStatus(stale, 409);
    assert.strictEqual(stale.body.details.actual, after.version);
  });

  await test('attribute endpoints set, merge and delete one attribute', async () => {
    let e = expectStatus(await call('PUT', '/entities/plant1/attrs/address', { ...t(), body: { value: 'Via Roma 1' } }), 200);
    assert.strictEqual(e.attrs.address.type, 'string');
    e = expectStatus(await call('PATCH', '/entities/plant1/attrs/address', { ...t(), body: { meta: { verified: true } } }), 200);
    assert.strictEqual(e.attrs.address.value, 'Via Roma 1');
    assert.deepStrictEqual(e.attrs.address.meta, { verified: true });
    e = expectStatus(await call('DELETE', '/entities/plant1/attrs/address', t()), 200);
    assert.ok(!('address' in e.attrs));
    expectStatus(await call('DELETE', '/entities/plant1/attrs/address', t()), 404);
  });

  await test('list filters by type, idPattern, projection, order and pagination', async () => {
    let r = expectStatus(await call('GET', '/entities?type=Press&attrs=temperature&orderBy=-id', t()), 200);
    assert.deepStrictEqual(r.entities.map(e => e.id), ['plant1:line3:press08', 'plant1:line3:press07']);
    assert.deepStrictEqual(Object.keys(r.entities[0].attrs), ['temperature']);
    r = expectStatus(await call('GET', '/entities?idPattern=plant1:line3*&limit=1&offset=1', t()), 200);
    assert.strictEqual(r.total, 3);
    assert.strictEqual(r.entities.length, 1);
    expectStatus(await call('GET', '/entities?orderBy=password', t()), 400);
  });

  await test('related query navigates relationships transitively', async () => {
    let r = expectStatus(await call('GET', '/entities?related=plant1', t()), 200);
    assert.deepStrictEqual(r.entities.map(e => e.id), ['plant1:line3']);
    r = expectStatus(await call('GET', '/entities?related=plant1&depth=2&type=Press', t()), 200);
    assert.strictEqual(r.total, 2);
    r = expectStatus(await call('GET', '/entities?related=plant1:line3:press07&direction=out&depth=3', t()), 200);
    assert.deepStrictEqual(r.entities.map(e => e.id), ['plant1', 'plant1:line3']);
  });

  await test('graph returns nodes and edges around an entity', async () => {
    const g = expectStatus(await call('GET', '/entities/plant1:line3/graph?depth=1', t()), 200);
    assert.strictEqual(g.edges.length, 3);
    assert.strictEqual(g.nodes.length, 4);
  });

  await test('batch upsert is atomic', async () => {
    const bad = await call('POST', '/entities/batch', {
      ...t(), body: { mode: 'upsert', entities: [{ id: 'sensor-1', type: 'Sensor', attrs: { v: 1 } }, { id: 'sensor-2', type: 'Sensor', attrs: { v: { type: 'int', value: 'x' } } }] }
    });
    expectStatus(bad, 400);
    expectStatus(await call('GET', '/entities/sensor-1', t()), 404);

    const ok = expectStatus(await call('POST', '/entities/batch', {
      ...t(), body: { mode: 'upsert', entities: [{ id: 'sensor-1', type: 'Sensor', attrs: { v: 1 } }, { id: 'plant1', attrs: { area: 1200 } }] }
    }), 200);
    assert.deepStrictEqual(ok.results.map(x => x.op).sort(), ['create', 'update']);
    const plant = expectStatus(await call('GET', '/entities/plant1', t()), 200);
    assert.strictEqual(plant.attrs.area.value, 1200);
  });

  await test('types lists entity types with counts', async () => {
    const { types } = expectStatus(await call('GET', '/types', t()), 200);
    assert.deepStrictEqual(types.find(x => x.type === 'Press'), { type: 'Press', count: 2 });
  });

  await test('API key is bound to its tenant and allowed types, and can be revoked', async () => {
    const created = expectStatus(await call('POST', '/apikeys', { ...t(), body: { name: 'line gateway', role: 'operator', allowedTypes: ['Sensor'] } }), 201);
    const apiKey = created.key;
    assert.ok(apiKey.startsWith(created.prefix));
    const r = expectStatus(await call('GET', '/entities', { apiKey }), 200);
    assert.deepStrictEqual(r.entities.map(e => e.id), ['sensor-1']);
    expectStatus(await call('GET', '/entities/plant1', { apiKey }), 404);
    expectStatus(await call('POST', '/entities', { apiKey, body: { id: 'p9', type: 'Press' } }), 403);
    expectStatus(await call('PATCH', '/entities/sensor-1', { apiKey, body: { attrs: { v: 2 } } }), 200);
    expectStatus(await call('GET', '/entities', { apiKey, tenant: 'default' }), 403);
    const { apiKeys } = expectStatus(await call('GET', '/apikeys', t()), 200);
    assert.ok(!('key' in apiKeys[0]) && !('key_hash' in apiKeys[0]));
    expectStatus(await call('DELETE', `/apikeys/${created.id}`, t()), 204);
    expectStatus(await call('GET', '/entities', { apiKey }), 401);
  });

  await test('password change revokes old tokens; refresh issues new ones', async () => {
    const session = await login(`viewer-${RUN}`);
    const refreshed = expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: session.refreshToken } }), 200);
    expectStatus(await call('GET', '/auth/me', { token: refreshed.accessToken }), 200);
    const changed = expectStatus(await call('POST', '/auth/password', {
      token: session.accessToken, body: { currentPassword: PASSWORD, newPassword: 'another-password-1' }
    }), 200);
    expectStatus(await call('GET', '/auth/me', { token: session.accessToken }), 401);
    expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: session.refreshToken } }), 401);
    expectStatus(await call('GET', '/auth/me', { token: changed.accessToken }), 200);
  });

  await test('delete entity cascades its relationships', async () => {
    expectStatus(await call('DELETE', '/entities/plant1:line3', t()), 204);
    expectStatus(await call('GET', '/entities/plant1:line3', t()), 404);
    const r = expectStatus(await call('GET', '/entities?related=plant1&depth=3', t()), 200);
    assert.strictEqual(r.total, 0);
  });

  await test('rejects malformed input', async () => {
    expectStatus(await call('GET', '/users/not-a-uuid', { token: adminToken }), 400);
    expectStatus(await call('PATCH', '/entities/plant1', { ...t(), ifMatch: 'abc', body: {} }), 400);
  });

  // Cleanup
  expectStatus(await call('DELETE', `/tenants/${TENANT}`, { token: adminToken }), 204);
  for (const id of createdUsers.slice(1)) await users.remove(id);
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
