#!/usr/bin/env node

/**
 * Broker - login sessions, access log and auth hardening.
 * Requires PostgreSQL (docker compose up -d db). With BROKER_REDIS_URL set
 * (e.g. redis://localhost:6379) the sessions live in Redis, otherwise in PostgreSQL.
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const express = require('express');
const WebSocket = require('ws');

// Short reuse interval so the test can step past it
process.env.BROKER_REFRESH_REUSE_INTERVAL_MS = '1000';
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `sess-${RUN}`;
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

async function call(method, path, { token, apiKey, tenant, body, headers: extra = {} } = {}) {
  const headers = { 'Content-Type': 'application/json', ...extra };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (apiKey) headers['X-API-Key'] = apiKey;
  if (tenant) headers['X-Tenant'] = tenant;
  const res = await fetch(`${base}${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

const expectStatus = (res, status) => {
  assert.strictEqual(res.status, status, `expected ${status}, got ${res.status}: ${JSON.stringify(res.body)}`);
  return res.body;
};

const login = async (username, password = PASSWORD, headers = {}) =>
  expectStatus(await call('POST', '/auth/login', { body: { username, password }, headers }), 200);

async function main() {
  const broker = new Broker();
  const app = express();
  app.set('trust proxy', 'loopback');
  app.use(express.json());
  app.use('/api/v1', broker.router);
  const server = app.listen(0);
  broker.attach(server);
  const port = server.address().port;
  base = `http://127.0.0.1:${port}/api/v1`;

  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}`);
    await broker.stop();
    server.close();
    process.exit(1);
  }
  console.log(`Session store: ${broker.services.sessions.kind}\n`);

  const users = broker.services.users;
  const admin = await users.create({ username: `sadmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  const alice = await users.create({ username: `alice-${RUN}`, password: PASSWORD });
  const { accessToken: adminToken } = await login(admin.username);
  expectStatus(await call('POST', '/tenants', { token: adminToken, body: { id: TENANT, name: 'Sessions test' } }), 201);
  await broker.services.tenants.setMember(TENANT, { userId: alice.id }, 'admin');

  await test('login opens a session listed for the user and the admin', async () => {
    const s = await login(alice.username, PASSWORD, { 'User-Agent': 'test-agent/1.0', 'X-Forwarded-For': '203.0.113.7' });
    assert.ok(s.sessionId);
    const mine = expectStatus(await call('GET', '/auth/sessions', { token: s.accessToken }), 200).sessions;
    const current = mine.find(x => x.id === s.sessionId);
    assert.ok(current && current.current);
    assert.strictEqual(current.userAgent, 'test-agent/1.0');
    assert.strictEqual(current.ip, '203.0.113.7');
    const all = expectStatus(await call('GET', `/sessions?userId=${alice.id}`, { token: adminToken }), 200);
    assert.strictEqual(all.store, broker.services.sessions.kind);
    assert.ok(all.sessions.some(x => x.id === s.sessionId && x.username === alice.username));
  });

  await test('logout ends the session: its access and refresh tokens stop working', async () => {
    const s = await login(alice.username);
    expectStatus(await call('POST', '/auth/logout', { token: s.accessToken }), 204);
    expectStatus(await call('GET', '/auth/me', { token: s.accessToken }), 401);
    expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: s.refreshToken } }), 401);
  });

  await test('refresh tokens rotate; reusing an old one revokes the session', async () => {
    const s = await login(alice.username);
    const r1 = expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: s.refreshToken } }), 200);
    assert.strictEqual(r1.sessionId, s.sessionId);
    expectStatus(await call('GET', '/auth/me', { token: r1.accessToken }), 200);
    // The first refresh token was already used and its reuse interval is over: treat as stolen
    await new Promise(r => setTimeout(r, 1200));
    expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: s.refreshToken } }), 401);
    expectStatus(await call('GET', '/auth/me', { token: r1.accessToken }), 401);
    expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: r1.refreshToken } }), 401);
    const { events } = expectStatus(await call('GET', `/auth-events?userId=${alice.id}&kind=refresh_reuse`, { token: adminToken }), 200);
    assert.ok(events.some(e => e.sessionId === s.sessionId));
  });

  await test('two tabs refreshing with the same token keep the session (reuse interval)', async () => {
    const s = await login(alice.username);
    const results = await Promise.all([1, 2, 3].map(() => call('POST', '/auth/refresh', { body: { refreshToken: s.refreshToken } })));
    assert.deepStrictEqual(results.map(r => r.status), [200, 200, 200]);
    for (const r of results) expectStatus(await call('GET', '/auth/me', { token: r.body.accessToken }), 200);
    // After the interval the replaced token counts as stolen and the session ends
    await new Promise(r => setTimeout(r, 1200));
    expectStatus(await call('POST', '/auth/refresh', { body: { refreshToken: s.refreshToken } }), 401);
    expectStatus(await call('GET', '/auth/me', { token: results[0].body.accessToken }), 401);
  });

  await test('an admin revokes a session and all sessions of a user; open streams are closed', async () => {
    const s1 = await login(alice.username);
    const s2 = await login(alice.username);
    const ws = new WebSocket(`ws://127.0.0.1:${port}/api/v1/stream?access_token=${s1.accessToken}&tenant=${TENANT}`);
    await new Promise((resolve, reject) => { ws.once('message', resolve); ws.once('error', reject); });
    const closed = new Promise(resolve => ws.once('close', (code) => resolve(code)));

    expectStatus(await call('DELETE', `/sessions/${s1.sessionId}`, { token: adminToken }), 204);
    assert.strictEqual(await closed, 4001);
    expectStatus(await call('GET', '/auth/me', { token: s1.accessToken }), 401);
    expectStatus(await call('GET', '/auth/me', { token: s2.accessToken }), 200);

    const { revoked } = expectStatus(await call('DELETE', `/users/${alice.id}/sessions`, { token: adminToken }), 200);
    assert.ok(revoked >= 1);
    expectStatus(await call('GET', '/auth/me', { token: s2.accessToken }), 401);
    const left = expectStatus(await call('GET', `/sessions?userId=${alice.id}`, { token: adminToken }), 200).sessions;
    assert.strictEqual(left.length, 0);
  });

  await test('users revoke their own other sessions but not others\'', async () => {
    const a = await login(alice.username);
    const b = await login(alice.username);
    expectStatus(await call('DELETE', `/auth/sessions/${b.sessionId}`, { token: a.accessToken }), 204);
    expectStatus(await call('GET', '/auth/me', { token: b.accessToken }), 401);
    const adminSession = (await login(admin.username)).sessionId;
    expectStatus(await call('DELETE', `/auth/sessions/${adminSession}`, { token: a.accessToken }), 404);
    expectStatus(await call('GET', '/sessions', { token: a.accessToken }), 403);
  });

  await test('disabling a user ends its sessions', async () => {
    const s = await login(alice.username);
    expectStatus(await call('PATCH', `/users/${alice.id}`, { token: adminToken, body: { disabled: true } }), 200);
    expectStatus(await call('GET', '/auth/me', { token: s.accessToken }), 401);
    assert.strictEqual(expectStatus(await call('GET', `/sessions?userId=${alice.id}`, { token: adminToken }), 200).sessions.length, 0);
    expectStatus(await call('PATCH', `/users/${alice.id}`, { token: adminToken, body: { disabled: false } }), 200);
  });

  await test('access log records logins and failed attempts', async () => {
    await call('POST', '/auth/login', { body: { username: alice.username, password: 'wrong-password' } });
    await login(alice.username);
    await new Promise(r => setTimeout(r, 200));
    const { events, total } = expectStatus(await call('GET', `/auth-events?userId=${alice.id}&limit=50`, { token: adminToken }), 200);
    assert.ok(total >= events.length);
    assert.ok(events.some(e => e.kind === 'login'));
    const failedLogins = expectStatus(await call('GET', '/auth-events?kind=login_failed&limit=200', { token: adminToken }), 200).events;
    assert.ok(failedLogins.some(e => e.username === alice.username));
    expectStatus(await call('GET', '/auth-events?kind=bogus', { token: adminToken }), 400);
    expectStatus(await call('GET', '/auth-events?limit=-1', { token: adminToken }), 400);
  });

  await test('parallel wrong passwords are limited (counted before the check)', async () => {
    const name = `ghost-${RUN}`;
    const results = await Promise.all(Array.from({ length: 12 }, () =>
      call('POST', '/auth/login', { body: { username: name, password: 'nope-nope-nope' } })));
    assert.ok(results.filter(r => r.status === 429).length >= 7, results.map(r => r.status).join(','));
  });

  await test('API keys cannot create keys or manage members', async () => {
    const { key } = expectStatus(await call('POST', '/apikeys', { token: adminToken, tenant: TENANT, body: { name: 'k', role: 'admin', allowedTypes: ['A'] } }), 201);
    expectStatus(await call('POST', '/apikeys', { apiKey: key, body: { name: 'escalate', role: 'admin' } }), 403);
    expectStatus(await call('POST', `/tenants/${TENANT}/members`, { apiKey: key, body: { username: admin.username, role: 'admin' } }), 403);
  });

  await test('POST /users with a bad membership creates nothing', async () => {
    const username = `half-${RUN}`;
    expectStatus(await call('POST', '/users', { token: adminToken, body: { username, password: PASSWORD, tenants: [{ tenant: TENANT, role: 'bogus' }] } }), 400);
    expectStatus(await call('POST', '/users', { token: adminToken, body: { username, password: PASSWORD, tenants: [{ tenant: 'no-such-tenant', role: 'viewer' }] } }), 404);
    const list = expectStatus(await call('GET', '/users', { token: adminToken }), 200).users;
    assert.ok(!list.some(u => u.username === username));
  });

  await test('attribute names cannot hit Object.prototype', async () => {
    expectStatus(await call('POST', '/entities', { token: adminToken, tenant: TENANT, body: { id: 'p1', type: 'A', attrs: { t: 1 } } }), 201);
    expectStatus(await call('DELETE', '/entities/p1/attrs/toString', { token: adminToken, tenant: TENANT }), 404);
    expectStatus(await call('PUT', '/entities/p1/attrs/__proto__', { token: adminToken, tenant: TENANT, body: { value: 5 } }), 400);
    expectStatus(await call('PUT', '/entities/p1/attrs/constructor', { token: adminToken, tenant: TENANT, body: { value: 5 } }), 400);
    expectStatus(await call('GET', '/commands?limit=-1', { token: adminToken, tenant: TENANT }), 400);
    expectStatus(await call('GET', '/commands?status=bogus', { token: adminToken, tenant: TENANT }), 400);
  });

  await test('type-restricted keys do not traverse hidden entities', async () => {
    const body = (id, type, to) => ({ id, type, attrs: to ? { next: { type: 'relationship', target: to } } : {} });
    for (const [id, type, to] of [['a2', 'A', null], ['b1', 'B', 'a2'], ['a1', 'A', 'b1']]) {
      expectStatus(await call('POST', '/entities', { token: adminToken, tenant: TENANT, body: body(id, type, to) }), 201);
    }
    const { key } = expectStatus(await call('POST', '/apikeys', { token: adminToken, tenant: TENANT, body: { name: 'onlyA', role: 'viewer', allowedTypes: ['A'] } }), 201);
    const graph = expectStatus(await call('GET', '/entities/a1/graph?depth=3', { apiKey: key }), 200);
    assert.deepStrictEqual(graph.nodes.map(n => n.id).sort(), ['a1']);
    const related = expectStatus(await call('GET', '/entities?related=a2&depth=3', { apiKey: key }), 200);
    assert.deepStrictEqual(related.entities.map(e => e.id), []);
    const full = expectStatus(await call('GET', '/entities?related=a2&depth=3', { token: adminToken, tenant: TENANT }), 200);
    assert.deepStrictEqual(full.entities.map(e => e.id).sort(), ['a1', 'b1']);
  });

  await test('service keys act as a superadmin on any tenant; only users create them', async () => {
    const created = expectStatus(await call('POST', '/service-keys', { token: adminToken, body: { name: `orchestrator-${RUN}` } }), 201);
    assert.strictEqual(created.scope, 'service');
    assert.strictEqual(created.tenant, null);
    const key = created.key;
    expectStatus(await call('GET', '/auth/me', { apiKey: key }), 200);
    // Any tenant through X-Tenant, users and service keys administration
    expectStatus(await call('GET', '/entities', { token: key, tenant: TENANT }), 200);
    const tenants = expectStatus(await call('GET', '/tenants', { token: key }), 200).tenants;
    assert.ok(tenants.some(t => t.id === TENANT));
    expectStatus(await call('GET', '/users', { token: key }), 200);
    expectStatus(await call('POST', '/subscriptions', {
      token: key, tenant: TENANT, body: { name: 'by service', notify: { channel: 'websocket' } }
    }), 201);
    expectStatus(await call('GET', '/entities'), 401);
    expectStatus(await call('GET', '/entities', { token: key }), 400); // X-Tenant is required
    // A service key cannot mint other service keys; revocation is immediate
    expectStatus(await call('POST', '/service-keys', { token: key, body: { name: 'more' } }), 403);
    expectStatus(await call('DELETE', `/service-keys/${created.id}`, { token: adminToken }), 204);
    expectStatus(await call('GET', '/users', { token: key }), 401);
    // A tenant key is not a service key
    const tenantKey = expectStatus(await call('POST', '/apikeys', { token: adminToken, tenant: TENANT, body: { name: 'plain', role: 'admin' } }), 201).key;
    expectStatus(await call('GET', '/users', { apiKey: tenantKey }), 403);
  });

  await test('BROKER_SERVICE_KEY provisions a service key', async () => {
    const key = `udc_boot${RUN}_${crypto.randomBytes(18).toString('base64url')}`;
    await broker.services.apiKeys.ensureBootstrapServiceKey(key);
    await broker.services.apiKeys.ensureBootstrapServiceKey(key); // idempotent
    expectStatus(await call('GET', '/users', { token: key }), 200);
    const { serviceKeys } = expectStatus(await call('GET', '/service-keys', { token: adminToken }), 200);
    const boot = serviceKeys.filter(k => k.prefix === `udc_${key.slice(4, 12)}_`);
    assert.strictEqual(boot.length, 1);
    expectStatus(await call('DELETE', `/service-keys/${boot[0].id}`, { token: adminToken }), 204);
  });

  // Cleanup
  expectStatus(await call('DELETE', `/tenants/${TENANT}`, { token: adminToken }), 204);
  await broker.revokeUserSessions(admin.id);
  await broker.revokeUserSessions(alice.id);
  await broker.pool.query('DELETE FROM users WHERE id = ANY($1)', [[admin.id, alice.id]]);
  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  await broker.stop();
  server.close();
  process.exit(failed ? 1 : 0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
