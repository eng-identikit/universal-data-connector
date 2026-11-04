#!/usr/bin/env node

/**
 * Broker - subscriptions, delivery and realtime stream integration tests.
 * Requires PostgreSQL (docker compose up -d db). Set NATS_URL to run the
 * same tests on JetStream instead of the in-memory queue.
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const express = require('express');
const WebSocket = require('ws');
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `subs-${RUN}`;
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

async function call(method, path, body, headers = {}) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}`, 'X-Tenant': TENANT, ...headers },
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

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function waitFor(fn, timeout = 4000, message = 'condition') {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return;
    await sleep(50);
  }
  throw new Error(`Timed out waiting for ${message}`);
}

// ─── Webhook receiver: /hook/<name>, optional failures per name ────────────
const received = {};
const failuresLeft = {};
const receiver = http.createServer((req, res) => {
  let data = '';
  req.on('data', c => { data += c; });
  req.on('end', () => {
    const name = req.url.split('/')[2];
    if (failuresLeft[name] > 0) {
      failuresLeft[name]--;
      res.writeHead(500).end();
      return;
    }
    (received[name] = received[name] || []).push({ body: JSON.parse(data), headers: req.headers });
    res.writeHead(204).end();
  });
});
const hook = (name) => `http://127.0.0.1:${receiver.address().port}/hook/${name}`;
const got = (name) => received[name] || [];
const values = (name, attr = 'temperature') => got(name).map(n => n.body.entities[0].attrs[attr]?.value);

async function main() {
  await new Promise(r => receiver.listen(0, '127.0.0.1', r));
  const broker = new Broker();
  const app = express();
  app.use(express.json());
  app.use('/api/v2', broker.router);
  const server = app.listen(0);
  broker.attach(server);
  const port = server.address().port;
  base = `http://127.0.0.1:${port}/api/v2`;

  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}`);
    process.exit(1);
  }
  const admin = await broker.services.users.create({ username: `subadmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  const login = await fetch(`${base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json());
  token = login.accessToken;
  await ok(call('POST', '/tenants', { id: TENANT, name: 'Subscriptions test' }), 201);

  const createSub = (body) => ok(call('POST', '/subscriptions', body), 201);
  const patch = (id, attrs) => ok(call('PATCH', `/entities/${id}`, { attrs }));

  console.log(`\n=== BROKER SUBSCRIPTION TESTS (${broker.nats.enabled ? 'JetStream' : 'memory queue'}) ===`);

  await test('rejects invalid subscriptions', async () => {
    const r = await call('POST', '/subscriptions', { name: 'bad', filter: 'temperature >', notify: { channel: 'http', url: hook('x') } });
    assert.strictEqual(r.status, 400);
    assert.match(r.body.message, /filter/);
    const v = await ok(call('POST', '/expressions/validate', { expression: 'temperature > 1 && has("alarm")' }));
    assert.deepStrictEqual(v, { valid: true, attrs: ['temperature', 'alarm'] });
  });

  await test('level filter, watched attributes and deadband', async () => {
    await createSub({
      name: 'hot sensors', select: { types: ['Sensor'] },
      trigger: { attrs: ['temperature'], deadband: { temperature: 1 } },
      filter: 'temperature > 20', notify: { channel: 'http', url: hook('level'), headers: { 'X-Token': 'secret' } }
    });
    await ok(call('POST', '/entities', { id: 's1', type: 'Sensor', attrs: { temperature: 25, humidity: 40 } }), 201);
    await waitFor(() => got('level').length === 1, 4000, 'create notification');
    assert.strictEqual(got('level')[0].body.reason, 'create');
    assert.strictEqual(got('level')[0].headers['x-token'], 'secret');
    await patch('s1', { temperature: 25.5 }); // within deadband
    await patch('s1', { humidity: 60 }); // not watched
    await patch('s1', { temperature: 10 }); // filter false
    await patch('s1', { temperature: 27 });
    await waitFor(() => got('level').length === 2, 4000, 'update notification');
    await sleep(300);
    assert.deepStrictEqual(values('level'), [25, 27]);
  });

  await test("edge filter notifies only when the condition becomes true", async () => {
    await createSub({ name: 'edge', select: { ids: ['s1'] }, filter: 'temperature > 50', filterMode: 'edge', notify: { channel: 'http', url: hook('edge') } });
    for (const t of [60, 70, 40, 55]) await patch('s1', { temperature: t });
    await waitFor(() => got('edge').length === 2, 4000, 'edge notifications');
    await sleep(300);
    assert.deepStrictEqual(values('edge'), [60, 55]);
  });

  await test('throttle sends the first change and the latest state at window end', async () => {
    await createSub({ name: 'throttled', select: { ids: ['s1'] }, throttleMs: 600, notify: { channel: 'http', url: hook('throttle') } });
    for (const t of [1, 2, 3, 4, 5]) await patch('s1', { temperature: t });
    await waitFor(() => got('throttle').length === 2, 3000, 'trailing notification');
    assert.deepStrictEqual(values('throttle'), [1, 5]);
  });

  await test('retries failed deliveries with backoff', async () => {
    failuresLeft.retry = 2;
    const sub = await createSub({ name: 'retry', select: { ids: ['s1'] }, delivery: { maxRetries: 5, backoffMs: 100 }, notify: { channel: 'http', url: hook('retry') } });
    await patch('s1', { temperature: 33 });
    await waitFor(() => got('retry').length === 1, 6000, 'delivery after retries');
    await broker.engine.flushStats();
    const s = await ok(call('GET', `/subscriptions/${sub.id}`));
    assert.strictEqual(s.stats.delivered, 1);
    assert.strictEqual(s.stats.failedAttempts, 2);
  });

  await test('dead-letter queue, replay and purge', async () => {
    failuresLeft.dead = 1000;
    const sub = await createSub({ name: 'dead', select: { ids: ['s1'] }, delivery: { maxRetries: 1, backoffMs: 100 }, notify: { channel: 'http', url: hook('dead') } });
    await patch('s1', { temperature: 34 });
    await waitFor(async () => (await call('GET', `/subscriptions/${sub.id}/dlq`)).body.items.length === 1, 6000, 'DLQ entry');
    const { items } = await ok(call('GET', `/subscriptions/${sub.id}/dlq`));
    assert.match(items[0].error, /HTTP 500/);
    assert.strictEqual(items[0].attempts, 2);
    failuresLeft.dead = 0;
    assert.deepStrictEqual(await ok(call('POST', `/subscriptions/${sub.id}/dlq/replay`, {})), { replayed: 1 });
    await waitFor(() => got('dead').length === 1, 4000, 'replayed delivery');
    assert.strictEqual((await ok(call('GET', `/subscriptions/${sub.id}/dlq`))).items.length, 0);
  });

  await test('related selector follows relationships, including new ones', async () => {
    await ok(call('POST', '/entities', { id: 'line1', type: 'Line' }), 201);
    await ok(call('POST', '/entities', { id: 'm1', type: 'Machine', attrs: { partOf: { target: 'line1' } } }), 201);
    await createSub({ name: 'line1', select: { related: { id: 'line1', depth: 2 } }, trigger: { on: ['update'] }, notify: { channel: 'http', url: hook('related') } });
    await ok(call('POST', '/entities', { id: 's2', type: 'Sensor', attrs: { temperature: 1, partOf: { target: 'm1' } } }), 201);
    await patch('s2', { temperature: 2 });
    await patch('s1', { temperature: 99 }); // not related
    await waitFor(() => got('related').length === 1, 4000, 'related notification');
    await sleep(300);
    assert.deepStrictEqual(got('related').map(n => n.body.entities[0].id), ['s2']);
  });

  await test('notification projection and values format', async () => {
    await createSub({ name: 'values', select: { ids: ['s2'] }, notify: { channel: 'http', url: hook('values'), attrs: ['temperature'], format: 'values' } });
    await patch('s2', { temperature: 3 });
    await waitFor(() => got('values').length === 1, 4000, 'values notification');
    assert.deepStrictEqual(got('values')[0].body.entities[0], { id: 's2', type: 'Sensor', temperature: 3 });
  });

  await test('periodic subscription sends current state', async () => {
    const sub = await createSub({ name: 'periodic', select: { types: ['Sensor'] }, trigger: { on: ['delete'] }, periodicMs: 1000, notify: { channel: 'http', url: hook('periodic') } });
    await waitFor(() => got('periodic').length >= 1, 3000, 'periodic notification');
    const n = got('periodic')[0].body;
    assert.strictEqual(n.reason, 'periodic');
    assert.deepStrictEqual(n.entities.map(e => e.id).sort(), ['s1', 's2']);
    await ok(call('POST', `/subscriptions/${sub.id}/pause`));
  });

  await test('pause stops notifications, resume restarts them', async () => {
    const sub = await createSub({ name: 'pausable', select: { ids: ['s2'] }, notify: { channel: 'http', url: hook('pause') } });
    await ok(call('POST', `/subscriptions/${sub.id}/pause`));
    await patch('s2', { temperature: 4 });
    await sleep(300);
    assert.strictEqual(got('pause').length, 0);
    await ok(call('POST', `/subscriptions/${sub.id}/resume`));
    await patch('s2', { temperature: 5 });
    await waitFor(() => got('pause').length === 1, 4000, 'notification after resume');
  });

  await test('test endpoint and preview', async () => {
    const sub = await createSub({ name: 'tester', select: { ids: ['s1'] }, notify: { channel: 'http', url: hook('tester') } });
    const r = await ok(call('POST', `/subscriptions/${sub.id}/test`, {}));
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.entityId, 's1');
    assert.strictEqual(got('tester')[0].body.reason, 'test');
    const bad = await createSub({ name: 'broken', notify: { channel: 'http', url: 'http://127.0.0.1:1/nothing' } });
    const r2 = await ok(call('POST', `/subscriptions/${bad.id}/test`, {}));
    assert.strictEqual(r2.ok, false);
    const p = await ok(call('POST', '/subscriptions/preview', { select: { types: ['Sensor'] }, filter: 'temperature > 50' }));
    assert.deepStrictEqual(p.entities.map(e => e.id), ['s1']);
  });

  await test('stream delivers entity changes and websocket-channel notifications', async () => {
    const wsSub = await createSub({ name: 'ws', select: { ids: ['s1'] }, notify: { channel: 'websocket' } });
    const ws = new WebSocket(`ws://127.0.0.1:${port}/api/v2/stream?access_token=${token}&tenant=${TENANT}`);
    const messages = [];
    ws.on('message', d => messages.push(JSON.parse(d)));
    await waitFor(() => messages.some(m => m.type === 'welcome'), 3000, 'welcome');
    ws.send(JSON.stringify({ type: 'subscribe', id: 'live', entities: { types: ['Sensor'], idPattern: 's*' } }));
    ws.send(JSON.stringify({ type: 'subscribe', id: 'notif', subscription: wsSub.id }));
    await waitFor(() => messages.filter(m => m.type === 'subscribed').length === 2, 3000, 'subscribed');
    await patch('s1', { temperature: 42 });
    await waitFor(() => messages.some(m => m.type === 'entity') && messages.some(m => m.type === 'notification'), 4000, 'stream messages');
    const e = messages.find(m => m.type === 'entity');
    assert.strictEqual(e.sub, 'live');
    assert.strictEqual(e.entity.attrs.temperature.value, 42);
    assert.strictEqual(messages.find(m => m.type === 'notification').notification.subscriptionId, wsSub.id);
    ws.close();

    const denied = new WebSocket(`ws://127.0.0.1:${port}/api/v2/stream?access_token=bad`);
    const code = await new Promise(r => denied.on('close', c => r(c)));
    assert.strictEqual(code, 4001);
  });

  if (process.env.TEST_MQTT_URL) {
    await test('mqtt channel publishes per entity on expanded topics', async () => {
      const mqtt = require('mqtt');
      const client = mqtt.connect(process.env.TEST_MQTT_URL);
      const msgs = [];
      await new Promise((resolve, reject) => { client.once('connect', resolve); client.once('error', reject); });
      await new Promise(r => client.subscribe(`udc-test/${RUN}/#`, { qos: 1 }, r));
      client.on('message', (topic, payload) => msgs.push({ topic, body: JSON.parse(payload) }));
      await createSub({
        name: 'mqtt', select: { types: ['Sensor'] },
        notify: { channel: 'mqtt', url: process.env.TEST_MQTT_URL, topic: `udc-test/${RUN}/{entityType}/{entityId}`, perEntity: true, format: 'values' }
      });
      await patch('s2', { temperature: 77 });
      await waitFor(() => msgs.length === 1, 5000, 'mqtt message');
      assert.strictEqual(msgs[0].topic, `udc-test/${RUN}/Sensor/s2`);
      assert.strictEqual(msgs[0].body.entities[0].temperature, 77);
      client.end(true);
    });
  }

  await test('mqtt/nats without notify.url need a default broker', async () => {
    const saved = { mqtt: process.env.BROKER_MQTT_URL, legacy: process.env.MQTT_URL };
    delete process.env.BROKER_MQTT_URL;
    delete process.env.MQTT_URL;
    try {
      const r = await call('POST', '/subscriptions', { name: 'no-broker', select: { ids: ['s2'] }, notify: { channel: 'mqtt', topic: 'x/{entityId}' } });
      assert.strictEqual(r.status, 400);
      assert.match(r.body.message, /no default MQTT broker/);
      process.env.BROKER_MQTT_URL = 'mqtt://localhost:1883';
      const health = await fetch(`${base}/health`).then(res => res.json());
      assert.deepStrictEqual(health.defaultBrokers, { mqtt: true, nats: broker.nats.enabled });
    } finally {
      if (saved.mqtt === undefined) delete process.env.BROKER_MQTT_URL; else process.env.BROKER_MQTT_URL = saved.mqtt;
      if (saved.legacy !== undefined) process.env.MQTT_URL = saved.legacy;
    }
  });

  if (broker.nats.enabled) {
    await test('nats channel publishes on the subject', async () => {
      const msgs = [];
      const sub = broker.nats.nc.subscribe(`udctest.${RUN}.>`, { callback: (err, m) => msgs.push({ subject: m.subject, body: broker.nats.decode(m.data) }) });
      await createSub({ name: 'nats', select: { ids: ['s2'] }, notify: { channel: 'nats', subject: `udctest.${RUN}.{entityId}`, perEntity: true } });
      await patch('s2', { temperature: 78 });
      await waitFor(() => msgs.length === 1, 5000, 'nats message');
      assert.strictEqual(msgs[0].subject, `udctest.${RUN}.s2`);
      sub.unsubscribe();
    });
  }

  await test('viewers see secret headers masked', async () => {
    const viewer = await broker.services.users.create({ username: `subviewer-${RUN}`, password: PASSWORD });
    await broker.services.tenants.setMember(TENANT, { userId: viewer.id }, 'viewer');
    const vt = await fetch(`${base}/auth/login`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: viewer.username, password: PASSWORD })
    }).then(r => r.json());
    const { subscriptions } = (await call('GET', '/subscriptions', undefined, { Authorization: `Bearer ${vt.accessToken}` })).body;
    assert.strictEqual(subscriptions.find(s => s.name === 'hot sensors').notify.headers['X-Token'], '••••');
    await broker.services.users.remove(viewer.id);
  });

  // Cleanup
  await ok(call('DELETE', `/tenants/${TENANT}`), 204);
  await broker.pool.query('DELETE FROM users WHERE id = $1', [admin.id]);
  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  await broker.stop();
  server.close();
  receiver.close();
  process.exit(failed ? 1 : 0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
