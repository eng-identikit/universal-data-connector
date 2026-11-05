#!/usr/bin/env node

/**
 * Broker - output transformers on subscriptions (library file, inline
 * expressions, preview, limits). Requires PostgreSQL (docker compose up -d db).
 * The library is a temporary copy of config/transformers.json.
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const express = require('express');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `tr-${RUN}`;
const PASSWORD = 'test-password-123';
const LIBRARY = path.join(os.tmpdir(), `udc-transformers-${RUN}.json`);
fs.copyFileSync(path.join(__dirname, '..', 'config', 'transformers.json'), LIBRARY);
process.env.BROKER_TRANSFORMERS_FILE = LIBRARY;
const Broker = require('../src/broker/Broker');

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

async function call(method, p, body) {
  const res = await fetch(`${base}${p}`, {
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
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function waitFor(fn, timeout = 5000, message = 'condition') {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return;
    await sleep(50);
  }
  throw new Error(`Timed out waiting for ${message}`);
}

// Webhook receiver keeping the raw body and content type
const received = {};
const receiver = http.createServer((req, res) => {
  let data = '';
  req.on('data', c => { data += c; });
  req.on('end', () => {
    const name = req.url.split('/')[2];
    (received[name] = received[name] || []).push({ raw: data, type: req.headers['content-type'] });
    res.writeHead(204).end();
  });
});
const hook = (name) => `http://127.0.0.1:${receiver.address().port}/hook/${name}`;
const got = (name) => received[name] || [];

async function main() {
  await new Promise(r => receiver.listen(0, '127.0.0.1', r));
  const broker = new Broker();
  const app = express();
  app.use(express.json());
  app.use('/api/v1', broker.router);
  const server = app.listen(0);
  broker.attach(server);
  base = `http://127.0.0.1:${server.address().port}/api/v1`;
  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}`);
    process.exit(1);
  }
  const admin = await broker.services.users.create({ username: `tradmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  token = (await fetch(`${base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json())).accessToken;
  await ok(call('POST', '/tenants', { id: TENANT, name: 'Transformers test' }), 201);
  await ok(call('POST', '/entities', { id: 'press7', type: 'Press', attrs: { temperature: { value: 20, unit: 'Cel' }, status: 'RUN' } }), 201);
  const patch = (id, attrs) => ok(call('PATCH', `/entities/${encodeURIComponent(id)}`, { attrs }));
  const createSub = (body) => ok(call('POST', '/subscriptions', body), 201);

  console.log(`\n=== BROKER TRANSFORMER TESTS (${broker.nats.enabled ? 'JetStream' : 'memory queue'}) ===`);

  await test('the library is read from the file', async () => {
    const { transformers } = await ok(call('GET', '/transformers'));
    assert.deepStrictEqual(transformers.map(t => t.id), ['csv', 'envelope', 'flat-values', 'influx-line']);
  });

  await test('library transformer: JSON output', async () => {
    await createSub({ name: 'flat', select: { ids: ['press7'] }, trigger: { attrs: ['temperature'] }, notify: { channel: 'http', url: hook('flat'), transformer: 'flat-values' } });
    await patch('press7', { temperature: 21 });
    await waitFor(() => got('flat').length === 1, 5000, 'flat notification');
    assert.match(got('flat')[0].type, /application\/json/);
    assert.deepStrictEqual(JSON.parse(got('flat')[0].raw), [{ id: 'press7', type: 'Press', temperature: 21, status: 'RUN' }]);
  });

  await test('library transformer: text output (InfluxDB line protocol)', async () => {
    await createSub({ name: 'influx', select: { ids: ['press7'] }, trigger: { attrs: ['temperature'] }, notify: { channel: 'http', url: hook('influx'), transformer: 'influx-line' } });
    await patch('press7', { temperature: 22.5 });
    await waitFor(() => got('influx').length === 1, 5000, 'influx notification');
    assert.match(got('influx')[0].type, /text\/plain/);
    assert.match(got('influx')[0].raw, /^Press,id=press7 temperature=22.5 \d{19}$/);
  });

  await test('inline expression; returning nothing skips the message', async () => {
    await createSub({
      name: 'inline', select: { ids: ['press7'] }, trigger: { attrs: ['temperature'] },
      notify: { channel: 'http', url: hook('inline'), transform: { expression: 'entity.attrs.temperature.value > 30 ? { "alarm": entity.id, "t": entity.attrs.temperature.value }' } }
    });
    await patch('press7', { temperature: 25 });
    await sleep(500);
    assert.strictEqual(got('inline').length, 0);
    await patch('press7', { temperature: 35 });
    await waitFor(() => got('inline').length === 1, 5000, 'inline notification');
    assert.deepStrictEqual(JSON.parse(got('inline')[0].raw), { alarm: 'press7', t: 35 });
  });

  await test('validation: unknown transformer, bad expression, both at once', async () => {
    const bad = async (notify, re) => {
      const r = await call('POST', '/subscriptions', { name: 'bad', notify: { channel: 'http', url: hook('x'), ...notify } });
      assert.strictEqual(r.status, 400, JSON.stringify(r.body));
      assert.match(r.body.message, re);
    };
    await bad({ transformer: 'nope' }, /not found/);
    await bad({ transform: { expression: '{ "a": }' } }, /notify.transform/);
    await bad({ transformer: 'csv', transform: { expression: '$' } }, /either/);
  });

  await test('preview runs on real entities and reports errors', async () => {
    const p = await ok(call('POST', '/transformers/preview', { transformer: 'csv', entityIds: ['press7'] }));
    assert.strictEqual(p.messages.length, 1);
    assert.match(p.messages[0].payload, /^press7,Press,temperature,35,Cel,/m);
    const inline = await ok(call('POST', '/transformers/preview', { expression: '$count(entities)', types: ['Press'], notify: { perEntity: true } }));
    assert.strictEqual(inline.messages[0].payload, '1');
    const loop = await call('POST', '/transformers/preview', { expression: '($f := function($x){ $f($x + 1) }; $f(0))', entityIds: ['press7'] });
    assert.strictEqual(loop.status, 400);
    assert.match(loop.body.message, /recursion|longer than/);
    const redos = await ok(call('POST', '/transformers/preview', { expression: "$contains('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab', /^(a+)+$/)", entityIds: ['press7'] }));
    assert.strictEqual(redos.messages[0].payload, 'false');
  });

  await test('library CRUD (superadmin); a transformer in use cannot be deleted', async () => {
    await ok(call('PUT', '/transformers/ids-only', { name: 'Ids', expression: 'entities.id' }));
    const r = await call('PUT', '/transformers/Bad Id', { expression: '$' });
    assert.strictEqual(r.status, 400);
    const conflict = await call('DELETE', '/transformers/flat-values');
    assert.strictEqual(conflict.status, 409);
    assert.strictEqual(conflict.body.details.subscriptions[0].name, 'flat');
    await ok(call('DELETE', '/transformers/ids-only'), 204);
    const ids = JSON.parse(fs.readFileSync(LIBRARY, 'utf8')).transformers.map(t => t.id);
    assert.ok(!ids.includes('ids-only'));
  });

  await test('hand edits of the file are picked up', async () => {
    const doc = JSON.parse(fs.readFileSync(LIBRARY, 'utf8'));
    doc.transformers.push({ id: 'hand-made', name: 'Hand made', expression: '{"n": $count(entities)}' });
    fs.writeFileSync(LIBRARY, JSON.stringify(doc, null, 2));
    await waitFor(async () => (await call('GET', '/transformers/hand-made')).status === 200, 8000, 'file reload');
  });

  await test('a transformer failing at delivery ends in the DLQ with its error', async () => {
    const sub = await createSub({
      name: 'failing', select: { ids: ['press7'] }, trigger: { attrs: ['temperature'] },
      delivery: { maxRetries: 0, backoffMs: 100 },
      notify: { channel: 'http', url: hook('failing'), transform: { expression: '$error("boom")' } }
    });
    await patch('press7', { temperature: 40 });
    await waitFor(async () => (await call('GET', `/subscriptions/${sub.id}/dlq`)).body?.items?.length === 1, 8000, 'dlq entry');
    const { items: entries } = (await call('GET', `/subscriptions/${sub.id}/dlq`)).body;
    assert.match(entries[0].error, /boom/);
    assert.strictEqual(got('failing').length, 0);
  });

  await ok(call('DELETE', `/tenants/${TENANT}`), 204);
  await broker.revokeUserSessions(admin.id);
  await broker.pool.query('DELETE FROM users WHERE id = $1', [admin.id]);
  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  await broker.stop();
  server.close();
  receiver.close();
  fs.rmSync(LIBRARY, { force: true });
  process.exit(failed ? 1 : 0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
