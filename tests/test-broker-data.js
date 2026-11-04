#!/usr/bin/env node

/**
 * Broker - history, ingest, connector bridge and command integration tests.
 * Requires PostgreSQL (docker compose up -d db); set NATS_URL to use JetStream.
 * The connector engine is replaced by a fake with the same surface.
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const EventEmitter = require('events');
const express = require('express');
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `data-${RUN}`;
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

async function waitFor(fn, timeout = 5000, message = 'condition') {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return;
    await new Promise(r => setTimeout(r, 100));
  }
  throw new Error(`Timed out waiting for ${message}`);
}

/** Minimal stand-in for DataConnectorEngine: connectors map, mapping configs, 'data' events. */
class FakeEngine extends EventEmitter {
  constructor() {
    super();
    this.writes = [];
    const plc = {
      writeNode: async (...args) => { this.writes.push(args); },
      writeBroken: async () => { throw new Error('PLC rejected write'); }
    };
    this.connectors = new Map([['plc1', { connector: plc, config: { id: 'plc1', tenant: TENANT, entityIdPrefix: 'plant1:' } }]]);
    this.mappingEngine = { mappingConfigs: new Map([['press 7', { measurements: [{ id: 'temp', unit: '°C' }] }]]) };
  }
}

async function main() {
  const broker = new Broker();
  const engine = new FakeEngine();
  const app = express();
  app.use(express.json());
  app.use('/api/v2', broker.router);
  const server = app.listen(0);
  base = `http://127.0.0.1:${server.address().port}/api/v2`;
  await broker.start();
  if (!broker.ready) {
    console.error(`Broker not ready: ${broker.lastError}`);
    process.exit(1);
  }
  broker.attachEngine(engine);

  const admin = await broker.services.users.create({ username: `dataadmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  token = (await fetch(`${base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json())).accessToken;
  await ok(call('POST', '/tenants', { id: TENANT }), 201);

  console.log(`\n=== BROKER DATA TESTS (${broker.nats.enabled ? 'JetStream' : 'memory queue'}) ===`);

  await test('history records value changes and aggregates them', async () => {
    const t0 = Date.now() - 60 * 60 * 1000;
    await ok(call('POST', '/entities', { id: 'h1', type: 'Sensor', attrs: { temp: { value: 10, ts: new Date(t0).toISOString() }, label: 'a' } }), 201);
    for (let i = 1; i <= 9; i++) {
      await ok(call('PATCH', '/entities/h1', { attrs: { temp: { value: 10 + i, ts: new Date(t0 + i * 60000).toISOString() } } }));
    }
    await ok(call('PATCH', '/entities/h1', { attrs: { temp: { unit: '°C' } } })); // metadata only: no new sample
    const { attributes } = await ok(call('GET', '/entities/h1/history/attributes'));
    assert.deepStrictEqual(attributes.map(a => [a.attr, a.samples, a.numeric]), [['label', 1, false], ['temp', 10, true]]);

    const from = new Date(t0 - 1000).toISOString();
    const s = await ok(call('GET', `/entities/h1/history?attrs=temp&from=${from}&maxPoints=2`));
    const buckets = s.series.temp;
    assert.ok(buckets.length >= 1 && buckets.length <= 3);
    assert.strictEqual(buckets.reduce((n, b) => n + b.samples, 0), 10);
    assert.strictEqual(Math.min(...buckets.map(b => b.min)), 10);
    assert.strictEqual(Math.max(...buckets.map(b => b.max)), 19);

    const { samples } = await ok(call('GET', `/entities/h1/history/temp/raw?from=${from}&limit=3`));
    assert.deepStrictEqual(samples.map(x => x.value), [19, 18, 17]);
  });

  await test('ingest upserts in bulk, skips unchanged entities and reports rejects', async () => {
    const entities = Array.from({ length: 1200 }, (_, i) => ({ id: `bulk-${i}`, type: 'Meter', attrs: { kwh: i } }));
    assert.deepStrictEqual(await ok(call('POST', '/ingest', { entities }), 202), { accepted: 1200 });
    await waitFor(async () => (await call('GET', '/entities?type=Meter&limit=1')).body.total === 1200, 10000, 'bulk ingest');
    const before = broker.ingest.stats.skipped;
    await ok(call('POST', '/ingest', { entities: entities.slice(0, 100) }), 202);
    await waitFor(() => broker.ingest.stats.skipped >= before + 100, 5000, 'skipped duplicates');
    const e = await ok(call('GET', '/entities/bulk-5'));
    assert.strictEqual(e.version, 1);

    const rejectedBefore = broker.ingest.stats.rejected;
    await ok(call('POST', '/ingest', { entities: [{ id: 'bulk-1', type: 'Meter', attrs: { kwh: { type: 'int', value: 'x' } } }, { id: 'bulk-2', type: 'Meter', attrs: { kwh: 99 } }] }), 202);
    await waitFor(() => broker.ingest.stats.rejected === rejectedBefore + 1, 5000, 'rejected item');
    await waitFor(async () => (await call('GET', '/entities/bulk-2')).body.attrs.kwh.value === 99, 5000, 'valid item applied');
    assert.strictEqual((await call('POST', '/ingest', { entities: [{ id: 'bad id', type: 'Meter' }] })).status, 400);
  });

  await test('connector data becomes entities with unit, quality and source', async () => {
    engine.emit('data', {
      sourceId: 'plc1',
      mappedData: {
        id: 'press 7', type: 'OPC_UA_Server',
        measurements: [{ id: 'temp', type: 'float', value: 81.5 }, { id: 'state.code', type: 'string', value: 'RUN' }],
        metadata: { timestamp: '2026-11-04T10:00:00Z', quality: 'GOOD' }
      }
    });
    await waitFor(async () => (await call('GET', '/entities/plant1:press_7')).status === 200, 5000, 'bridged entity');
    const e = await ok(call('GET', '/entities/plant1:press_7'));
    assert.strictEqual(e.type, 'OPC_UA_Server');
    assert.deepStrictEqual(e.attrs.temp, { type: 'float', value: 81.5, ts: '2026-11-04T10:00:00.000Z', source: 'plc1', quality: 'good', unit: '°C' });
    assert.strictEqual(e.attrs.state_code.value, 'RUN');
  });

  await test('commands write to the connector and track the outcome', async () => {
    await ok(call('PUT', '/entities/plant1:press_7/attrs/setpoint', {
      type: 'float', value: 80, writable: true, command: { connector: 'plc1', method: 'writeNode', args: ['ns=2;s=SP', '$value', 'Double'] }
    }));
    const cmd = await ok(call('POST', '/entities/plant1:press_7/attrs/setpoint/command', { value: 85 }));
    assert.strictEqual(cmd.status, 'succeeded');
    assert.deepStrictEqual(engine.writes.pop(), ['ns=2;s=SP', 85, 'Double']);

    await ok(call('PATCH', '/entities/plant1:press_7/attrs/setpoint', { command: { connector: 'plc1', method: 'writeBroken' } }));
    const failedCmd = await ok(call('POST', '/entities/plant1:press_7/attrs/setpoint/command', { value: 1 }));
    assert.strictEqual(failedCmd.status, 'failed');
    assert.match(failedCmd.error, /PLC rejected write/);

    await ok(call('PATCH', '/entities/plant1:press_7/attrs/setpoint', { command: { connector: 'ghost', method: 'writeNode' } }));
    const ghost = await ok(call('POST', '/entities/plant1:press_7/attrs/setpoint/command?wait=15000', { value: 1 }));
    assert.strictEqual(ghost.status, 'failed');
    assert.match(ghost.error, /not running/);

    assert.strictEqual((await call('POST', '/entities/plant1:press_7/attrs/temp/command', { value: 1 })).status, 400);
    await ok(call('PATCH', '/entities/plant1:press_7/attrs/setpoint', { command: { connector: 'plc1', method: 'exec' } }));
    assert.strictEqual((await call('POST', '/entities/plant1:press_7/attrs/setpoint/command', { value: 1 })).status, 400);

    const { commands } = await ok(call('GET', '/commands?entityId=plant1:press_7'));
    assert.deepStrictEqual(commands.map(c => c.status), ['failed', 'failed', 'succeeded']);
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
