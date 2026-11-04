#!/usr/bin/env node

/**
 * Broker - cluster tests: two broker instances in one process sharing
 * PostgreSQL and NATS (docker compose up -d db nats). Stop other broker
 * processes on the same database first, they would join the work split.
 */

require('dotenv').config();
process.env.BROKER_CLUSTER = 'true';
process.env.BROKER_CLUSTER_TICK_MS = '400';
// The fake connector engines have no config files to share
process.env.BROKER_CONFIG_SYNC = 'false';
process.env.NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const EventEmitter = require('events');
const express = require('express');
const Broker = require('../src/broker/Broker');
const { PARTITIONS } = require('../src/broker/cluster/ClusterCoordinator');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `cluster-${RUN}`;
const PASSWORD = 'test-password-123';
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

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function waitFor(fn, timeout = 8000, message = 'condition') {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return;
    await sleep(100);
  }
  throw new Error(`Timed out waiting for ${message}`);
}

/** Connector engine stand-in: tracks which connectors are running. */
class FakeEngine extends EventEmitter {
  constructor(ids) {
    super();
    this.isRunning = true;
    this.running = new Set();
    this.gate = null;
    this.connectors = new Map(ids.map(id => [id, { connector: {}, config: { id, tenant: TENANT } }]));
    this.mappingEngine = { mappingConfigs: new Map() };
  }
  setConnectorGate(gate) { this.gate = gate; }
  isConnectorActive(id) { return this.connectors.has(id) && (!this.gate || this.gate(id)); }
  async startConnector(id) { this.running.add(id); }
  async stopConnector(id) { this.running.delete(id); }
}

async function startInstance() {
  const broker = new Broker();
  const app = express();
  app.use(express.json());
  app.use('/api/v1', broker.router);
  const server = app.listen(0);
  broker.attach(server);
  const engine = new FakeEngine(['plc-a', 'plc-b', 'plc-c']);
  broker.attachEngine(engine);
  await broker.start();
  if (!broker.ready) throw new Error(`Broker not ready: ${broker.lastError}`);
  return { broker, server, engine, base: `http://127.0.0.1:${server.address().port}/api/v1` };
}

async function main() {
  // Webhook receiver counting deliveries per notification and per subscription
  const received = [];
  const receiver = http.createServer((req, res) => {
    let data = '';
    req.on('data', c => { data += c; });
    req.on('end', () => {
      received.push(JSON.parse(data));
      res.writeHead(204).end();
    });
  });
  await new Promise(r => receiver.listen(0, '127.0.0.1', r));
  const hook = `http://127.0.0.1:${receiver.address().port}/hook`;

  const a = await startInstance();
  const b = await startInstance();
  const admin = await a.broker.services.users.create({ username: `clusteradmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  const login = await fetch(`${a.base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json());
  const call = async (inst, method, path, body) => {
    const res = await fetch(`${inst.base}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${login.accessToken}`, 'X-Tenant': TENANT },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    const text = await res.text();
    return { status: res.status, body: text ? JSON.parse(text) : null };
  };
  assert.strictEqual((await call(a, 'POST', '/tenants', { id: TENANT })).status, 201);

  const parts = (inst) => inst.broker.cluster.partitions;
  const conns = (inst) => inst.broker.cluster.connectors;

  console.log('\n=== BROKER CLUSTER TESTS ===');

  await test('subscription partitions are split without overlap', async () => {
    await waitFor(() => parts(a).size + parts(b).size === PARTITIONS && parts(a).size >= PARTITIONS / 2 - 1 && parts(b).size >= PARTITIONS / 2 - 1, 10000, 'balanced partitions');
    for (const p of parts(a)) assert.ok(!parts(b).has(p), `partition ${p} owned twice`);
  });

  await test('each connector runs on exactly one instance', async () => {
    await waitFor(() => conns(a).size + conns(b).size === 3, 10000, 'connector leases');
    for (const id of ['plc-a', 'plc-b', 'plc-c']) {
      assert.strictEqual(Number(a.engine.isConnectorActive(id)) + Number(b.engine.isConnectorActive(id)), 1, id);
      assert.strictEqual(Number(a.engine.running.has(id)) + Number(b.engine.running.has(id)), 1, `${id} running`);
    }
  });

  await test('every subscription is notified exactly once, whichever instance takes the write', async () => {
    const subs = [];
    for (let i = 0; i < 12; i++) {
      const r = await call(i % 2 ? a : b, 'POST', '/subscriptions', { name: `s${i}`, select: { types: ['Pump'] }, trigger: { on: ['update'] }, notify: { channel: 'http', url: hook } });
      assert.strictEqual(r.status, 201);
      subs.push(r.body.id);
    }
    // Both instances must know every subscription (control messages)
    await waitFor(() => subs.every(id => a.broker.engine.get(id) && b.broker.engine.get(id)), 5000, 'subscription registry');
    assert.strictEqual((await call(a, 'POST', '/entities', { id: 'p1', type: 'Pump', attrs: { rpm: 1 } })).status, 201);
    await call(b, 'PATCH', '/entities/p1', { attrs: { rpm: 2 } });
    await call(a, 'PATCH', '/entities/p1', { attrs: { rpm: 3 } });
    await waitFor(() => received.length >= 24, 8000, '24 notifications');
    await sleep(800);
    assert.strictEqual(received.length, 24);
    const perSub = {};
    for (const n of received) perSub[n.subscriptionId] = (perSub[n.subscriptionId] || 0) + 1;
    assert.ok(subs.every(id => perSub[id] === 2), JSON.stringify(perSub));
  });

  await test('cluster status lists both instances and leases', async () => {
    const s = await call(a, 'GET', '/cluster');
    assert.strictEqual(s.status, 200);
    const alive = s.body.instances.filter(i => i.alive).map(i => i.id);
    assert.ok(alive.includes(a.broker.instanceId) && alive.includes(b.broker.instanceId));
    assert.strictEqual(s.body.leases.filter(l => l.resource.startsWith('sub:') && l.valid).length, PARTITIONS);
  });

  await test('an instance joining under load causes no notification gap', async () => {
    received.length = 0;
    const c = await startInstance();
    const values = [];
    // Keep writing while partitions move from a/b to c
    for (let v = 100; v < 130; v++) {
      await call(v % 2 ? a : b, 'PATCH', '/entities/p1', { attrs: { rpm: v } });
      values.push(v);
      await sleep(150);
    }
    await waitFor(() => parts(c).size > 0, 5000, 'partitions on the new instance');
    await sleep(1500);
    const seen = {};
    for (const n of received) {
      const key = `${n.subscriptionId}|${n.entities[0].attrs.rpm.value}`;
      seen[key] = true;
    }
    const subs = [...new Set(received.map(n => n.subscriptionId))];
    assert.strictEqual(subs.length, 12);
    const missing = subs.flatMap(s => values.filter(v => !seen[`${s}|${v}`]).map(v => `${s.slice(0, 8)}:${v}`));
    assert.deepStrictEqual(missing, [], `missing notifications: ${missing.join(', ')}`);
    await c.broker.stop();
    c.server.close();
    await waitFor(() => parts(a).size + parts(b).size === PARTITIONS, 8000, 'rebalance after c leaves');
  });

  await test('failover: the surviving instance takes over partitions and connectors', async () => {
    await a.broker.stop();
    a.server.close();
    await waitFor(() => parts(b).size === PARTITIONS && conns(b).size === 3, 10000, 'takeover');
    assert.deepStrictEqual([...b.engine.running].sort(), ['plc-a', 'plc-b', 'plc-c']);
    received.length = 0;
    await call(b, 'PATCH', '/entities/p1', { attrs: { rpm: 4 } });
    await waitFor(() => received.length >= 12, 8000, 'notifications after failover');
    await sleep(500);
    assert.strictEqual(received.length, 12);
  });

  await call(b, 'DELETE', `/tenants/${TENANT}`);
  await b.broker.pool.query('DELETE FROM users WHERE id = $1', [admin.id]);
  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  await b.broker.stop();
  b.server.close();
  receiver.close();
  process.exit(failed ? 1 : 0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
