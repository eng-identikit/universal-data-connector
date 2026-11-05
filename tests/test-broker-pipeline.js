#!/usr/bin/env node

/**
 * Broker - notification pipeline unit tests (no database, no NATS):
 * subscription engine semantics, delivery worker, ingest merge/buffer,
 * expression safety and NATS subject guards.
 */

const assert = require('assert');
const EventEmitter = require('events');
const SubscriptionEngine = require('../src/broker/subscriptions/SubscriptionEngine');
const DeliveryWorker = require('../src/broker/notify/DeliveryWorker');
const IngestService = require('../src/broker/ingest/IngestService');
const Channels = require('../src/broker/notify/channels');
const { validateCreate } = require('../src/broker/subscriptions/subscriptionSchema');
const { compile, ExpressionError } = require('../src/broker/expr/expression');

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

/** Engine over in-memory subscriptions; loadOne answers after 50 ms like a database. */
function makeEngine(subs) {
  const sent = [];
  const queue = { publish: async (subject, n) => { sent.push(n); } };
  const store = {
    loadAll: async () => subs,
    loadOne: (id) => sleep(50).then(() => subs.find(s => s.id === id) || null),
    addStats: async () => {}
  };
  const engine = new SubscriptionEngine({ subscriptions: store, entities: {}, queue, bus: new EventEmitter() });
  return { engine, sent, store };
}

const sub = (o) => ({
  id: 's1', tenant: 't', name: 'test', enabled: true, select: {},
  trigger: { on: ['create', 'update', 'delete'], attrs: [], deadband: {} },
  filterMode: 'level', notify: { channel: 'http', url: 'http://x' }, delivery: { maxRetries: 2, backoffMs: 1000 }, ...o
});
const ent = (v) => ({ id: 'e1', type: 'T', attrs: { temp: { type: 'float', value: v } } });
const update = (a, b) => ({ tenant: 't', op: 'update', previous: ent(a), entity: ent(b) });

(async () => {
  console.log('\nSubscription engine');

  await test('deadband fires on a slow drift (reference = first value seen)', async () => {
    const { engine, sent } = makeEngine([sub({ trigger: { on: ['update'], attrs: ['temp'], deadband: { temp: 1 } } })]);
    await engine.start();
    let v = 20;
    for (let i = 0; i < 100; i++) { await engine.handleChange(update(v, v + 0.5)); v += 0.5; }
    // 20 → 70 in steps of 0.5: one notification every 1.5
    assert.ok(sent.length >= 30 && sent.length <= 35, `got ${sent.length}`);
  });

  await test('edge + deadband: a crossing held back by the deadband is still reported', async () => {
    const { engine, sent } = makeEngine([sub({ filter: 'temp > 30', filterMode: 'edge', trigger: { on: ['update'], attrs: ['temp'], deadband: { temp: 5 } } })]);
    await engine.start();
    engine.lastNotified.set('s1|e1', { temp: 28 });
    for (const [a, b] of [[28, 31], [31, 33], [33, 34], [34, 40]]) await engine.handleChange(update(a, b));
    assert.deepStrictEqual(sent.map(n => n.entities[0].attrs.temp.value), [34]);
  });

  await test('edge mode with prev.* in the filter notifies once per crossing', async () => {
    const { engine, sent } = makeEngine([sub({ filter: 'temp - prev.temp > 5', filterMode: 'edge', trigger: { on: ['update'], attrs: [] } })]);
    await engine.start();
    for (const [a, b] of [[0, 10], [10, 20], [20, 30]]) await engine.handleChange(update(a, b));
    assert.strictEqual(sent.length, 1);
  });

  await test('throttle: no stale trailing send once the filter no longer matches', async () => {
    const { engine, sent } = makeEngine([sub({ filter: 'temp > 30', throttleMs: 100 })]);
    await engine.start();
    for (const [a, b] of [[20, 35], [35, 36], [36, 25]]) await engine.handleChange(update(a, b));
    await sleep(150);
    assert.deepStrictEqual(sent.map(n => n.entities[0].attrs.temp.value), [35]);
  });

  await test('throttle: a delete drops the pending update (no update after delete)', async () => {
    const { engine, sent } = makeEngine([sub({ throttleMs: 100 })]);
    await engine.start();
    await engine.handleChange(update(1, 2));
    await engine.handleChange(update(2, 3));
    await engine.handleChange({ tenant: 't', op: 'delete', previous: ent(3), entity: null });
    await sleep(150);
    assert.deepStrictEqual(sent.map(n => n.reason), ['update', 'delete']);
  });

  await test('throttle: trailing send skipped when the subscription was paused meanwhile', async () => {
    const { engine, sent } = makeEngine([sub({ throttleMs: 100 })]);
    await engine.start();
    await engine.handleChange(update(1, 2));
    await engine.handleChange(update(2, 3));
    engine.get('s1').enabled = false;
    await sleep(150);
    assert.strictEqual(sent.length, 1);
  });

  await test('stop() sends the pending trailing notification', async () => {
    const { engine, sent } = makeEngine([sub({ throttleMs: 10000 })]);
    await engine.start();
    await engine.handleChange(update(1, 2));
    await engine.handleChange(update(2, 3));
    await engine.stop();
    assert.deepStrictEqual(sent.map(n => n.entities[0].attrs.temp.value), [2, 3]);
  });

  await test('reload keeps the subscription available until the new version is loaded', async () => {
    const { engine } = makeEngine([sub()]);
    await engine.start();
    const reloading = engine.reload('s1');
    assert.ok(engine.get('s1'), 'subscription missing during reload');
    await reloading;
    assert.ok(engine.get('s1'));
  });

  console.log('\nDelivery worker');

  const makeWorker = (engine, { fail = false } = {}) => {
    const calls = { sent: 0, retries: [], dead: [], completed: [] };
    const channels = { send: async () => { calls.sent++; if (fail) throw new Error('endpoint down'); } };
    const store = {
      scheduleRetry: async (r) => { calls.retries.push(r); },
      addDead: async (...a) => { calls.dead.push(a); },
      completeRetry: async (id) => { calls.completed.push(id); }
    };
    return { worker: new DeliveryWorker({ queue: {}, engine, channels, subscriptions: store }), calls };
  };
  const notification = (id = 'n1') => ({ id, subscriptionId: 's1', tenant: 't', entities: [ent(1)] });

  await test('a notification is delivered while its subscription is being reloaded', async () => {
    const { engine } = makeEngine([sub()]);
    await engine.start();
    const { worker, calls } = makeWorker(engine);
    const reloading = engine.reload('s1');
    assert.deepStrictEqual(await worker.deliver(notification(), 1), { ack: true });
    await reloading;
    assert.strictEqual(calls.sent, 1);
  });

  await test('before the engine has loaded, the subscription is read from the database', async () => {
    const { engine } = makeEngine([sub()]); // not started: nothing in memory
    const { worker, calls } = makeWorker(engine);
    assert.deepStrictEqual(await worker.deliver(notification(), 1), { ack: true });
    assert.strictEqual(calls.sent, 1);
  });

  await test('deleted or paused subscriptions: notification dropped', async () => {
    const { engine } = makeEngine([sub({ id: 'other' })]);
    const { worker } = makeWorker(engine);
    assert.deepStrictEqual(await worker.deliver(notification(), 1), { drop: true });
    const paused = makeEngine([sub({ enabled: false })]);
    assert.deepStrictEqual(await makeWorker(paused.engine).worker.deliver(notification(), 1), { drop: true });
  });

  await test('a failure moves the notification to the retry table and acks the queue', async () => {
    const { engine } = makeEngine([sub()]);
    await engine.start();
    const { worker, calls } = makeWorker(engine, { fail: true });
    assert.deepStrictEqual(await worker.deliver(notification('n1'), 1), { ack: true });
    assert.strictEqual(calls.retries.length, 1);
    assert.strictEqual(calls.retries[0].attempts, 1);
    assert.ok(calls.retries[0].dueAt > new Date());
    // Endpoint now known as failing: the next notification is queued without an attempt
    assert.deepStrictEqual(await worker.deliver(notification('n2'), 1), { ack: true });
    assert.strictEqual(calls.sent, 1);
    assert.strictEqual(calls.retries[1].attempts, 0);
  });

  await test('retries: success completes the row, maxRetries exceeded goes to the DLQ', async () => {
    const { engine } = makeEngine([sub()]);
    await engine.start();
    const ok = makeWorker(engine);
    await ok.worker._retry({ id: 'n1', subscriptionId: 's1', notification: notification('n1'), attempts: 1 });
    assert.deepStrictEqual(ok.calls.completed, ['n1']);
    const bad = makeWorker(engine, { fail: true });
    await bad.worker._retry({ id: 'n2', subscriptionId: 's1', notification: notification('n2'), attempts: 2 }); // attempt 3 > maxRetries 2
    assert.strictEqual(bad.calls.dead.length, 1);
    assert.deepStrictEqual(bad.calls.completed, ['n2']);
    await bad.worker._retry({ id: 'n3', subscriptionId: 's1', notification: notification('n3'), attempts: 1 });
    assert.deepStrictEqual(bad.calls.completed, ['n2'], 'a rescheduled retry must keep its row');
  });

  console.log('\nIngest');

  await test('merge keeps the order of updates across different job keys', () => {
    const merged = IngestService.mergeJobs([
      { tenant: 't', allowedTypes: null, updates: [{ id: 'e', attrs: { a: 1 } }] },
      { tenant: 't', allowedTypes: ['X'], updates: [{ id: 'e', attrs: { a: 2 } }] },
      { tenant: 't', allowedTypes: null, updates: [{ id: 'e', attrs: { a: 3 } }] },
      { tenant: 't', allowedTypes: null, updates: [{ id: 'e', attrs: { a: 4 } }] },
      { garbage: true }
    ]);
    assert.deepStrictEqual(merged.map(b => b.updates.map(u => u.attrs.a)), [[1], [2], [3, 4]]);
  });

  await test('updates are kept for the next flush when publishing fails', async () => {
    let down = true;
    const published = [];
    const queue = { publish: async (subject, job) => { if (down) throw new Error('NATS down'); published.push(...job.updates); } };
    const ingest = new IngestService({ entities: {}, queue });
    ingest.push('t', { id: 'e1', attrs: { a: { value: 1 } } });
    await ingest.flush();
    ingest.push('t', { id: 'e1', attrs: { b: { value: 2 } } });
    down = false;
    await ingest.flush();
    assert.strictEqual(published.length, 1);
    assert.deepStrictEqual(Object.keys(published[0].attrs).sort(), ['a', 'b']);
  });

  console.log('\nExpressions and subjects');

  await test("'matches' cannot hang the event loop (ReDoS)", () => {
    const f = compile("s matches '^(a+)+$'");
    const started = Date.now();
    assert.strictEqual(f.test({ attrs: { s: { value: 'a'.repeat(30) + 'b' } } }), false);
    assert.ok(Date.now() - started < 200, `took ${Date.now() - started} ms`);
  });

  await test('backreferences are rejected', () => {
    assert.throws(() => compile(String.raw`s matches '(a)\\1'`), ExpressionError);
  });

  await test('prototype names are not attributes or functions', () => {
    const e = { id: 'x', attrs: {} };
    assert.strictEqual(compile("has('constructor')").test(e), false);
    assert.strictEqual(compile('toString == null').test(e), true);
    assert.throws(() => compile('constructor(1) > 0'), ExpressionError);
  });

  await test("NATS subjects under 'udc.' are rejected", async () => {
    const base = { name: 'n', notify: { channel: 'nats', url: 'nats://x:4222', subject: 'udc.ingest.t.0' } };
    assert.throws(() => validateCreate(base), /reserved/);
    assert.throws(() => validateCreate({ ...base, notify: { ...base.notify, subject: 'a.>' } }));
    const published = [];
    const nc = { publish: (s) => published.push(s), flush: async () => {} };
    const channels = new Channels({ natsClient: { connected: true, nc }, hub: {} });
    const s = sub({ tenant: 'udc', notify: { channel: 'nats', subject: '{tenant}.ingest' } });
    await assert.rejects(channels.send(s, { id: 'n', entities: [ent(1)] }), /reserved/);
    assert.strictEqual(published.length, 0);
  });

  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  process.exit(failed ? 1 : 0);
})();
