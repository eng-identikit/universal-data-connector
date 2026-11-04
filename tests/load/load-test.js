#!/usr/bin/env node

/**
 * Broker load test: seeds entities, then sustains a target update rate through
 * /ingest while measuring how the broker keeps up.
 *
 *   node tests/load/load-test.js --password <admin password> [options]
 *
 *   --base        API base (default http://localhost:8080/api/v2, the UI nginx)
 *   --entities    entities to create (default 100000), spread over 100 types
 *   --rate        updates per second (default 10000)
 *   --duration    seconds of sustained load (default 60)
 *   --subs        subscriptions (default 100): subscription i watches type i % 100
 *                 with filter "seq % 10 == 0" (~10% of that type's updates notify)
 *   --hook-host   host the backend uses to reach this machine's webhook receiver
 *                 (default host.docker.internal)
 *
 * Reported: sent vs applied rate (ingest backlog in JetStream), end-to-end write
 * latency (probe entity: /ingest → readable), notification latency
 * (entity ts → webhook received), totals.
 */

const http = require('http');

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1]]);
  return acc;
}, []));
const BASE = args.base || 'http://localhost:8080/api/v2';
const ENTITIES = Number(args.entities || 100000);
const RATE = Number(args.rate || 10000);
const DURATION = Number(args.duration || 60);
const SUBS = Number(args.subs || 100);
const TYPES = 100;
const BATCH = 5000;
const TENANT = `load-${Date.now().toString(36)}`;
const HOOK_HOST = args['hook-host'] || 'host.docker.internal';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const pct = (values, p) => {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
let token;

async function api(method, path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}`, 'X-Tenant': TENANT },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  if (res.status >= 400) throw new Error(`${method} ${path}: ${res.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : null;
}

const entityId = (i) => `meter-${i}`;
const entityType = (i) => `Meter${i % TYPES}`;

async function main() {
  if (!args.password) throw new Error('--password <admin password> is required');
  token = (await fetch(`${BASE}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: args.user || 'admin', password: args.password })
  }).then(r => r.json())).accessToken;
  if (!token) throw new Error('login failed');
  await api('POST', '/tenants', { id: TENANT, name: 'Load test' });
  console.log(`Tenant ${TENANT}: ${ENTITIES} entities, ${RATE} updates/s for ${DURATION}s, ${SUBS} subscriptions`);

  // ─── Webhook receiver ────────────────────────────────────────────────
  const notifLatency = [];
  let notifications = 0;
  const receiver = http.createServer((req, res) => {
    let data = '';
    req.on('data', c => { data += c; });
    req.on('end', () => {
      notifications++;
      try {
        const n = JSON.parse(data);
        for (const e of n.entities) {
          const ts = Date.parse(e.attrs?.kwh?.ts);
          if (ts) notifLatency.push(Date.now() - ts);
        }
      } catch { /* ignore */ }
      res.writeHead(204).end();
    });
  });
  await new Promise(r => receiver.listen(0, '0.0.0.0', r));
  const hook = `http://${HOOK_HOST}:${receiver.address().port}/hook`;

  // ─── Seed ────────────────────────────────────────────────────────────
  let t0 = Date.now();
  for (let i = 0; i < ENTITIES; i += BATCH) {
    const entities = [];
    for (let j = i; j < Math.min(i + BATCH, ENTITIES); j++) {
      entities.push({ id: entityId(j), type: entityType(j), attrs: { kwh: { value: 0, unit: 'kWh' }, seq: 0 } });
    }
    await api('POST', '/ingest', { entities });
  }
  for (;;) {
    const { total } = await api('GET', '/entities?limit=1');
    process.stdout.write(`\r  seeding: ${total}/${ENTITIES}`);
    if (total >= ENTITIES) break;
    await sleep(1000);
  }
  const seedS = (Date.now() - t0) / 1000;
  console.log(`\n  seeded in ${seedS.toFixed(1)}s (${Math.round(ENTITIES / seedS)} entities/s)`);

  for (let i = 0; i < SUBS; i++) {
    await api('POST', '/subscriptions', {
      name: `load-${i}`, select: { types: [`Meter${i % TYPES}`] }, trigger: { on: ['update'], attrs: ['kwh'] },
      filter: 'seq % 10 == 0', notify: { channel: 'http', url: hook, attrs: ['kwh'] }, delivery: { maxRetries: 3, backoffMs: 500 }
    });
  }
  await api('POST', '/entities', { id: 'probe', type: 'Probe', attrs: { t: 0 } });
  await sleep(2000);

  // ─── Sustained load ──────────────────────────────────────────────────
  console.log('  sustained load:');
  const tickMs = 100;
  const perTick = Math.round(RATE * tickMs / 1000);
  let sent = 0;
  let seq = 0;
  let failedRequests = 0;
  const writeLatency = [];
  const backlog = [];
  const end = Date.now() + DURATION * 1000;
  t0 = Date.now();
  let lastReport = Date.now();
  let lastSent = 0;
  const inflight = new Set();

  // Probe: write with ingest, poll until readable
  const probe = (async () => {
    while (Date.now() < end) {
      const mark = Date.now();
      await api('POST', '/ingest', { entities: [{ id: 'probe', type: 'Probe', attrs: { t: mark } }] }).catch(() => {});
      for (let k = 0; k < 200; k++) {
        const e = await api('GET', '/entities/probe').catch(() => null);
        if (e?.attrs?.t?.value === mark) {
          writeLatency.push(Date.now() - mark);
          break;
        }
        await sleep(20);
      }
      await sleep(500);
    }
  })();

  while (Date.now() < end) {
    const tickStart = Date.now();
    const entities = [];
    for (let k = 0; k < perTick; k++) {
      seq++;
      const i = Math.floor(Math.random() * ENTITIES);
      entities.push({ id: entityId(i), type: entityType(i), attrs: { kwh: Math.round(Math.random() * 1e6) / 100, seq } });
    }
    for (let k = 0; k < entities.length; k += BATCH) {
      const p = api('POST', '/ingest', { entities: entities.slice(k, k + BATCH) })
        .then(() => { sent += Math.min(BATCH, entities.length - k); })
        .catch(() => { failedRequests++; })
        .finally(() => inflight.delete(p));
      inflight.add(p);
    }
    if (Date.now() - lastReport >= 5000) {
      const status = await api('GET', '/cluster').catch(() => null);
      const pending = status?.queues?.ingest?.pending ?? null;
      backlog.push(pending);
      const rate = Math.round((sent - lastSent) / ((Date.now() - lastReport) / 1000));
      const alive = (status?.instances || []).filter(i => i.alive);
      const loop = alive.map(i => i.info?.loopDelayP99Ms ?? '?').join('/');
      const apply = alive.map(i => i.info?.ingest?.applyMsAvg ?? '?').join('/');
      console.log(`    t=${Math.round((Date.now() - t0) / 1000)}s sent ${rate}/s  ingest backlog ${pending ?? '?'} batches  notifications ${notifications}  write p95 ${pct(writeLatency, 95) ?? '-'} ms  loop p99 ${loop} ms  apply ${apply} ms`);
      lastReport = Date.now();
      lastSent = sent;
    }
    await sleep(Math.max(0, tickMs - (Date.now() - tickStart)));
  }
  await Promise.allSettled([...inflight]);
  await probe;
  const loadS = (Date.now() - t0) / 1000;

  // Drain: wait for the ingest backlog to empty
  const drainStart = Date.now();
  for (;;) {
    const status = await api('GET', '/cluster').catch(() => null);
    const pending = status?.queues?.ingest?.pending ?? 0;
    const inFlight = status?.queues?.ingest?.inFlight ?? 0;
    if (pending + inFlight === 0 || Date.now() - drainStart > 120000) break;
    await sleep(1000);
  }
  const drainS = (Date.now() - drainStart) / 1000;
  await sleep(3000);

  console.log('\n  results');
  console.log(`    updates sent          ${sent} in ${loadS.toFixed(1)}s = ${Math.round(sent / loadS)}/s (target ${RATE}/s), failed requests ${failedRequests}`);
  console.log(`    backlog drained in    ${drainS.toFixed(1)}s after the load stopped`);
  console.log(`    effective apply rate  ${Math.round(sent / (loadS + drainS))}/s (sent / (load + drain))`);
  console.log(`    write latency         p50 ${pct(writeLatency, 50)} ms  p95 ${pct(writeLatency, 95)} ms  max ${pct(writeLatency, 100)} ms  (${writeLatency.length} probes)`);
  console.log(`    notifications         ${notifications} received`);
  console.log(`    notification latency  p50 ${pct(notifLatency, 50)} ms  p95 ${pct(notifLatency, 95)} ms  max ${pct(notifLatency, 100)} ms`);

  if (args.keep !== 'true') {
    await api('DELETE', `/tenants/${TENANT}`).catch(e => console.log(`cleanup: ${e.message}`));
  }
  receiver.close();
  process.exit(0);
}

main().catch(error => {
  console.error(error.message);
  process.exit(1);
});
