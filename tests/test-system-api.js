#!/usr/bin/env node

/**
 * System API (/api/v1/system) tests: status, sources, data, config, mapping.
 * Self-contained: runs the routes in-process on a temporary config directory
 * with an in-memory engine and a stub caller (no database, no broker).
 */

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');

const REPO = path.resolve(__dirname, '..');
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'udc-system-api-'));
const CONFIG = path.join(TMP, 'config');

const SECRET = 'hunter2';
const httpSource = (id, enabled, url) => ({
  id, type: 'http', enabled, tenant: 'default',
  config: { url, method: 'GET', polling: { enabled: true, interval: 60000 } }
});

fs.mkdirSync(CONFIG);
const writeJson = (file, value) => fs.writeFileSync(path.join(CONFIG, file), JSON.stringify(value, null, 2));
const readJson = (file) => JSON.parse(fs.readFileSync(path.join(CONFIG, file), 'utf8'));

// Paths are resolved from the working directory when the modules load
process.chdir(TMP);
process.env.UDC_RAW_STORAGE = 'memory';
process.env.LOG_LEVEL = process.env.LOG_LEVEL || 'error';

const express = require('express');
const configManager = require(path.join(REPO, 'src/config/ConfigManager'));
const DataConnectorEngine = require(path.join(REPO, 'src/core/DataConnectorEngine'));
const systemRoutes = require(path.join(REPO, 'src/api'));
const mappingRoutes = require(path.join(REPO, 'src/api/routes/mapping'));
const { MASK, maskSecrets, restoreMasked } = require(path.join(REPO, 'src/api/secrets'));

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

async function main() {
  // Polled by the enabled http source
  const target = http.createServer((req, res) => res.end(JSON.stringify({ temperature: 21 })));
  await new Promise(r => target.listen(0, r));
  const targetUrl = `http://localhost:${target.address().port}/data`;

  writeJson('sources.json', {
    sources: [
      httpSource('poller', true, targetUrl),
      {
        ...httpSource('secured', false, 'https://bob:topsecret@api.example.com/x'),
        config: {
          url: 'https://bob:topsecret@api.example.com/x', method: 'GET',
          authentication: { type: 'basic', username: 'bob', password: SECRET }
        }
      },
      {
        id: 'keyed', type: 'http', enabled: false,
        config: { url: 'https://api.example.com', authentication: { type: 'apikey', key: 'X-API-Key', value: 'k-123' } }
      }
    ]
  });
  writeJson('mapping.json', { transport: { mqtt: { enabled: false } }, outputFormats: { json: { enabled: true } }, devices: [] });
  writeJson('storage.json', { storage: { type: 'redis', config: { host: 'redis', port: 6379, password: 'redispw' } } });

  await configManager.initialize();
  const engine = new DataConnectorEngine({ type: 'memory', config: {} });
  await engine.initialize();
  await engine.start();
  global.connector = { getEngine: () => engine };
  mappingRoutes.initialize(engine);

  const app = express();
  app.use(express.json());
  // Stand-in for the broker's system guard: X-Test-Role picks the caller
  app.use((req, res, next) => {
    req.auth = req.get('x-test-role') === 'superadmin'
      ? { kind: 'user', isSuperadmin: true }
      : { kind: 'user', isSuperadmin: false };
    next();
  });
  app.set('serverInstance', {
    getEngine: () => engine,
    reloadSourcesConfiguration: (sources) => engine.reloadSourcesConfiguration(sources),
    reloadStorageConfiguration: (storage) => engine.reloadStorageConfiguration(storage)
  });
  app.use('/api/v1/system', systemRoutes);
  const server = app.listen(0);
  await new Promise(r => server.once('listening', r));
  const base = `http://localhost:${server.address().port}/api/v1/system`;

  const call = async (method, p, body, role = 'superadmin') => {
    const res = await fetch(`${base}${p}`, {
      method,
      headers: { 'Content-Type': 'application/json', 'X-Test-Role': role },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch { /* not JSON */ }
    return { status: res.status, body: json, text };
  };

  console.log('\n=== SYSTEM API TESTS ===');

  await test('status, health and stats answer 200; connectors is an array', async () => {
    const status = await call('GET', '/status');
    assert.strictEqual(status.status, 200);
    assert.ok(Array.isArray(status.body.connectors));
    assert.ok(status.body.connectors.some(c => c.id === 'poller' && c.runtime));
    assert.strictEqual((await call('GET', '/status/health')).status, 200);
    assert.strictEqual((await call('GET', '/status/stats')).status, 200);
  });

  await test('mapping health and JSON export work; NGSI-LD export is gone', async () => {
    const health = await call('GET', '/mapping/health');
    assert.strictEqual(health.status, 200, health.text);
    assert.strictEqual(typeof health.body.registeredMappers, 'number');
    assert.strictEqual((await call('GET', '/mapping/export/json')).status, 200);
    assert.strictEqual((await call('GET', '/mapping/export/ngsi-ld')).status, 404);
    assert.strictEqual((await call('GET', '/history/status')).status, 404);
  });

  await test('limits are clamped to 1..1000', async () => {
    assert.strictEqual((await call('GET', '/data/latest?limit=-3')).body.limit, 100);
    assert.strictEqual((await call('GET', '/data/latest?limit=99999999999')).body.limit, 1000);
    assert.strictEqual((await call('GET', '/sources/poller/data?limit=-1')).body.limit, 100);
  });

  await test('search with repeated q and ranges with invalid dates answer 400', async () => {
    assert.strictEqual((await call('GET', '/data/search?q=a&q=b')).status, 400);
    assert.strictEqual((await call('GET', '/data/range?startTime=nope&endTime=2026-01-01')).status, 400);
    assert.strictEqual((await call('GET', '/data/source/poller?startTime=2026-02-01&endTime=2026-01-01')).status, 400);
    assert.strictEqual((await call('GET', '/data/export?format=xml')).status, 400);
  });

  await test('engine status reports the storage actually in use', async () => {
    const r = await call('GET', '/config/engine/status');
    assert.strictEqual(r.body.engine.dataStore.type, 'memory');
  });

  await test('storage config reports the UDC_RAW_STORAGE override and masks passwords', async () => {
    const viewer = await call('GET', '/config/storage', undefined, 'viewer');
    assert.strictEqual(viewer.body.storage.overriddenBy, 'UDC_RAW_STORAGE=memory');
    assert.strictEqual(viewer.body.storage.current.config.password, MASK);
    const admin = await call('GET', '/config/storage');
    assert.strictEqual(admin.body.storage.current.config.password, 'redispw');
  });

  await test('source runtime status is the one of the requested source', async () => {
    const r = await call('GET', '/sources/poller');
    assert.ok(r.body.source.runtime && typeof r.body.source.runtime.status === 'string');
    assert.strictEqual(r.body.source.runtime.poller, undefined);
    const missing = await call('GET', '/sources/secured/status');
    assert.strictEqual(missing.body.status, 'not_initialized');
  });

  await test('secrets are masked for non-superadmins', async () => {
    const viewer = (await call('GET', '/config/sources', undefined, 'viewer')).body.sources;
    const secured = viewer.find(s => s.id === 'secured');
    assert.strictEqual(secured.config.authentication.password, MASK);
    assert.strictEqual(secured.config.url, `https://bob:${MASK}@api.example.com/x`);
    assert.strictEqual(viewer.find(s => s.id === 'keyed').config.authentication.value, MASK);
    const listed = (await call('GET', '/sources', undefined, 'viewer')).body.sources;
    assert.strictEqual(listed.find(s => s.id === 'secured').config.authentication.password, MASK);
    const admin = (await call('GET', '/config/sources')).body.sources;
    assert.strictEqual(admin.find(s => s.id === 'secured').config.authentication.password, SECRET);
  });

  await test('a masked configuration sent back keeps the stored secrets', async () => {
    const masked = maskSecrets(configManager.getSourceById('secured'));
    masked.name = 'Secured API';
    const r = await call('PUT', '/config/sources/secured', masked);
    assert.strictEqual(r.status, 200, r.text);
    const saved = readJson('sources.json').sources.find(s => s.id === 'secured');
    assert.strictEqual(saved.name, 'Secured API');
    assert.strictEqual(saved.config.authentication.password, SECRET);
    assert.strictEqual(saved.config.url, 'https://bob:topsecret@api.example.com/x');
    assert.deepStrictEqual(restoreMasked(maskSecrets({ a: [{ id: 1, token: 't' }] }), { a: [{ id: 1, token: 't' }] }), { a: [{ id: 1, token: 't' }] });
  });

  await test('the source id cannot be changed', async () => {
    const r = await call('PUT', '/config/sources/secured', { id: 'other' });
    assert.strictEqual(r.status, 400);
  });

  await test('invalid sources answer 400 and never reach sources.json', async () => {
    const before = fs.readFileSync(path.join(CONFIG, 'sources.json'), 'utf8');
    const bad = { id: 'bad', type: 'foo', config: {} };
    assert.strictEqual((await call('POST', '/config/sources', bad)).status, 400);
    assert.strictEqual((await call('POST', '/config/sources/reload', { sources: [bad] })).status, 400);
    assert.strictEqual((await call('POST', '/config/sources/configure', { sources: [bad] })).status, 400);
    const dup = httpSource('poller', false, targetUrl);
    assert.strictEqual((await call('POST', '/config/sources/reload', { sources: [dup, dup] })).status, 400);
    assert.strictEqual(fs.readFileSync(path.join(CONFIG, 'sources.json'), 'utf8'), before);
    assert.strictEqual(configManager.getSources().length, 3);
  });

  await test('validate accepts every connector type and the broker fields', async () => {
    const r = await call('POST', '/config/sources/validate', {
      id: 'plc', type: 'modbus', tenant: 'plant-a', entityIdPrefix: 'p1:', autoMapping: true, config: { host: '10.0.0.1' }
    });
    assert.strictEqual(r.status, 200, r.text);
  });

  await test('adding, updating and removing a source applies it to the engine', async () => {
    assert.strictEqual((await call('POST', '/config/sources', httpSource('extra', true, targetUrl))).status, 201);
    assert.ok(engine.connectors.has('extra'));
    const updated = await call('PUT', '/config/sources/extra', { config: { url: `${targetUrl}?v=2`, polling: { enabled: true, interval: 60000 } } });
    assert.strictEqual(updated.status, 200, updated.text);
    assert.strictEqual(engine.connectors.get('extra').config.config.url, `${targetUrl}?v=2`);
    assert.strictEqual((await call('DELETE', '/config/sources/extra')).status, 200);
    assert.ok(!engine.connectors.has('extra'));
  });

  await test('discovery reads the connector instance, configure saves and recreates it', async () => {
    const before = engine.connectors.get('poller').connector;
    before.discoveredNodes = ['ns=1;s=A'];
    const discovery = await call('GET', '/sources/poller/discovery');
    assert.strictEqual(discovery.status, 200, discovery.text);
    assert.deepStrictEqual(discovery.body.discovered.nodes, ['ns=1;s=A']);
    const r = await call('POST', '/sources/poller/configure', { nodes: ['ns=1;s=A'] });
    assert.strictEqual(r.status, 200, r.text);
    assert.deepStrictEqual(readJson('sources.json').sources.find(s => s.id === 'poller').config.nodes, ['ns=1;s=A']);
    assert.notStrictEqual(engine.connectors.get('poller').connector, before);
    assert.strictEqual((await call('POST', '/sources/poller/configure', { nodes: 'x' })).status, 400);
  });

  await test('config reload recreates and restarts the connectors', async () => {
    const before = engine.connectors.get('poller').connector;
    const r = await call('POST', '/config/reload');
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(engine.isRunning);
    assert.notStrictEqual(engine.connectors.get('poller').connector, before);
    assert.ok(engine.dataStore.cleanupInterval, 'the data store is still running');
  });

  await test('start/stop/restart answer 409 for a connector owned by another instance', async () => {
    engine.setConnectorGate(() => false);
    try {
      for (const action of ['start', 'stop', 'restart']) {
        assert.strictEqual((await call('POST', `/sources/poller/${action}`)).status, 409);
      }
    } finally {
      engine.setConnectorGate(null);
    }
  });

  await test('stopping a source updates its status', async () => {
    assert.strictEqual((await call('POST', '/sources/poller/stop')).status, 200);
    assert.strictEqual((await call('GET', '/sources/poller/status')).body.status, 'disconnected');
  });

  await test('mapping.json keeps its other sections; clearing data keeps device mappings', async () => {
    engine.mappingEngine.mappingConfigs.set('dev1', { id: 'dev1', type: 'Sensor', measurements: [] });
    await engine.mappingEngine.saveMappingConfig();
    const saved = readJson('mapping.json');
    assert.deepStrictEqual(saved.transport, { mqtt: { enabled: false } });
    assert.ok(saved.outputFormats);
    assert.strictEqual((await call('DELETE', '/mapping/entities')).status, 200);
    assert.ok(engine.mappingEngine.mappingConfigs.has('dev1'));
  });

  await engine.stop();
  server.close();
  target.close();
  process.chdir(REPO);
  fs.rmSync(TMP, { recursive: true, force: true });

  console.log(`\nPassed: ${passed}, Failed: ${failed}`);
  process.exit(failed ? 1 : 0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
