#!/usr/bin/env node

/**
 * Broker - MCP endpoint (/api/v1/mcp) with the official MCP client:
 * tools, resources, authentication, tenant and type restrictions.
 * Requires PostgreSQL (docker compose up -d db).
 */

require('dotenv').config();
const assert = require('assert');
const crypto = require('crypto');
const express = require('express');
const { Client, StreamableHTTPClientTransport } = require('@modelcontextprotocol/client');
const Broker = require('../src/broker/Broker');

const RUN = crypto.randomBytes(3).toString('hex');
const TENANT = `mcp-${RUN}`;
const OTHER = `mcp2-${RUN}`;
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

async function call(token, method, path, body, tenant = TENANT) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}`, 'X-Tenant': tenant },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  const result = { status: res.status, body: text ? JSON.parse(text) : null };
  if (res.status >= 400) throw new Error(`${method} ${path}: ${res.status} ${text}`);
  return result.body;
}

/** MCP client connected with the given headers. */
async function connect(headers) {
  const client = new Client({ name: 'udc-test', version: '1.0.0' });
  await client.connect(new StreamableHTTPClientTransport(new URL(`${base}/mcp`), { requestInit: { headers } }));
  return client;
}

const text = (result) => result.content.map(c => c.text).join('\n');

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

  const admin = await broker.services.users.create({ username: `mcpadmin-${RUN}`, password: PASSWORD, isSuperadmin: true });
  const login = await fetch(`${base}/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: admin.username, password: PASSWORD })
  }).then(r => r.json());
  const token = login.accessToken;
  await call(token, 'POST', '/tenants', { id: TENANT, name: 'MCP test' });
  await call(token, 'POST', '/tenants', { id: OTHER, name: 'MCP other' });

  // A small plant: plant ← line ← two presses, one with history; a secret entity in another tenant
  await call(token, 'POST', '/entities/batch', {
    mode: 'upsert',
    entities: [
      { id: 'plant1', type: 'Plant', location: { type: 'Point', coordinates: [9.19, 45.46] } },
      { id: 'plant1:line3', type: 'Line', attrs: { partOf: { type: 'relationship', target: 'plant1' } } },
      { id: 'plant1:line3:press07', type: 'Press', location: { type: 'Point', coordinates: [9.191, 45.461] },
        attrs: { temperature: { value: 70, unit: 'Cel' }, status: 'RUN', partOf: { type: 'relationship', target: 'plant1:line3' } } },
      { id: 'plant1:line3:press08', type: 'Press', attrs: { temperature: { value: 55, unit: 'Cel' }, status: 'STOP', partOf: { type: 'relationship', target: 'plant1:line3' } } }
    ]
  });
  for (const v of [71, 72, 90, 74]) {
    await call(token, 'PATCH', '/entities/plant1:line3:press07', { attrs: { temperature: { value: v, quality: v > 85 ? 'bad' : 'good' } } });
  }
  await call(token, 'POST', '/entities', { id: 'secret', type: 'Press', attrs: { temperature: 1 } }, OTHER);

  const viewerKey = (await call(token, 'POST', '/apikeys', { name: 'llm', role: 'viewer' })).key;
  const pressesOnly = (await call(token, 'POST', '/apikeys', { name: 'llm-press', role: 'viewer', allowedTypes: ['Press'] })).key;

  console.log('\n=== BROKER MCP TESTS ===');
  let client;

  await test('connects with an API key as bearer token and lists the tools', async () => {
    client = await connect({ Authorization: `Bearer ${viewerKey}` });
    const { tools } = await client.listTools();
    assert.deepStrictEqual(tools.map(t => t.name).sort(), [
      'udc_describe_data_model', 'udc_get_attribute_history', 'udc_get_entity',
      'udc_get_raw_samples', 'udc_get_related_entities', 'udc_search_entities'
    ]);
    assert.ok(tools.every(t => t.annotations?.readOnlyHint === true));
    assert.match(client.getInstructions() || '', /Tenant: mcp-/);
  });

  await test('describe the data model', async () => {
    const r = await client.callTool({ name: 'udc_describe_data_model', arguments: {} });
    assert.ok(!r.isError, text(r));
    assert.match(text(r), /## Press — 2 entities/);
    assert.match(text(r), /temperature: float, Cel/);
    assert.match(text(r), /partOf: relationship → e\.g\. plant1:line3/);
    const json = await client.callTool({ name: 'udc_describe_data_model', arguments: { types: ['Line'], response_format: 'json' } });
    assert.deepStrictEqual(json.structuredContent.types.map(t => t.type), ['Line']);
  });

  await test('search: by type, relationship, position and pagination', async () => {
    const byType = await client.callTool({ name: 'udc_search_entities', arguments: { types: ['Press'], response_format: 'json' } });
    assert.deepStrictEqual(byType.structuredContent.entities.map(e => e.id).sort(), ['plant1:line3:press07', 'plant1:line3:press08']);
    const inPlant = await client.callTool({ name: 'udc_search_entities', arguments: { related_to: 'plant1', depth: 3, response_format: 'json' } });
    assert.strictEqual(inPlant.structuredContent.total, 3);
    const page = await client.callTool({ name: 'udc_search_entities', arguments: { types: ['Press'], limit: 1, response_format: 'json' } });
    assert.strictEqual(page.structuredContent.has_more, true);
    assert.strictEqual(page.structuredContent.next_offset, 1);
    const md = await client.callTool({ name: 'udc_search_entities', arguments: { types: ['Press'], limit: 1 } });
    assert.match(text(md), /More results: call again with offset=1/);
    if (broker.services.entities.geoEnabled) {
      const near = await client.callTool({ name: 'udc_search_entities', arguments: { near: { lon: 9.19, lat: 45.46, max_distance_m: 1000 }, response_format: 'json' } });
      assert.strictEqual(near.structuredContent.entities[0].id, 'plant1');
    }
    const none = await client.callTool({ name: 'udc_search_entities', arguments: { types: ['Robot'] } });
    assert.match(text(none), /No entities match/);
  });

  await test('get an entity with its recorded history', async () => {
    const r = await client.callTool({ name: 'udc_get_entity', arguments: { entity_id: 'plant1:line3:press07' } });
    assert.match(text(r), /- temperature: 74 Cel/);
    assert.match(text(r), /- partOf → plant1:line3/);
    assert.match(text(r), /## Recorded history[\s\S]*- temperature: 5 samples/);
    const missing = await client.callTool({ name: 'udc_get_entity', arguments: { entity_id: 'nope' } });
    assert.strictEqual(missing.isError, true);
    assert.match(text(missing), /not found.*udc_search_entities/);
  });

  await test('related entities', async () => {
    const r = await client.callTool({ name: 'udc_get_related_entities', arguments: { entity_id: 'plant1', direction: 'in', depth: 2, response_format: 'json' } });
    assert.deepStrictEqual(r.structuredContent.nodes.map(n => n.id).sort(), ['plant1', 'plant1:line3', 'plant1:line3:press07', 'plant1:line3:press08']);
    assert.ok(r.structuredContent.edges.some(e => e.source === 'plant1:line3' && e.attr === 'partOf' && e.target === 'plant1'));
  });

  await test('attribute history and raw samples', async () => {
    const h = await client.callTool({ name: 'udc_get_attribute_history', arguments: { entity_id: 'plant1:line3:press07', attrs: ['temperature'], from: '1h' } });
    assert.ok(!h.isError, text(h));
    assert.match(text(h), /5 samples; min 70, max 90/);
    const raw = await client.callTool({ name: 'udc_get_raw_samples', arguments: { entity_id: 'plant1:line3:press07', attr: 'temperature', limit: 2, response_format: 'json' } });
    assert.deepStrictEqual(raw.structuredContent.samples.map(s => s.value), [74, 90]);
    const badDate = await client.callTool({ name: 'udc_get_attribute_history', arguments: { entity_id: 'plant1:line3:press07', from: 'yesterday' } });
    assert.strictEqual(badDate.isError, true);
    assert.match(text(badDate), /ISO-8601.*24h/);
  });

  await test('invalid arguments are rejected before running', async () => {
    const r = await client.callTool({ name: 'udc_search_entities', arguments: { limit: 1000 } });
    assert.strictEqual(r.isError, true);
  });

  await test('resources: data model and entity by URI', async () => {
    const model = await client.readResource({ uri: 'udc://data-model' });
    assert.ok(JSON.parse(model.contents[0].text).types.some(t => t.type === 'Press'));
    const entity = await client.readResource({ uri: 'udc://entities/plant1%3Aline3' });
    assert.strictEqual(JSON.parse(entity.contents[0].text).type, 'Line');
  });

  await test('the key sees only its tenant', async () => {
    const r = await client.callTool({ name: 'udc_get_entity', arguments: { entity_id: 'secret' } });
    assert.strictEqual(r.isError, true);
    await client.close();
  });

  await test('a type-restricted key sees only its types', async () => {
    const c = await connect({ 'X-API-Key': pressesOnly });
    const model = await c.callTool({ name: 'udc_describe_data_model', arguments: { response_format: 'json' } });
    assert.deepStrictEqual(model.structuredContent.types.map(t => t.type), ['Press']);
    const plant = await c.callTool({ name: 'udc_get_entity', arguments: { entity_id: 'plant1' } });
    assert.strictEqual(plant.isError, true);
    await c.close();
  });

  await test('a user token works with X-Tenant', async () => {
    const c = await connect({ Authorization: `Bearer ${token}`, 'X-Tenant': OTHER });
    const r = await c.callTool({ name: 'udc_search_entities', arguments: { response_format: 'json' } });
    assert.deepStrictEqual(r.structuredContent.entities.map(e => e.id), ['secret']);
    await c.close();
  });

  await test('no or wrong credentials: 401', async () => {
    for (const headers of [{}, { Authorization: 'Bearer udc_00000000_wrong' }]) {
      const res = await fetch(`${base}/mcp`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', ...headers },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' })
      });
      assert.strictEqual(res.status, 401);
    }
  });

  await test('mcp-info lists the tools for the UI', async () => {
    const info = await call(token, 'GET', '/mcp-info');
    assert.strictEqual(info.enabled, true);
    assert.strictEqual(info.tools.length, 6);
  });

  for (const t of [TENANT, OTHER]) await call(token, 'DELETE', `/tenants/${t}`);
  await broker.revokeUserSessions(admin.id);
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
