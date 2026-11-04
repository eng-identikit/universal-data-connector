#!/usr/bin/env node

/**
 * Broker - entity model unit tests (no database required)
 */

const assert = require('assert');
const model = require('../src/broker/model/entityModel');
const { globToLike } = require('../src/broker/services/EntityService');

const NOW = '2026-11-04T10:00:00.000Z';
const LATER = '2026-11-04T10:05:00.000Z';
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed++;
    console.log(`  ✓ ${name}`);
  } catch (error) {
    failed++;
    console.log(`  ✗ ${name}\n    Error: ${error.message}`);
  }
}

const throwsBadRequest = (fn, pattern) => assert.throws(fn, (e) => e.status === 400 && (!pattern || pattern.test(e.message)));

console.log('\n=== BROKER ENTITY MODEL TESTS ===');

test('normalizes a full entity and infers attribute types', () => {
  const e = model.normalizeEntity({
    id: 'plant1:line3:press07',
    type: 'Press',
    attrs: { temperature: 82.4, count: 3, running: true, label: 'P7', partOf: { target: 'plant1:line3' } }
  }, NOW);
  assert.strictEqual(e.attrs.temperature.type, 'float');
  // integers are inferred as float too, so later fractional values are accepted
  assert.strictEqual(e.attrs.count.type, 'float');
  assert.strictEqual(e.attrs.running.type, 'bool');
  assert.strictEqual(e.attrs.label.type, 'string');
  assert.deepStrictEqual(e.attrs.partOf, { target: 'plant1:line3', type: 'relationship', ts: NOW });
  assert.strictEqual(e.attrs.temperature.ts, NOW);
  assert.strictEqual(e.location, null);
});

test('rejects invalid ids, types and attribute names', () => {
  throwsBadRequest(() => model.normalizeEntity({ id: 'has space', type: 'T' }, NOW), /Invalid id/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: '1bad' }, NOW), /Invalid type/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { 'bad-name': 1 } }, NOW), /Invalid attribute name/);
});

test('rejects unknown entity and attribute fields', () => {
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', temperature: 1 }, NOW), /Unknown entity field/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { value: 1, foo: 2 } } }, NOW), /unknown field/);
});

test('checks values against declared types', () => {
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { type: 'int', value: 1.5 } } }, NOW), /does not match/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { type: 'datetime', value: 'nope' } } }, NOW), /does not match/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { type: 'weird', value: 1 } } }, NOW), /unknown type/);
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { value: 1, quality: 'meh' } } }, NOW), /quality/);
});

test('patch merges attributes, keeps metadata and updates ts only on value change', () => {
  const existing = model.normalizeEntity({
    id: 'a', type: 'T', attrs: { t: { value: 1.5, unit: '°C', quality: 'good' }, s: 'x' }
  }, NOW);
  const same = model.applyPatch(existing, { attrs: { t: { value: 1.5 } } }, LATER);
  assert.strictEqual(same.attrs.t.ts, NOW);
  const changed = model.applyPatch(existing, { attrs: { t: { value: 2.5 } } }, LATER);
  assert.strictEqual(changed.attrs.t.ts, LATER);
  assert.strictEqual(changed.attrs.t.unit, '°C');
  assert.strictEqual(changed.attrs.t.type, 'float');
  assert.ok('s' in changed.attrs);
});

test('patch with null removes an attribute; null is refused on create', () => {
  const existing = model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: 1, s: 'x' } }, NOW);
  const next = model.applyPatch(existing, { attrs: { s: null } }, LATER);
  assert.ok(!('s' in next.attrs));
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { s: null } }, NOW), /null/);
});

test('patch cannot change id or type', () => {
  const existing = model.normalizeEntity({ id: 'a', type: 'T' }, NOW);
  throwsBadRequest(() => model.applyPatch(existing, { type: 'Other' }, LATER), /type/);
  throwsBadRequest(() => model.applyPatch(existing, { id: 'b' }, LATER), /id/);
});

test('switching a relationship to a value drops target', () => {
  const existing = model.normalizeEntity({ id: 'a', type: 'T', attrs: { r: { target: 'b' } } }, NOW);
  const next = model.applyPatch(existing, { attrs: { r: { type: 'string', value: 'v' } } }, LATER);
  assert.strictEqual(next.attrs.r.target, undefined);
  assert.strictEqual(next.attrs.r.value, 'v');
});

test('explicit ts is normalized and validated', () => {
  const e = model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { value: 1, ts: '2026-01-01T00:00:00Z' } } }, NOW);
  assert.strictEqual(e.attrs.t.ts, '2026-01-01T00:00:00.000Z');
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', attrs: { t: { value: 1, ts: 'yesterday' } } }, NOW), /ts/);
});

test('validates GeoJSON location', () => {
  const e = model.normalizeEntity({ id: 'a', type: 'T', location: { type: 'Point', coordinates: [9.19, 45.46] } }, NOW);
  assert.deepStrictEqual(e.location, { type: 'Point', coordinates: [9.19, 45.46] });
  throwsBadRequest(() => model.normalizeEntity({ id: 'a', type: 'T', location: { type: 'Circle' } }, NOW), /location/);
});

test('extracts relationships', () => {
  const e = model.normalizeEntity({ id: 'a', type: 'T', attrs: { partOf: { target: 'b' }, t: 1 } }, NOW);
  assert.deepStrictEqual(model.extractRelationships(e.attrs), [{ attr: 'partOf', target: 'b' }]);
});

test('glob patterns become escaped LIKE patterns', () => {
  assert.strictEqual(globToLike('plant1:line3:*'), 'plant1:line3:%');
  assert.strictEqual(globToLike('a_b%*'), 'a\\_b\\%%');
});

console.log(`\nPassed: ${passed}, Failed: ${failed}`);
process.exit(failed ? 1 : 0);
