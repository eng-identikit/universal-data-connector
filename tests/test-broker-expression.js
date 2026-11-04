#!/usr/bin/env node

/**
 * Broker - filter expression language unit tests
 */

const assert = require('assert');
const { compile, ExpressionError } = require('../src/broker/expr/expression');

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

const entity = {
  id: 'plant1:line3:press07',
  type: 'Press',
  version: 4,
  attrs: {
    temperature: { type: 'float', value: 82.4, unit: '°C', quality: 'good', ts: new Date(Date.now() - 120000).toISOString(), meta: { sensor: 'PT100' } },
    status: { type: 'string', value: 'RUN' },
    count: { type: 'int', value: 10 },
    tags: { type: 'json', value: ['hot', 'line3'] },
    partOf: { type: 'relationship', target: 'plant1:line3' }
  }
};
const previous = { ...entity, attrs: { ...entity.attrs, temperature: { ...entity.attrs.temperature, value: 75 } } };
const t = (src, e = entity, p = previous) => compile(src).test(e, p);

console.log('\n=== BROKER EXPRESSION TESTS ===');

test('comparisons and logic', () => {
  assert.ok(t("temperature > 80 && status == 'RUN'"));
  assert.ok(!t("temperature > 80 and status == 'STOP'"));
  assert.ok(t("temperature < 50 or count >= 10"));
  assert.ok(t('!(temperature < 50)'));
  assert.ok(t('not status == "STOP"'));
});

test('attribute fields, meta and entity fields', () => {
  assert.ok(t("temperature.unit == '°C' && temperature.quality == 'good'"));
  assert.ok(t("temperature.meta.sensor == 'PT100'"));
  assert.ok(t("@type == 'Press' && @id matches '^plant1:' && @version == 4"));
  assert.ok(t("partOf == 'plant1:line3'"));
});

test('previous state and arithmetic', () => {
  assert.ok(t('temperature - prev.temperature > 5'));
  assert.ok(t('abs(prev.temperature - temperature) >= 7.4'));
  assert.ok(t('round(temperature) == 82 && round(temperature, 1) == 82.4'));
  assert.ok(!t('prev.temperature > 80'));
  assert.ok(!t('prev.temperature > 0', entity, null));
});

test('in, contains, has, functions', () => {
  assert.ok(t("@type in ['Press', 'Lathe']"));
  assert.ok(t("tags contains 'hot'"));
  assert.ok(t("has('status') && !has('alarm')"));
  assert.ok(t("lower(status) == 'run' && startsWith(@id, 'plant1')"));
  assert.ok(t('age(temperature.ts) > 60 && age(temperature.ts) < 3600'));
  assert.ok(t('max(count, 3, 20) == 20'));
});

test('type mismatches never throw and evaluate false', () => {
  assert.ok(!t("status > 5"));
  assert.ok(!t('missing > 5'));
  assert.ok(t('missing == null'));
  assert.ok(!t("status - 1 > 0"));
  assert.ok(!t('count / 0 > 1'));
});

test('collects referenced attributes', () => {
  assert.deepStrictEqual(compile("temperature > prev.pressure && has('alarm') && @id == 'x'").attrs.sort(), ['alarm', 'pressure', 'temperature']);
});

test('syntax errors carry a position', () => {
  const bad = ['temperature >', "status == 'RUN", 'foo(1)', 'temperature.color == 1', '(a > 1', 'a > 1 b', "a matches '('", "has(a)", '@name == 1', ''];
  for (const src of bad) {
    assert.throws(() => compile(src), ExpressionError, src);
  }
});

test('rejects overly long or deep expressions', () => {
  assert.throws(() => compile('a'.repeat(3000)), ExpressionError);
  assert.throws(() => compile('('.repeat(100) + '1' + ')'.repeat(100)), ExpressionError);
});

console.log(`\nPassed: ${passed}, Failed: ${failed}`);
process.exit(failed ? 1 : 0);
