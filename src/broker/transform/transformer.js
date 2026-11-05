const jsonata = require('jsonata');
const { badRequest } = require('../errors');
const { SafeRegExp } = require('../expr/safeRegex');

/**
 * Output transformers for subscriptions: a JSONata expression turns the
 * notification into the payload the receiver expects.
 *
 * Input of the expression (one per message, see notify.perEntity):
 *   { id, subscriptionId, subscriptionName, tenant, reason, timestamp,
 *     entities: [{ id, type, version, attrs: { name: { type, value, unit, ts, quality, ... } }, ... }],
 *     entity }   // entities[0], handy with perEntity
 *
 * Output: any JSON value (sent as JSON) or a string (sent as is, e.g. CSV or
 * line protocol). An expression that returns nothing (undefined) skips the message.
 *
 * Expressions are sandboxed by JSONata (no access to JavaScript or I/O); depth
 * and run time are capped and regexes are compiled safely (no ReDoS), so a
 * runaway expression cannot block the broker.
 */

const MAX_EXPRESSION_LENGTH = 20000;
const MAX_DEPTH = 300;
const TIME_LIMIT_MS = Number(process.env.BROKER_TRANSFORM_TIMEOUT_MS) || 500;
const CACHE_SIZE = 500;

const cache = new Map();

class TransformError extends Error {}

/** Compiles (cached) or throws a 400 with the position of the error. */
function compile(expression) {
  if (typeof expression !== 'string' || !expression.trim()) throw badRequest('Transformer expression is required');
  if (expression.length > MAX_EXPRESSION_LENGTH) throw badRequest(`Transformer expression is longer than ${MAX_EXPRESSION_LENGTH} characters`);
  let compiled = cache.get(expression);
  if (compiled) return compiled;
  try {
    compiled = jsonata(expression, { timeout: TIME_LIMIT_MS, stack: MAX_DEPTH, RegexEngine: SafeRegExp });
  } catch (error) {
    throw badRequest(`Transformer: ${error.message}`, { position: error.position, token: error.token });
  }
  if (cache.size >= CACHE_SIZE) cache.delete(cache.keys().next().value);
  cache.set(expression, compiled);
  return compiled;
}

const LIMIT_MESSAGES = {
  D1011: `recursion deeper than ${MAX_DEPTH}`,
  D1012: `ran longer than ${TIME_LIMIT_MS} ms`
};

/** Evaluates with depth and time limits; throws TransformError on failure. */
async function evaluate(expression, input) {
  const compiled = compile(expression);
  try {
    return await compiled.evaluate(input);
  } catch (error) {
    const reason = LIMIT_MESSAGES[error.code] || error.message || error.code;
    throw new TransformError(`Transformer failed: ${reason}${error.position !== undefined ? ` (position ${error.position})` : ''}`);
  }
}

/**
 * Serializes a transformer result into a message body.
 * Returns null when the expression produced nothing (the message is skipped).
 */
function toBody(result) {
  if (result === undefined) return null;
  if (typeof result === 'string') return { body: result, contentType: 'text/plain; charset=utf-8' };
  return { body: JSON.stringify(result), contentType: 'application/json' };
}

module.exports = { compile, evaluate, toBody, TransformError, MAX_EXPRESSION_LENGTH };
