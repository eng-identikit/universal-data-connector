const { badRequest } = require('../errors');

/** Forwards rejected promises from async route handlers to the error handler (Express 4). */
const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

/** Expected entity version from `If-Match: 12` / `If-Match: "12"`, or undefined. */
function expectedVersion(req) {
  const header = req.get('if-match');
  if (!header || header === '*') return undefined;
  const value = Number(header.replace(/^W\//, '').replace(/"/g, ''));
  if (!Number.isInteger(value) || value < 1) throw badRequest('If-Match must be an entity version number');
  return value;
}

module.exports = { asyncHandler, expectedVersion };
