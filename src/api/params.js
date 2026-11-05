/**
 * Query parameter helpers for the system routes.
 */

/** Integer limit within [1, max]; missing or invalid values give the default. */
function parseLimit(value, defaultValue = 100, max = 1000) {
  const n = parseInt(value, 10);
  if (!Number.isFinite(n) || n < 1) return defaultValue;
  return Math.min(n, max);
}

/** True when the value is a parseable date (ISO 8601 or epoch ms). */
function isValidTime(value) {
  return typeof value === 'string' && !Number.isNaN(new Date(value).getTime());
}

/**
 * Validates an optional startTime/endTime pair. Returns an error message, or null when valid.
 */
function timeRangeError(startTime, endTime) {
  if (startTime === undefined && endTime === undefined) return null;
  if (!isValidTime(startTime) || !isValidTime(endTime)) return 'startTime and endTime must both be valid dates';
  if (new Date(startTime) > new Date(endTime)) return 'startTime must not be after endTime';
  return null;
}

module.exports = { parseLimit, isValidTime, timeRangeError };
