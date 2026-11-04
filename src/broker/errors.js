/**
 * Error carrying an HTTP status and a stable machine-readable code.
 * Rendered by the broker API error handler as { error, message, details }.
 */
class BrokerError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

const badRequest = (message, details) => new BrokerError(400, 'BadRequest', message, details);
const unauthorized = (message = 'Authentication required') => new BrokerError(401, 'Unauthorized', message);
const forbidden = (message = 'Insufficient permissions') => new BrokerError(403, 'Forbidden', message);
const notFound = (message) => new BrokerError(404, 'NotFound', message);
const conflict = (message, details) => new BrokerError(409, 'Conflict', message, details);
const tooManyRequests = (message) => new BrokerError(429, 'TooManyRequests', message);
const unavailable = (message) => new BrokerError(503, 'Unavailable', message);

module.exports = {
  BrokerError, badRequest, unauthorized, forbidden, notFound, conflict, tooManyRequests, unavailable
};
