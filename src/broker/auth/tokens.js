const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const logger = require('../../utils/logger');

const ISSUER = 'udc-broker';
const ACCESS_TTL = process.env.BROKER_ACCESS_TOKEN_TTL || '15m';
const REFRESH_TTL = process.env.BROKER_REFRESH_TOKEN_TTL || '7d';

let secret = process.env.BROKER_JWT_SECRET;
if (!secret) {
  secret = crypto.randomBytes(48).toString('base64');
  logger.warn('BROKER_JWT_SECRET not set: using a random secret. Tokens will not survive a restart and will not work across instances.');
}

/** Tokens of a session: both carry its id (sid); the refresh token also its current jti. */
function issueTokens(user, session) {
  const claims = { sub: user.id, tv: user.token_version, sid: session.id };
  return {
    accessToken: jwt.sign({ ...claims, typ: 'access' }, secret, { issuer: ISSUER, expiresIn: ACCESS_TTL }),
    refreshToken: jwt.sign({ ...claims, typ: 'refresh' }, secret, { issuer: ISSUER, expiresIn: REFRESH_TTL, jwtid: session.jti })
  };
}

/** When a refresh token issued now would expire: the session lifetime. */
function refreshExpiry() {
  return new Date(jwt.decode(jwt.sign({}, secret, { expiresIn: REFRESH_TTL })).exp * 1000);
}

/** Returns the decoded claims, or null if the token is invalid, expired or of another type. */
function verifyToken(token, expectedType) {
  try {
    const claims = jwt.verify(token, secret, { issuer: ISSUER, algorithms: ['HS256'] });
    return claims.typ === expectedType ? claims : null;
  } catch {
    return null;
  }
}

// API keys: udc_<8 hex prefix>_<secret>. Only the SHA-256 hash is stored;
// the keys are high-entropy so a fast hash is sufficient.
function generateApiKey() {
  const prefix = crypto.randomBytes(4).toString('hex');
  const key = `udc_${prefix}_${crypto.randomBytes(24).toString('base64url')}`;
  return { key, prefix, hash: hashApiKey(key) };
}

function hashApiKey(key) {
  return crypto.createHash('sha256').update(key).digest('hex');
}

module.exports = { issueTokens, refreshExpiry, verifyToken, generateApiKey, hashApiKey };
