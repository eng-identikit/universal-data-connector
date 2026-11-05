/**
 * Secret masking for the system routes: connector and storage configurations
 * carry credentials. Only a superadmin sees them in clear; everybody else gets
 * MASK in their place. When a masked configuration comes back in a write
 * (the UI round-trips what it read), restoreMasked() puts the stored values back.
 */

const MASK = '••••';

// Field names holding credentials (any depth): password, passwd, passphrase, pwd, secret,
// clientSecret, token, accessToken, credentials, key, apiKey, private_key... but not
// keepalive, keyPrefix, tokenUrl or monkey.
const SECRET_KEY_RE = /(^pass|pass(word|wd|phrase)|^pwd|secret|token|credentials?|^key|(api|access|private|secret|auth|client|signing|encryption)[_-]?key)$/i;

// user:password@ in connection strings (mqtt://u:p@host, postgres://u:p@host/db)
const URL_PASSWORD_RE = /^([a-z][a-z0-9+.-]*:\/\/[^:/?#@\s]*:)([^@/?#\s]+)(@)/i;

function isSecretKey(key) {
  return SECRET_KEY_RE.test(key);
}

function maskString(value) {
  return value.replace(URL_PASSWORD_RE, `$1${MASK}$3`);
}

/** Deep copy of value with secrets replaced by MASK (empty values stay as they are). */
function maskSecrets(value, key = '') {
  if (Array.isArray(value)) return value.map(v => maskSecrets(v));
  if (value && typeof value === 'object') {
    // API-key authentication: { type: 'apikey', key: '<header>', value: '<secret>' }
    const apiKeyAuth = typeof value.type === 'string' && /^api[-_]?key$/i.test(value.type);
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [
      k,
      apiKeyAuth && k === 'value' && typeof v === 'string' && v !== '' ? MASK : maskSecrets(v, k)
    ]));
  }
  if (typeof value === 'string') {
    if (isSecretKey(key) && value !== '') return MASK;
    return maskString(value);
  }
  if (isSecretKey(key) && typeof value === 'number') return MASK;
  return value;
}

/**
 * Returns incoming with every masked value replaced by the stored value at the
 * same position. Arrays of objects with an id are matched by id, others by index.
 */
function restoreMasked(incoming, stored) {
  if (incoming === MASK) return stored;
  if (typeof incoming === 'string') {
    if (incoming.includes(MASK) && typeof stored === 'string' && maskString(stored) === incoming) return stored;
    return incoming;
  }
  if (Array.isArray(incoming)) {
    const storedArray = Array.isArray(stored) ? stored : [];
    return incoming.map((item, i) => {
      const match = item && typeof item === 'object' && item.id !== undefined
        ? storedArray.find(s => s && s.id === item.id)
        : storedArray[i];
      return restoreMasked(item, match);
    });
  }
  if (incoming && typeof incoming === 'object') {
    const storedObject = stored && typeof stored === 'object' ? stored : {};
    return Object.fromEntries(Object.entries(incoming).map(([k, v]) => [k, restoreMasked(v, storedObject[k])]));
  }
  return incoming;
}

/** True when the caller (set by the broker's system guard) is a superadmin. */
function isSuperadmin(req) {
  return req.auth?.kind === 'user' && !!req.auth.isSuperadmin;
}

/** Secrets in clear for a superadmin, masked for everybody else. */
function forCaller(req, value) {
  return isSuperadmin(req) ? value : maskSecrets(value);
}

module.exports = { MASK, isSecretKey, maskSecrets, restoreMasked, isSuperadmin, forCaller };
