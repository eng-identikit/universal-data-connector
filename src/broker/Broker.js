const express = require('express');
const logger = require('../utils/logger');
const { createPool } = require('./db/pool');
const { migrate } = require('./db/migrate');
const { BrokerError, unavailable } = require('./errors');
const { authenticate, requireTenantRole } = require('./auth/middleware');
const EntityService = require('./services/EntityService');
const UserService = require('./services/UserService');
const TenantService = require('./services/TenantService');
const ApiKeyService = require('./services/ApiKeyService');
const authRoutes = require('./api/authRoutes');
const adminRoutes = require('./api/adminRoutes');
const entityRoutes = require('./api/entityRoutes');

const RETRY_INTERVAL_MS = 5000;

/**
 * Context broker: entity store, users/tenants/API keys and the /api/v2 router.
 *
 * start() does not block server startup: if PostgreSQL is unreachable it keeps
 * retrying in the background and /api/v2 answers 503 until it is ready.
 */
class Broker {
  constructor() {
    this.pool = createPool();
    this.pool.on('error', (error) => logger.error(`Broker DB pool error: ${error.message}`));
    this.services = {
      entities: new EntityService(this.pool),
      users: new UserService(this.pool),
      tenants: new TenantService(this.pool),
      apiKeys: new ApiKeyService(this.pool)
    };
    this.ready = false;
    this.lastError = null;
    this.retryTimer = null;
    this.stopped = false;
    this.router = this._buildRouter();
  }

  async start() {
    try {
      await migrate(this.pool);
      await this.services.users.bootstrap();
      this.ready = true;
      this.lastError = null;
      logger.info('Broker ready');
    } catch (error) {
      this.lastError = error.message;
      logger.error(`Broker not ready (${error.message}), retrying in ${RETRY_INTERVAL_MS / 1000}s`);
      if (!this.stopped) this.retryTimer = setTimeout(() => this.start(), RETRY_INTERVAL_MS);
    }
  }

  async stop() {
    this.stopped = true;
    clearTimeout(this.retryTimer);
    await this.pool.end();
  }

  status() {
    return { ready: this.ready, error: this.lastError };
  }

  /**
   * Guard for the legacy /api routes until they are replaced (phase 3):
   * any authenticated caller may read, only a superadmin may change configuration.
   */
  legacyGuard() {
    return (req, res, next) => {
      if (!this.ready) return res.status(503).json({ error: 'Unavailable', message: 'Broker is starting' });
      this.authenticate(req, res, (error) => {
        if (!error && req.method !== 'GET' && !(req.auth.kind === 'user' && req.auth.isSuperadmin)) {
          error = new BrokerError(403, 'Forbidden', 'Superadmin required');
        }
        if (!error) return next();
        const status = error.status || 500;
        res.status(status).json({ error: error.code || 'InternalError', message: status === 500 ? 'Unexpected error' : error.message });
      });
    };
  }

  _buildRouter() {
    const router = express.Router();
    const auth = authenticate(this.services);
    this.authenticate = auth;
    const tenantRole = (role) => requireTenantRole(role, this.services);
    const deps = { services: this.services, authenticate: auth, tenantRole };

    router.get('/health', (req, res) => res.status(this.ready ? 200 : 503).json(this.status()));
    router.use((req, res, next) => next(this.ready ? undefined : unavailable('Broker is starting: database not ready')));

    router.use('/auth', authRoutes(deps));
    router.use(auth);
    router.use(entityRoutes(deps));
    router.use(adminRoutes(deps));

    router.use((req, res) => res.status(404).json({ error: 'NotFound', message: `No route for ${req.method} ${req.originalUrl}` }));
    // eslint-disable-next-line no-unused-vars
    router.use((error, req, res, next) => {
      if (error instanceof BrokerError) {
        return res.status(error.status).json({ error: error.code, message: error.message, details: error.details });
      }
      if (error.type === 'entity.parse.failed') {
        return res.status(400).json({ error: 'BadRequest', message: 'Malformed JSON body' });
      }
      if (error.type === 'entity.too.large') {
        return res.status(413).json({ error: 'PayloadTooLarge', message: 'Request body too large' });
      }
      if (error.code === '22P02') {
        // PostgreSQL invalid_text_representation, e.g. a malformed UUID in the path
        return res.status(400).json({ error: 'BadRequest', message: 'Invalid identifier format' });
      }
      logger.error(`Broker API error on ${req.method} ${req.originalUrl}:`, error);
      res.status(500).json({ error: 'InternalError', message: 'Unexpected error' });
    });
    return router;
  }
}

module.exports = Broker;
