const os = require('os');
const crypto = require('crypto');
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
const SubscriptionService = require('./subscriptions/SubscriptionService');
const SubscriptionEngine = require('./subscriptions/SubscriptionEngine');
const NatsClient = require('./messaging/NatsClient');
const ChangeBus = require('./messaging/ChangeBus');
const { JetStreamQueue, MemoryQueue } = require('./messaging/WorkQueue');
const Channels = require('./notify/channels');
const DeliveryWorker = require('./notify/DeliveryWorker');
const StreamHub = require('./realtime/StreamHub');
const authRoutes = require('./api/authRoutes');
const adminRoutes = require('./api/adminRoutes');
const entityRoutes = require('./api/entityRoutes');
const subscriptionRoutes = require('./api/subscriptionRoutes');

const RETRY_INTERVAL_MS = 5000;

/**
 * Context broker: entity store, subscriptions, notifications, realtime stream,
 * users/tenants/API keys and the /api/v2 router.
 *
 * start() does not block server startup: until PostgreSQL (and NATS, when
 * NATS_URL is set) are reachable it keeps retrying in the background and
 * /api/v2 answers 503.
 */
class Broker {
  constructor() {
    this.instanceId = `${os.hostname()}-${process.pid}-${crypto.randomBytes(2).toString('hex')}`;
    this.pool = createPool();
    this.pool.on('error', (error) => logger.error(`Broker DB pool error: ${error.message}`));
    this.nats = new NatsClient();
    this.bus = new ChangeBus(this.nats, this.instanceId);

    this.services = {
      entities: new EntityService(this.pool),
      users: new UserService(this.pool),
      tenants: new TenantService(this.pool),
      apiKeys: new ApiKeyService(this.pool),
      subscriptions: new SubscriptionService(this.pool)
    };

    this.notifyQueue = this.nats.enabled
      ? new JetStreamQueue(this.nats, { stream: 'UDC_NOTIFY', durable: 'udc-delivery' })
      : new MemoryQueue({ name: 'notify' });
    this.engine = new SubscriptionEngine({
      subscriptions: this.services.subscriptions,
      entities: this.services.entities,
      queue: this.notifyQueue,
      bus: this.bus
    });
    this.hub = new StreamHub({ services: this.services, bus: this.bus, natsClient: this.nats });
    this.channels = new Channels({ natsClient: this.nats, hub: this.hub });
    this.worker = new DeliveryWorker({
      queue: this.notifyQueue,
      engine: this.engine,
      channels: this.channels,
      subscriptions: this.services.subscriptions
    });

    // Committed entity writes fan out to every instance through the bus
    this.services.entities.on('entityChanged', (change) => this.bus.publish(change));
    // Subscription CRUD: apply here before answering, then tell the other instances
    this.services.subscriptions.onChange = async (tenant, id) => {
      await this.engine.reload(id);
      this.bus.control({ kind: 'subscription', tenant, id });
    };

    this.ready = false;
    this.dbReady = false;
    this.messagingReady = false;
    this.lastError = null;
    this.retryTimer = null;
    this.stopped = false;
    this.router = this._buildRouter();
  }

  async start() {
    try {
      if (!this.dbReady) {
        await migrate(this.pool);
        await this.services.users.bootstrap();
        this.dbReady = true;
      }
      if (!this.messagingReady) {
        await this.nats.connect();
        this.bus.start();
        await this.worker.start();
        await this.engine.start();
        this.messagingReady = true;
      }
      this.ready = true;
      this.lastError = null;
      logger.info(`Broker ready (instance ${this.instanceId}, ${this.nats.enabled ? 'NATS JetStream' : 'in-memory queues'})`);
    } catch (error) {
      this.lastError = error.message;
      logger.error(`Broker not ready (${error.message}), retrying in ${RETRY_INTERVAL_MS / 1000}s`);
      if (!this.stopped) this.retryTimer = setTimeout(() => this.start(), RETRY_INTERVAL_MS);
    }
  }

  /** Attaches the realtime WebSocket stream to the HTTP server. */
  attach(server) {
    this.hub.attach(server);
  }

  async stop() {
    this.stopped = true;
    clearTimeout(this.retryTimer);
    await this.engine.stop().catch(() => {});
    this.notifyQueue.stop();
    this.hub.close();
    this.bus.stop();
    await this.channels.close();
    await this.nats.close();
    await this.pool.end();
  }

  status() {
    return {
      ready: this.ready,
      instance: this.instanceId,
      database: this.dbReady,
      messaging: this.nats.enabled ? (this.nats.connected ? 'nats' : 'nats-disconnected') : 'memory',
      error: this.lastError
    };
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
    const deps = { services: this.services, authenticate: auth, tenantRole, engine: this.engine, worker: this.worker, broker: this };

    router.get('/health', (req, res) => res.status(this.ready ? 200 : 503).json(this.status()));
    router.use((req, res, next) => next(this.ready ? undefined : unavailable('Broker is starting: database or messaging not ready')));

    router.use('/auth', authRoutes(deps));
    router.use(auth);
    router.use(entityRoutes(deps));
    router.use(subscriptionRoutes(deps));
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
