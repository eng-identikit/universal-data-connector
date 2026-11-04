const os = require('os');
const crypto = require('crypto');
const { monitorEventLoopDelay } = require('perf_hooks');
const express = require('express');
const logger = require('../utils/logger');
const { createPool } = require('./db/pool');
const { migrate } = require('./db/migrate');
const { BrokerError, unavailable } = require('./errors');
const { authenticate, requireTenantRole, requireSuperadmin } = require('./auth/middleware');
const EntityService = require('./services/EntityService');
const UserService = require('./services/UserService');
const TenantService = require('./services/TenantService');
const ApiKeyService = require('./services/ApiKeyService');
const HistoryService = require('./services/HistoryService');
const SubscriptionService = require('./subscriptions/SubscriptionService');
const SubscriptionEngine = require('./subscriptions/SubscriptionEngine');
const { defaultBrokers } = require('./subscriptions/subscriptionSchema');
const NatsClient = require('./messaging/NatsClient');
const ChangeBus = require('./messaging/ChangeBus');
const { JetStreamQueue, MemoryQueue } = require('./messaging/WorkQueue');
const Channels = require('./notify/channels');
const DeliveryWorker = require('./notify/DeliveryWorker');
const StreamHub = require('./realtime/StreamHub');
const IngestService = require('./ingest/IngestService');
const ConnectorBridge = require('./ingest/ConnectorBridge');
const CommandService = require('./commands/CommandService');
const ClusterCoordinator = require('./cluster/ClusterCoordinator');
const ConfigSync = require('./cluster/ConfigSync');
const authRoutes = require('./api/authRoutes');
const adminRoutes = require('./api/adminRoutes');
const entityRoutes = require('./api/entityRoutes');
const subscriptionRoutes = require('./api/subscriptionRoutes');
const dataRoutes = require('./api/dataRoutes');

const RETRY_INTERVAL_MS = 5000;

/**
 * Context broker: entity store, subscriptions, notifications, realtime stream,
 * users/tenants/API keys and the /api/v1 router.
 *
 * start() does not block server startup: until PostgreSQL (and NATS, when
 * NATS_URL is set) are reachable it keeps retrying in the background and
 * /api/v1 answers 503.
 */
class Broker {
  constructor() {
    this.instanceId = `${os.hostname()}-${process.pid}-${crypto.randomBytes(2).toString('hex')}`;
    // Event-loop delay: the saturation signal of a Node instance (shown on the Cluster page)
    this.loopDelay = monitorEventLoopDelay({ resolution: 20 });
    this.loopDelay.enable();
    this.pool = createPool();
    this.pool.on('error', (error) => logger.error(`Broker DB pool error: ${error.message}`));
    this.nats = new NatsClient();
    this.bus = new ChangeBus(this.nats, this.instanceId);

    const history = new HistoryService(this.pool);
    this.services = {
      history,
      entities: new EntityService(this.pool, { history }),
      users: new UserService(this.pool),
      tenants: new TenantService(this.pool),
      apiKeys: new ApiKeyService(this.pool),
      subscriptions: new SubscriptionService(this.pool)
    };
    this.services.commands = new CommandService({ pool: this.pool, entities: this.services.entities, natsClient: this.nats });

    this.notifyQueue = this.nats.enabled
      ? new JetStreamQueue(this.nats, { stream: 'UDC_NOTIFY', durable: 'udc-delivery' })
      : new MemoryQueue({ name: 'notify' });
    this.engine = new SubscriptionEngine({
      subscriptions: this.services.subscriptions,
      entities: this.services.entities,
      queue: this.notifyQueue,
      bus: this.bus
    });
    this.ingest = new IngestService({
      entities: this.services.entities,
      queue: this.nats.enabled
        ? new JetStreamQueue(this.nats, { stream: 'UDC_INGEST', durable: 'udc-ingest', partitions: IngestService.PARTITIONS })
        : new MemoryQueue({ name: 'ingest' })
    });
    this.bridge = new ConnectorBridge({ ingest: this.ingest });
    this.hub = new StreamHub({ services: this.services, bus: this.bus, natsClient: this.nats });
    this.channels = new Channels({ natsClient: this.nats, hub: this.hub });
    this.worker = new DeliveryWorker({
      queue: this.notifyQueue,
      engine: this.engine,
      channels: this.channels,
      subscriptions: this.services.subscriptions
    });

    // Several instances on the same database and NATS: share connectors and subscriptions
    this.cluster = null;
    this.configSync = null;
    if (process.env.BROKER_CLUSTER === 'true') {
      if (this.nats.enabled) {
        this.cluster = new ClusterCoordinator({
          pool: this.pool,
          instanceId: this.instanceId,
          engine: this.engine,
          metrics: () => this.metrics(),
          ingestPartitions: IngestService.PARTITIONS
        });
        this.ingest.queue.setPartitionFilter((p) => this.cluster.ownsIngestPartition(p));
        // BROKER_CONFIG_SYNC=false keeps config files local (e.g. instances with different sources on purpose)
        if (process.env.BROKER_CONFIG_SYNC !== 'false') {
          this.configSync = new ConfigSync({ pool: this.pool, bus: this.bus, instanceId: this.instanceId });
        }
      } else {
        logger.warn('BROKER_CLUSTER=true ignored: clustering needs NATS_URL');
      }
    }

    // Committed entity writes fan out to every instance through the bus
    this.services.entities.on('entityChanged', (change) => this.bus.publish(change));
    this.services.entities.on('entitiesChanged', (changes) => this.bus.publishMany(changes));
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
        await this.services.history.configureRetention();
        await this.services.entities.detectGeo();
        this.dbReady = true;
      }
      if (!this.messagingReady) {
        await this.nats.connect();
        this.bus.start();
        await this.worker.start();
        await this.ingest.start();
        await this.engine.start();
        if (this.cluster) await this.cluster.start();
        if (this.configSync) this.configSync.start();
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

  /** Connects the connector engine: devices become entities, commands reach connectors. */
  attachEngine(engine) {
    this.bridge.attach(engine);
    this.services.commands.attach(engine);
    if (this.cluster) this.cluster.attachConnectorEngine(engine);
    if (this.configSync) this.configSync.attach(engine);
  }

  async stop() {
    this.stopped = true;
    clearTimeout(this.retryTimer);
    if (this.cluster) await this.cluster.stop();
    await this.ingest.stop().catch(() => {});
    await this.engine.stop().catch(() => {});
    this.services.commands.close();
    this.notifyQueue.stop();
    this.ingest.queue.stop();
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
      geo: this.services.entities.geoEnabled,
      cluster: !!this.cluster,
      messaging: this.nats.enabled ? (this.nats.connected ? 'nats' : 'nats-disconnected') : 'memory',
      // Notification channels usable without an explicit notify.url
      defaultBrokers: defaultBrokers(),
      error: this.lastError
    };
  }

  /**
   * Guard for /api/v1/system (connectors, mapping, storage, engine status):
   * global infrastructure shared by all tenants, so any authenticated caller
   * may read and only a superadmin may change it.
   */
  systemGuard() {
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

  /** Runtime metrics of this instance; the loop delay window restarts at each call. */
  metrics() {
    const m = {
      loopDelayP50Ms: Math.round(this.loopDelay.percentile(50) / 1e6),
      loopDelayP99Ms: Math.round(this.loopDelay.percentile(99) / 1e6),
      memoryMb: Math.round(process.memoryUsage().rss / 1048576),
      stream: this.hub.stats(),
      ingest: { applyMsAvg: this.ingest.stats.applyMsAvg ?? null, written: this.ingest.stats.written, rejected: this.ingest.stats.rejected }
    };
    this.loopDelay.reset();
    return m;
  }

  /** Middleware chain for /api/v1/system: auth guard, plus config sharing in a cluster. */
  systemMiddleware() {
    const chain = [this.systemGuard()];
    if (this.configSync) chain.push(this.configSync.middleware());
    return chain;
  }

  /** Instances, leases and queue backlogs, for the Cluster page. */
  async clusterStatus() {
    const [notify, ingest] = await Promise.all([this.notifyQueue.info(), this.ingest.queue.info()]);
    const queues = { notify, ingest, ingestStats: this.ingest.stats };
    if (this.cluster) return { enabled: true, ...(await this.cluster.status()), queues };
    return {
      enabled: false,
      self: this.instanceId,
      instances: [{ id: this.instanceId, host: os.hostname(), alive: true, startedAt: null, heartbeatAt: new Date().toISOString(), info: this.metrics() }],
      leases: [],
      queues
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
    router.use(dataRoutes(deps));
    router.get('/cluster', requireSuperadmin, (req, res, next) => this.clusterStatus().then(s => res.json(s)).catch(next));
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
