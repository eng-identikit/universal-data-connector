const os = require('os');
const logger = require('../../utils/logger');

const TICK_MS = parseInt(process.env.BROKER_CLUSTER_TICK_MS || '5000', 10);
const LEASE_MS = TICK_MS * 3;
const PARTITIONS = 32;

/**
 * Work distribution between broker instances sharing PostgreSQL and NATS.
 *
 * Every TICK_MS each instance heartbeats and renews its leases (TTL 3 ticks).
 * Two kinds of leased resources, both balanced to a fair share
 * (ceil(resources / live instances)):
 *
 *   sub:<0..31>        subscription partitions (hash of the subscription id):
 *                      the owner evaluates changes, throttles and periodic sends
 *   connector:<id>     each connector runs on exactly one instance
 *   ingest:<0..15>     ingest queue partitions, applied by one instance at a time
 *
 * A crashed instance stops renewing; its leases expire and are taken over
 * within ~LEASE_MS. The delivery queue needs no lease (shared JetStream
 * consumer), and the realtime stream runs everywhere.
 */
class ClusterCoordinator {
  constructor({ pool, instanceId, engine, metrics = () => ({}), ingestPartitions = 0 }) {
    this.pool = pool;
    this.id = instanceId;
    this.engine = engine; // SubscriptionEngine
    this.metrics = metrics;
    // Ingest queue partitions: each consumed by one instance (messages wait in JetStream during handover)
    this.ingestPartitions = ingestPartitions;
    this.ingest = new Set();
    this.connectorEngine = null;
    this.partitions = new Set();
    // Released partitions still evaluated here until another instance holds them:
    // during a rebalance changes are evaluated twice (at-least-once) rather than not at all
    this.draining = new Map(); // partition → deadline (ms)
    this.connectors = new Set();
    this.timer = null;
    this.ticking = false;
    this.startedAt = new Date();
  }

  /** Subscription ownership by partition, consulted by the subscription engine. */
  ownsSubscription(id) {
    const p = partitionOf(id);
    return this.partitions.has(p) || this.draining.has(p);
  }

  ownsConnector(id) {
    return this.connectors.has(id);
  }

  ownsIngestPartition(p) {
    return this.ingest.has(p);
  }

  attachConnectorEngine(engine) {
    this.connectorEngine = engine;
    engine.setConnectorGate((id) => this.ownsConnector(id));
  }

  /** ingestQueue: lets a released ingest partition finish its batch before the lease goes. */
  async start({ ingestQueue = null } = {}) {
    this.ingestQueue = ingestQueue;
    // Partition cached on the compiled subscription: owns() runs for every change
    this.engine.owns = (sub) => {
      if (sub.partition === undefined) sub.partition = partitionOf(sub.id);
      return this.partitions.has(sub.partition) || this.draining.has(sub.partition);
    };
    await this.tick();
    this.timer = setInterval(() => this.tick(), TICK_MS);
    if (this.timer.unref) this.timer.unref();
    logger.info(`Cluster coordinator started (instance ${this.id}, ${this.partitions.size}/${PARTITIONS} partitions)`);
  }

  async tick() {
    if (this.ticking) return;
    this.ticking = true;
    try {
      await this._heartbeat();
      const live = await this._liveInstances();
      await this._renew();

      const beforePartitions = new Set([...this.partitions, ...this.draining.keys()]);
      await this._balance('sub:', Array.from({ length: PARTITIONS }, (_, i) => `sub:${i}`), live, this.partitions);
      await this._updateDraining();
      const nowOwned = new Set([...this.partitions, ...this.draining.keys()]);
      if (!sameSet(beforePartitions, nowOwned)) this.engine.refreshOwnership();

      if (this.ingestPartitions) {
        await this._balance('ingest:', Array.from({ length: this.ingestPartitions }, (_, i) => `ingest:${i}`), live, this.ingest);
      }

      if (this.connectorEngine) {
        const before = new Set(this.connectors);
        const ids = [...this.connectorEngine.connectors.keys()].map(id => `connector:${id}`);
        await this._balance('connector:', ids, live, this.connectors);
        await this._applyConnectors(before);
      }
    } catch (error) {
      logger.error(`Cluster tick failed: ${error.message}`);
    } finally {
      this.ticking = false;
    }
  }

  async _heartbeat() {
    const info = {
      partitions: this.partitions.size,
      connectors: [...this.connectors],
      ...this.metrics()
    };
    await this.pool.query(
      `INSERT INTO cluster_instances (id, host, started_at, heartbeat_at, info) VALUES ($1, $2, $3, now(), $4)
       ON CONFLICT (id) DO UPDATE SET heartbeat_at = now(), info = EXCLUDED.info`,
      [this.id, os.hostname(), this.startedAt, info]
    );
  }

  async _liveInstances() {
    const { rows } = await this.pool.query(
      `SELECT id FROM cluster_instances WHERE heartbeat_at > now() - ($1 || ' milliseconds')::interval ORDER BY id`,
      [String(LEASE_MS)]
    );
    return Math.max(1, rows.length);
  }

  /** Draining partitions end once another instance holds them, or after one lease period. */
  async _updateDraining() {
    if (!this.draining.size) return;
    const { rows } = await this.pool.query(
      `SELECT resource FROM cluster_leases WHERE resource = ANY($1) AND owner <> $2 AND expires_at > now()`,
      [[...this.draining.keys()].map(p => `sub:${p}`), this.id]
    );
    const taken = new Set(rows.map(r => Number(r.resource.slice(4))));
    for (const [p, deadline] of this.draining) {
      if (taken.has(p) || this.partitions.has(p) || Date.now() > deadline) this.draining.delete(p);
    }
  }

  /** Extends our leases; resources whose lease was lost (expired and taken) are forgotten. */
  async _renew() {
    const mine = [
      ...[...this.partitions].map(p => `sub:${p}`),
      ...[...this.ingest].map(p => `ingest:${p}`),
      ...[...this.connectors].map(c => `connector:${c}`)
    ];
    // Only leases we still hold: released (draining) ones must stay expired so others can take them
    const { rows } = await this.pool.query(
      `UPDATE cluster_leases SET expires_at = now() + ($2 || ' milliseconds')::interval
        WHERE owner = $1 AND resource = ANY($3) RETURNING resource`,
      [this.id, String(LEASE_MS), mine]
    );
    const held = new Set(rows.map(r => r.resource));
    for (const p of [...this.partitions]) if (!held.has(`sub:${p}`)) this.partitions.delete(p);
    for (const p of [...this.ingest]) if (!held.has(`ingest:${p}`)) this.ingest.delete(p);
    for (const c of [...this.connectors]) if (!held.has(`connector:${c}`)) this.connectors.delete(c);
  }

  /** Releases resources above the fair share, acquires free/expired ones below it. */
  async _balance(prefix, resources, live, owned) {
    const numeric = prefix === 'sub:' || prefix === 'ingest:';
    const key = (r) => (numeric ? Number(r.slice(prefix.length)) : r.slice(prefix.length));
    const valid = new Set(resources.map(key));
    // Resources that no longer exist (removed connectors)
    for (const r of [...owned]) {
      if (!valid.has(r)) {
        owned.delete(r);
        await this.pool.query('DELETE FROM cluster_leases WHERE resource = $1 AND owner = $2', [`${prefix}${r}`, this.id]);
      }
    }
    if (!resources.length) return;

    const target = Math.ceil(resources.length / live);
    if (owned.size > target) {
      const extra = [...owned].slice(0, owned.size - target);
      const names = extra.map(r => `${prefix}${r}`);
      if (prefix === 'sub:') {
        // Expire now so another instance can take them, but keep evaluating until it does
        await this.pool.query('UPDATE cluster_leases SET expires_at = now() WHERE owner = $1 AND resource = ANY($2)', [this.id, names]);
        extra.forEach(p => this.draining.set(p, Date.now() + LEASE_MS));
      } else if (prefix === 'ingest:') {
        // Stop fetching first and let the batch in progress finish: the next owner
        // must never apply the same partition concurrently (order of updates)
        extra.forEach(p => owned.delete(p));
        if (this.ingestQueue) await Promise.all(extra.map(p => this.ingestQueue.whenIdle(p, LEASE_MS / 3)));
        await this.pool.query('DELETE FROM cluster_leases WHERE owner = $1 AND resource = ANY($2)', [this.id, names]);
      } else {
        // Connectors must never run twice (one connection per device): hand over directly
        await this.pool.query('DELETE FROM cluster_leases WHERE owner = $1 AND resource = ANY($2)', [this.id, names]);
      }
      extra.forEach(r => owned.delete(r));
      return;
    }
    if (owned.size === target) return;

    const { rows } = await this.pool.query(
      `INSERT INTO cluster_leases AS l (resource, owner, acquired_at, expires_at)
       SELECT r, $1, now(), now() + ($3 || ' milliseconds')::interval FROM unnest($2::text[]) AS r
       ON CONFLICT (resource) DO UPDATE SET owner = EXCLUDED.owner, acquired_at = now(), expires_at = EXCLUDED.expires_at
         WHERE l.expires_at < now()
       RETURNING resource`,
      [this.id, resources.filter(r => !owned.has(key(r))), String(LEASE_MS)]
    );
    // Keep only up to the fair share; give back what we took in excess
    const acquired = rows.map(r => r.resource);
    const keep = acquired.slice(0, target - owned.size);
    const giveBack = acquired.slice(keep.length);
    if (giveBack.length) await this.pool.query('DELETE FROM cluster_leases WHERE owner = $1 AND resource = ANY($2)', [this.id, giveBack]);
    keep.forEach(r => owned.add(key(r)));
  }

  /** Starts connectors we just acquired and stops those we lost. */
  async _applyConnectors(before) {
    if (!this.connectorEngine.isRunning) return;
    for (const id of this.connectors) {
      if (before.has(id)) continue;
      logger.info(`Cluster: this instance now runs connector '${id}'`);
      await this.connectorEngine.startConnector(id).catch(e => logger.error(`Cluster: start '${id}' failed: ${e.message}`));
    }
    for (const id of before) {
      if (this.connectors.has(id) || !this.connectorEngine.connectors.has(id)) continue;
      logger.info(`Cluster: connector '${id}' moved to another instance`);
      await this.connectorEngine.stopConnector(id).catch(e => logger.error(`Cluster: stop '${id}' failed: ${e.message}`));
    }
  }

  async status() {
    const [instances, leases] = await Promise.all([
      this.pool.query(
        `SELECT id, host, started_at, heartbeat_at, info,
                heartbeat_at > now() - ($1 || ' milliseconds')::interval AS alive
           FROM cluster_instances ORDER BY started_at`,
        [String(LEASE_MS)]
      ),
      this.pool.query('SELECT resource, owner, acquired_at, expires_at, expires_at > now() AS valid FROM cluster_leases ORDER BY resource')
    ]);
    return {
      self: this.id,
      partitions: PARTITIONS,
      tickMs: TICK_MS,
      instances: instances.rows.map(r => ({
        id: r.id, host: r.host, alive: r.alive, startedAt: r.started_at.toISOString(),
        heartbeatAt: r.heartbeat_at.toISOString(), info: r.info
      })),
      leases: leases.rows.map(r => ({
        resource: r.resource, owner: r.owner, valid: r.valid,
        acquiredAt: r.acquired_at.toISOString(), expiresAt: r.expires_at.toISOString()
      }))
    };
  }

  /** Graceful shutdown: hand leases over immediately instead of waiting for expiry. */
  async stop() {
    clearInterval(this.timer);
    await this.pool.query('DELETE FROM cluster_leases WHERE owner = $1', [this.id]).catch(() => {});
    await this.pool.query('DELETE FROM cluster_instances WHERE id = $1', [this.id]).catch(() => {});
    // Dead instances older than a day are pruned by whoever stops last
    await this.pool.query("DELETE FROM cluster_instances WHERE heartbeat_at < now() - interval '1 day'").catch(() => {});
  }
}

/** Stable partition of a subscription id (FNV-1a). */
function partitionOf(id) {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h % PARTITIONS;
}

function sameSet(a, b) {
  return a.size === b.size && [...a].every(x => b.has(x));
}

module.exports = ClusterCoordinator;
module.exports.partitionOf = partitionOf;
module.exports.PARTITIONS = PARTITIONS;
