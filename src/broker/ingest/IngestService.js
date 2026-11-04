const logger = require('../../utils/logger');

const FLUSH_MS = parseInt(process.env.BROKER_INGEST_FLUSH_MS || '200', 10);
const MAX_BATCH = 500;
// Batches are split by hash(entity id): one partition is applied by one worker at a
// time cluster-wide, so concurrent batches never wait on each other's row locks
const PARTITIONS = parseInt(process.env.BROKER_INGEST_PARTITIONS || '16', 10);

/**
 * Jobs fetched together from one partition become one batch per (tenant,
 * allowedTypes), keeping their order (later updates of an entity win).
 */
function mergeJobs(jobs) {
  const groups = new Map();
  for (const job of jobs) {
    const key = `${job.tenant}|${job.allowedTypes ? job.allowedTypes.join(',') : ''}`;
    const group = groups.get(key);
    if (group) group.updates.push(...job.updates);
    else groups.set(key, { tenant: job.tenant, allowedTypes: job.allowedTypes, updates: [...job.updates] });
  }
  return [...groups.values()];
}

/** Stable partition of an entity id (FNV-1a). */
function partitionOf(id) {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h % PARTITIONS;
}

/**
 * High-rate write path for connectors and POST /ingest.
 *
 *   push(tenant, update)        buffers and coalesces per entity (latest attribute
 *                               values win) and flushes every FLUSH_MS
 *   enqueue(tenant, updates)    publishes an already-batched request
 *
 * Batches go through a durable work queue (JetStream UDC_INGEST, shared by all
 * instances) and are applied with EntityService.ingestBatch. A crash loses at
 * most the FLUSH_MS of updates still in the local buffer.
 */
class IngestService {
  constructor({ entities, queue }) {
    this.entities = entities;
    this.queue = queue;
    this.buffer = new Map(); // tenant → Map<id, update>
    this.timer = null;
    this.stats = { received: 0, published: 0, written: 0, skipped: 0, rejected: 0, lastError: null };
  }

  async start() {
    this.timer = setInterval(() => this.flush(), FLUSH_MS);
    if (this.timer.unref) this.timer.unref();
    // JetStream: one consumer per partition; memory: partitions serialized, up to PARTITIONS in parallel
    await this.queue.consume((job) => this._apply(job), { concurrency: PARTITIONS, batch: 20, merge: mergeJobs });
  }

  push(tenant, update) {
    this.stats.received++;
    let entities = this.buffer.get(tenant);
    if (!entities) this.buffer.set(tenant, entities = new Map());
    const prev = entities.get(update.id);
    entities.set(update.id, prev ? { ...prev, ...update, attrs: { ...prev.attrs, ...update.attrs } } : update);
  }

  async enqueue(tenant, updates, { allowedTypes = null } = {}) {
    this.stats.received += updates.length;
    await this._publishAll(tenant, updates, allowedTypes);
  }

  async flush() {
    const buffer = this.buffer;
    if (!buffer.size) return;
    this.buffer = new Map();
    for (const [tenant, entities] of buffer) {
      const updates = [...entities.values()];
      await this._publishAll(tenant, updates, null)
        .catch(error => logger.error(`Ingest publish failed (${updates.length} updates dropped): ${error.message}`));
    }
  }

  async _publishAll(tenant, updates, allowedTypes) {
    const byPartition = new Map();
    for (const u of updates) {
      const p = partitionOf(u.id);
      if (!byPartition.has(p)) byPartition.set(p, []);
      byPartition.get(p).push(u);
    }
    const subject = `udc.ingest.${tenant.replace(/[.\s*>]/g, '_')}`;
    await Promise.all([...byPartition].map(async ([partition, items]) => {
      for (let i = 0; i < items.length; i += MAX_BATCH) {
        const job = { tenant, updates: items.slice(i, i + MAX_BATCH), allowedTypes };
        await this.queue.publish(subject, job, { partition });
        this.stats.published += job.updates.length;
      }
    }));
  }

  async _apply(job) {
    const started = Date.now();
    try {
      const result = await this.entities.ingestBatch(job.tenant, job.updates, { allowedTypes: job.allowedTypes });
      // Moving average of the time to apply one batch (observability: /ingest/stats)
      const ms = Date.now() - started;
      this.stats.applyMsAvg = this.stats.applyMsAvg ? Math.round(this.stats.applyMsAvg * 0.9 + ms * 0.1) : ms;
      this.stats.batches = (this.stats.batches || 0) + 1;
      this.stats.written += result.written;
      this.stats.skipped += result.skipped;
      this.stats.rejected += result.rejected.length;
      if (result.rejected.length) {
        this.stats.lastError = `${result.rejected[0].id}: ${result.rejected[0].error}`;
        logger.warn(`Ingest: ${result.rejected.length} updates rejected in tenant '${job.tenant}', e.g. ${this.stats.lastError}`);
      }
      return { ack: true };
    } catch (error) {
      // Tenant deleted: retrying cannot succeed
      if (error.code === '23503') return { drop: true };
      this.stats.lastError = error.message;
      logger.error(`Ingest batch failed, will retry: ${error.message}`);
      return { retryInMs: 2000 };
    }
  }

  async stop() {
    clearInterval(this.timer);
    await this.flush();
  }
}

module.exports = IngestService;
module.exports.PARTITIONS = PARTITIONS;
