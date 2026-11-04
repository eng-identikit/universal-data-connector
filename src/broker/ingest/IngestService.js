const logger = require('../../utils/logger');

const FLUSH_MS = parseInt(process.env.BROKER_INGEST_FLUSH_MS || '200', 10);
const MAX_BATCH = 500;

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
    await this.queue.consume((job) => this._apply(job), { concurrency: 4, batch: 20 });
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
    for (let i = 0; i < updates.length; i += MAX_BATCH) {
      await this._publish({ tenant, updates: updates.slice(i, i + MAX_BATCH), allowedTypes });
    }
  }

  async flush() {
    const buffer = this.buffer;
    if (!buffer.size) return;
    this.buffer = new Map();
    for (const [tenant, entities] of buffer) {
      const updates = [...entities.values()];
      for (let i = 0; i < updates.length; i += MAX_BATCH) {
        await this._publish({ tenant, updates: updates.slice(i, i + MAX_BATCH), allowedTypes: null })
          .catch(error => logger.error(`Ingest publish failed (${updates.length} updates dropped): ${error.message}`));
      }
    }
  }

  async _publish(job) {
    await this.queue.publish(`udc.ingest.${job.tenant.replace(/[.\s*>]/g, '_')}`, job);
    this.stats.published += job.updates.length;
  }

  async _apply(job) {
    try {
      const result = await this.entities.ingestBatch(job.tenant, job.updates, { allowedTypes: job.allowedTypes });
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
