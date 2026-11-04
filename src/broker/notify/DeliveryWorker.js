const logger = require('../../utils/logger');

const MAX_BACKOFF_MS = 5 * 60 * 1000;

/**
 * Consumes the notification queue and sends each notification through its
 * subscription channel. Failures are retried with exponential backoff
 * (delivery.backoffMs × 2^attempt, capped at 5 min, with jitter); after
 * delivery.maxRetries the notification goes to the dead-letter queue.
 * Notifications of deleted or paused subscriptions are dropped.
 */
class DeliveryWorker {
  constructor({ queue, engine, channels, subscriptions, concurrency = 50 }) {
    this.queue = queue;
    this.engine = engine;
    this.channels = channels;
    this.store = subscriptions;
    this.concurrency = concurrency;
  }

  start() {
    return this.queue.consume((notification, { attempt }) => this.deliver(notification, attempt), { concurrency: this.concurrency });
  }

  async deliver(notification, attempt) {
    const sub = this.engine.get(notification.subscriptionId);
    if (!sub || !sub.enabled) return { drop: true };

    const started = Date.now();
    try {
      await this.channels.send(sub, notification);
      this.engine.recordDelivery(sub.id, { ok: true, latencyMs: Date.now() - started });
      return { ack: true };
    } catch (error) {
      const { maxRetries = 10, backoffMs = 1000 } = sub.delivery || {};
      const dead = attempt > maxRetries;
      this.engine.recordDelivery(sub.id, { ok: false, error: error.message, dead });
      if (dead) {
        logger.warn(`Subscription ${sub.id}: notification ${notification.id} moved to DLQ after ${attempt} attempts: ${error.message}`);
        await this.store.addDead(sub.tenant, sub.id, notification, error.message, attempt);
        return { ack: true };
      }
      const delay = Math.min(backoffMs * 2 ** (attempt - 1), MAX_BACKOFF_MS);
      return { retryInMs: Math.round(delay * (0.8 + Math.random() * 0.4)) };
    }
  }

  /** Sends a notification immediately, outside the queue (subscription "test" button). */
  async test(sub, notification) {
    const started = Date.now();
    try {
      await this.channels.send(sub, notification);
      return { ok: true, latencyMs: Date.now() - started };
    } catch (error) {
      return { ok: false, latencyMs: Date.now() - started, error: error.message };
    }
  }
}

module.exports = DeliveryWorker;
