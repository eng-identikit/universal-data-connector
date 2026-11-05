const logger = require('../../utils/logger');

const MAX_BACKOFF_MS = 5 * 60 * 1000;
const RETRY_POLL_MS = 1000;
// A claimed retry stays reserved this long (longer than any channel timeout)
const RETRY_CLAIM_MS = 5 * 60 * 1000;
// Retries claimed per subscription and poll: one dead endpoint cannot take every slot
const RETRIES_PER_SUBSCRIPTION = 5;

/**
 * Consumes the notification queue and sends each notification through its
 * subscription channel.
 *
 * A failed attempt is not retried on the queue: the notification moves to the
 * notification_retries table (due after delivery.backoffMs × 2^attempt, capped
 * at 5 min, with jitter) and the queue message is acknowledged. A slow or dead
 * endpoint therefore never holds queue capacity that other subscriptions need.
 * Every instance polls the table for due retries. After delivery.maxRetries the
 * notification goes to the dead-letter queue.
 *
 * While a subscription's endpoint is failing, its new notifications go straight
 * to the retry table instead of waiting for a timeout on the queue.
 *
 * Notifications of deleted or paused subscriptions are dropped.
 */
class DeliveryWorker {
  constructor({ queue, engine, channels, subscriptions, concurrency = 50, retryConcurrency = 20 }) {
    this.queue = queue;
    this.engine = engine;
    this.channels = channels;
    this.store = subscriptions;
    this.concurrency = concurrency;
    this.retryConcurrency = retryConcurrency;
    this.failingUntil = new Map(); // subscription id → ms
    this.retrying = 0;
    this.polling = false;
    this.pollTimer = null;
    this.stopped = false;
  }

  async start() {
    this.stopped = false;
    this.pollTimer = setInterval(() => this._poll(), RETRY_POLL_MS);
    if (this.pollTimer.unref) this.pollTimer.unref();
    await this.queue.consume((notification, { attempt }) => this.deliver(notification, attempt), { concurrency: this.concurrency });
  }

  stop() {
    this.stopped = true;
    clearInterval(this.pollTimer);
  }

  /** Queue handler: first attempt (attempt > 1 only after a crash mid-delivery). */
  async deliver(notification, attempt = 1) {
    // While the engine (re)loads a subscription it is read from the database
    const sub = await this.engine.lookup(notification.subscriptionId);
    if (!sub || !sub.enabled) return { drop: true };
    try {
      if (this._failing(sub.id)) {
        // Queue behind the endpoint's retries without spending an attempt
        await this._schedule(sub, notification, attempt - 1, 'endpoint failing, queued for retry', this._backoff(sub, 1));
        return { ack: true };
      }
      await this._attempt(sub, notification, attempt);
      return { ack: true };
    } catch (error) {
      // The retry could not be stored (database unavailable): retry on the queue instead
      logger.error(`Subscription ${sub.id}: retry scheduling failed: ${error.message}`);
      return { retryInMs: 5000 };
    }
  }

  /**
   * One delivery attempt. Resolves to 'ok', 'retry' (stored in the retry table)
   * or 'dead' (moved to the DLQ); rejects only if the outcome could not be stored.
   */
  async _attempt(sub, notification, attempt) {
    const started = Date.now();
    try {
      await this.channels.send(sub, notification);
      this.engine.recordDelivery(sub.id, { ok: true, latencyMs: Date.now() - started });
      this.failingUntil.delete(sub.id);
      return 'ok';
    } catch (error) {
      const { maxRetries = 10 } = sub.delivery || {};
      const dead = attempt > maxRetries;
      this.engine.recordDelivery(sub.id, { ok: false, error: error.message, dead });
      const delay = this._backoff(sub, attempt);
      this.failingUntil.set(sub.id, Date.now() + delay);
      if (dead) {
        logger.warn(`Subscription ${sub.id}: notification ${notification.id} moved to DLQ after ${attempt} attempts: ${error.message}`);
        await this.store.addDead(sub.tenant, sub.id, notification, error.message, attempt);
        return 'dead';
      }
      await this._schedule(sub, notification, attempt, error.message, delay);
      return 'retry';
    }
  }

  _failing(id) {
    const until = this.failingUntil.get(id);
    if (until === undefined) return false;
    if (until > Date.now()) return true;
    this.failingUntil.delete(id);
    return false;
  }

  _backoff(sub, attempt) {
    const { backoffMs = 1000 } = sub.delivery || {};
    const delay = Math.min(backoffMs * 2 ** (Math.max(1, attempt) - 1), MAX_BACKOFF_MS);
    return Math.round(delay * (0.8 + Math.random() * 0.4));
  }

  _schedule(sub, notification, attempts, error, delayMs) {
    return this.store.scheduleRetry({
      tenant: sub.tenant,
      subscriptionId: sub.id,
      notification,
      attempts,
      error,
      dueAt: new Date(Date.now() + delayMs)
    });
  }

  async _poll() {
    if (this.polling || this.stopped) return;
    const free = this.retryConcurrency - this.retrying;
    if (free <= 0) return;
    this.polling = true;
    try {
      const due = await this.store.claimRetries(free, RETRIES_PER_SUBSCRIPTION, RETRY_CLAIM_MS);
      for (const row of due) {
        this.retrying++;
        this._retry(row)
          .catch(e => logger.error(`Retry of notification ${row.id} failed: ${e.message}`))
          .finally(() => { this.retrying--; });
      }
    } catch (error) {
      logger.error(`Delivery retry poll failed: ${error.message}`);
    } finally {
      this.polling = false;
    }
  }

  async _retry(row) {
    const sub = await this.engine.lookup(row.subscriptionId);
    if (!sub || !sub.enabled) return this.store.completeRetry(row.id);
    // 'retry' rescheduled the same row; otherwise it is done
    const outcome = await this._attempt(sub, row.notification, row.attempts + 1);
    if (outcome !== 'retry') await this.store.completeRetry(row.id);
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
