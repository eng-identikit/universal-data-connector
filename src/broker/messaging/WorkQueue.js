const nats = require('nats');
const logger = require('../../utils/logger');

/**
 * Durable work queue with at-least-once processing.
 *
 *   publish(subject, job, { id })    enqueue (id deduplicates on JetStream)
 *   consume(handler, opts)           handler(job, { attempt }) resolves to
 *                                      { ack: true } | { retryInMs } | { drop: true }
 *
 * JetStreamQueue shares a durable consumer between instances (work
 * distribution); MemoryQueue is the single-process fallback used without NATS.
 */

class JetStreamQueue {
  constructor(natsClient, { stream, durable, ackWaitMs = 30000 }) {
    this.nats = natsClient;
    this.stream = stream;
    this.durable = durable;
    this.ackWaitMs = ackWaitMs;
    this.stopped = false;
    this.iterator = null;
  }

  async publish(subject, job, { id } = {}) {
    await this.nats.js.publish(subject, this.nats.encode(job), id ? { msgID: id } : undefined);
  }

  async consume(handler, { concurrency = 50, batch = 200 } = {}) {
    await this.nats.jsm.consumers.add(this.stream, {
      durable_name: this.durable,
      ack_policy: nats.AckPolicy.Explicit,
      ack_wait: nats.nanos(this.ackWaitMs),
      max_ack_pending: Math.max(concurrency * 4, batch)
    });
    const consumer = await this.nats.js.consumers.get(this.stream, this.durable);
    let running = 0;
    const waiters = [];
    const release = () => {
      running--;
      const next = waiters.shift();
      if (next) next();
    };

    (async () => {
      while (!this.stopped) {
        try {
          this.iterator = await consumer.consume({ max_messages: batch });
          for await (const msg of this.iterator) {
            if (running >= concurrency) await new Promise(resolve => waiters.push(resolve));
            running++;
            this._process(msg, handler).finally(release);
          }
        } catch (error) {
          if (this.stopped) break;
          logger.error(`WorkQueue ${this.stream}: consumer error, restarting: ${error.message}`);
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    })();
  }

  async _process(msg, handler) {
    let job;
    try {
      job = this.nats.decode(msg.data);
    } catch {
      msg.term();
      return;
    }
    // Keep the message from being redelivered while a slow handler runs
    const keepAlive = setInterval(() => msg.working(), Math.max(1000, this.ackWaitMs / 2));
    try {
      const result = await handler(job, { attempt: msg.info.redeliveryCount });
      if (result?.retryInMs !== undefined) msg.nak(result.retryInMs);
      else if (result?.drop) msg.term();
      else msg.ack();
    } catch (error) {
      logger.error(`WorkQueue ${this.stream}: handler failed: ${error.message}`);
      msg.nak(5000);
    } finally {
      clearInterval(keepAlive);
    }
  }

  async info() {
    try {
      const c = await this.nats.jsm.consumers.info(this.stream, this.durable);
      return { pending: c.num_pending, inFlight: c.num_ack_pending, redelivered: c.num_redelivered };
    } catch {
      return null;
    }
  }

  stop() {
    this.stopped = true;
    if (this.iterator) this.iterator.stop();
  }
}

class MemoryQueue {
  constructor({ name }) {
    this.name = name;
    this.items = [];
    this.handler = null;
    this.running = 0;
    this.concurrency = 50;
    this.timers = new Set();
    this.seen = new Set();
    this.stopped = false;
  }

  async publish(subject, job, { id } = {}) {
    if (id) {
      if (this.seen.has(id)) return;
      this.seen.add(id);
      if (this.seen.size > 100000) this.seen.clear();
    }
    this.items.push({ job, attempt: 1 });
    this._pump();
  }

  async consume(handler, { concurrency = 50 } = {}) {
    this.handler = handler;
    this.concurrency = concurrency;
    this._pump();
  }

  _pump() {
    while (this.handler && !this.stopped && this.running < this.concurrency && this.items.length) {
      const item = this.items.shift();
      this.running++;
      this._process(item).finally(() => {
        this.running--;
        this._pump();
      });
    }
  }

  async _process(item) {
    let result;
    try {
      result = await this.handler(item.job, { attempt: item.attempt });
    } catch (error) {
      logger.error(`MemoryQueue ${this.name}: handler failed: ${error.message}`);
      result = { retryInMs: 5000 };
    }
    if (result?.retryInMs !== undefined && !this.stopped) {
      const timer = setTimeout(() => {
        this.timers.delete(timer);
        this.items.push({ job: item.job, attempt: item.attempt + 1 });
        this._pump();
      }, result.retryInMs);
      if (timer.unref) timer.unref();
      this.timers.add(timer);
    }
  }

  async info() {
    return { pending: this.items.length, inFlight: this.running, retrying: this.timers.size };
  }

  stop() {
    this.stopped = true;
    this.timers.forEach(clearTimeout);
  }
}

module.exports = { JetStreamQueue, MemoryQueue };
