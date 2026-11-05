const nats = require('nats');
const logger = require('../../utils/logger');

// Messages fetched (and merged) per cycle by a serial partition consumer
const SERIAL_FETCH = 20;

/**
 * Durable work queue with at-least-once processing.
 *
 *   publish(subject, job, { id })    enqueue (id deduplicates on JetStream)
 *   consume(handler, opts)           handler(job, { attempt }) resolves to
 *                                      { ack: true } | { retryInMs } | { drop: true }
 *
 * JetStreamQueue shares a durable consumer between instances (work
 * distribution); MemoryQueue is the single-process fallback used without NATS.
 *
 * Partitioned queues (options.partitions = P, publish with { partition }) process
 * the jobs of one partition strictly one at a time across all instances, while
 * different partitions run in parallel. Used by ingest so that concurrent
 * batches never lock the same entity rows.
 */

class JetStreamQueue {
  constructor(natsClient, { stream, durable, ackWaitMs = 30000, partitions = 0 }) {
    this.nats = natsClient;
    this.stream = stream;
    this.durable = durable;
    this.ackWaitMs = ackWaitMs;
    this.partitions = partitions;
    this.stopped = false;
    this.iterators = [];
    // Cluster: which partitions this instance consumes (lease owner); all when unset
    this.partitionFilter = null;
    this.active = new Map(); // partition → promise of the group being applied
    this.inFlight = new Set(); // unpartitioned: promises of messages being processed
  }

  _owns(partition) {
    return !this.partitionFilter || this.partitionFilter(partition);
  }

  /**
   * Resolves once no group of the partition is being applied here (or after
   * timeoutMs). Called after the partition was given up and before its lease is
   * released, so the next owner never runs concurrently with this one.
   */
  async whenIdle(partition, timeoutMs = 10000) {
    const running = this.active.get(partition);
    if (!running) return;
    let timer;
    await Promise.race([running, new Promise(r => { timer = setTimeout(r, timeoutMs); })]);
    clearTimeout(timer);
  }

  setPartitionFilter(filter) {
    this.partitionFilter = filter;
  }

  /** subject must be '<prefix>.<token>'; partitioned queues append '.<partition>'. */
  async publish(subject, job, { id, partition } = {}) {
    const target = this.partitions ? `${subject}.${partition % this.partitions}` : subject;
    await this.nats.js.publish(target, this.nats.encode(job), id ? { msgID: id } : undefined);
  }

  /**
   * merge (partitioned queues): jobs → jobs, combines the jobs fetched together
   * so that a backlog is applied in fewer, larger batches.
   */
  async consume(handler, { concurrency = 50, batch = 200, merge = null } = {}) {
    if (!this.partitions) {
      await this._consumer(this.durable, undefined, handler, { concurrency, batch, maxAckPending: Math.max(concurrency * 4, batch) });
      return;
    }
    // An unpartitioned consumer from an older version would overlap the filtered ones
    await this.nats.jsm.consumers.delete(this.stream, this.durable).catch(() => {});
    const prefix = this.stream === 'UDC_INGEST' ? 'udc.ingest' : this.durable;
    for (let p = 0; p < this.partitions; p++) {
      await this._serialConsumer(`${this.durable}-p${p}`, `${prefix}.*.${p}`, handler, p, merge);
    }
  }

  /**
   * Serial processing per partition: fetch up to SERIAL_FETCH pending messages,
   * merge them, apply, ack, fetch again. A partition is consumed by a single
   * instance (lease owner in a cluster), so its jobs never run concurrently and
   * keep their order. The more backlog, the larger the merged batches: the fixed
   * cost per cycle (round trips, event-loop latency) is amortized.
   */
  async _serialConsumer(durable, filterSubject, handler, partition, merge) {
    const config = {
      durable_name: durable,
      ack_policy: nats.AckPolicy.Explicit,
      ack_wait: nats.nanos(this.ackWaitMs),
      max_ack_pending: SERIAL_FETCH,
      filter_subject: filterSubject
    };
    try {
      await this.nats.jsm.consumers.add(this.stream, config);
    } catch (error) {
      // Created by an older version with other limits
      await this.nats.jsm.consumers.update(this.stream, durable, { ack_wait: config.ack_wait, max_ack_pending: SERIAL_FETCH });
    }
    const consumer = await this.nats.js.consumers.get(this.stream, durable);
    (async () => {
      while (!this.stopped) {
        if (!this._owns(partition)) {
          await new Promise(r => setTimeout(r, 500));
          continue;
        }
        try {
          // Wait for one message, then take whatever else is already pending (no wait)
          const first = await consumer.next({ expires: 5000 });
          if (!first) continue;
          const fetched = [first];
          const more = this.nats.js.fetch(this.stream, durable, { batch: SERIAL_FETCH - 1, no_wait: true });
          for await (const msg of more) fetched.push(msg);
          // The partition may have been given up (or the queue stopped) while waiting
          if (this.stopped || !this._owns(partition)) {
            fetched.forEach(m => m.nak());
            continue;
          }
          const group = this._processGroup(fetched, handler, merge);
          this.active.set(partition, group);
          try {
            await group;
          } finally {
            this.active.delete(partition);
          }
        } catch (error) {
          if (this.stopped) break;
          logger.error(`WorkQueue ${durable}: fetch error, retrying: ${error.message}`);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    })();
  }

  async _consumer(durable, filterSubject, handler, { concurrency, batch, maxAckPending }) {
    await this.nats.jsm.consumers.add(this.stream, {
      durable_name: durable,
      ack_policy: nats.AckPolicy.Explicit,
      ack_wait: nats.nanos(this.ackWaitMs),
      max_ack_pending: maxAckPending,
      ...(filterSubject && { filter_subject: filterSubject })
    });
    const consumer = await this.nats.js.consumers.get(this.stream, durable);
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
          const iterator = await consumer.consume({ max_messages: batch });
          this.iterators.push(iterator);
          for await (const msg of iterator) {
            if (running >= concurrency) await new Promise(resolve => waiters.push(resolve));
            running++;
            const processing = this._process(msg, handler).finally(() => {
              this.inFlight.delete(processing);
              release();
            });
            this.inFlight.add(processing);
          }
        } catch (error) {
          if (this.stopped) break;
          logger.error(`WorkQueue ${this.stream}: consumer error, restarting: ${error.message}`);
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    })();
  }

  /** Applies fetched messages (merged when possible) and settles them all together. */
  async _processGroup(msgs, handler, merge) {
    const decoded = [];
    for (const msg of msgs) {
      try {
        decoded.push({ msg, job: this.nats.decode(msg.data) });
      } catch {
        msg.term();
      }
    }
    if (!decoded.length) return;
    const keepAlive = setInterval(() => decoded.forEach(d => d.msg.working()), Math.max(1000, this.ackWaitMs / 2));
    try {
      const jobs = merge ? merge(decoded.map(d => d.job)) : decoded.map(d => d.job);
      const attempt = Math.max(...decoded.map(d => d.msg.info.redeliveryCount));
      let retryInMs;
      for (const job of jobs) {
        const result = await handler(job, { attempt });
        if (result?.retryInMs !== undefined) retryInMs = Math.max(retryInMs ?? 0, result.retryInMs);
      }
      if (retryInMs !== undefined) {
        decoded.forEach(d => d.msg.nak(retryInMs));
      } else {
        decoded.slice(0, -1).forEach(d => d.msg.ack());
        // Confirmed ack on the last one: the next fetch must not race the acks
        await decoded[decoded.length - 1].msg.ackAck().catch(() => decoded[decoded.length - 1].msg.ack());
      }
    } catch (error) {
      logger.error(`WorkQueue ${this.stream}: handler failed: ${error.message}`);
      decoded.forEach(d => d.msg.nak(5000));
    } finally {
      clearInterval(keepAlive);
    }
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

  /** Backlog summed over the consumers (all partitions). */
  async info() {
    const names = this.partitions
      ? Array.from({ length: this.partitions }, (_, p) => `${this.durable}-p${p}`)
      : [this.durable];
    try {
      const infos = await Promise.all(names.map(n => this.nats.jsm.consumers.info(this.stream, n)));
      return {
        pending: infos.reduce((n, c) => n + c.num_pending, 0),
        inFlight: infos.reduce((n, c) => n + c.num_ack_pending, 0),
        redelivered: infos.reduce((n, c) => n + c.num_redelivered, 0),
        ...(this.partitions && { partitions: this.partitions })
      };
    } catch {
      return null;
    }
  }

  /** Stops fetching and resolves once the work in progress has been settled (max 10 s). */
  async stop() {
    this.stopped = true;
    this.iterators.forEach(it => it.stop());
    let timer;
    await Promise.race([
      Promise.allSettled([...this.active.values(), ...this.inFlight]),
      new Promise(r => { timer = setTimeout(r, 10000); })
    ]);
    clearTimeout(timer);
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
    this.busy = new Set(); // partitions with a job running
    this.processing = new Set();
    this.stopped = false;
  }

  // Single process: no partition is shared with another instance
  async whenIdle() {}

  async publish(subject, job, { id, partition } = {}) {
    if (id) {
      if (this.seen.has(id)) return;
      this.seen.add(id);
      if (this.seen.size > 100000) this.seen.clear();
    }
    this.items.push({ job, attempt: 1, partition });
    this._pump();
  }

  async consume(handler, { concurrency = 50 } = {}) {
    this.handler = handler;
    this.concurrency = concurrency;
    this._pump();
  }

  _pump() {
    while (this.handler && !this.stopped && this.running < this.concurrency) {
      // Next job whose partition is idle (jobs without partition never wait)
      const index = this.items.findIndex(i => i.partition === undefined || !this.busy.has(i.partition));
      if (index === -1) return;
      const [item] = this.items.splice(index, 1);
      this.running++;
      if (item.partition !== undefined) this.busy.add(item.partition);
      const processing = this._process(item).finally(() => {
        this.running--;
        this.processing.delete(processing);
        if (item.partition !== undefined) this.busy.delete(item.partition);
        this._pump();
      });
      this.processing.add(processing);
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
        this.items.push({ job: item.job, attempt: item.attempt + 1, partition: item.partition });
        this._pump();
      }, result.retryInMs);
      if (timer.unref) timer.unref();
      this.timers.add(timer);
    }
  }

  async info() {
    return { pending: this.items.length, inFlight: this.running, retrying: this.timers.size };
  }

  async stop() {
    this.stopped = true;
    this.timers.forEach(clearTimeout);
    let timer;
    await Promise.race([Promise.allSettled([...this.processing]), new Promise(r => { timer = setTimeout(r, 10000); })]);
    clearTimeout(timer);
  }
}

module.exports = { JetStreamQueue, MemoryQueue };
