const EventEmitter = require('events');
const nats = require('nats');
const logger = require('../../utils/logger');

const DAY_NS = nats.nanos(24 * 60 * 60 * 1000);

// JetStream streams used by the broker (created/updated on connect)
const STREAMS = [
  // Pending notifications, consumed once by the delivery workers of any instance
  { name: 'UDC_NOTIFY', subjects: ['udc.notify.>'], retention: nats.RetentionPolicy.Workqueue, max_age: 7 * DAY_NS },
  // High-rate attribute updates from connectors and /ingest, applied in batches
  { name: 'UDC_INGEST', subjects: ['udc.ingest.>'], retention: nats.RetentionPolicy.Workqueue, max_age: DAY_NS }
];

/**
 * Shared NATS connection for the broker. Connects in the background and
 * reconnects forever; emits 'connected' once streams are in place.
 * Without NATS_URL the broker runs single-instance with in-memory queues.
 */
class NatsClient extends EventEmitter {
  constructor(url = process.env.NATS_URL) {
    super();
    this.url = url;
    this.nc = null;
    this.js = null;
    this.jsm = null;
    this.codec = nats.JSONCodec();
  }

  get enabled() {
    return !!this.url;
  }

  get connected() {
    return !!this.nc && !this.nc.isClosed();
  }

  async connect() {
    if (!this.enabled) return;
    try {
      this.nc = await nats.connect({
        servers: this.url,
        name: `udc-broker-${process.pid}`,
        reconnect: true,
        maxReconnectAttempts: -1,
        waitOnFirstConnect: true
      });
      this.jsm = await this.nc.jetstreamManager();
      this.js = this.nc.jetstream();
      await this._ensureStreams();
      logger.info(`Broker connected to NATS at ${this.url}`);
      this._watchStatus();
      this.emit('connected');
    } catch (error) {
      logger.error(`Broker NATS connection failed: ${error.message}`);
      throw error;
    }
  }

  async _ensureStreams() {
    for (const stream of STREAMS) {
      const config = { ...stream, storage: nats.StorageType.File, discard: nats.DiscardPolicy.Old };
      try {
        await this.jsm.streams.info(stream.name);
        await this.jsm.streams.update(stream.name, config);
      } catch (error) {
        if (!/stream not found/i.test(error.message)) throw error;
        await this.jsm.streams.add(config);
      }
    }
  }

  async _watchStatus() {
    for await (const s of this.nc.status()) {
      if (s.type === 'disconnect') logger.warn('Broker NATS disconnected');
      if (s.type === 'reconnect') logger.info('Broker NATS reconnected');
    }
  }

  encode(value) {
    return this.codec.encode(value);
  }

  decode(data) {
    return this.codec.decode(data);
  }

  async close() {
    if (this.nc && !this.nc.isClosed()) await this.nc.drain().catch(() => {});
  }
}

module.exports = NatsClient;
