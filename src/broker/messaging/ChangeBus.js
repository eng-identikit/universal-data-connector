const EventEmitter = require('events');
const logger = require('../../utils/logger');

const SUBJECT_PREFIX = 'udc.changes';

/**
 * Fan-out of entity changes to every broker instance.
 *
 * publish() is called by the instance that committed the write; every
 * instance (itself included) receives the change as a 'change' event:
 *   { tenant, op: create|update|delete, entity, previous, origin }
 *
 * With NATS the transport is core pub/sub (low latency, at-most-once): the
 * durable guarantee lives in the notification queue, not here. Without NATS
 * changes are delivered in-process.
 *
 * Also carries 'control' messages (e.g. subscription reloads) between instances.
 */
class ChangeBus extends EventEmitter {
  constructor(natsClient, instanceId) {
    super();
    this.setMaxListeners(50);
    this.nats = natsClient;
    this.instanceId = instanceId;
    this.subscriptions = [];
  }

  start() {
    if (!this.nats.enabled) return;
    const attach = () => {
      const nc = this.nats.nc;
      this.subscriptions.push(nc.subscribe(`${SUBJECT_PREFIX}.>`, { callback: (err, msg) => this._onMessage(err, msg, 'change') }));
      this.subscriptions.push(nc.subscribe('udc.control', { callback: (err, msg) => this._onMessage(err, msg, 'control') }));
    };
    if (this.nats.connected) attach();
    else this.nats.once('connected', attach);
  }

  _onMessage(err, msg, event) {
    if (err) return logger.error(`ChangeBus ${event} error: ${err.message}`);
    try {
      this.emit(event, this.nats.decode(msg.data));
    } catch (error) {
      logger.error(`ChangeBus: invalid ${event} message: ${error.message}`);
    }
  }

  publish(change) {
    const message = { ...change, origin: this.instanceId };
    if (this.nats.connected) {
      try {
        this.nats.nc.publish(`${SUBJECT_PREFIX}.${change.tenant}`, this.nats.encode(message));
        return;
      } catch (error) {
        logger.error(`ChangeBus publish failed, delivering locally: ${error.message}`);
      }
    }
    setImmediate(() => this.emit('change', message));
  }

  /** Broadcast a control message, e.g. { kind: 'subscriptions', tenant, id }. */
  control(message) {
    const payload = { ...message, origin: this.instanceId };
    if (this.nats.connected) this.nats.nc.publish('udc.control', this.nats.encode(payload));
    else setImmediate(() => this.emit('control', payload));
  }

  stop() {
    this.subscriptions.forEach(s => s.unsubscribe());
    this.subscriptions = [];
  }
}

module.exports = ChangeBus;
