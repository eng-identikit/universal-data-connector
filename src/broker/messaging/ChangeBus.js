const EventEmitter = require('events');
const logger = require('../../utils/logger');

const SUBJECT_PREFIX = 'udc.changes';
// Changes per NATS message; keeps payloads well below the 1 MB default limit
const BATCH_CHUNK = 100;

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
    let data;
    try {
      data = this.nats.decode(msg.data);
    } catch (error) {
      return logger.error(`ChangeBus: invalid ${event} message: ${error.message}`);
    }
    if (event !== 'change') return this.emit(event, data);
    // Batched changes (ingest) arrive as { batch: [...] }
    const changes = Array.isArray(data.batch) ? data.batch : [data];
    changes.forEach(c => this.emit('change', expand(c)));
  }

  publish(change) {
    this.publishMany([change]);
  }

  /**
   * One NATS message per chunk of changes: per-message overhead, not payload
   * size, limits fan-out at high update rates (every instance decodes everything).
   */
  publishMany(changes) {
    if (this.nats.connected) {
      try {
        const messages = changes.map(c => ({ ...compact(c), origin: this.instanceId }));
        for (let i = 0; i < messages.length; i += BATCH_CHUNK) {
          const chunk = messages.slice(i, i + BATCH_CHUNK);
          const payload = chunk.length === 1 ? chunk[0] : { batch: chunk };
          this.nats.nc.publish(`${SUBJECT_PREFIX}.${chunk[0].tenant}`, this.nats.encode(payload));
        }
        return;
      } catch (error) {
        logger.error(`ChangeBus publish failed, delivering locally: ${error.message}`);
      }
    }
    setImmediate(() => changes.forEach(c => this.emit('change', { ...c, origin: this.instanceId })));
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

/**
 * Wire format of an update: the new entity plus only the previous value of the
 * attributes that changed (null = the attribute did not exist before), instead
 * of the whole previous entity. Every instance decodes every change, so the
 * payload size matters at high rates.
 */
function compact(change) {
  if (change.op !== 'update' || !change.previous || !change.entity) return change;
  const { entity, previous } = change;
  const before = {};
  for (const [name, attr] of Object.entries(previous.attrs)) {
    if (entity.attrs[name] !== attr && JSON.stringify(entity.attrs[name]) !== JSON.stringify(attr)) before[name] = attr;
  }
  for (const name of Object.keys(entity.attrs)) if (!(name in previous.attrs)) before[name] = null;
  const out = { tenant: change.tenant, op: 'update', entity, before, prevVersion: previous.version };
  if (JSON.stringify(previous.location ?? null) !== JSON.stringify(entity.location ?? null)) out.prevLocation = previous.location ?? null;
  return out;
}

function expand(change) {
  if (!change.before) return change;
  const { entity, before } = change;
  const attrs = { ...entity.attrs };
  for (const [name, attr] of Object.entries(before)) {
    if (attr === null) delete attrs[name];
    else attrs[name] = attr;
  }
  const previous = { ...entity, version: change.prevVersion, attrs };
  if ('prevLocation' in change) {
    if (change.prevLocation) previous.location = change.prevLocation;
    else delete previous.location;
  }
  return { tenant: change.tenant, op: 'update', entity, previous, origin: change.origin };
}

module.exports = ChangeBus;
module.exports.compact = compact;
module.exports.expand = expand;
