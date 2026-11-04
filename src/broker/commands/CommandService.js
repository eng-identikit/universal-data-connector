const logger = require('../../utils/logger');
const { badRequest, notFound } = require('../errors');

const COMMAND_TIMEOUT_MS = parseInt(process.env.BROKER_COMMAND_TIMEOUT_MS || '10000', 10);
const SUBJECT_PREFIX = 'udc.cmd';

/**
 * Commands: writing a value to a device through the connector that reads it.
 *
 * The attribute must be writable and describe the write call:
 *   "setpoint": { "type": "float", "value": 80, "writable": true,
 *     "command": { "connector": "opcua-plc1", "method": "writeNode", "args": ["ns=2;s=SP", "$value", "Double"] } }
 * "$value" in args is replaced by the commanded value. Only connector methods
 * whose name starts with "write" can be called.
 *
 * With NATS the command is sent as a request on udc.cmd.<connector>; only the
 * instance running that connector answers. Without NATS it runs in-process.
 * Lifecycle: pending → sent → succeeded | failed (error, e.g. timeout).
 */
class CommandService {
  constructor({ pool, entities, natsClient }) {
    this.pool = pool;
    this.entities = entities;
    this.nats = natsClient;
    this.engine = null;
    this.responder = null;
  }

  /** Connects the local connector engine and answers command requests for its connectors. */
  attach(engine) {
    this.engine = engine;
    if (!this.nats.enabled) return;
    const subscribe = () => {
      this.responder = this.nats.nc.subscribe(`${SUBJECT_PREFIX}.*`, {
        callback: async (err, msg) => {
          if (err) return;
          const request = this.nats.decode(msg.data);
          // Not running here: stay silent so the instance that runs it answers
          if (!this.engine?.connectors.get(request.connector)) return;
          try {
            await this._executeLocal(request);
            msg.respond(this.nats.encode({ ok: true }));
          } catch (error) {
            msg.respond(this.nats.encode({ ok: false, error: error.message }));
          }
        }
      });
    };
    if (this.nats.connected) subscribe();
    else this.nats.once('connected', subscribe);
  }

  /**
   * Creates and dispatches a command; resolves once it completed or after
   * waitMs (the command keeps running and its status is updated later).
   */
  async create(ctx, entityId, attrName, value, { createdBy, waitMs = 5000 } = {}) {
    const entity = await this.entities.get(ctx, entityId);
    const attr = entity.attrs[attrName];
    if (!attr) throw notFound(`Attribute '${attrName}' not found on entity '${entityId}'`);
    if (!attr.writable) throw badRequest(`Attribute '${attrName}' is not writable`);
    const target = validateTarget(attr.command);

    const { rows } = await this.pool.query(
      `INSERT INTO commands (tenant, entity_id, attr, value, connector, target, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [ctx.tenant, entityId, attrName, JSON.stringify(value ?? null), target.connector, target, createdBy || null]
    );
    const command = toCommand(rows[0]);
    const done = this._dispatch(command, target, value);
    const finished = await Promise.race([done, new Promise(r => setTimeout(() => r(null), Math.min(waitMs, COMMAND_TIMEOUT_MS + 1000)))]);
    return finished || { ...command, status: 'sent' };
  }

  async _dispatch(command, target, value) {
    await this._setStatus(command.id, 'sent');
    let error = null;
    try {
      const request = { connector: target.connector, method: target.method, args: target.args, value };
      if (this.nats.connected) {
        let reply;
        try {
          reply = this.nats.decode((await this.nats.nc.request(`${SUBJECT_PREFIX}.${natsToken(target.connector)}`, this.nats.encode(request), { timeout: COMMAND_TIMEOUT_MS })).data);
        } catch (e) {
          throw new Error(/timeout|no responders/i.test(e.message) || e.code === '503' ? `Connector '${target.connector}' is not running` : e.message);
        }
        if (!reply.ok) throw new Error(reply.error);
      } else {
        if (!this.engine?.connectors.get(target.connector)) throw new Error(`Connector '${target.connector}' is not running`);
        await withTimeout(this._executeLocal(request), COMMAND_TIMEOUT_MS);
      }
    } catch (e) {
      error = e.message;
      logger.warn(`Command ${command.id} on ${command.entityId}.${command.attr} failed: ${error}`);
    }
    return this._setStatus(command.id, error ? 'failed' : 'succeeded', error);
  }

  async _executeLocal({ connector: id, method, args, value }) {
    const connector = this.engine.connectors.get(id)?.connector;
    if (!connector) throw new Error(`Connector '${id}' is not running`);
    if (typeof connector[method] !== 'function') throw new Error(`Connector '${id}' has no method '${method}'`);
    const resolved = (args || ['$value']).map(a => (a === '$value' ? value : a));
    await connector[method](...resolved);
  }

  async _setStatus(id, status, error = null) {
    const { rows } = await this.pool.query(
      `UPDATE commands SET status = $2, error = $3,
              completed_at = CASE WHEN $2 IN ('succeeded', 'failed') THEN now() ELSE completed_at END
        WHERE id = $1 RETURNING *`,
      [id, status, error]
    );
    return rows[0] ? toCommand(rows[0]) : null;
  }

  async list(ctx, { entityId, status, limit } = {}) {
    const params = [ctx.tenant];
    const where = ['tenant = $1'];
    if (entityId) { params.push(entityId); where.push(`entity_id = $${params.length}`); }
    if (status) { params.push(status); where.push(`status = $${params.length}`); }
    params.push(Math.min(Number(limit) || 100, 1000));
    const { rows } = await this.pool.query(
      `SELECT * FROM commands WHERE ${where.join(' AND ')} ORDER BY created_at DESC LIMIT $${params.length}`, params
    );
    return rows.map(toCommand);
  }

  async get(ctx, id) {
    const { rows } = await this.pool.query('SELECT * FROM commands WHERE tenant = $1 AND id = $2', [ctx.tenant, id]);
    if (!rows.length) throw notFound(`Command '${id}' not found`);
    return toCommand(rows[0]);
  }

  close() {
    if (this.responder) this.responder.unsubscribe();
  }
}

function validateTarget(command) {
  if (!command || typeof command !== 'object') throw badRequest("Attribute has no 'command' configuration { connector, method, args }");
  const { connector, method, args } = command;
  if (typeof connector !== 'string' || !connector) throw badRequest("command.connector is required");
  if (typeof method !== 'string' || !/^write[A-Za-z]*$/.test(method)) throw badRequest("command.method must be a connector write method (e.g. writeNode, writeRegister)");
  if (args !== undefined && (!Array.isArray(args) || args.length > 10)) throw badRequest('command.args must be an array (max 10)');
  return { connector, method, args: args || ['$value'] };
}

function withTimeout(promise, ms) {
  return Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error(`Timed out after ${ms} ms`)), ms))]);
}

const natsToken = (s) => s.replace(/[.\s*>]/g, '_');

function toCommand(row) {
  return {
    id: row.id,
    entityId: row.entity_id,
    attr: row.attr,
    value: row.value,
    status: row.status,
    connector: row.connector,
    target: row.target,
    error: row.error,
    createdBy: row.created_by,
    createdAt: row.created_at.toISOString(),
    completedAt: row.completed_at ? row.completed_at.toISOString() : null
  };
}

module.exports = CommandService;
