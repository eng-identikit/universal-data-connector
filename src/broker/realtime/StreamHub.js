const WebSocket = require('ws');
const logger = require('../../utils/logger');
const { resolveCaller, resolveTenant } = require('../auth/middleware');
const { globRegex } = require('../subscriptions/SubscriptionEngine');

const PATH = '/api/v1/stream';
const MAX_BUFFERED_BYTES = 5 * 1024 * 1024;
const MAX_CLIENT_SUBSCRIPTIONS = 50;
const PING_MS = 30000;

/**
 * Authenticated WebSocket stream on /api/v1/stream.
 *
 * Connect: ws://host/api/v1/stream?access_token=<jwt>&tenant=<id>   (or api_key=<key>)
 * Client → server:
 *   { type: 'subscribe', id, entities: { types?, idPattern?, ids? } }   live entity changes
 *   { type: 'subscribe', id, subscription: '<uuid>' }                  notifications of a websocket-channel subscription
 *   { type: 'unsubscribe', id }
 * Server → client:
 *   { type: 'welcome', tenant, role }
 *   { type: 'entity', sub, op, entity }
 *   { type: 'notification', sub, notification }
 *   { type: 'error', message }
 *
 * The connection is closed with code 4001 when the access token expires;
 * clients reconnect with a fresh token.
 */
class StreamHub {
  constructor({ services, bus, natsClient }) {
    this.services = services;
    this.bus = bus;
    this.nats = natsClient;
    this.wss = null;
    this.clients = new Set();
    this.dropped = 0;
    this.pingTimer = null;
  }

  attach(server) {
    this.wss = new WebSocket.Server({ server, path: PATH, maxPayload: 64 * 1024 });
    this.wss.on('connection', (ws, req) => this._onConnection(ws, req).catch(error => {
      this._send(ws, { type: 'error', message: error.message });
      ws.close(error.status === 401 ? 4001 : 4003, error.message.slice(0, 120));
    }));

    this.bus.on('change', (change) => this._onChange(change));

    // websocket-channel notifications delivered on another instance reach clients connected here
    if (this.nats.enabled) {
      const subscribe = () => this.nats.nc.subscribe('udc.ws.>', {
        callback: (err, msg) => {
          if (err) return;
          const { tenant, subscriptionId, notification } = this.nats.decode(msg.data);
          this._deliverLocal(tenant, subscriptionId, notification);
        }
      });
      if (this.nats.connected) subscribe();
      else this.nats.once('connected', subscribe);
    }

    this.pingTimer = setInterval(() => {
      for (const c of this.clients) {
        if (!c.alive) { c.ws.terminate(); continue; }
        c.alive = false;
        c.ws.ping();
      }
    }, PING_MS);
    if (this.pingTimer.unref) this.pingTimer.unref();
    logger.info(`Broker stream listening on ${PATH}`);
  }

  async _onConnection(ws, req) {
    const url = new URL(req.url, 'http://localhost');
    const auth = await resolveCaller(this.services, {
      token: url.searchParams.get('access_token'),
      apiKey: url.searchParams.get('api_key')
    });
    const ctx = await resolveTenant(this.services, auth, url.searchParams.get('tenant'), 'viewer');
    const client = { ws, ctx, subs: new Map(), alive: true, expiryTimer: null };
    this.clients.add(client);

    if (auth.tokenExp) {
      client.expiryTimer = setTimeout(() => ws.close(4001, 'Token expired'), Math.max(0, auth.tokenExp * 1000 - Date.now()));
    }
    ws.on('pong', () => { client.alive = true; });
    ws.on('close', () => {
      clearTimeout(client.expiryTimer);
      this.clients.delete(client);
    });
    ws.on('message', (data) => this._onMessage(client, data).catch(error => this._send(ws, { type: 'error', message: error.message })));
    this._send(ws, { type: 'welcome', tenant: ctx.tenant, role: ctx.role });
  }

  async _onMessage(client, data) {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      throw new Error('Invalid JSON message');
    }
    if (msg.type === 'ping') return this._send(client.ws, { type: 'pong' });
    if (msg.type === 'unsubscribe') {
      client.subs.delete(String(msg.id));
      return;
    }
    if (msg.type !== 'subscribe' || msg.id === undefined) throw new Error("Expected { type: 'subscribe' | 'unsubscribe', id }");
    if (client.subs.size >= MAX_CLIENT_SUBSCRIPTIONS && !client.subs.has(String(msg.id))) throw new Error('Too many stream subscriptions');

    if (msg.subscription) {
      // Throws 404 if the subscription is not in the client's tenant
      await this.services.subscriptions.get(client.ctx.tenant, msg.subscription);
      client.subs.set(String(msg.id), { kind: 'notifications', subscriptionId: msg.subscription });
    } else {
      const f = msg.entities || {};
      client.subs.set(String(msg.id), {
        kind: 'entities',
        types: Array.isArray(f.types) && f.types.length ? f.types : null,
        ids: Array.isArray(f.ids) && f.ids.length ? new Set(f.ids) : null,
        idPattern: typeof f.idPattern === 'string' && f.idPattern ? globRegex(f.idPattern) : null
      });
    }
    this._send(client.ws, { type: 'subscribed', id: msg.id });
  }

  _onChange({ tenant, op, entity, previous }) {
    const target = op === 'delete' ? previous : entity;
    for (const client of this.clients) {
      if (client.ctx.tenant !== tenant) continue;
      if (client.ctx.allowedTypes && !client.ctx.allowedTypes.includes(target.type)) continue;
      for (const [id, s] of client.subs) {
        if (s.kind !== 'entities') continue;
        if (s.types && !s.types.includes(target.type)) continue;
        if (s.ids && !s.ids.has(target.id)) continue;
        if (s.idPattern && !s.idPattern.test(target.id)) continue;
        this._send(client.ws, { type: 'entity', sub: id, op, entity: target });
      }
    }
  }

  /** Called by the websocket channel; fans out to clients on every instance. */
  publishNotification(tenant, subscriptionId, notification) {
    if (this.nats.connected) {
      this.nats.nc.publish(`udc.ws.${tenant.replace(/[.\s*>]/g, '_')}.${subscriptionId}`, this.nats.encode({ tenant, subscriptionId, notification }));
    } else {
      this._deliverLocal(tenant, subscriptionId, notification);
    }
  }

  _deliverLocal(tenant, subscriptionId, notification) {
    for (const client of this.clients) {
      if (client.ctx.tenant !== tenant) continue;
      for (const [id, s] of client.subs) {
        if (s.kind === 'notifications' && s.subscriptionId === subscriptionId) {
          this._send(client.ws, { type: 'notification', sub: id, notification });
        }
      }
    }
  }

  _send(ws, message) {
    if (ws.readyState !== WebSocket.OPEN) return;
    // Slow consumer: drop rather than buffer without limit
    if (ws.bufferedAmount > MAX_BUFFERED_BYTES) {
      this.dropped++;
      return;
    }
    ws.send(JSON.stringify(message));
  }

  stats() {
    return { clients: this.clients.size, dropped: this.dropped };
  }

  close() {
    clearInterval(this.pingTimer);
    if (this.wss) this.wss.close();
  }
}

module.exports = StreamHub;
