const axios = require('axios');
const mqtt = require('mqtt');
const nats = require('nats');
const logger = require('../../utils/logger');

/**
 * Notification channels. send(sub, notification) resolves when the message
 * was accepted by the destination and rejects with a readable error otherwise.
 *
 * A notification is { id, subscriptionId, subscriptionName, tenant, reason, timestamp, entities }.
 * With notify.perEntity each entity is sent as its own message (one HTTP call,
 * one MQTT/NATS publish per entity); placeholders in topics/subjects are then
 * expanded per entity.
 */

/** Applies the attribute projection and the output format to the notification. */
function shape(sub, notification) {
  const { attrs = [], format = 'full' } = sub.notify;
  const entities = notification.entities.map(e => {
    const picked = attrs.length ? Object.fromEntries(Object.entries(e.attrs || {}).filter(([k]) => attrs.includes(k))) : (e.attrs || {});
    if (format === 'values') {
      const values = { id: e.id, type: e.type };
      for (const [k, a] of Object.entries(picked)) values[k] = a.type === 'relationship' ? a.target : a.value;
      return values;
    }
    return { ...e, attrs: picked };
  });
  return { ...notification, entities };
}

function messages(sub, notification) {
  const shaped = shape(sub, notification);
  if (!sub.notify.perEntity) return [{ entity: shaped.entities[0], body: shaped }];
  return shaped.entities.map(entity => ({ entity, body: { ...shaped, entities: [entity] } }));
}

function expand(template, sub, entity) {
  return template
    .replace(/\{tenant\}/g, sub.tenant)
    .replace(/\{subscription\}/g, sub.id)
    .replace(/\{entityId\}/g, entity ? entity.id : '_')
    .replace(/\{entityType\}/g, entity ? entity.type : '_');
}

// NATS subjects and MQTT topics: entity ids may contain '.', which NATS treats as a separator
const natsToken = (s) => s.replace(/[.\s*>]/g, '_');

class Channels {
  constructor({ natsClient, hub }) {
    this.natsClient = natsClient;
    this.hub = hub;
    this.mqttClients = new Map();
    this.natsConnections = new Map();
  }

  async send(sub, notification) {
    switch (sub.notify.channel) {
      case 'http': return this._http(sub, notification);
      case 'mqtt': return this._mqtt(sub, notification);
      case 'nats': return this._nats(sub, notification);
      case 'websocket': return this._websocket(sub, notification);
      default: throw new Error(`Unknown channel '${sub.notify.channel}'`);
    }
  }

  async _http(sub, notification) {
    const { url, method = 'POST', headers = {}, timeoutMs = 10000 } = sub.notify;
    for (const { body } of messages(sub, notification)) {
      try {
        await axios({
          url,
          method,
          data: body,
          timeout: timeoutMs,
          headers: { 'Content-Type': 'application/json', 'User-Agent': 'udc-broker', 'X-UDC-Subscription': sub.id, ...headers },
          maxRedirects: 0,
          validateStatus: (s) => s >= 200 && s < 300
        });
      } catch (error) {
        if (error.response) throw new Error(`HTTP ${error.response.status} from ${url}`);
        throw new Error(`${error.code || 'HTTP error'}: ${error.message}`);
      }
    }
  }

  _mqttClient(sub) {
    const url = sub.notify.url || process.env.BROKER_MQTT_URL || process.env.MQTT_URL;
    if (!url) throw new Error('No MQTT broker: set notify.url or BROKER_MQTT_URL');
    const key = `${url}|${sub.notify.username || ''}`;
    let client = this.mqttClients.get(key);
    if (!client) {
      client = mqtt.connect(url, {
        username: sub.notify.username || undefined,
        password: sub.notify.password || undefined,
        clientId: `udc-broker-${process.pid}-${Math.random().toString(16).slice(2, 8)}`,
        reconnectPeriod: 5000,
        connectTimeout: 10000
      });
      client.on('error', (e) => logger.warn(`MQTT notify client ${url}: ${e.message}`));
      this.mqttClients.set(key, client);
    }
    return client;
  }

  async _mqtt(sub, notification) {
    const client = this._mqttClient(sub);
    if (!client.connected) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          cleanup();
          reject(new Error('MQTT broker not reachable'));
        }, 5000);
        const onConnect = () => { cleanup(); resolve(); };
        const cleanup = () => { clearTimeout(timer); client.off('connect', onConnect); };
        client.once('connect', onConnect);
      });
    }
    const { qos = 1, retain = false } = sub.notify;
    for (const { entity, body } of messages(sub, notification)) {
      const topic = expand(sub.notify.topic, sub, entity);
      await new Promise((resolve, reject) => {
        client.publish(topic, JSON.stringify(body), { qos, retain }, (err) => (err ? reject(new Error(`MQTT publish: ${err.message}`)) : resolve()));
      });
    }
  }

  async _natsConnection(sub) {
    const url = sub.notify.url;
    if (!url) {
      if (!this.natsClient.connected) throw new Error('Broker NATS connection not available: set notify.url');
      return this.natsClient.nc;
    }
    let nc = this.natsConnections.get(url);
    if (!nc || nc.isClosed()) {
      nc = await nats.connect({ servers: url, name: 'udc-broker-notify', maxReconnectAttempts: -1, timeout: 5000 });
      this.natsConnections.set(url, nc);
    }
    return nc;
  }

  async _nats(sub, notification) {
    const nc = await this._natsConnection(sub);
    for (const { entity, body } of messages(sub, notification)) {
      const subject = sub.notify.subject
        .replace(/\{tenant\}/g, natsToken(sub.tenant))
        .replace(/\{subscription\}/g, sub.id)
        .replace(/\{entityId\}/g, entity ? natsToken(entity.id) : '_')
        .replace(/\{entityType\}/g, entity ? entity.type : '_');
      nc.publish(subject, JSON.stringify(body));
    }
    // flush() confirms the server received the messages
    await nc.flush();
  }

  async _websocket(sub, notification) {
    // Live channel: clients that are not connected simply miss it, so this never fails
    this.hub.publishNotification(sub.tenant, sub.id, shape(sub, notification));
  }

  async close() {
    for (const c of this.mqttClients.values()) c.end(true);
    for (const nc of this.natsConnections.values()) await nc.close().catch(() => {});
  }
}

module.exports = Channels;
module.exports.shape = shape;
module.exports.expand = expand;
