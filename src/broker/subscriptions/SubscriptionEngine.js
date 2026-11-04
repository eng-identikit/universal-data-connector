const crypto = require('crypto');
const { LRUCache } = require('lru-cache');
const logger = require('../../utils/logger');
const { compile } = require('../expr/expression');

const STATS_FLUSH_MS = 5000;
const PERIODIC_CHUNK = 500;
const PERIODIC_MAX_ENTITIES = 10000;

/**
 * Evaluates entity changes against the subscriptions and enqueues notifications.
 *
 * Per change (from the ChangeBus, so every instance sees every change):
 *   1. subscription owned by this instance (cluster partitioning), enabled, not expired
 *   2. trigger.on includes the operation
 *   3. selector: types, idPattern, ids, related (cached reachable set), bbox
 *   4. update only: one of trigger.attrs changed (all attrs if empty), with deadband
 *      relative to the last notified value
 *   5. filter true ('level') or turned true ('edge')
 *   6. throttle: at most one notification per entity every throttleMs; the latest
 *      state is sent at the end of the window
 */
class SubscriptionEngine {
  constructor({ subscriptions, entities, queue, bus }) {
    this.store = subscriptions;
    this.entities = entities;
    this.queue = queue;
    this.bus = bus;
    this.subs = new Map(); // id → compiled subscription
    this.byTenant = new Map(); // tenant → Set<id>
    this.lastNotified = new LRUCache({ max: 200000 }); // `${sub}|${entity}` → { attr: value } for deadband
    this.throttle = new Map(); // `${sub}|${entity}` → { last, pending, timer }
    this.periodicTimers = new Map();
    this.related = new Map(); // sub id → Set of entity ids
    this.statDeltas = {};
    this.owns = () => true; // replaced by the cluster coordinator
    this.chain = new Map(); // tenant → promise, keeps per-tenant change order
    this.statsTimer = null;
  }

  async start() {
    for (const sub of await this.store.loadAll()) await this._register(sub);
    this.bus.on('change', (change) => this._enqueueChange(change));
    this.bus.on('control', (msg) => {
      // Own changes were already applied synchronously by the API call
      if (msg.kind !== 'subscription' || msg.origin === this.bus.instanceId) return;
      this.reload(msg.id).catch(e => logger.error(`Subscription reload failed: ${e.message}`));
    });
    this.statsTimer = setInterval(() => this.flushStats(), STATS_FLUSH_MS);
    if (this.statsTimer.unref) this.statsTimer.unref();
    logger.info(`Subscription engine started with ${this.subs.size} subscriptions`);
  }

  async stop() {
    clearInterval(this.statsTimer);
    this.periodicTimers.forEach(clearInterval);
    this.throttle.forEach(t => clearTimeout(t.timer));
    await this.flushStats();
  }

  get(id) {
    return this.subs.get(id) || null;
  }

  /** Reloads one subscription from the database (after CRUD on any instance). */
  async reload(id) {
    this._unregister(id);
    const sub = await this.store.loadOne(id);
    if (sub) await this._register(sub);
  }

  /** Re-evaluates ownership after the cluster partition assignment changed. */
  refreshOwnership() {
    for (const sub of this.subs.values()) this._schedulePeriodic(sub);
  }

  async _register(raw) {
    const sub = { ...raw };
    try {
      sub.compiledFilter = sub.filter ? compile(sub.filter) : null;
    } catch (error) {
      logger.error(`Subscription ${sub.id}: invalid filter ignored (${error.message})`);
      sub.compiledFilter = null;
    }
    sub.idPattern = sub.select.idPattern ? globRegex(sub.select.idPattern) : null;
    this.subs.set(sub.id, sub);
    if (!this.byTenant.has(sub.tenant)) this.byTenant.set(sub.tenant, new Set());
    this.byTenant.get(sub.tenant).add(sub.id);
    if (sub.select.related) await this._refreshRelated(sub);
    this._schedulePeriodic(sub);
  }

  _unregister(id) {
    const sub = this.subs.get(id);
    if (!sub) return;
    this.subs.delete(id);
    this.byTenant.get(sub.tenant)?.delete(id);
    this.related.delete(id);
    clearInterval(this.periodicTimers.get(id));
    this.periodicTimers.delete(id);
    for (const [key, t] of this.throttle) {
      if (key.startsWith(`${id}|`)) {
        clearTimeout(t.timer);
        this.throttle.delete(key);
      }
    }
  }

  _active(sub) {
    return sub.enabled && (!sub.expiresAt || Date.parse(sub.expiresAt) > Date.now()) && this.owns(sub);
  }

  // ─── Change processing ────────────────────────────────────────────────

  _enqueueChange(change) {
    const prev = this.chain.get(change.tenant) || Promise.resolve();
    const next = prev.then(() => this.handleChange(change)).catch(e => logger.error(`Subscription engine error: ${e.message}`));
    this.chain.set(change.tenant, next);
    next.then(() => {
      if (this.chain.get(change.tenant) === next) this.chain.delete(change.tenant);
    });
  }

  async handleChange({ tenant, op, entity, previous }) {
    const ids = this.byTenant.get(tenant);
    if (!ids || !ids.size) return;

    // Relationship changes can move entities in or out of 'related' selectors
    if (relationshipsChanged(previous, entity)) {
      for (const id of ids) {
        const sub = this.subs.get(id);
        if (sub?.select.related) await this._refreshRelated(sub);
      }
    }

    const target = op === 'delete' ? previous : entity;
    for (const id of ids) {
      const sub = this.subs.get(id);
      if (!sub || !this._active(sub) || !sub.trigger.on.includes(op)) continue;
      if (!this._selects(sub, target)) continue;

      if (op === 'update' && !this._triggered(sub, previous, entity)) continue;

      if (sub.compiledFilter) {
        const now = sub.compiledFilter.test(target, op === 'create' ? null : previous);
        if (!now) continue;
        if (sub.filterMode === 'edge' && op === 'update' && sub.compiledFilter.test(previous, null)) continue;
      }

      this._stat(sub.id, 'matched');
      this._emit(sub, target, op);
    }
  }

  _selects(sub, entity) {
    const s = sub.select;
    if (s.types?.length && !s.types.includes(entity.type)) return false;
    if (sub.idPattern && !sub.idPattern.test(entity.id)) return false;
    if (s.ids?.length && !s.ids.includes(entity.id)) return false;
    if (s.related && !this.related.get(sub.id)?.has(entity.id)) return false;
    if (s.bbox && !inBbox(entity.location, s.bbox)) return false;
    return true;
  }

  _triggered(sub, previous, entity) {
    const watched = sub.trigger.attrs?.length ? sub.trigger.attrs : unionKeys(previous.attrs, entity.attrs);
    const deadband = sub.trigger.deadband || {};
    const last = this.lastNotified.get(`${sub.id}|${entity.id}`);
    return watched.some(name => {
      const now = valueOf(entity.attrs[name]);
      if (deadband[name] !== undefined && typeof now === 'number') {
        const ref = last && name in last ? last[name] : valueOf(previous.attrs[name]);
        return typeof ref !== 'number' || Math.abs(now - ref) > deadband[name];
      }
      return JSON.stringify(now) !== JSON.stringify(valueOf(previous.attrs[name]));
    });
  }

  _emit(sub, entity, reason) {
    if (!sub.throttleMs || reason === 'delete') return this._send(sub, [entity], reason);
    const key = `${sub.id}|${entity.id}`;
    const now = Date.now();
    const state = this.throttle.get(key) || { last: 0, pending: null, timer: null };
    this.throttle.set(key, state);
    if (now - state.last >= sub.throttleMs && !state.timer) {
      state.last = now;
      return this._send(sub, [entity], reason);
    }
    // Inside the window: keep only the latest state, flush when the window ends
    state.pending = { entity, reason };
    if (!state.timer) {
      state.timer = setTimeout(() => {
        state.timer = null;
        state.last = Date.now();
        const p = state.pending;
        state.pending = null;
        if (p && this.subs.has(sub.id)) this._send(this.subs.get(sub.id), [p.entity], p.reason);
      }, Math.max(0, sub.throttleMs - (now - state.last)));
    }
  }

  _send(sub, entities, reason) {
    if (sub.trigger.deadband && Object.keys(sub.trigger.deadband).length) {
      for (const e of entities) {
        const snapshot = {};
        for (const name of Object.keys(sub.trigger.deadband)) snapshot[name] = valueOf(e.attrs?.[name]);
        this.lastNotified.set(`${sub.id}|${e.id}`, snapshot);
      }
    }
    const notification = {
      id: crypto.randomUUID(),
      subscriptionId: sub.id,
      subscriptionName: sub.name,
      tenant: sub.tenant,
      reason,
      timestamp: new Date().toISOString(),
      entities
    };
    this._stat(sub.id, 'lastNotificationAt', notification.timestamp);
    return this.queue.publish(`udc.notify.${subjectToken(sub.tenant)}.${sub.id}`, notification, { id: notification.id })
      .catch(error => logger.error(`Subscription ${sub.id}: enqueue failed: ${error.message}`));
  }

  // ─── Periodic notifications ───────────────────────────────────────────

  _schedulePeriodic(sub) {
    clearInterval(this.periodicTimers.get(sub.id));
    this.periodicTimers.delete(sub.id);
    if (!sub.periodicMs || !this.owns(sub)) return;
    const timer = setInterval(() => {
      const current = this.subs.get(sub.id);
      if (current && this._active(current)) {
        this.runPeriodic(current).catch(e => logger.error(`Periodic subscription ${sub.id}: ${e.message}`));
      }
    }, sub.periodicMs);
    if (timer.unref) timer.unref();
    this.periodicTimers.set(sub.id, timer);
  }

  async runPeriodic(sub) {
    const entities = await this.matchingEntities(sub, PERIODIC_MAX_ENTITIES);
    for (let i = 0; i < entities.length; i += PERIODIC_CHUNK) {
      await this._send(sub, entities.slice(i, i + PERIODIC_CHUNK), 'periodic');
    }
  }

  /** Current entities satisfying selector and filter (used by periodic and the UI preview). */
  async matchingEntities(sub, max = 100) {
    const ctx = { tenant: sub.tenant, allowedTypes: null };
    const query = { limit: Math.min(max, 1000), orderBy: 'id' };
    if (sub.select.types?.length) query.type = sub.select.types.join(',');
    if (sub.select.idPattern) query.idPattern = sub.select.idPattern;
    if (sub.select.related) Object.assign(query, { related: sub.select.related.id, depth: sub.select.related.depth, direction: sub.select.related.direction });
    if (sub.select.bbox) query.bbox = sub.select.bbox.join(',');
    const filter = sub.compiledFilter || (sub.filter ? compile(sub.filter) : null);
    const out = [];
    for (let offset = 0; out.length < max; offset += query.limit) {
      const page = await this.entities.list(ctx, { ...query, offset });
      for (const e of page.entities) {
        if (sub.select.ids?.length && !sub.select.ids.includes(e.id)) continue;
        if (!filter || filter.test(e, null)) out.push(e);
        if (out.length >= max) break;
      }
      if (offset + page.entities.length >= page.total) break;
    }
    return out;
  }

  async _refreshRelated(sub) {
    const { id, depth, direction } = sub.select.related;
    this.related.set(sub.id, new Set(await this.entities.reachableIds(sub.tenant, id, depth, direction)));
  }

  // ─── Statistics ───────────────────────────────────────────────────────

  _stat(id, key, value) {
    const d = this.statDeltas[id] || (this.statDeltas[id] = {});
    if (key.startsWith('last')) d[key] = value;
    else d[key] = (d[key] || 0) + (value ?? 1);
  }

  /** Called by the delivery worker. */
  recordDelivery(id, { ok, latencyMs, error, dead }) {
    const now = new Date().toISOString();
    if (ok) {
      this._stat(id, 'delivered');
      this._stat(id, 'lastSuccessAt', now);
      this._stat(id, 'lastLatencyMs', latencyMs);
    } else {
      this._stat(id, 'failedAttempts');
      this._stat(id, 'lastFailureAt', now);
      this._stat(id, 'lastError', String(error).slice(0, 1000));
      if (dead) this._stat(id, 'dead');
    }
  }

  async flushStats() {
    // Drop throttle windows that expired with nothing pending
    const now = Date.now();
    for (const [key, t] of this.throttle) {
      const sub = this.subs.get(key.slice(0, key.indexOf('|')));
      if (!t.timer && (!sub || now - t.last > (sub.throttleMs || 0))) this.throttle.delete(key);
    }

    const deltas = this.statDeltas;
    this.statDeltas = {};
    if (!Object.keys(deltas).length) return;
    try {
      await this.store.addStats(deltas);
    } catch (error) {
      logger.error(`Subscription stats flush failed: ${error.message}`);
    }
  }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

const valueOf = (attr) => (attr ? (attr.type === 'relationship' ? attr.target : attr.value) : undefined);

function unionKeys(a = {}, b = {}) {
  return [...new Set([...Object.keys(a), ...Object.keys(b)])];
}

function relationshipsChanged(previous, entity) {
  const rels = (e) => JSON.stringify(Object.entries(e?.attrs || {})
    .filter(([, a]) => a.type === 'relationship').map(([k, a]) => [k, a.target]).sort());
  return rels(previous) !== rels(entity);
}

function globRegex(glob) {
  return new RegExp(`^${glob.split('*').map(p => p.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*')}$`);
}

/** Point locations only; other geometries match when their first coordinate is inside. */
function inBbox(location, [minLon, minLat, maxLon, maxLat]) {
  if (!location) return false;
  let c = location.coordinates;
  while (Array.isArray(c) && Array.isArray(c[0])) c = c[0];
  if (!Array.isArray(c) || c.length < 2) return false;
  const [lon, lat] = c;
  return lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat;
}

// NATS subject tokens cannot contain '.', '*', '>' or spaces
const subjectToken = (s) => s.replace(/[.\s*>]/g, '_');

module.exports = SubscriptionEngine;
module.exports.globRegex = globRegex;
