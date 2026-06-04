const axios = require('axios');
const BaseConnector = require('../BaseConnector');
const logger = require('../../utils/logger');

/**
 * i3X Connector — Industrial Information Interoperability eXchange (CESMII)
 * REST API protocol for accessing contextualized manufacturing data.
 *
 * Supports two read modes:
 *   - polling: periodically calls POST /objects/value
 *   - sse:     connects to GET /subscriptions/{id}/stream (Server-Sent Events)
 */
class I3XConnector extends BaseConnector {
  constructor(config) {
    super(config);
    this.http = null;
    this.pollingTimer = null;
    this.subscriptionId = null;
    this.sseAbortController = null;
  }

  validateConfig() {
    super.validateConfig();
    const { config } = this.config;
    if (!config.baseUrl) {
      throw new Error('i3X connector requires a baseUrl');
    }
  }

  async initialize() {
    await super.initialize();
    const { config } = this.config;

    this.http = axios.create({
      baseURL: config.baseUrl.replace(/\/$/, ''),
      timeout: config.timeout || 15000,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Universal-Data-Connector/1.0',
        ...config.headers
      }
    });

    this._applyAuth(config.authentication);
    logger.debug(`i3X connector '${this.id}' initialized → ${config.baseUrl}`);
  }

  _applyAuth(auth) {
    if (!auth) return;
    switch (auth.type?.toLowerCase()) {
      case 'bearer':
        this.http.defaults.headers['Authorization'] = `Bearer ${auth.token}`;
        break;
      case 'basic':
        this.http.defaults.auth = { username: auth.username, password: auth.password };
        break;
      case 'apikey':
        this.http.defaults.headers[auth.header || 'X-API-Key'] = auth.key;
        break;
    }
  }

  async connect() {
    try {
      const { config } = this.config;

      // Verify connectivity by fetching namespaces
      await this._getNamespaces();
      logger.info(`i3X connector '${this.id}' connected to ${config.baseUrl}`);

      const mode = config.mode || 'polling';

      if (mode === 'sse') {
        await this._startSSE();
      } else {
        this._startPolling();
      }

      this.onConnected();
    } catch (error) {
      logger.error(`i3X connector '${this.id}' failed to connect:`, error.message);
      throw error;
    }
  }

  async disconnect() {
    this._stopPolling();
    await this._stopSSE();
    logger.info(`i3X connector '${this.id}' disconnected`);
  }

  // ─── Polling mode ─────────────────────────────────────────────────────────

  _startPolling() {
    const { config } = this.config;
    const interval = config.interval || 5000;

    logger.info(`i3X connector '${this.id}' polling every ${interval}ms`);

    this.pollingTimer = setInterval(async () => {
      if (!this.isRunning) return;
      try {
        await this._fetchValues();
      } catch (error) {
        logger.error(`i3X polling error on '${this.id}':`, error.message);
      }
    }, interval);

    // First fetch immediately
    this._fetchValues().catch(err =>
      logger.error(`i3X initial fetch error on '${this.id}':`, err.message)
    );
  }

  _stopPolling() {
    if (this.pollingTimer) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = null;
    }
  }

  async _fetchValues() {
    const { config } = this.config;
    const elementIds = config.elementIds || [];

    if (elementIds.length === 0) {
      // Auto-discover objects if no elementIds configured
      const objects = await this._getObjects();
      const discovered = objects.map(o => o.elementId || o.id).filter(Boolean);
      if (discovered.length === 0) {
        logger.warn(`i3X connector '${this.id}': no elementIds and no objects discovered`);
        return;
      }
      config.elementIds = discovered;
      logger.info(`i3X connector '${this.id}': auto-discovered ${discovered.length} elements`);
    }

    const response = await this.http.post('/objects/value', {
      elementIds: config.elementIds,
      depth: config.depth || 0
    });

    const values = response.data;
    this.onData(this._normalizeValues(values));
  }

  // ─── SSE subscription mode ─────────────────────────────────────────────────

  async _startSSE() {
    const { config } = this.config;

    // Create subscription
    const subResponse = await this.http.post('/subscriptions', {
      elementIds: config.elementIds || [],
      type: 'stream'
    });

    this.subscriptionId = subResponse.data?.id || subResponse.data?.subscriptionId;
    if (!this.subscriptionId) {
      throw new Error('i3X server did not return a subscriptionId');
    }

    logger.info(`i3X connector '${this.id}' subscription created: ${this.subscriptionId}`);

    this._connectSSEStream();
  }

  _connectSSEStream() {
    const { config } = this.config;
    this.sseAbortController = new AbortController();

    const url = `${config.baseUrl.replace(/\/$/, '')}/subscriptions/${this.subscriptionId}/stream`;

    const headers = { ...this.http.defaults.headers };
    delete headers['Content-Type'];
    headers['Accept'] = 'text/event-stream';

    fetch(url, { headers, signal: this.sseAbortController.signal })
      .then(async (res) => {
        if (!res.ok) throw new Error(`SSE HTTP ${res.status}`);

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (this.isRunning) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();

          for (const line of lines) {
            if (line.startsWith('data:')) {
              try {
                const payload = JSON.parse(line.slice(5).trim());
                this.onData(this._normalizeValues(payload));
              } catch {
                // ignore malformed SSE frames
              }
            }
          }
        }
      })
      .catch((err) => {
        if (err.name === 'AbortError') return;
        logger.error(`i3X SSE stream error on '${this.id}':`, err.message);
        if (this.isRunning) this.onDisconnected();
      });
  }

  async _stopSSE() {
    if (this.sseAbortController) {
      this.sseAbortController.abort();
      this.sseAbortController = null;
    }

    if (this.subscriptionId) {
      try {
        await this.http.delete(`/subscriptions/${this.subscriptionId}`);
      } catch {
        // best-effort cleanup
      }
      this.subscriptionId = null;
    }
  }

  // ─── API helpers ───────────────────────────────────────────────────────────

  async _getNamespaces() {
    const res = await this.http.get('/namespaces');
    return res.data;
  }

  async _getObjects(typeId) {
    const params = typeId ? { typeId } : {};
    const res = await this.http.get('/objects', { params });
    return Array.isArray(res.data) ? res.data : (res.data?.items || []);
  }

  async _getObjectTypes(namespaceUri) {
    const params = namespaceUri ? { namespace: namespaceUri } : {};
    const res = await this.http.get('/objecttypes', { params });
    return Array.isArray(res.data) ? res.data : (res.data?.items || []);
  }

  async readValue(elementId) {
    const res = await this.http.post('/objects/value', { elementIds: [elementId], depth: 0 });
    const values = this._normalizeValues(res.data);
    return values[elementId] ?? values;
  }

  async writeValue(elementId, value) {
    await this.http.put(`/objects/${encodeURIComponent(elementId)}/value`, { value });
    logger.debug(`i3X write → ${elementId} = ${value}`);
  }

  async getHistory(elementIds, startTime, endTime) {
    const res = await this.http.post('/objects/history', {
      elementIds: Array.isArray(elementIds) ? elementIds : [elementIds],
      startTime: startTime instanceof Date ? startTime.toISOString() : startTime,
      endTime: endTime instanceof Date ? endTime.toISOString() : endTime
    });
    return res.data;
  }

  // ─── Data normalisation ────────────────────────────────────────────────────

  /**
   * Converts i3X response (array or object map of VQT entries) into a flat
   * { elementId: value } map that the rest of UDC expects.
   */
  _normalizeValues(raw) {
    if (!raw) return {};
    const out = {};

    const entries = Array.isArray(raw) ? raw : Object.entries(raw).map(([k, v]) => ({ elementId: k, ...v }));

    for (const entry of entries) {
      const id = entry.elementId || entry.id;
      if (!id) continue;

      if (entry.vqt !== undefined) {
        // Explicit VQT wrapper
        out[id] = {
          value: entry.vqt?.value ?? entry.vqt,
          quality: entry.vqt?.quality,
          timestamp: entry.vqt?.timestamp || entry.timestamp || new Date().toISOString()
        };
      } else if (entry.value !== undefined) {
        out[id] = {
          value: entry.value,
          quality: entry.quality,
          timestamp: entry.timestamp || new Date().toISOString()
        };
      } else {
        out[id] = entry;
      }
    }

    return out;
  }

  getStatus() {
    const { config } = this.config;
    return {
      ...super.getStatus(),
      baseUrl: config.baseUrl,
      mode: config.mode || 'polling',
      subscriptionId: this.subscriptionId,
      elementCount: (config.elementIds || []).length
    };
  }
}

module.exports = I3XConnector;
