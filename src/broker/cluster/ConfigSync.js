const fs = require('fs').promises;
const logger = require('../../utils/logger');
const configManager = require('../../config/ConfigManager');

/**
 * Keeps config/sources.json, config/mapping.json and config/transformers.json identical on every
 * instance of a cluster. PostgreSQL (system_config) holds the shared copy:
 *
 *   - at startup the database copy wins; an empty database is seeded from the files
 *   - after a successful change through /api/v1/system, or an auto-discovery
 *     save of mapping.json, the local file is pushed and the other instances
 *     are told (control message) to pull it and reload
 */
class ConfigSync {
  constructor({ pool, bus, instanceId, transformers = null }) {
    this.pool = pool;
    this.transformers = transformers;
    this.bus = bus;
    this.instanceId = instanceId;
    this.engine = null;
    this.started = false;
    this.initialized = false;
  }

  files() {
    return {
      sources: configManager.sourcesConfigFile,
      mapping: this.engine.mappingEngine.mappingConfigPath,
      ...(this.transformers && { transformers: this.transformers.file })
    };
  }

  attach(engine) {
    this.engine = engine;
    // Auto-discovery writes mapping.json on the instance running the connector
    const mapping = engine.mappingEngine;
    const save = mapping.saveMappingConfig.bind(mapping);
    mapping.saveMappingConfig = async (...args) => {
      const result = await save(...args);
      if (this.initialized) this.push('mapping').catch(e => logger.error(`Config sync (mapping): ${e.message}`));
      return result;
    };
    if (this.started) this._initialize();
  }

  start() {
    this.started = true;
    this.bus.on('control', (msg) => {
      if (msg.kind !== 'config' || msg.origin === this.instanceId || !this.initialized) return;
      this.pull(msg.key).catch(e => logger.error(`Config sync pull (${msg.key}): ${e.message}`));
    });
    if (this.engine) this._initialize();
  }

  async _initialize() {
    try {
      for (const key of Object.keys(this.files())) {
        const { rows } = await this.pool.query('SELECT value FROM system_config WHERE key = $1', [key]);
        const local = await this._read(key);
        if (!rows.length) {
          if (local) await this._store(key, local);
        } else if (!sameJson(rows[0].value, local)) {
          await this._write(key, rows[0].value);
          await this._reload(key);
        }
      }
      this.initialized = true;
      logger.info('Config sync: connector configuration shared through the database');
    } catch (error) {
      logger.error(`Config sync initialization failed: ${error.message}`);
    }
  }

  /** Express middleware for /api/v1/system: pushes the affected file after a successful change. */
  middleware() {
    return (req, res, next) => {
      if (req.method !== 'GET') {
        res.on('finish', () => {
          if (res.statusCode >= 400 || !this.initialized) return;
          const keys = [];
          if (/^\/(config\/)?sources/.test(req.path)) keys.push('sources');
          if (/^\/mapping/.test(req.path)) keys.push('mapping');
          keys.forEach(key => this.push(key).catch(e => logger.error(`Config sync (${key}): ${e.message}`)));
        });
      }
      next();
    };
  }

  async push(key) {
    const value = await this._read(key);
    if (!value) return;
    await this._store(key, value);
    this.bus.control({ kind: 'config', key });
  }

  async pull(key) {
    const { rows } = await this.pool.query('SELECT value FROM system_config WHERE key = $1', [key]);
    if (!rows.length) return;
    if (sameJson(rows[0].value, await this._read(key))) return;
    await this._write(key, rows[0].value);
    await this._reload(key);
    logger.info(`Config sync: ${key} updated from another instance`);
  }

  async _read(key) {
    try {
      return JSON.parse(await fs.readFile(this.files()[key], 'utf8'));
    } catch {
      return null;
    }
  }

  async _write(key, value) {
    await fs.writeFile(this.files()[key], JSON.stringify(value, null, 2), 'utf8');
  }

  async _store(key, value) {
    await this.pool.query(
      `INSERT INTO system_config (key, value, updated_by, updated_at) VALUES ($1, $2, $3, now())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = now()`,
      [key, value, this.instanceId]
    );
  }

  async _reload(key) {
    if (key === 'sources') {
      await this.engine.reloadSourcesConfiguration();
    } else if (key === 'transformers') {
      this.transformers.load();
    } else if (key === 'mapping') {
      this.engine.mappingEngine.mappingConfigs.clear();
      await this.engine.mappingEngine.loadMappingConfig();
    }
  }
}

/** Deep equality ignoring key order (jsonb does not preserve it). */
function sameJson(a, b) {
  const canonical = (v) => (Array.isArray(v)
    ? v.map(canonical)
    : v && typeof v === 'object'
      ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])]))
      : v);
  return JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
}

module.exports = ConfigSync;
