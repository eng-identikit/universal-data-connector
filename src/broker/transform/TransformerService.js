const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');
const logger = require('../../utils/logger');
const { badRequest, notFound } = require('../errors');
const { compile } = require('./transformer');

const ID_RE = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const DEFAULT_FILE = path.join(process.cwd(), 'config', 'transformers.json');
const WATCH_INTERVAL_MS = 3000;

/**
 * Library of named output transformers, kept in config/transformers.json:
 *
 *   { "transformers": [
 *       { "id": "influx-line", "name": "InfluxDB line protocol", "description": "...",
 *         "expression": "<JSONata>" } ] }
 *
 * Subscriptions reference them with notify.transformer = "<id>" (or carry an
 * inline notify.transform.expression). The file can be edited by hand (changes
 * are picked up within a few seconds) or through /api/v1/transformers; in a
 * cluster ConfigSync shares it between instances.
 */
class TransformerService extends EventEmitter {
  constructor(file = process.env.BROKER_TRANSFORMERS_FILE || DEFAULT_FILE) {
    super();
    this.file = file;
    this.items = new Map();
    this.watching = false;
  }

  start() {
    this.load();
    if (!this.watching) {
      this.watching = true;
      // Hand edits of the file apply without a restart; the watcher does not keep the process alive
      fs.watchFile(this.file, { interval: WATCH_INTERVAL_MS }, () => {
        this.load();
        this.emit('changed');
      }).unref();
    }
  }

  stop() {
    fs.unwatchFile(this.file);
    this.watching = false;
  }

  /** (Re)reads the file. Invalid entries are skipped with a warning; a broken file keeps the last good library. */
  load() {
    let doc;
    try {
      doc = JSON.parse(fs.readFileSync(this.file, 'utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') {
        this.items = new Map();
        return;
      }
      logger.error(`Transformers: cannot read ${this.file}, keeping the previous library: ${error.message}`);
      return;
    }
    const items = new Map();
    for (const raw of Array.isArray(doc?.transformers) ? doc.transformers : []) {
      try {
        const t = validate(raw);
        items.set(t.id, t);
      } catch (error) {
        logger.warn(`Transformers: skipping '${raw?.id}': ${error.message}`);
      }
    }
    this.items = items;
  }

  list() {
    return [...this.items.values()].sort((a, b) => a.id.localeCompare(b.id));
  }

  get(id) {
    const t = this.items.get(id);
    if (!t) throw notFound(`Transformer '${id}' not found`);
    return t;
  }

  find(id) {
    return this.items.get(id) || null;
  }

  async put(id, input) {
    const t = validate({ ...input, id });
    const items = new Map(this.items);
    const existing = items.get(id);
    items.set(id, { ...t, createdAt: existing?.createdAt || new Date().toISOString(), updatedAt: new Date().toISOString() });
    await this._save(items);
    return items.get(id);
  }

  async remove(id) {
    this.get(id);
    const items = new Map(this.items);
    items.delete(id);
    await this._save(items);
  }

  /** Atomic write (temporary file + rename); the in-memory library follows. */
  async _save(items) {
    const doc = { transformers: [...items.values()].sort((a, b) => a.id.localeCompare(b.id)) };
    await fs.promises.mkdir(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    await fs.promises.writeFile(tmp, `${JSON.stringify(doc, null, 2)}\n`, 'utf8');
    await fs.promises.rename(tmp, this.file);
    this.items = items;
    this.emit('changed');
  }
}

function validate(raw) {
  if (!raw || typeof raw !== 'object') throw badRequest('Transformer must be an object');
  const { id, name, description, expression, createdAt, updatedAt } = raw;
  if (typeof id !== 'string' || !ID_RE.test(id)) throw badRequest('Transformer id must be 1-64 chars of [a-z0-9_-], starting with a letter or digit');
  if (name !== undefined && (typeof name !== 'string' || name.length > 200)) throw badRequest('name must be a string (max 200)');
  if (description !== undefined && description !== null && (typeof description !== 'string' || description.length > 2000)) {
    throw badRequest('description must be a string (max 2000)');
  }
  compile(expression);
  return {
    id,
    name: name || id,
    description: description || null,
    expression,
    ...(createdAt && { createdAt }),
    ...(updatedAt && { updatedAt })
  };
}

TransformerService.ID_RE = ID_RE;
module.exports = TransformerService;
