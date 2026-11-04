const logger = require('../../utils/logger');

const QUALITY = { GOOD: 'good', BAD: 'bad', UNCERTAIN: 'uncertain' };

const sanitizeId = (s) => String(s).replace(/[^A-Za-z0-9_\-:.]/g, '_').slice(0, 256) || '_';
const sanitizeAttr = (s) => {
  const name = String(s).replace(/[^A-Za-z0-9_]/g, '_').slice(0, 64);
  return /^[A-Za-z_]/.test(name) ? name : `_${name}`.slice(0, 64);
};
const sanitizeType = (s) => {
  const type = String(s || 'Device').replace(/[^A-Za-z0-9_]/g, '_').slice(0, 64);
  return /^[A-Za-z]/.test(type) ? type : `T${type}`.slice(0, 64);
};

function attrType(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return 'float';
  if (typeof value === 'boolean') return 'bool';
  if (typeof value === 'string') return 'string';
  return 'json';
}

/**
 * Turns the connectors' mapped devices ({ id, type, measurements, metadata })
 * into broker entity updates:
 *   entity id   = source.entityIdPrefix + device id (invalid characters → '_')
 *   attributes  = measurements, with ts/quality from the device metadata,
 *                 source = connector id and unit from mapping.json
 *   tenant      = source.tenant (default 'default')
 */
class ConnectorBridge {
  constructor({ ingest }) {
    this.ingest = ingest;
    this.engine = null;
  }

  attach(engine) {
    this.engine = engine;
    engine.on('data', (event) => {
      try {
        this.handle(event);
      } catch (error) {
        logger.error(`Connector bridge: ${error.message}`);
      }
    });
  }

  handle({ sourceId, mappedData }) {
    if (!mappedData?.id || !Array.isArray(mappedData.measurements)) return;
    const source = this.engine.connectors.get(sourceId)?.config || {};
    const update = this.toUpdate(source, sourceId, mappedData);
    if (Object.keys(update.attrs).length) this.ingest.push(source.tenant || 'default', update);
  }

  toUpdate(source, sourceId, device) {
    const meta = device.metadata || {};
    const ts = meta.timestamp && !Number.isNaN(Date.parse(meta.timestamp)) ? new Date(meta.timestamp).toISOString() : new Date().toISOString();
    const quality = QUALITY[String(meta.quality || '').toUpperCase()];
    const units = this._units(device.id);

    const attrs = {};
    for (const m of device.measurements) {
      if (m.value === undefined || m.value === null || !m.id) continue;
      const attr = { type: attrType(m.value), value: m.value, ts, source: sourceId };
      if (quality) attr.quality = quality;
      const unit = units.get(m.id);
      if (unit && attr.type !== 'json') attr.unit = unit;
      attrs[sanitizeAttr(m.id)] = attr;
    }
    return {
      id: sanitizeId(`${source.entityIdPrefix || ''}${device.id}`),
      type: sanitizeType(device.type),
      attrs
    };
  }

  /** Measurement units declared in mapping.json for a device. */
  _units(deviceId) {
    const config = this.engine?.mappingEngine?.mappingConfigs?.get(deviceId);
    return new Map((config?.measurements || []).filter(m => m.unit).map(m => [m.id, m.unit]));
  }
}

module.exports = ConnectorBridge;
module.exports.sanitizeId = sanitizeId;
module.exports.sanitizeAttr = sanitizeAttr;
module.exports.sanitizeType = sanitizeType;
