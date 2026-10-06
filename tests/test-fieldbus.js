#!/usr/bin/env node

/**
 * PROFINET / EtherCAT connector tests (no hardware required).
 * The S7 and ADS clients are replaced with in-memory fakes.
 */

const EventEmitter = require('events');
const assert = require('assert');
const ProfinetConnector = require('../src/connectors/protocols/ProfinetConnector');
const EtherCATConnector = require('../src/connectors/protocols/EtherCATConnector');

const results = { passed: 0, failed: 0 };

async function test(name, fn) {
  try {
    await fn();
    results.passed++;
    console.log(`  ✓ ${name}`);
  } catch (error) {
    results.failed++;
    console.log(`  ✗ ${name}\n    ${error.stack}`);
  }
}

function nextData(connector, timeout = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('no data received')), timeout);
    connector.once('data', data => { clearTimeout(timer); resolve(data); });
  });
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

class FakeS7 {
  constructor(memory) {
    this.memory = memory; // address -> value
    this.items = [];
    this.writes = [];
    this.translate = t => t;
  }
  initiateConnection(params, cb) { this.params = params; setImmediate(() => cb(this.connectError)); }
  setTranslationCB(cb) { this.translate = cb; }
  addItems(items) { this.items.push(...items); }
  readAllItems(cb) {
    const values = {};
    let bad = false;
    for (const tag of this.items) {
      const addr = this.translate(tag);
      if (addr in this.memory) values[tag] = this.memory[addr];
      else { values[tag] = 'BAD 255'; bad = true; }
    }
    setImmediate(() => cb(bad, values));
  }
  writeItems(tag, value, cb) {
    this.memory[this.translate(tag)] = value;
    this.writes.push({ tag, value });
    setImmediate(() => cb(false));
    return 0;
  }
  dropConnection(cb) { this.dropped = true; cb && cb(); }
}

class FakeAds extends EventEmitter {
  constructor(settings) {
    super();
    this.settings = settings;
    this.raw = new Map(); // `${netId}:${port}:${ig}:${io}` -> Buffer
    this.symbols = {
      'GVL_IO.bSensor': { indexGroup: 0x4020, indexOffset: 0, size: 1, type: 'BOOL', value: true },
      'GVL_IO.rTemp': { indexGroup: 0x4020, indexOffset: 4, size: 4, type: 'REAL', value: 21.5 },
      'GVL_IO.bValve': { indexGroup: 0x4020, indexOffset: 8, size: 1, type: 'BOOL', value: false }
    };
    this.writtenValues = {};
  }
  key(ig, io, t = {}) {
    return `${t.amsNetId || this.settings.targetAmsNetId}:${t.adsPort || this.settings.targetAdsPort}:${ig}:${io}`;
  }
  setRaw(ig, io, buf, t) { this.raw.set(this.key(ig, io, t), buf); }
  async connect() { if (this.failConnect) throw new Error('router unreachable'); return {}; }
  async disconnect() { this.disconnected = true; }
  async getSymbol(path) {
    const s = this.symbols[path];
    if (!s) throw new Error('symbol not found');
    return s;
  }
  async readRawMulti(commands) {
    return commands.map(c => {
      const sym = Object.values(this.symbols).find(s => s.indexGroup === c.indexGroup && s.indexOffset === c.indexOffset);
      if (sym) return { command: c, success: true, value: Buffer.from(JSON.stringify(sym.value)) };
      const buf = this.raw.get(this.key(c.indexGroup, c.indexOffset));
      return buf ? { command: c, success: true, value: buf } : { command: c, success: false, errorStr: 'Invalid index offset' };
    });
  }
  async convertFromRaw(buf) { return JSON.parse(buf.toString()); }
  async readRaw(ig, io, size, t) {
    const buf = this.raw.get(this.key(ig, io, t));
    if (!buf) throw new Error(`ADS error 1793 (${ig}/${io})`);
    return buf.subarray(0, size);
  }
  async writeRaw(ig, io, value, t) { this.setRaw(ig, io, value, t); }
  async writeValue(path, value) { this.writtenValues[path] = value; this.symbols[path].value = value; }
}

// ---------------------------------------------------------------------------
// PROFINET
// ---------------------------------------------------------------------------

async function profinetTests() {
  console.log('\n=== PROFINET ===');

  const baseConfig = {
    id: 'pn-test',
    type: 'profinet',
    config: {
      controllerIp: '192.168.0.1',
      pollingInterval: 20,
      devices: [
        {
          name: 'ET200SP_1',
          stationName: 'et200sp-1',
          inputs: [
            { name: 'sensor', address: 'I0.0' },
            { name: 'temperature', address: 'IW64', scale: { rawMin: 0, rawMax: 27648, engMin: 0, engMax: 100 } }
          ],
          outputs: [{ name: 'valve', address: 'Q0.0' }]
        },
        {
          name: 'Drive_1',
          inputs: [{ name: 'speed', address: 'ID100' }]
        }
      ]
    }
  };

  await test('rejects IO point without address in s7 mode', () => {
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    delete cfg.config.devices[0].inputs[0].address;
    assert.throws(() => new ProfinetConnector(cfg), /requires address/);
  });

  await test('accepts IO point without address in simulation mode', () => {
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    cfg.config.mode = 'simulation';
    delete cfg.config.devices[0].inputs[0].address;
    new ProfinetConnector(cfg);
  });

  await test('rejects unknown mode and duplicate device names', () => {
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    cfg.config.mode = 'dcp';
    assert.throws(() => new ProfinetConnector(cfg), /unsupported mode/);
    const dup = JSON.parse(JSON.stringify(baseConfig));
    dup.config.devices[1].name = 'ET200SP_1';
    assert.throws(() => new ProfinetConnector(dup), /Duplicate/);
  });

  await test('reads process image, scales analog, flags unreachable device', async () => {
    const fake = new FakeS7({ 'I0.0': true, 'IW64': 13824, 'Q0.0': false }); // ID100 missing → BAD
    const connector = new ProfinetConnector(JSON.parse(JSON.stringify(baseConfig)));
    connector.createS7Client = () => fake;
    await connector.initialize();
    await connector.start();

    assert.deepStrictEqual(fake.params, { host: '192.168.0.1', port: 102, rack: 0, slot: 1, timeout: 5000 });
    assert.strictEqual(fake.translate('ET200SP_1.temperature'), 'IW64');

    const data = await nextData(connector);
    const et200 = data.devices.ET200SP_1;
    assert.strictEqual(et200.status, 'ONLINE');
    assert.strictEqual(et200.inputs.sensor, true);
    assert.strictEqual(et200.inputs.temperature, 50);
    assert.strictEqual(et200.outputs.valve, false);
    assert.strictEqual(data.devices.Drive_1.status, 'ERROR');
    assert.strictEqual(data.devices.Drive_1.inputs.speed, null);

    await connector.writeOutput('ET200SP_1', 'valve', true);
    assert.deepStrictEqual(fake.writes, [{ tag: 'ET200SP_1.valve', value: true }]);
    assert.strictEqual(fake.memory['Q0.0'], true);
    await assert.rejects(connector.writeOutput('ET200SP_1', 'sensor', 1), /Output 'sensor' not found/);

    await connector.stop();
    assert.ok(fake.dropped);
  });

  await test('disconnects after repeated total read failures', async () => {
    const fake = new FakeS7({});
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    cfg.retryConfig = { enabled: false };
    const connector = new ProfinetConnector(cfg);
    connector.createS7Client = () => fake;
    connector.on('error', () => {});
    const disconnected = new Promise(resolve => connector.once('disconnected', resolve));
    await connector.start();
    await disconnected;
    assert.strictEqual(connector.isConnected, false);
    assert.ok(connector.getStatus().stats.failedCycles >= 3);
    await connector.stop();
  });

  await test('simulation mode emits data', async () => {
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    cfg.config.mode = 'simulation';
    const connector = new ProfinetConnector(cfg);
    await connector.start();
    const data = await nextData(connector);
    assert.strictEqual(data.devices.ET200SP_1.status, 'ONLINE');
    await connector.stop();
  });
}

// ---------------------------------------------------------------------------
// EtherCAT
// ---------------------------------------------------------------------------

async function ethercatTests() {
  console.log('\n=== EtherCAT ===');

  const MASTER = '192.168.1.120.3.1';
  const baseConfig = {
    id: 'ecat-test',
    type: 'ethercat',
    config: {
      targetAmsNetId: '192.168.1.120.1.1',
      masterAmsNetId: MASTER,
      pollingInterval: 20,
      diagnosticsInterval: 50,
      slaves: [
        {
          name: 'EL1008',
          position: 1,
          inputs: [{ name: 'sensor', symbol: 'GVL_IO.bSensor' }]
        },
        {
          name: 'EL3202',
          address: '0x3eb', // 1003
          inputs: [
            { name: 'temp', symbol: 'GVL_IO.rTemp' },
            { name: 'raw_ch1', indexGroup: '0xF020', indexOffset: 10, type: 'int' },
            { name: 'bit3', indexGroup: 0xF020, indexOffset: 12, type: 'BYTE', bit: 3 }
          ],
          outputs: [
            { name: 'valve', symbol: 'GVL_IO.bValve' },
            { name: 'do_byte', indexGroup: 0xF030, indexOffset: 0, type: 'BYTE' },
            { name: 'do_bit2', indexGroup: 0xF030, indexOffset: 1, type: 'BYTE', bit: 2 }
          ],
          sdo: [{ name: 'vendorId', index: '0x1018', subIndex: 1, type: 'UDINT' }]
        }
      ]
    }
  };

  function setupBus(fake) {
    const m = { amsNetId: MASTER, adsPort: 0xFFFF };
    const u16 = (...v) => { const b = Buffer.alloc(v.length * 2); v.forEach((x, i) => b.writeUInt16LE(x, i * 2)); return b; };
    fake.setRaw(0x6, 0, u16(3), m);
    fake.setRaw(0x7, 0, u16(1001, 1002, 1003), m);
    // states: slave0 OP, slave1 OP, slave2 SAFEOP+ERROR with missing link
    fake.setRaw(0x9, 0, Buffer.from([0x08, 0x00, 0x08, 0x00, 0x14, 0x04]), m);
    fake.setRaw(0xF302, (0x1018 << 16) | 1, Buffer.from([0x02, 0x00, 0x00, 0x00]), { amsNetId: MASTER, adsPort: 1003 });
    const raw = Buffer.alloc(2); raw.writeInt16LE(-1234, 0);
    fake.setRaw(0xF020, 10, raw);
    fake.setRaw(0xF020, 12, Buffer.from([0b00001000]));
    fake.setRaw(0xF030, 0, Buffer.from([0]));
    fake.setRaw(0xF030, 1, Buffer.from([0b00000001]));
  }

  await test('raw codec and slave state decoding', () => {
    assert.strictEqual(EtherCATConnector.decodeRaw(EtherCATConnector.encodeRaw(-5, 'INT'), 'INT'), -5);
    assert.strictEqual(EtherCATConnector.decodeRaw(EtherCATConnector.encodeRaw(1.5, 'LREAL'), 'LREAL'), 1.5);
    assert.strictEqual(EtherCATConnector.decodeRaw(Buffer.from([0b100]), 'BYTE', 2), true);
    const s = EtherCATConnector.decodeSlaveState(0x14, 0x04);
    assert.strictEqual(s.state, 'SAFEOP');
    assert.deepStrictEqual(s.flags, ['ERROR']);
    assert.deepStrictEqual(s.link, ['MISSING_LINK']);
  });

  await test('validation: targetAmsNetId, IO addressing, raw type', () => {
    const noNet = JSON.parse(JSON.stringify(baseConfig));
    delete noNet.config.targetAmsNetId;
    assert.throws(() => new EtherCATConnector(noNet), /targetAmsNetId/);
    const noAddr = JSON.parse(JSON.stringify(baseConfig));
    delete noAddr.config.slaves[0].inputs[0].symbol;
    assert.throws(() => new EtherCATConnector(noAddr), /requires symbol/);
    const badType = JSON.parse(JSON.stringify(baseConfig));
    badType.config.slaves[1].inputs[1].type = 'FOO';
    assert.throws(() => new EtherCATConnector(badType), /raw type/);
    const sim = { id: 's', type: 'ethercat', config: { mode: 'simulation', slaves: [{ name: 'a', inputs: [{ name: 'x', type: 'digital' }] }] } };
    new EtherCATConnector(sim);
  });

  await test('reads PDOs (symbol + raw), slave states, SDO; writes outputs', async () => {
    let fake;
    const connector = new EtherCATConnector(JSON.parse(JSON.stringify(baseConfig)));
    connector.createAdsClient = settings => { fake = new FakeAds(settings); setupBus(fake); return fake; };
    await connector.initialize();
    await connector.start();

    assert.strictEqual(fake.settings.targetAdsPort, 851);
    assert.notStrictEqual(fake.settings.rawClient, true);

    const data = await nextData(connector);
    assert.strictEqual(data.slaves.EL1008.inputs.sensor, true);
    assert.strictEqual(data.slaves.EL1008.state, 'OP');
    assert.strictEqual(data.slaves.EL1008.address, 1002); // resolved from position 1
    const el3202 = data.slaves.EL3202;
    assert.strictEqual(el3202.inputs.temp, 21.5);
    assert.strictEqual(el3202.inputs.raw_ch1, -1234);
    assert.strictEqual(el3202.inputs.bit3, true);
    assert.strictEqual(el3202.state, 'SAFEOP+ERROR');
    assert.strictEqual(el3202.sdo.vendorId, 2);
    assert.deepStrictEqual(data.master, { ...data.master, slaveCount: 3, slavesInOp: 2 });

    await connector.writeOutput('EL3202', 'valve', true);
    assert.strictEqual(fake.writtenValues['GVL_IO.bValve'], true);
    await connector.writeOutput('EL3202', 'do_byte', 0xAA);
    assert.strictEqual(fake.raw.get(fake.key(0xF030, 0))[0], 0xAA);
    await connector.writeOutput('EL3202', 'do_bit2', true);
    assert.strictEqual(fake.raw.get(fake.key(0xF030, 1))[0], 0b101);
    await connector.writeOutput('EL3202', 'do_bit2', false);
    assert.strictEqual(fake.raw.get(fake.key(0xF030, 1))[0], 0b001);

    await connector.writeSdo('EL3202', 0x8000, 0x19, 3, 'UINT');
    assert.strictEqual(await connector.readSdo('EL3202', 0x8000, 0x19, 'UINT'), 3);
    await assert.rejects(connector.readSdo('missing', 0x1000), /not found/);

    const status = connector.getStatus();
    assert.strictEqual(status.slaves.find(s => s.name === 'EL3202').link[0], 'MISSING_LINK');

    await connector.stop();
    assert.ok(fake.disconnected);
  });

  await test('raw-only config uses rawClient (no PLC runtime needed)', async () => {
    let fake;
    const cfg = {
      id: 'raw', type: 'ethercat',
      config: { targetAmsNetId: '1.2.3.4.1.1', pollingInterval: 20, slaves: [{ name: 's', inputs: [{ name: 'x', indexGroup: 0xF020, indexOffset: 0, type: 'BYTE' }] }] }
    };
    const connector = new EtherCATConnector(cfg);
    connector.createAdsClient = settings => { fake = new FakeAds(settings); fake.setRaw(0xF020, 0, Buffer.from([7])); return fake; };
    await connector.start();
    const data = await nextData(connector);
    assert.strictEqual(fake.settings.rawClient, true);
    assert.strictEqual(data.slaves.s.inputs.x, 7);
    await connector.stop();
  });

  await test('fails to start on unknown PLC symbol', async () => {
    const cfg = JSON.parse(JSON.stringify(baseConfig));
    cfg.config.slaves[0].inputs[0].symbol = 'GVL_IO.doesNotExist';
    const connector = new EtherCATConnector(cfg);
    connector.createAdsClient = settings => { const f = new FakeAds(settings); setupBus(f); return f; };
    connector.on('error', () => {});
    await assert.rejects(connector.start(), /cannot resolve PLC symbol 'GVL_IO.doesNotExist'/);
  });

  await test('connection lost → reconnect restores connected state', async () => {
    let fake;
    const connector = new EtherCATConnector(JSON.parse(JSON.stringify(baseConfig)));
    connector.createAdsClient = settings => { fake = new FakeAds(settings); setupBus(fake); return fake; };
    await connector.start();
    fake.emit('connectionLost', true);
    assert.strictEqual(connector.isConnected, false);
    const reconnected = new Promise(resolve => connector.once('connected', resolve));
    fake.emit('reconnect', true, []);
    await reconnected;
    assert.strictEqual(connector.isConnected, true);
    await connector.stop();
  });
}

(async () => {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║       Universal Data Connector - Fieldbus Connector Tests     ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  await profinetTests();
  await ethercatTests();
  console.log(`\nPassed: ${results.passed}  Failed: ${results.failed}`);
  process.exit(results.failed > 0 ? 1 : 0);
})();
