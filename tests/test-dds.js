#!/usr/bin/env node

/**
 * DDS connector tests.
 * Unit tests replace RTI Connector with an in-memory fake; the loopback test
 * runs against a real DDS domain (domain 87, local only) when the optional
 * 'rticonnextdds-connector' package is installed, and is skipped otherwise.
 */

const path = require('path');
const assert = require('assert');
const DdsConnector = require('../src/connectors/protocols/DdsConnector');
const ConnectorFactory = require('../src/connectors/ConnectorFactory');

const results = { passed: 0, failed: 0, skipped: 0 };

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

function nextData(connector, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('no data received')), timeout);
    connector.once('data', data => { clearTimeout(timer); resolve(data); });
  });
}

// ---------------------------------------------------------------------------
// Fake RTI Connector
// ---------------------------------------------------------------------------

class FakeTimeoutError extends Error {}

function createFakeRti() {
  const rti = { TimeoutError: FakeTimeoutError, DDSError: Error, connectors: [] };

  class FakeInput {
    constructor(name) { this.name = name; this.queue = []; this.taken = []; }
    take() { this.taken = this.queue; this.queue = []; }
    get samples() {
      const taken = this.taken;
      return {
        validDataIter: taken.filter(s => s.valid !== false).map(s => ({
          getJson: () => s.data,
          info: { get: field => (field === 'source_timestamp' ? s.ts : undefined) }
        }))
      };
    }
  }

  class FakeOutput {
    constructor(name) {
      this.name = name;
      this.current = {};
      this.written = [];
      this.instance = { setFromJson: json => Object.assign(this.current, json) };
    }
    clearMembers() { this.current = {}; }
    write(params) { this.written.push({ data: { ...this.current }, params }); }
  }

  rti.Connector = class {
    constructor(configName, url) {
      if (rti.failCreate) throw new Error('Invalid participant profile, xml path or xml profile');
      this.configName = configName;
      this.url = url;
      this.inputs = {};
      this.outputs = {};
      this.pending = null;
      this.closed = false;
      rti.connectors.push(this);
    }
    getInput(name) {
      if (name.startsWith('Bad')) throw new Error('Invalid Subscription::DataReader name');
      return (this.inputs[name] = this.inputs[name] || new FakeInput(name));
    }
    getOutput(name) { return (this.outputs[name] = this.outputs[name] || new FakeOutput(name)); }
    wait(timeout) {
      if (this.pending) throw new Error('Can not concurrently wait on the same Connector object');
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { this.pending = null; reject(new FakeTimeoutError('Timeout error')); }, timeout);
        this.pending = { resolve: () => { clearTimeout(timer); this.pending = null; resolve(); }, reject };
        if (this.failNextWait) {
          clearTimeout(timer);
          this.pending = null;
          this.failNextWait = false;
          reject(new Error('DDS_RETCODE_ERROR'));
        }
      });
    }
    publish(readerName, data, ts) {
      this.getInput(readerName).queue.push({ data, ts });
      if (this.pending) this.pending.resolve();
    }
    async close() {
      if (this.pending) throw new Error('closed while waiting');
      this.closed = true;
    }
  };

  return rti;
}

function makeConnector(config, rti) {
  const connector = new DdsConnector({
    id: 'dds-test',
    type: 'dds',
    config,
    retryConfig: { enabled: false }
  });
  connector.on('error', () => {});
  connector.loadRti = () => rti;
  return connector;
}

const baseConfig = {
  participant: 'Lib::Participant',
  configFile: 'config/dds/UdcDdsExample.xml',
  waitTimeout: 50,
  inputs: [{ name: 'sensors', reader: 'Sub::SensorReader', deviceIdField: 'sensor_id' }],
  outputs: [{ name: 'setpoint', writer: 'Pub::SetpointWriter' }]
};

// ---------------------------------------------------------------------------
// Unit tests (fake RTI)
// ---------------------------------------------------------------------------

async function unitTests() {
  console.log('\n=== DDS (unit) ===');

  await test('factory creates dds connector', () => {
    assert.ok(ConnectorFactory.isTypeSupported('dds'));
    const c = ConnectorFactory.create('dds', { id: 'f', type: 'dds', config: { mode: 'simulation', inputs: [{ name: 'a' }] } });
    assert.ok(c instanceof DdsConnector);
  });

  await test('config validation', () => {
    const make = config => new DdsConnector({ id: 'v', type: 'dds', config });
    assert.throws(() => make({ ...baseConfig, mode: 'rtps' }), /unsupported mode/);
    assert.throws(() => make({ ...baseConfig, participant: undefined }), /requires participant/);
    assert.throws(() => make({ ...baseConfig, configFile: undefined }), /requires configFile/);
    assert.throws(() => make({ ...baseConfig, inputs: [], outputs: [] }), /at least one input/);
    assert.throws(() => make({ ...baseConfig, inputs: [{ name: 'x' }] }), /requires reader/);
    assert.throws(() => make({ ...baseConfig, outputs: [{ name: 'sensors', writer: 'P::W' }] }), /duplicate/);
    assert.throws(() => make({ ...baseConfig, inputs: [{ name: 'timestamp', reader: 'S::R' }] }), /reserved/);
    make({ mode: 'simulation', inputs: [{ name: 'x' }] }); // no reader/participant needed
  });

  await test('creates participant with resolved XML URL group', async () => {
    const rti = createFakeRti();
    const c = makeConnector({ ...baseConfig, configFile: ['a.xml', 'b.xml'] }, rti);
    await c.start();
    const p = rti.connectors[0];
    assert.strictEqual(p.configName, 'Lib::Participant');
    assert.strictEqual(p.url, `${path.resolve('a.xml')};${path.resolve('b.xml')}`);
    assert.ok(p.inputs['Sub::SensorReader']);
    assert.ok(p.outputs['Pub::SetpointWriter']);
    assert.ok(c.isConnected);
    await c.stop();
    assert.ok(p.closed);
  });

  await test('emits one data event per valid sample with source timestamp and deviceId', async () => {
    const rti = createFakeRti();
    const c = makeConnector(baseConfig, rti);
    await c.start();
    const p = rti.connectors[0];

    const received = [];
    c.on('data', d => received.push(d));
    const tsNs = Date.UTC(2026, 9, 15, 10, 0, 0) * 1e6;
    p.getInput('Sub::SensorReader').queue.push({ data: { sensor_id: 'S1', temperature: 21.5 }, ts: tsNs });
    p.getInput('Sub::SensorReader').queue.push({ data: null, valid: false }); // dispose: metadata only
    p.publish('Sub::SensorReader', { sensor_id: 'S2', temperature: 30 }, tsNs);
    await new Promise(r => setTimeout(r, 50));

    assert.strictEqual(received.length, 2);
    assert.deepStrictEqual(received[0].sensors, { sensor_id: 'S1', temperature: 21.5 });
    assert.strictEqual(received[0].deviceId, 'S1');
    assert.strictEqual(received[0].timestamp, '2026-10-15T10:00:00.000Z');
    assert.strictEqual(received[0].source, 'dds-test');
    assert.strictEqual(received[1].deviceId, 'S2');
    assert.strictEqual(c.getStatus().inputs[0].samplesReceived, 2);
    await c.stop();
  });

  await test('generic mapper turns a sample into measurements', async () => {
    const { MappingEngine } = require('../src/mappingTools');
    const engine = new MappingEngine();
    const mapped = await engine.mapData(
      { timestamp: '2026-10-15T10:00:00.000Z', source: 'dds-test', type: 'dds', quality: 'GOOD', deviceId: 'S1',
        sensors: { sensor_id: 'S1', temperature: 21.5, running: true } },
      'dds',
      { sourceId: 'dds-test', sourceType: 'dds' }
    );
    const ids = mapped.measurements.map(m => m.id).sort();
    assert.deepStrictEqual(ids, ['sensors_running', 'sensors_temperature']);
    assert.strictEqual(mapped.id, 'S1');
  });

  await test('writeOutput clears members, sets JSON and writes', async () => {
    const rti = createFakeRti();
    const c = makeConnector(baseConfig, rti);
    await c.start();
    const writer = rti.connectors[0].outputs['Pub::SetpointWriter'];

    await c.writeOutput('setpoint', { target_id: 'T1', value: 10 });
    await c.writeOutput('setpoint', { value: 12 }, { action: 'write' });
    assert.deepStrictEqual(writer.written[0].data, { target_id: 'T1', value: 10 });
    assert.deepStrictEqual(writer.written[1].data, { value: 12 }); // no stale target_id
    assert.deepStrictEqual(writer.written[1].params, { action: 'write' });
    await assert.rejects(c.writeOutput('nope', {}), /not found/);
    await assert.rejects(c.writeOutput('setpoint', 5), /JSON object/);
    await c.stop();
    await assert.rejects(c.writeOutput('setpoint', { value: 1 }), /not connected/);
  });

  await test('invalid reader name fails connect and releases participant', async () => {
    const rti = createFakeRti();
    const c = makeConnector({ ...baseConfig, inputs: [{ name: 'x', reader: 'Bad::Reader' }] }, rti);
    await assert.rejects(c.start(), /Invalid Subscription::DataReader name/);
    assert.ok(rti.connectors[0].closed);
    assert.ok(!c.isConnected);
  });

  await test('wait error triggers disconnect and reconnection', async () => {
    const rti = createFakeRti();
    const c = new DdsConnector({ id: 'r', type: 'dds', config: baseConfig, retryConfig: { maxRetries: 2, retryDelay: 20 } });
    c.on('error', () => {});
    c.loadRti = () => rti;
    await c.start();
    const first = rti.connectors[0];

    const reconnected = new Promise(resolve => c.once('reconnecting', resolve));
    first.failNextWait = true;
    first.publish('Sub::SensorReader', { sensor_id: 'S1' }); // wake current wait; next wait fails
    await reconnected;
    await new Promise(r => setTimeout(r, 50));
    assert.ok(first.closed);
    assert.strictEqual(rti.connectors.length, 2);
    assert.ok(c.isConnected);

    const data = nextData(c);
    rti.connectors[1].publish('Sub::SensorReader', { sensor_id: 'S3' });
    assert.strictEqual((await data).deviceId, 'S3');
    await c.stop();
  });

  await test('simulation mode generates samples from field specs', async () => {
    const c = makeConnector({
      mode: 'simulation',
      simulationInterval: 20,
      inputs: [{ name: 'sim', fields: { temp: { min: 10, max: 20 }, on: 'boolean', tag: { type: 'string', value: 'A' } } }],
      outputs: [{ name: 'out' }]
    }, null);
    await c.start();
    const data = await nextData(c);
    assert.ok(data.sim.temp >= 10 && data.sim.temp <= 20);
    assert.strictEqual(typeof data.sim.on, 'boolean');
    assert.strictEqual(data.sim.tag, 'A');
    await c.writeOutput('out', { value: 1 });
    assert.strictEqual(c.getStatus().outputs[0].samplesWritten, 1);
    await c.stop();
  });

  await test('missing optional package gives an actionable error', () => {
    const c = new DdsConnector({ id: 'm', type: 'dds', config: baseConfig });
    const Module = require('module');
    const original = Module._load;
    Module._load = function (request, ...rest) {
      if (request === 'rticonnextdds-connector') throw new Error("Cannot find module 'rticonnextdds-connector'");
      return original.call(this, request, ...rest);
    };
    try {
      assert.throws(() => c.loadRti(), /npm install rticonnextdds-connector/);
    } finally {
      Module._load = original;
    }
  });
}

// ---------------------------------------------------------------------------
// Loopback test against a real DDS domain
// ---------------------------------------------------------------------------

async function loopbackTests() {
  console.log('\n=== DDS (real domain loopback) ===');

  let rti;
  try {
    rti = require('rticonnextdds-connector');
  } catch (error) {
    results.skipped++;
    console.log(`  - skipped: rticonnextdds-connector not available (${error.message.split('\n')[0]})`);
    return;
  }

  const xml = path.join(__dirname, 'fixtures', 'dds-test.xml');

  await test('receives samples from and publishes to another DDS participant', async () => {
    const udc = new DdsConnector({
      id: 'dds-loopback',
      type: 'dds',
      config: {
        participant: 'UdcTestParticipants::Udc',
        configFile: xml,
        waitTimeout: 200,
        inputs: [{ name: 'sensors', reader: 'Sub::SensorReader', deviceIdField: 'sensor_id' }],
        outputs: [{ name: 'setpoint', writer: 'Pub::SetpointWriter' }]
      },
      retryConfig: { enabled: false }
    });
    udc.on('error', () => {});
    await udc.start();

    const device = new rti.Connector('UdcTestParticipants::Device', xml);
    try {
      const sensorWriter = device.getOutput('Pub::SensorWriter');
      const setpointReader = device.getInput('Sub::SetpointReader');
      await sensorWriter.waitForSubscriptions(5000);

      const data = nextData(udc, 5000);
      sensorWriter.instance.setFromJson({ sensor_id: 'S1', temperature: 21.5, running: true });
      sensorWriter.write();
      const received = await data;
      assert.deepStrictEqual(received.sensors, { sensor_id: 'S1', temperature: 21.5, running: true });
      assert.strictEqual(received.deviceId, 'S1');
      assert.ok(Math.abs(Date.parse(received.timestamp) - Date.now()) < 10000);

      await setpointReader.waitForPublications(5000);
      await udc.writeOutput('setpoint', { target_id: 'T1', value: 42 });
      await setpointReader.wait({ timeout: 5000 });
      setpointReader.take();
      const samples = [...setpointReader.samples.validDataIter].map(s => s.getJson());
      assert.deepStrictEqual(samples, [{ target_id: 'T1', value: 42 }]);
    } finally {
      await device.close();
      await udc.stop();
    }
  });
}

async function main() {
  await unitTests();
  await loopbackTests();

  console.log(`\nDDS tests: ${results.passed} passed, ${results.failed} failed${results.skipped ? `, ${results.skipped} skipped` : ''}`);
  process.exit(results.failed > 0 ? 1 : 0);
}

main();
