const BaseConnector = require('../BaseConnector');
const logger = require('../../utils/logger');

/**
 * ProfinetConnector - PROFINET IO connector
 *
 * PROFINET IO cyclic data is exchanged at Layer 2 (EtherType 0x8892) with
 * real-time cycle times that a Node.js process cannot provide. The standard
 * way for SCADA/IIoT software to access PROFINET IO data is through the
 * IO Controller: every IO Device module is mapped by the engineering tool
 * (TIA Portal / STEP 7) into the controller's process image (I/Q areas).
 *
 * Supported modes:
 *  - 's7' (default): reads/writes the IO Devices' process image on a Siemens
 *    IO Controller (S7-300/400/1200/1500, ET200 CPU) via S7comm (ISO-on-TCP 102).
 *    Each IO point declares its I/Q/PI/PQ/M/DB address as assigned in the
 *    hardware configuration, e.g. "I0.0", "IW64", "QB2", "PIW256", "DB10,REAL4".
 *  - 'simulation': generates random values, for testing without hardware.
 *
 * S7-1200/1500: enable "Permit access with PUT/GET communication" in the CPU
 * protection settings.
 */
const SUPPORTED_MODES = ['s7', 'simulation'];
const DEFAULT_POLLING_INTERVAL = 100;
const MIN_POLLING_INTERVAL = 20;

class ProfinetConnector extends BaseConnector {
  constructor(config) {
    super(config);
    this.conn = null;
    this.devices = new Map();
    this.tagIndex = new Map(); // tag -> { device, point, direction }
    this.pollingTimer = null;
    this.cycleInProgress = false;
    this.consecutiveFailures = 0;
    this.stats = { cycles: 0, failedCycles: 0, lastCycleTime: null, lastError: null };

    this.validateConfig();
  }

  get mode() {
    return this.config.config.mode || 's7';
  }

  validateConfig() {
    super.validateConfig();

    const { config } = this.config;

    if (!SUPPORTED_MODES.includes(this.mode)) {
      throw new Error(`PROFINET connector: unsupported mode '${this.mode}' (supported: ${SUPPORTED_MODES.join(', ')})`);
    }

    if (!config.controllerIp) {
      throw new Error('PROFINET connector requires controllerIp (IO Controller IP address)');
    }

    if (!config.devices || !Array.isArray(config.devices) || config.devices.length === 0) {
      throw new Error('PROFINET connector requires devices array with at least one device definition');
    }

    const deviceNames = new Set();
    config.devices.forEach((device, index) => {
      if (!device.name) {
        throw new Error(`Device at index ${index} requires name`);
      }
      if (deviceNames.has(device.name)) {
        throw new Error(`Duplicate PROFINET device name '${device.name}'`);
      }
      deviceNames.add(device.name);

      for (const direction of ['inputs', 'outputs']) {
        const points = device[direction] || [];
        if (!Array.isArray(points)) {
          throw new Error(`Device '${device.name}': ${direction} must be an array`);
        }
        points.forEach((point, pIndex) => {
          if (!point.name) {
            throw new Error(`Device '${device.name}': ${direction}[${pIndex}] requires name`);
          }
          if (this.mode === 's7' && !point.address) {
            throw new Error(
              `Device '${device.name}': ${direction} '${point.name}' requires address (e.g. "I0.0", "IW64", "Q0.1") ` +
              `in s7 mode. Use mode "simulation" to run without an IO Controller.`
            );
          }
        });
      }
    });
  }

  async initialize() {
    await super.initialize();
    logger.debug(`Initialized PROFINET connector '${this.id}' (mode: ${this.mode}, controller: ${this.config.config.controllerIp})`);
  }

  // Overridable for tests
  createS7Client() {
    const NodeS7 = require('nodes7');
    return new NodeS7({ silent: true });
  }

  buildDevices() {
    this.devices.clear();
    this.tagIndex.clear();

    for (const deviceConfig of this.config.config.devices) {
      const device = {
        name: deviceConfig.name,
        stationName: deviceConfig.stationName,
        slot: deviceConfig.slot,
        type: deviceConfig.type || 'IO-Device',
        vendorId: deviceConfig.vendorId,
        deviceId: deviceConfig.deviceId,
        inputs: (deviceConfig.inputs || []).map(p => ({ ...p })),
        outputs: (deviceConfig.outputs || []).map(p => ({ ...p })),
        status: 'UNKNOWN',
        values: { inputs: {}, outputs: {} },
        lastUpdate: null,
        errorCount: 0
      };

      for (const direction of ['inputs', 'outputs']) {
        for (const point of device[direction]) {
          const tag = `${device.name}.${point.name}`;
          this.tagIndex.set(tag, { device, point, direction });
        }
      }

      this.devices.set(device.name, device);
    }
  }

  async connect() {
    const { config } = this.config;
    this.stopPolling();
    this.buildDevices();

    try {
      if (this.mode === 'simulation') {
        logger.warn(`PROFINET connector '${this.id}' running in SIMULATION mode - values are random`);
        for (const device of this.devices.values()) {
          device.status = 'ONLINE';
        }
      } else {
        await this.connectS7();
        logger.info(
          `PROFINET connector '${this.id}' connected to IO Controller ${config.controllerIp} ` +
          `(${this.devices.size} devices, ${this.tagIndex.size} IO points)`
        );
      }

      this.consecutiveFailures = 0;
      this.onConnected();
      this.startPolling();
    } catch (error) {
      logger.error(`PROFINET connector '${this.id}' connection failed:`, error);
      this.emit('error', error);
      throw error;
    }
  }

  async connectS7() {
    const { config } = this.config;

    await this.dropS7Connection();
    this.conn = this.createS7Client();

    await new Promise((resolve, reject) => {
      this.conn.initiateConnection({
        host: config.controllerIp,
        port: config.port || 102,
        rack: config.rack ?? 0,
        slot: config.slot ?? 1,
        timeout: config.timeout || 5000
      }, (err) => {
        if (err) {
          reject(err instanceof Error ? err : new Error(`S7 connection to ${config.controllerIp} failed: ${err}`));
          return;
        }
        resolve();
      });
    });

    const addresses = {};
    for (const [tag, { point }] of this.tagIndex.entries()) {
      addresses[tag] = point.address;
    }
    this.conn.setTranslationCB(tag => addresses[tag]);
    this.conn.addItems(Object.keys(addresses));
  }

  async dropS7Connection() {
    if (!this.conn) return;
    const conn = this.conn;
    this.conn = null;
    await new Promise(resolve => {
      try {
        conn.dropConnection(() => resolve());
        setTimeout(resolve, 2000); // don't hang if the callback never fires
      } catch (error) {
        logger.debug(`PROFINET connector '${this.id}' error dropping S7 connection: ${error.message}`);
        resolve();
      }
    });
  }

  async disconnect() {
    this.stopPolling();

    try {
      await this.dropS7Connection();
      for (const device of this.devices.values()) {
        device.status = 'OFFLINE';
      }
      logger.info(`PROFINET connector '${this.id}' disconnected`);
    } catch (error) {
      logger.error(`Error disconnecting PROFINET connector '${this.id}':`, error);
    }

    this.isConnected = false;
  }

  startPolling() {
    const { config } = this.config;
    let interval = config.pollingInterval || config.cycleTime || DEFAULT_POLLING_INTERVAL;
    if (interval < MIN_POLLING_INTERVAL) {
      logger.warn(
        `PROFINET connector '${this.id}': polling interval ${interval}ms is below ${MIN_POLLING_INTERVAL}ms; ` +
        `using ${MIN_POLLING_INTERVAL}ms (PROFINET RT cycles are handled by the IO Controller, not by this connector)`
      );
      interval = MIN_POLLING_INTERVAL;
    }

    this.pollingTimer = setInterval(() => {
      if (this.isConnected && this.isRunning && !this.cycleInProgress) {
        this.processCycle();
      }
    }, interval);

    logger.debug(`PROFINET connector '${this.id}' started polling (interval: ${interval}ms)`);
  }

  stopPolling() {
    if (this.pollingTimer) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = null;
      logger.debug(`PROFINET connector '${this.id}' stopped polling`);
    }
  }

  async processCycle() {
    this.cycleInProgress = true;
    try {
      if (this.mode === 'simulation') {
        this.simulateCycle();
      } else {
        await this.readS7Cycle();
      }

      this.stats.cycles++;
      this.stats.lastCycleTime = new Date().toISOString();
      this.onData(this.buildDataPayload());
    } catch (error) {
      this.stats.failedCycles++;
      this.stats.lastError = error.message;
      this.consecutiveFailures++;
      logger.error(`PROFINET connector '${this.id}' cycle error: ${error.message}`);

      const maxFailures = this.config.config.maxConsecutiveFailures || 3;
      if (this.consecutiveFailures >= maxFailures) {
        this.stopPolling();
        for (const device of this.devices.values()) {
          device.status = 'OFFLINE';
        }
        this.onError(error);
      }
    } finally {
      this.cycleInProgress = false;
    }
  }

  readS7Cycle() {
    return new Promise((resolve, reject) => {
      if (!this.conn) {
        reject(new Error('S7 connection not available'));
        return;
      }

      this.conn.readAllItems((anythingBad, values) => {
        const now = new Date().toISOString();
        const deviceHasBad = new Map();
        let goodCount = 0;

        for (const [tag, { device, point, direction }] of this.tagIndex.entries()) {
          const raw = values ? values[tag] : undefined;
          const bad = raw === undefined || (typeof raw === 'string' && raw.startsWith('BAD'));
          if (bad) {
            deviceHasBad.set(device.name, true);
            device.values[direction][point.name] = null;
          } else {
            goodCount++;
            device.values[direction][point.name] = this.applyScaling(raw, point);
          }
        }

        for (const device of this.devices.values()) {
          if (deviceHasBad.get(device.name)) {
            device.status = 'ERROR';
            device.errorCount++;
          } else {
            device.status = 'ONLINE';
            device.lastUpdate = now;
          }
        }

        // Every item bad → controller unreachable (or all addresses invalid)
        if (anythingBad && goodCount === 0 && this.tagIndex.size > 0) {
          reject(new Error(`No valid data from IO Controller ${this.config.config.controllerIp}`));
          return;
        }

        this.consecutiveFailures = 0;
        resolve();
      });
    });
  }

  applyScaling(value, point) {
    if (typeof value !== 'number' || !point.scale) {
      return value;
    }
    // Linear scaling from raw range to engineering range, e.g. analog 0..27648 → 0..100 °C
    const { rawMin = 0, rawMax = 27648, engMin = 0, engMax = 100 } = point.scale;
    if (rawMax === rawMin) return value;
    return engMin + ((value - rawMin) * (engMax - engMin)) / (rawMax - rawMin);
  }

  simulateCycle() {
    const now = new Date().toISOString();
    for (const device of this.devices.values()) {
      for (const input of device.inputs) {
        device.values.inputs[input.name] = this.simulateInputValue(input);
      }
      for (const output of device.outputs) {
        device.values.outputs[output.name] = output.value ?? 0;
      }
      device.lastUpdate = now;
    }
  }

  buildDataPayload() {
    const data = {
      timestamp: new Date().toISOString(),
      source: this.id,
      type: this.type,
      devices: {}
    };

    for (const device of this.devices.values()) {
      data.devices[device.name] = {
        name: device.name,
        stationName: device.stationName,
        slot: device.slot,
        type: device.type,
        status: device.status,
        inputs: { ...device.values.inputs },
        outputs: { ...device.values.outputs }
      };
    }

    return data;
  }

  simulateInputValue(input) {
    switch (input.type) {
      case 'digital':
      case 'bool':
        return Math.random() > 0.5;
      case 'byte':
        return Math.floor(Math.random() * 256);
      case 'word':
      case 'analog':
        return Math.floor(Math.random() * 27649);
      case 'real':
        return Math.random() * 100;
      default:
        return 0;
    }
  }

  findPoint(deviceName, pointName, direction) {
    const device = this.devices.get(deviceName);
    if (!device) {
      throw new Error(`Device '${deviceName}' not found`);
    }
    const point = device[direction].find(p => p.name === pointName);
    if (!point) {
      throw new Error(`${direction === 'outputs' ? 'Output' : 'Input'} '${pointName}' not found on device '${deviceName}'`);
    }
    return { device, point };
  }

  async writeOutput(deviceName, outputName, value) {
    if (!this.isConnected) {
      throw new Error('PROFINET connector is not connected');
    }

    const { device, point } = this.findPoint(deviceName, outputName, 'outputs');

    if (this.mode === 'simulation') {
      point.value = value;
    } else {
      await this.writeS7(`${deviceName}.${outputName}`, value);
    }

    device.values.outputs[outputName] = value;
    logger.info(`PROFINET connector '${this.id}' wrote ${value} to ${deviceName}.${outputName}${point.address ? ` (${point.address})` : ''}`);
    return true;
  }

  async writeS7(tag, value, attempt = 0) {
    if (!this.conn) {
      throw new Error('S7 connection not available');
    }

    const queued = await new Promise((resolve, reject) => {
      const result = this.conn.writeItems(tag, value, (anythingBad) => {
        if (anythingBad) {
          reject(new Error(`S7 write of '${tag}' failed`));
          return;
        }
        resolve(true);
      });
      // nodes7 returns 1 if another write is still in progress
      if (result === 1) resolve(false);
    });

    if (!queued) {
      if (attempt >= 20) {
        throw new Error(`S7 write of '${tag}' timed out waiting for previous write`);
      }
      await new Promise(r => setTimeout(r, 50));
      return this.writeS7(tag, value, attempt + 1);
    }
  }

  async readDiagnostics(deviceName) {
    const device = this.devices.get(deviceName);
    if (!device) {
      throw new Error(`Device '${deviceName}' not found`);
    }

    return {
      deviceName: device.name,
      stationName: device.stationName,
      status: device.status,
      lastUpdate: device.lastUpdate,
      errorCount: device.errorCount,
      timestamp: new Date().toISOString(),
      connector: { mode: this.mode, ...this.stats }
    };
  }

  getStatus() {
    return {
      ...super.getStatus(),
      mode: this.mode,
      controllerIp: this.config.config.controllerIp,
      deviceCount: this.devices.size,
      ioPointCount: this.tagIndex.size,
      stats: { ...this.stats },
      devices: Array.from(this.devices.values()).map(d => ({
        name: d.name,
        stationName: d.stationName,
        type: d.type,
        status: d.status,
        lastUpdate: d.lastUpdate
      }))
    };
  }
}

module.exports = ProfinetConnector;
