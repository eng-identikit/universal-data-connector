const BaseConnector = require('../BaseConnector');
const logger = require('../../utils/logger');

/**
 * EtherCATConnector - EtherCAT connector via Beckhoff ADS (TwinCAT)
 *
 * An EtherCAT master needs raw Ethernet access and hard real-time cycles,
 * which a Node.js process cannot provide. This connector talks to the
 * EtherCAT master (TwinCAT 2/3) over ADS/AMS (TCP 48898) using `ads-client`:
 *
 *  - Process data: PLC symbols linked to the slaves' PDOs (e.g. "GVL_IO.bSensor1")
 *    or raw process image access via indexGroup/indexOffset
 *    (0xF020 = input image, 0xF030 = output image of the PLC task).
 *    All points are read in a single ADS sum command per cycle.
 *  - Slave diagnostics: EtherCAT state machine (INIT/PREOP/SAFEOP/OP) and
 *    link state of every slave, read from the EtherCAT master device
 *    (requires masterAmsNetId, e.g. "192.168.1.120.3.1").
 *  - CoE mailbox: SDO upload/download (object dictionary) via index group 0xF302.
 *
 * Modes:
 *  - 'ads' (default): real communication through TwinCAT ADS.
 *  - 'simulation': random values, for testing without hardware.
 *
 * Connecting from a PC without TwinCAT: set localAmsNetId/localAdsPort and add a
 * static route for this host on the TwinCAT system.
 */
const SUPPORTED_MODES = ['ads', 'simulation'];
const DEFAULT_POLLING_INTERVAL = 100;
const MIN_POLLING_INTERVAL = 10;
const MAX_SUM_COMMANDS = 500;

// EtherCAT master ADS interface (Beckhoff TcEtherCAT library)
const ECAT_MASTER_PORT = 0xFFFF;
const ADSIGRP_MASTER_COUNT_SLAVE = 0x0006;
const ADSIGRP_MASTER_SLAVE_ADDRESSES = 0x0007;
const ADSIGRP_MASTER_SLAVE_STATES = 0x0009;
const ADSIGRP_CANOPEN_SDO = 0xF302;

const SLAVE_STATES = { 1: 'INIT', 2: 'PREOP', 3: 'BOOT', 4: 'SAFEOP', 8: 'OP' };
const SLAVE_STATE_FLAGS = { 0x10: 'ERROR', 0x20: 'INVALID_VPRS', 0x40: 'INITCMD_ERROR', 0x80: 'DISABLED' };
const LINK_STATE_FLAGS = { 0x01: 'NOT_PRESENT', 0x02: 'WITHOUT_COMM', 0x04: 'MISSING_LINK', 0x08: 'ADDITIONAL_LINK' };

// Raw codec for indexGroup/indexOffset and SDO access
const RAW_TYPES = {
  BOOL:  { size: 1, read: (b, o) => b.readUInt8(o) !== 0, write: (b, v, o) => b.writeUInt8(v ? 1 : 0, o) },
  BYTE:  { size: 1, read: (b, o) => b.readUInt8(o), write: (b, v, o) => b.writeUInt8(v, o) },
  USINT: { size: 1, read: (b, o) => b.readUInt8(o), write: (b, v, o) => b.writeUInt8(v, o) },
  SINT:  { size: 1, read: (b, o) => b.readInt8(o), write: (b, v, o) => b.writeInt8(v, o) },
  WORD:  { size: 2, read: (b, o) => b.readUInt16LE(o), write: (b, v, o) => b.writeUInt16LE(v, o) },
  UINT:  { size: 2, read: (b, o) => b.readUInt16LE(o), write: (b, v, o) => b.writeUInt16LE(v, o) },
  INT:   { size: 2, read: (b, o) => b.readInt16LE(o), write: (b, v, o) => b.writeInt16LE(v, o) },
  DWORD: { size: 4, read: (b, o) => b.readUInt32LE(o), write: (b, v, o) => b.writeUInt32LE(v, o) },
  UDINT: { size: 4, read: (b, o) => b.readUInt32LE(o), write: (b, v, o) => b.writeUInt32LE(v, o) },
  DINT:  { size: 4, read: (b, o) => b.readInt32LE(o), write: (b, v, o) => b.writeInt32LE(v, o) },
  REAL:  { size: 4, read: (b, o) => b.readFloatLE(o), write: (b, v, o) => b.writeFloatLE(v, o) },
  LREAL: { size: 8, read: (b, o) => b.readDoubleLE(o), write: (b, v, o) => b.writeDoubleLE(v, o) },
  LINT:  { size: 8, read: (b, o) => Number(b.readBigInt64LE(o)), write: (b, v, o) => b.writeBigInt64LE(BigInt(v), o) },
  ULINT: { size: 8, read: (b, o) => Number(b.readBigUInt64LE(o)), write: (b, v, o) => b.writeBigUInt64LE(BigInt(v), o) }
};

function parseNumber(value) {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') return value.trim().toLowerCase().startsWith('0x') ? parseInt(value, 16) : parseInt(value, 10);
  return NaN;
}

function decodeRaw(buffer, type, bit) {
  const codec = RAW_TYPES[type];
  if (!codec) throw new Error(`Unsupported EtherCAT data type '${type}'`);
  if (bit !== undefined) {
    return (buffer.readUInt8(0) & (1 << bit)) !== 0;
  }
  return codec.read(buffer, 0);
}

function encodeRaw(value, type) {
  const codec = RAW_TYPES[type];
  if (!codec) throw new Error(`Unsupported EtherCAT data type '${type}'`);
  const buffer = Buffer.alloc(codec.size);
  codec.write(buffer, value, 0);
  return buffer;
}

function decodeSlaveState(deviceState, linkState) {
  const flags = Object.entries(SLAVE_STATE_FLAGS)
    .filter(([mask]) => deviceState & Number(mask))
    .map(([, name]) => name);
  const link = Object.entries(LINK_STATE_FLAGS)
    .filter(([mask]) => linkState & Number(mask))
    .map(([, name]) => name);
  return {
    state: SLAVE_STATES[deviceState & 0x0F] || 'UNKNOWN',
    flags,
    link: link.length ? link : ['OK'],
    raw: { deviceState, linkState }
  };
}

class EtherCATConnector extends BaseConnector {
  constructor(config) {
    super(config);
    this.client = null;
    this.slaves = new Map(); // name -> slave
    this.points = [];        // flat list of cyclic IO points
    this.pollingTimer = null;
    this.diagnosticsTimer = null;
    this.cycleInProgress = false;
    this.diagnosticsInProgress = false;
    this.consecutiveFailures = 0;
    this.masterInfo = null;
    this.stats = { cycles: 0, failedCycles: 0, lastCycleTime: null, lastError: null };

    this.validateConfig();
  }

  get mode() {
    return this.config.config.mode || 'ads';
  }

  validateConfig() {
    super.validateConfig();

    const { config } = this.config;

    if (!SUPPORTED_MODES.includes(this.mode)) {
      throw new Error(`EtherCAT connector: unsupported mode '${this.mode}' (supported: ${SUPPORTED_MODES.join(', ')})`);
    }

    if (this.mode === 'ads' && !config.targetAmsNetId) {
      throw new Error('EtherCAT connector requires targetAmsNetId (TwinCAT AMS Net ID, e.g. "192.168.1.120.1.1") in ads mode');
    }

    if (!config.slaves || !Array.isArray(config.slaves) || config.slaves.length === 0) {
      throw new Error('EtherCAT connector requires slaves array with at least one slave definition');
    }

    const names = new Set();
    config.slaves.forEach((slave, index) => {
      if (!slave.name) {
        throw new Error(`Slave at index ${index} requires name`);
      }
      if (names.has(slave.name)) {
        throw new Error(`Duplicate EtherCAT slave name '${slave.name}'`);
      }
      names.add(slave.name);

      for (const direction of ['inputs', 'outputs']) {
        const points = slave[direction] || [];
        if (!Array.isArray(points)) {
          throw new Error(`Slave '${slave.name}': ${direction} must be an array`);
        }
        points.forEach((point, pIndex) => {
          if (!point.name) {
            throw new Error(`Slave '${slave.name}': ${direction}[${pIndex}] requires name`);
          }
          if (this.mode !== 'ads') return;
          const hasRaw = point.indexGroup !== undefined && point.indexOffset !== undefined;
          if (!point.symbol && !hasRaw) {
            throw new Error(
              `Slave '${slave.name}': ${direction} '${point.name}' requires symbol (e.g. "GVL_IO.bInput1") ` +
              `or indexGroup + indexOffset + type. Use mode "simulation" to run without TwinCAT.`
            );
          }
          if (!point.symbol && !RAW_TYPES[(point.type || '').toUpperCase()]) {
            throw new Error(
              `Slave '${slave.name}': ${direction} '${point.name}' needs a raw type (${Object.keys(RAW_TYPES).join(', ')})`
            );
          }
        });
      }

      (slave.sdo || []).forEach((sdo, sIndex) => {
        if (!sdo.name || sdo.index === undefined || !RAW_TYPES[(sdo.type || '').toUpperCase()]) {
          throw new Error(`Slave '${slave.name}': sdo[${sIndex}] requires name, index and a valid type`);
        }
      });
    });
  }

  async initialize() {
    await super.initialize();
    const { config } = this.config;
    logger.debug(
      `Initialized EtherCAT connector '${this.id}' (mode: ${this.mode}` +
      (this.mode === 'ads' ? `, target: ${config.targetAmsNetId}:${config.targetAdsPort || 851})` : ')')
    );
  }

  // Overridable for tests
  createAdsClient(settings) {
    const { Client } = require('ads-client');
    return new Client(settings);
  }

  buildSlaves() {
    this.slaves.clear();
    this.points = [];

    for (const slaveConfig of this.config.config.slaves) {
      const slave = {
        name: slaveConfig.name,
        position: slaveConfig.position,
        address: slaveConfig.address !== undefined ? parseNumber(slaveConfig.address) : undefined,
        vendorId: slaveConfig.vendorId,
        productCode: slaveConfig.productCode,
        inputs: (slaveConfig.inputs || []).map(p => ({ ...p })),
        outputs: (slaveConfig.outputs || []).map(p => ({ ...p })),
        sdo: (slaveConfig.sdo || []).map(s => ({ ...s })),
        state: 'UNKNOWN',
        stateInfo: null,
        values: { inputs: {}, outputs: {}, sdo: {} },
        lastUpdate: null
      };

      for (const direction of ['inputs', 'outputs']) {
        for (const point of slave[direction]) {
          if (point.type) point.type = point.type.toUpperCase();
          if (point.indexGroup !== undefined) point.indexGroup = parseNumber(point.indexGroup);
          if (point.indexOffset !== undefined) point.indexOffset = parseNumber(point.indexOffset);
          this.points.push({ slave, point, direction });
        }
      }
      for (const sdo of slave.sdo) {
        sdo.type = sdo.type.toUpperCase();
        sdo.index = parseNumber(sdo.index);
        sdo.subIndex = parseNumber(sdo.subIndex ?? 0);
      }

      this.slaves.set(slave.name, slave);
    }
  }

  async connect() {
    this.stopPolling();
    this.buildSlaves();

    try {
      if (this.mode === 'simulation') {
        logger.warn(`EtherCAT connector '${this.id}' running in SIMULATION mode - values are random`);
        for (const slave of this.slaves.values()) {
          slave.state = 'OP';
        }
      } else {
        await this.connectAds();
      }

      this.consecutiveFailures = 0;
      this.onConnected();
      this.startPolling();
    } catch (error) {
      logger.error(`EtherCAT connector '${this.id}' connection failed:`, error);
      await this.closeClient();
      this.emit('error', error);
      throw error;
    }
  }

  async connectAds() {
    const { config } = this.config;
    await this.closeClient();

    const settings = {
      targetAmsNetId: config.targetAmsNetId,
      targetAdsPort: config.targetAdsPort || 851,
      timeoutDelay: config.timeout || 2000,
      autoReconnect: true,
      reconnectInterval: config.reconnectInterval || 2000,
      hideConsoleWarnings: true
    };
    for (const key of ['routerAddress', 'routerTcpPort', 'localAddress', 'localTcpPort', 'localAmsNetId', 'localAdsPort']) {
      if (config[key] !== undefined) settings[key] = config[key];
    }
    // No PLC symbols needed → don't require a PLC runtime on the target port
    if (this.points.every(p => !p.point.symbol)) {
      settings.rawClient = true;
    }

    this.client = this.createAdsClient(settings);
    this.attachClientEvents(this.client);

    await this.client.connect();
    logger.info(`EtherCAT connector '${this.id}' connected to TwinCAT ${config.targetAmsNetId}:${settings.targetAdsPort}`);

    await this.resolveSymbols();

    if (config.masterAmsNetId) {
      try {
        await this.readSlaveStates();
        await this.readSdoValues();
      } catch (error) {
        logger.warn(`EtherCAT connector '${this.id}': cannot read EtherCAT master diagnostics at ${config.masterAmsNetId}: ${error.message}`);
      }
    }
  }

  attachClientEvents(client) {
    client.on('connectionLost', () => {
      if (client !== this.client) return;
      logger.warn(`EtherCAT connector '${this.id}': ADS connection lost, waiting for automatic reconnection`);
      this.isConnected = false;
      for (const slave of this.slaves.values()) slave.state = 'UNKNOWN';
      this.emit('disconnected');
      this.emit('reconnecting');
    });

    client.on('reconnect', async () => {
      if (client !== this.client || !this.isRunning) return;
      try {
        await this.resolveSymbols();
        this.consecutiveFailures = 0;
        this.onConnected();
      } catch (error) {
        logger.error(`EtherCAT connector '${this.id}' failed to restore symbols after reconnect: ${error.message}`);
      }
    });

    // PLC online change / download: symbol addresses may have moved
    client.on('plcSymbolVersionChange', async () => {
      if (client !== this.client) return;
      logger.info(`EtherCAT connector '${this.id}': PLC symbol version changed, re-resolving symbols`);
      try {
        await this.resolveSymbols();
      } catch (error) {
        logger.error(`EtherCAT connector '${this.id}' failed to re-resolve symbols: ${error.message}`);
      }
    });

    client.on('warning', message => logger.debug(`EtherCAT connector '${this.id}' ADS warning: ${message}`));
  }

  async resolveSymbols() {
    for (const { slave, point } of this.points) {
      if (!point.symbol) continue;
      try {
        const symbol = await this.client.getSymbol(point.symbol);
        point.resolved = {
          indexGroup: symbol.indexGroup,
          indexOffset: symbol.indexOffset,
          size: symbol.size,
          dataType: symbol.type
        };
      } catch (error) {
        point.resolved = null;
        throw new Error(`Slave '${slave.name}': cannot resolve PLC symbol '${point.symbol}': ${error.message}`);
      }
    }
  }

  async closeClient() {
    if (!this.client) return;
    const client = this.client;
    this.client = null;
    client.removeAllListeners();
    try {
      await client.disconnect();
    } catch (error) {
      logger.debug(`EtherCAT connector '${this.id}' error closing ADS client: ${error.message}`);
    }
  }

  async disconnect() {
    this.stopPolling();

    try {
      await this.closeClient();
      for (const slave of this.slaves.values()) {
        slave.state = 'UNKNOWN';
      }
      logger.info(`EtherCAT connector '${this.id}' disconnected`);
    } catch (error) {
      logger.error(`Error disconnecting EtherCAT connector '${this.id}':`, error);
    }

    this.isConnected = false;
  }

  startPolling() {
    const { config } = this.config;
    let interval = config.pollingInterval || config.cycleTime || DEFAULT_POLLING_INTERVAL;
    if (interval < MIN_POLLING_INTERVAL) {
      logger.warn(
        `EtherCAT connector '${this.id}': polling interval ${interval}ms is below ${MIN_POLLING_INTERVAL}ms; ` +
        `using ${MIN_POLLING_INTERVAL}ms (the EtherCAT cycle itself runs in TwinCAT)`
      );
      interval = MIN_POLLING_INTERVAL;
    }

    this.pollingTimer = setInterval(() => {
      if (this.isConnected && this.isRunning && !this.cycleInProgress) {
        this.processCycle();
      }
    }, interval);

    if (this.mode === 'ads' && config.masterAmsNetId) {
      const diagInterval = config.diagnosticsInterval || 1000;
      this.diagnosticsTimer = setInterval(() => {
        if (this.isConnected && this.isRunning && !this.diagnosticsInProgress) {
          this.processDiagnostics();
        }
      }, diagInterval);
    }

    logger.debug(`EtherCAT connector '${this.id}' started polling (interval: ${interval}ms)`);
  }

  stopPolling() {
    if (this.pollingTimer) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = null;
    }
    if (this.diagnosticsTimer) {
      clearInterval(this.diagnosticsTimer);
      this.diagnosticsTimer = null;
    }
  }

  async processCycle() {
    this.cycleInProgress = true;
    try {
      if (this.mode === 'simulation') {
        this.simulateCycle();
      } else {
        await this.readProcessData();
      }

      this.consecutiveFailures = 0;
      this.stats.cycles++;
      this.stats.lastCycleTime = new Date().toISOString();
      this.onData(this.buildDataPayload());
    } catch (error) {
      this.stats.failedCycles++;
      this.stats.lastError = error.message;
      this.consecutiveFailures++;
      logger.error(`EtherCAT connector '${this.id}' cycle error: ${error.message}`);

      const maxFailures = this.config.config.maxConsecutiveFailures || 3;
      if (this.consecutiveFailures >= maxFailures && this.isConnected) {
        // ads-client reconnects by itself on socket loss; this covers persistent ADS errors
        this.stopPolling();
        await this.closeClient();
        this.onError(error);
      }
    } finally {
      this.cycleInProgress = false;
    }
  }

  async processDiagnostics() {
    this.diagnosticsInProgress = true;
    try {
      await this.readSlaveStates();
      await this.readSdoValues();
    } catch (error) {
      logger.warn(`EtherCAT connector '${this.id}' diagnostics error: ${error.message}`);
    } finally {
      this.diagnosticsInProgress = false;
    }
  }

  async readProcessData() {
    if (!this.client) throw new Error('ADS client not available');

    const readable = this.points.filter(({ point }) => !point.symbol || point.resolved);
    const commands = readable.map(({ point }) => {
      if (point.symbol) {
        return { indexGroup: point.resolved.indexGroup, indexOffset: point.resolved.indexOffset, size: point.resolved.size };
      }
      return { indexGroup: point.indexGroup, indexOffset: point.indexOffset, size: RAW_TYPES[point.type].size };
    });

    const results = [];
    for (let i = 0; i < commands.length; i += MAX_SUM_COMMANDS) {
      results.push(...await this.client.readRawMulti(commands.slice(i, i + MAX_SUM_COMMANDS)));
    }

    const now = new Date().toISOString();
    const failedSlaves = new Set();
    let failures = 0;

    for (let i = 0; i < readable.length; i++) {
      const { slave, point, direction } = readable[i];
      const result = results[i];
      if (!result || !result.success || !result.value) {
        failures++;
        failedSlaves.add(slave.name);
        slave.values[direction][point.name] = null;
        continue;
      }
      slave.values[direction][point.name] = point.symbol
        ? await this.client.convertFromRaw(result.value, point.resolved.dataType)
        : decodeRaw(result.value, point.type, point.bit);
    }

    for (const slave of this.slaves.values()) {
      if (!failedSlaves.has(slave.name)) slave.lastUpdate = now;
    }

    if (readable.length > 0 && failures === readable.length) {
      throw new Error(`All ${failures} ADS reads failed (first error: ${results[0]?.errorStr || 'unknown'})`);
    }
  }

  masterTarget(adsPort = ECAT_MASTER_PORT) {
    return { amsNetId: this.config.config.masterAmsNetId, adsPort };
  }

  async readSlaveStates() {
    if (!this.client || !this.config.config.masterAmsNetId) return null;

    const countBuf = await this.client.readRaw(ADSIGRP_MASTER_COUNT_SLAVE, 0, 2, this.masterTarget());
    const count = countBuf.readUInt16LE(0);
    if (count === 0) {
      this.masterInfo = { slaveCount: 0, lastUpdate: new Date().toISOString() };
      return [];
    }

    const [addrBuf, stateBuf] = await Promise.all([
      this.client.readRaw(ADSIGRP_MASTER_SLAVE_ADDRESSES, 0, count * 2, this.masterTarget()),
      this.client.readRaw(ADSIGRP_MASTER_SLAVE_STATES, 0, count * 2, this.masterTarget())
    ]);

    const busSlaves = [];
    for (let i = 0; i < count; i++) {
      busSlaves.push({
        position: i,
        address: addrBuf.readUInt16LE(i * 2),
        ...decodeSlaveState(stateBuf.readUInt8(i * 2), stateBuf.readUInt8(i * 2 + 1))
      });
    }

    for (const slave of this.slaves.values()) {
      const match = slave.address !== undefined
        ? busSlaves.find(s => s.address === slave.address)
        : busSlaves.find(s => s.position === slave.position);
      if (match) {
        if (slave.address === undefined) slave.address = match.address;
        slave.state = match.flags.includes('ERROR') ? `${match.state}+ERROR` : match.state;
        slave.stateInfo = match;
      } else {
        slave.state = 'NOT_FOUND';
        slave.stateInfo = null;
      }
    }

    const configured = this.slaves.size;
    const inOp = busSlaves.filter(s => s.state === 'OP' && s.flags.length === 0).length;
    this.masterInfo = { slaveCount: count, slavesInOp: inOp, configuredSlaves: configured, lastUpdate: new Date().toISOString() };
    return busSlaves;
  }

  async readSdoValues() {
    for (const slave of this.slaves.values()) {
      if (!slave.sdo.length || slave.address === undefined) continue;
      for (const sdo of slave.sdo) {
        try {
          slave.values.sdo[sdo.name] = await this.readSdo(slave.name, sdo.index, sdo.subIndex, sdo.type);
        } catch (error) {
          slave.values.sdo[sdo.name] = null;
          logger.debug(`EtherCAT connector '${this.id}' SDO ${slave.name}.${sdo.name} read failed: ${error.message}`);
        }
      }
    }
  }

  getSlaveForMailbox(slaveName) {
    if (!this.client) throw new Error('EtherCAT connector is not connected');
    if (!this.config.config.masterAmsNetId) throw new Error('SDO access requires masterAmsNetId');
    const slave = this.slaves.get(slaveName);
    if (!slave) throw new Error(`Slave '${slaveName}' not found`);
    if (slave.address === undefined) throw new Error(`Slave '${slaveName}' has no EtherCAT address (set "address", e.g. 1001)`);
    return slave;
  }

  /**
   * CoE SDO upload (read object dictionary entry)
   * @param {string} slaveName
   * @param {number|string} index - e.g. 0x1018 or "0x1018"
   * @param {number} subIndex
   * @param {string} type - raw type (UINT, UDINT, REAL...)
   */
  async readSdo(slaveName, index, subIndex = 0, type = 'UDINT') {
    const slave = this.getSlaveForMailbox(slaveName);
    const t = type.toUpperCase();
    const offset = ((parseNumber(index) << 16) | (parseNumber(subIndex) & 0xFF)) >>> 0;
    const buffer = await this.client.readRaw(ADSIGRP_CANOPEN_SDO, offset, RAW_TYPES[t].size, this.masterTarget(slave.address));
    return decodeRaw(buffer, t);
  }

  /**
   * CoE SDO download (write object dictionary entry)
   */
  async writeSdo(slaveName, index, subIndex, value, type = 'UDINT') {
    const slave = this.getSlaveForMailbox(slaveName);
    const offset = ((parseNumber(index) << 16) | (parseNumber(subIndex) & 0xFF)) >>> 0;
    await this.client.writeRaw(ADSIGRP_CANOPEN_SDO, offset, encodeRaw(value, type.toUpperCase()), this.masterTarget(slave.address));
    logger.info(`EtherCAT connector '${this.id}' wrote SDO ${slaveName} 0x${parseNumber(index).toString(16)}:${subIndex} = ${value}`);
    return true;
  }

  simulateCycle() {
    const now = new Date().toISOString();
    for (const slave of this.slaves.values()) {
      for (const input of slave.inputs) {
        slave.values.inputs[input.name] = this.simulateInputValue(input);
      }
      for (const output of slave.outputs) {
        slave.values.outputs[output.name] = output.value ?? 0;
      }
      slave.lastUpdate = now;
    }
  }

  buildDataPayload() {
    const data = {
      timestamp: new Date().toISOString(),
      source: this.id,
      type: this.type,
      slaves: {}
    };
    if (this.masterInfo) {
      data.master = { ...this.masterInfo };
    }

    for (const slave of this.slaves.values()) {
      const slaveData = {
        name: slave.name,
        position: slave.position,
        address: slave.address,
        state: slave.state,
        inputs: { ...slave.values.inputs },
        outputs: { ...slave.values.outputs }
      };
      if (slave.sdo.length) slaveData.sdo = { ...slave.values.sdo };
      data.slaves[slave.name] = slaveData;
    }

    return data;
  }

  simulateInputValue(input) {
    switch ((input.type || '').toLowerCase()) {
      case 'digital':
      case 'bool':
        return Math.random() > 0.5;
      case 'analog':
      case 'int':
        return Math.floor(Math.random() * 32768);
      case 'real':
      case 'lreal':
        return Math.random() * 100;
      default:
        return 0;
    }
  }

  async writeOutput(slaveName, outputName, value) {
    if (!this.isConnected) {
      throw new Error('EtherCAT connector is not connected');
    }

    const slave = this.slaves.get(slaveName);
    if (!slave) {
      throw new Error(`Slave '${slaveName}' not found`);
    }
    const output = slave.outputs.find(o => o.name === outputName);
    if (!output) {
      throw new Error(`Output '${outputName}' not found on slave '${slaveName}'`);
    }

    if (this.mode === 'simulation') {
      output.value = value;
    } else if (output.symbol) {
      await this.client.writeValue(output.symbol, value);
    } else if (output.bit !== undefined) {
      // Bit inside a byte of the process image: read-modify-write
      const current = await this.client.readRaw(output.indexGroup, output.indexOffset, 1);
      const byte = value ? current.readUInt8(0) | (1 << output.bit) : current.readUInt8(0) & ~(1 << output.bit);
      await this.client.writeRaw(output.indexGroup, output.indexOffset, Buffer.from([byte]));
    } else {
      await this.client.writeRaw(output.indexGroup, output.indexOffset, encodeRaw(value, output.type));
    }

    slave.values.outputs[outputName] = value;
    logger.info(`EtherCAT connector '${this.id}' wrote ${value} to ${slaveName}.${outputName}`);
    return true;
  }

  getStatus() {
    const { config } = this.config;
    return {
      ...super.getStatus(),
      mode: this.mode,
      targetAmsNetId: config.targetAmsNetId,
      targetAdsPort: config.targetAdsPort || 851,
      masterAmsNetId: config.masterAmsNetId,
      master: this.masterInfo,
      slaveCount: this.slaves.size,
      ioPointCount: this.points.length,
      stats: { ...this.stats },
      slaves: Array.from(this.slaves.values()).map(s => ({
        name: s.name,
        position: s.position,
        address: s.address,
        state: s.state,
        link: s.stateInfo ? s.stateInfo.link : undefined,
        lastUpdate: s.lastUpdate
      }))
    };
  }
}

EtherCATConnector.decodeRaw = decodeRaw;
EtherCATConnector.encodeRaw = encodeRaw;
EtherCATConnector.decodeSlaveState = decodeSlaveState;

module.exports = EtherCATConnector;
