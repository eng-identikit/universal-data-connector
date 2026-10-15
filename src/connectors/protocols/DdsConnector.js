const path = require('path');
const BaseConnector = require('../BaseConnector');
const logger = require('../../utils/logger');

/**
 * DdsConnector - OMG DDS (Data Distribution Service) connector
 *
 * Joins a DDS domain as a DomainParticipant and subscribes/publishes topics.
 * The wire protocol is standard DDS-RTPS, so it interoperates with any DDS
 * implementation (RTI Connext, eProsima Fast DDS, Eclipse Cyclone DDS,
 * OpenDDS) and with ROS 2 nodes, provided topic name, type and QoS match.
 *
 * Supported modes:
 *  - 'connector' (default): uses RTI Connector for JavaScript
 *    (optional npm package 'rticonnextdds-connector'). Types, topics, QoS and
 *    the participant's DataReaders/DataWriters are declared in an RTI
 *    XML-Based Application Creation file (see config/dds/UdcDdsExample.xml).
 *    Each connector input maps to a DataReader ("Subscriber::Reader"), each
 *    output to a DataWriter ("Publisher::Writer").
 *  - 'simulation': generates random samples, for testing without a DDS domain.
 *
 * Every valid sample received is emitted as one data event:
 *   { timestamp, source, type, quality, deviceId?, <inputName>: <sample JSON> }
 * where timestamp is the sample's DDS source timestamp.
 */
const SUPPORTED_MODES = ['connector', 'simulation'];
const DEFAULT_WAIT_TIMEOUT = 1000;
const DEFAULT_SIMULATION_INTERVAL = 1000;
const RESERVED_NAMES = ['timestamp', 'source', 'type', 'quality', 'id', 'deviceId'];

class DdsConnector extends BaseConnector {
  constructor(config) {
    super(config);
    this.rti = null;
    this.participant = null;
    this.inputs = new Map();  // name -> { config, reader, samplesReceived, lastSample, lastUpdate }
    this.outputs = new Map(); // name -> { config, writer, samplesWritten, lastValue, lastUpdate }
    this.readLoop = null;
    this.readLoopActive = false;
    this.simulationTimer = null;
    this.stats = { samplesReceived: 0, samplesWritten: 0, lastError: null };

    this.validateConfig();
  }

  get mode() {
    return this.config.config.mode || 'connector';
  }

  validateConfig() {
    super.validateConfig();

    const { config } = this.config;

    if (!SUPPORTED_MODES.includes(this.mode)) {
      throw new Error(`DDS connector: unsupported mode '${this.mode}' (supported: ${SUPPORTED_MODES.join(', ')})`);
    }

    const inputs = config.inputs || [];
    const outputs = config.outputs || [];
    if (!Array.isArray(inputs) || !Array.isArray(outputs)) {
      throw new Error('DDS connector: inputs and outputs must be arrays');
    }
    if (inputs.length === 0 && outputs.length === 0) {
      throw new Error('DDS connector requires at least one input (DataReader) or output (DataWriter)');
    }

    if (this.mode === 'connector') {
      if (!config.participant) {
        throw new Error('DDS connector requires participant ("ParticipantLibrary::Participant" in the XML configuration)');
      }
      if (!config.configFile || (Array.isArray(config.configFile) && config.configFile.length === 0)) {
        throw new Error('DDS connector requires configFile (path to the RTI XML application configuration)');
      }
    }

    const names = new Set();
    const checkEntry = (entry, index, kind, entityField, example) => {
      if (!entry.name) {
        throw new Error(`DDS connector: ${kind}[${index}] requires name`);
      }
      if (RESERVED_NAMES.includes(entry.name)) {
        throw new Error(`DDS connector: ${kind} name '${entry.name}' is reserved`);
      }
      if (names.has(entry.name)) {
        throw new Error(`DDS connector: duplicate input/output name '${entry.name}'`);
      }
      names.add(entry.name);
      if (this.mode === 'connector' && !entry[entityField]) {
        throw new Error(
          `DDS connector: ${kind} '${entry.name}' requires ${entityField} (e.g. "${example}") in connector mode. ` +
          `Use mode "simulation" to run without a DDS domain.`
        );
      }
    };
    inputs.forEach((input, i) => checkEntry(input, i, 'inputs', 'reader', 'MySubscriber::MyReader'));
    outputs.forEach((output, i) => checkEntry(output, i, 'outputs', 'writer', 'MyPublisher::MyWriter'));
  }

  async initialize() {
    await super.initialize();
    logger.debug(`Initialized DDS connector '${this.id}' (mode: ${this.mode}, participant: ${this.config.config.participant || '-'})`);
  }

  // Overridable for tests
  loadRti() {
    try {
      return require('rticonnextdds-connector');
    } catch (error) {
      throw new Error(
        `DDS connector requires the optional package 'rticonnextdds-connector' ` +
        `(npm install rticonnextdds-connector): ${error.message}`
      );
    }
  }

  getConfigUrl() {
    const files = [].concat(this.config.config.configFile);
    // ';' loads every file of the URL group (RTI "URL Groups" syntax)
    return files.map(f => path.resolve(f)).join(';');
  }

  buildEndpoints() {
    const { config } = this.config;
    this.inputs.clear();
    this.outputs.clear();

    for (const input of config.inputs || []) {
      this.inputs.set(input.name, { config: input, reader: null, samplesReceived: 0, lastSample: null, lastUpdate: null });
    }
    for (const output of config.outputs || []) {
      this.outputs.set(output.name, { config: output, writer: null, samplesWritten: 0, lastValue: null, lastUpdate: null });
    }
  }

  async connect() {
    await this.releaseParticipant();
    this.buildEndpoints();

    try {
      if (this.mode === 'simulation') {
        logger.warn(`DDS connector '${this.id}' running in SIMULATION mode - values are random`);
        this.onConnected();
        this.startSimulation();
        return;
      }

      this.rti = this.rti || this.loadRti();
      const { config } = this.config;
      this.participant = new this.rti.Connector(config.participant, this.getConfigUrl());

      for (const input of this.inputs.values()) {
        input.reader = this.participant.getInput(input.config.reader);
      }
      for (const output of this.outputs.values()) {
        output.writer = this.participant.getOutput(output.config.writer);
      }

      logger.info(
        `DDS connector '${this.id}' joined the DDS domain as '${config.participant}' ` +
        `(${this.inputs.size} readers, ${this.outputs.size} writers)`
      );

      this.onConnected();
      if (this.inputs.size > 0) {
        this.startReadLoop();
      }
    } catch (error) {
      await this.releaseParticipant();
      logger.error(`DDS connector '${this.id}' connection failed:`, error);
      this.emit('error', error);
      throw error;
    }
  }

  async disconnect() {
    await this.releaseParticipant();
    this.isConnected = false;
    logger.info(`DDS connector '${this.id}' disconnected`);
  }

  async releaseParticipant() {
    this.stopSimulation();

    // Stop the read loop first: the participant cannot be closed while a wait() is pending
    this.readLoopActive = false;
    if (this.readLoop) {
      await this.readLoop;
      this.readLoop = null;
    }

    if (!this.participant) return;
    const participant = this.participant;
    this.participant = null;
    try {
      await participant.close();
    } catch (error) {
      logger.debug(`DDS connector '${this.id}' error closing participant: ${error && error.message}`);
    }
  }

  startReadLoop() {
    this.readLoopActive = true;
    this.readLoop = this.runReadLoop();
  }

  // A single loop waits on the whole participant: RTI Connector does not allow
  // concurrent waits on the same Connector/Input objects.
  async runReadLoop() {
    const waitTimeout = this.config.config.waitTimeout || DEFAULT_WAIT_TIMEOUT;
    const participant = this.participant;

    while (this.readLoopActive && this.participant === participant) {
      try {
        await participant.wait(waitTimeout);
      } catch (error) {
        if (error instanceof this.rti.TimeoutError) continue;
        this.handleLoopError(error);
        return;
      }

      if (!this.readLoopActive) return;

      try {
        this.takeAll();
      } catch (error) {
        this.handleLoopError(error);
        return;
      }
    }
  }

  handleLoopError(error) {
    this.readLoopActive = false;
    this.stats.lastError = error.message;
    logger.error(`DDS connector '${this.id}' read error: ${error.message}`);
    this.onError(error);
  }

  takeAll() {
    for (const input of this.inputs.values()) {
      input.reader.take();
      for (const sample of input.reader.samples.validDataIter) {
        this.handleSample(input, sample.getJson(), this.sourceTimestamp(sample));
      }
    }
  }

  sourceTimestamp(sample) {
    try {
      const ns = sample.info.get('source_timestamp');
      if (typeof ns === 'number' && ns > 0) {
        return new Date(Math.floor(ns / 1e6)).toISOString();
      }
    } catch (error) {
      // sample info not available - fall back to reception time
    }
    return null;
  }

  handleSample(input, data, timestamp) {
    const now = new Date().toISOString();
    input.samplesReceived++;
    input.lastSample = data;
    input.lastUpdate = now;
    this.stats.samplesReceived++;

    const payload = {
      timestamp: timestamp || now,
      source: this.id,
      type: this.type,
      quality: 'GOOD',
      [input.config.name]: data
    };

    const idField = input.config.deviceIdField;
    if (idField && data && data[idField] !== undefined && data[idField] !== null) {
      payload.deviceId = String(data[idField]);
    }

    this.onData(payload);
  }

  startSimulation() {
    const interval = this.config.config.simulationInterval || DEFAULT_SIMULATION_INTERVAL;
    this.simulationTimer = setInterval(() => {
      if (!this.isConnected || !this.isRunning) return;
      for (const input of this.inputs.values()) {
        this.handleSample(input, this.simulateSample(input.config), null);
      }
    }, interval);
  }

  stopSimulation() {
    if (this.simulationTimer) {
      clearInterval(this.simulationTimer);
      this.simulationTimer = null;
    }
  }

  simulateSample(inputConfig) {
    const fields = inputConfig.fields || { value: 'number' };
    const sample = {};
    for (const [name, spec] of Object.entries(fields)) {
      const kind = typeof spec === 'string' ? spec : (spec.type || 'number');
      switch (kind) {
        case 'boolean':
        case 'bool':
          sample[name] = Math.random() > 0.5;
          break;
        case 'string':
          sample[name] = typeof spec === 'object' && spec.value !== undefined ? spec.value : `${name}-${Math.floor(Math.random() * 1000)}`;
          break;
        default: {
          const min = typeof spec === 'object' && spec.min !== undefined ? spec.min : 0;
          const max = typeof spec === 'object' && spec.max !== undefined ? spec.max : 100;
          sample[name] = min + Math.random() * (max - min);
        }
      }
    }
    return sample;
  }

  /**
   * Publish a sample on an output's DataWriter.
   * @param {string} outputName - Output name from the configuration
   * @param {Object} value - Sample as JSON, matching the topic type
   * @param {Object} [params] - Optional write params (e.g. { action: 'dispose' })
   */
  async writeOutput(outputName, value, params) {
    if (!this.isConnected) {
      throw new Error('DDS connector is not connected');
    }

    const output = this.outputs.get(outputName);
    if (!output) {
      throw new Error(`Output '${outputName}' not found`);
    }
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error(`DDS output '${outputName}': value must be a JSON object matching the topic type`);
    }

    if (this.mode === 'connector') {
      // Start from defaults so members omitted from value don't keep the previous sample's values
      output.writer.clearMembers();
      output.writer.instance.setFromJson(value);
      output.writer.write(params);
    }

    output.samplesWritten++;
    output.lastValue = value;
    output.lastUpdate = new Date().toISOString();
    this.stats.samplesWritten++;
    logger.info(`DDS connector '${this.id}' wrote sample to '${outputName}'${output.config.writer ? ` (${output.config.writer})` : ''}`);
    return true;
  }

  getStatus() {
    return {
      ...super.getStatus(),
      mode: this.mode,
      participant: this.config.config.participant,
      stats: { ...this.stats },
      inputs: Array.from(this.inputs.values()).map(i => ({
        name: i.config.name,
        reader: i.config.reader,
        samplesReceived: i.samplesReceived,
        lastUpdate: i.lastUpdate
      })),
      outputs: Array.from(this.outputs.values()).map(o => ({
        name: o.config.name,
        writer: o.config.writer,
        samplesWritten: o.samplesWritten,
        lastUpdate: o.lastUpdate
      }))
    };
  }
}

module.exports = DdsConnector;
