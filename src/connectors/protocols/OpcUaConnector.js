const {
  OPCUAClient,
  MessageSecurityMode,
  SecurityPolicy,
  AttributeIds,
  ClientSubscription,
  TimestampsToReturn,
  MonitoringMode,
  DataChangeFilter,
  DeadbandType,
  DataChangeTrigger,
  OPCUACertificateManager,
  DataType
} = require('node-opcua');
const fs = require('fs');
const path = require('path');
const os = require('os');
const BaseConnector = require('../BaseConnector');
const logger = require('../../utils/logger');

class OpcUaConnector extends BaseConnector {
  constructor(config) {
    super(config);
    this.client = null;
    this.session = null;
    this.subscription = null;
    this.monitoredItems = new Map();
    this.nodeMetadata = [];
    this.lastValues = {};
    this.pollingInterval = null;
    this.discoveredNodeIds = []; // 🔥 AGGIUNGI QUESTA LINEA
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = config.connectionStrategy?.maxRetry || 5;
  }

  validateConfig() {
    super.validateConfig();

    const { config } = this.config;
    if (!config.endpoint) {
      throw new Error('OPC UA connector requires an endpoint URL');
    }
    // Nodes are optional - if empty, auto-discovery will be triggered
    if (config.nodes && !Array.isArray(config.nodes)) {
      throw new Error('OPC UA nodes must be an array');
    }
    this.autoDiscovery = !config.nodes || config.nodes.length === 0;
  }

  async initialize() {
    await super.initialize();

    const { config } = this.config;

    // Client identity and PKI (trusted/rejected server certificates). In containers:
    //   UDC_CERTIFICATES_DIR      writable PKI folder (default ./certificates)
    //   OPCUA_CERTIFICATE_FILE    client certificate provided from outside (e.g. a Kubernetes
    //   OPCUA_PRIVATE_KEY_FILE    Secret): every replica presents the same identity, so a
    //                             server trusts it once whichever replica runs the connector
    //   OPCUA_APPLICATION_URI     must match the URI in the certificate
    const certificateFolder = process.env.UDC_CERTIFICATES_DIR || path.join(process.cwd(), 'certificates');
    const providedCertificate = process.env.OPCUA_CERTIFICATE_FILE;
    const certificateFile = providedCertificate || path.join(certificateFolder, 'server_certificate.pem');
    const applicationUri = OpcUaConnector.applicationUri();

    const certificateManager = new OPCUACertificateManager({
      rootFolder: certificateFolder,
      name: ''
    });

    try {
      await certificateManager.initialize();
    } catch (error) {
      logger.warn(`Failed to initialize certificate manager in ${certificateFolder}: ${error.message}`);
    }

    const privateKeyFile = process.env.OPCUA_PRIVATE_KEY_FILE || certificateManager.privateKey;
    if (providedCertificate && !process.env.OPCUA_PRIVATE_KEY_FILE) {
      logger.warn('OPCUA_CERTIFICATE_FILE is set without OPCUA_PRIVATE_KEY_FILE: the generated private key will not match it');
    }

    // Create certificate if it doesn't exist (never over a provided one)
    if (!providedCertificate && !fs.existsSync(certificateFile)) {
      try {
        await certificateManager.createSelfSignedCertificate({
          subject: '/CN=localhost/O=Engineering Ingegneria Informatica S.p.A./L=Palermo',
          startDate: new Date(),
          dns: [],
          validity: 365 * 5,
          applicationUri,
          outputFile: certificateFile
        });
        logger.info(`Created self-signed certificate at ${certificateFile}`);
      } catch (error) {
        logger.warn(`Failed to create self-signed certificate: ${error.message}`);
      }
    }

    const resolvedCertificateFilePath = path.resolve(certificateFile).replace(/\\/g, '/');
    const resolvedPrivateKeyFilePath = path.resolve(privateKeyFile).replace(/\\/g, '/');

    // Create OPC UA client with proper options from OPCUABinding example
    this.client = OPCUAClient.create({
      applicationName: "Universal Data Connector",
      applicationUri,
      endpointMustExist: false,
      securityMode: this.getSecurityMode(config.securityMode),
      securityPolicy: this.getSecurityPolicy(config.securityPolicy),
      defaultSecureTokenLifetime: 400000,
      keepSessionAlive: true,
      requestedSessionTimeout: 100000, // 100 seconds
      connectionStrategy: {
        maxRetry: 10,
        initialDelay: 2000,
        maxDelay: 10 * 1000
      },
      certificateFile: resolvedCertificateFilePath,
      privateKeyFile: resolvedPrivateKeyFilePath,
      clientCertificateManager: certificateManager,
      ...config.clientOptions
    });

    // Setup client event handlers
    this.client.on('connection_reestablished', () => {
      logger.info(`OPC UA client '${this.id}' connection reestablished`);
      this.onConnected();
    });

    this.client.on('after_reconnection', (err) => {
      logger.info(`OPC UA client '${this.id}' after_reconnection completed: ${err}`);
    });

    this.client.on('connection_lost', () => {
      logger.warn(`OPC UA client '${this.id}' connection lost`);
      this.onDisconnected();
    });

    this.client.on('close', (err) => {
      logger.info(`OPC UA client '${this.id}' closed: ${err}`);
    });

    this.client.on('backoff', (retryCount, delay) => {
      logger.error(`OPC UA client '${this.id}' connection failed (attempt ${retryCount}), retrying in ${delay}ms`);
    });

    this.client.on('start_reconnection', () => {
      logger.error(`OPC UA client '${this.id}' starting reconnection`);
    });

    logger.debug(`Initialized OPC UA connector for endpoint: ${config.endpoint}`);
  }

  /*async connect(){
    try {
      const { config } = this.config;

      logger.info(`Connecting to OPC UA server: ${config.endpoint}`);

      // Connect to the OPC UA server
      await this.client.connect(config.endpoint);

      // Create session with proper userIdentity (from OPCUABinding.js example)
      let userIdentity = null;
      if (config.username && config.password) {
        userIdentity = {
          userName: config.username,
          password: config.password,
          type: require('node-opcua-types').UserTokenType.UserName
        };
      }

      this.session = await this.client.createSession(userIdentity);

      logger.info(`OPC UA session created for connector '${this.id}'`);

      // Create subscription for monitoring data changes
      await this.createSubscription();

      // If auto-discovery is enabled, discover nodes
      if (this.autoDiscovery) {
        logger.info(`Auto-discovery enabled for connector '${this.id}'`);
        const discoveredNodes = await this.discoverNodes();
        logger.info(`Auto-discovery completed. Use /api/sources/${this.id}/discovery to see discovered nodes`);


        // Stampa JSON formattato
        logger.info(`🔍 NODI SCOPERTI (${discoveredNodes.length} totali):`);
        logger.info(JSON.stringify(discoveredNodes, null, 2));

        logger.info(`Auto-discovery completed. Use /api/sources/${this.id}/discovery to see discovered nodes`);
        this.discoveredNodes = discoveredNodes;
      } else {
        // Monitor configured nodes
        await this.monitorNodes();
      }

      this.onConnected();

    } catch (error) {
      logger.error(`Failed to connect OPC UA connector '${this.id}':`, error);
      await this.cleanup();
      throw error;
    }
  } */

  async connect() {
    try {
      const { config } = this.config;

      logger.info(`Connecting to OPC UA server at ${config.endpoint}...`);

      await this.client.connect(config.endpoint);
      logger.info('OPC UA client connected');

      this.session = await this.client.createSession();
      logger.info('OPC UA session created');

      // 🔥 AUTO-DISCOVERY: Se nodes è vuoto, leggi rootNode da config
      if (!config.nodes || config.nodes.length === 0) {
        if (config.rootNode) {
          logger.info(`🔍 Auto-discovery enabled from rootNode: ${config.rootNode}`);

          const discoveredNodes = await this.browseNodes(config.rootNode);

          if (discoveredNodes.length > 0) {
            // 🔥 SALVA IN ENTRAMBI I POSTI
            config.nodes = discoveredNodes.map(n => n.nodeId);
            this.discoveredNodeIds = config.nodes; // Salva anche qui per il polling
            this.nodeMetadata = discoveredNodes;

            logger.info(`✅ Auto-discovered ${config.nodes.length} nodes from ${config.rootNode}`);

            // Stampa i nodi scoperti
            console.log('\n' + '='.repeat(80));
            console.log('🔍 NODI OPC UA SCOPERTI:');
            console.log('='.repeat(80));
            discoveredNodes.forEach((node, idx) => {
              console.log(`  [${idx + 1}] ${node.browseName} → ${node.nodeId}`);
            });
            console.log('='.repeat(80) + '\n');
          } else {
            logger.warn(`No nodes discovered from ${config.rootNode}`);
          }
        } else {
          logger.warn('No nodes configured and no rootNode specified for auto-discovery');
        }
      } else {
        // Se i nodi sono già configurati, salvali
        this.discoveredNodeIds = config.nodes;
      }

      // Avvia monitoring o polling se ci sono nodi
      if (config.nodes && config.nodes.length > 0) {
        if (config.subscriptionMode === true) {
          await this.createSubscription();
          await this.monitorNodes();
        } else {
          // Usa polling mode
          this.startPolling();
        }
      } else {
        logger.warn('No nodes to monitor');
      }

      this.emit('connected');

    } catch (error) {
      logger.error('Failed to connect to OPC UA server:', error);
      this.emit('error', error);
      throw error;
    }
  }

  /* async disconnect() {
    await this.cleanup();
  } */
  async disconnect() {
    try {
      logger.info('Disconnecting from OPC UA server...');

      // Ferma polling se attivo
      if (this.pollingInterval) {
        clearInterval(this.pollingInterval);
        this.pollingInterval = null;
        logger.info('Polling stopped');
      }

      // Ferma monitoring
      if (this.subscription) {
        await this.subscription.terminate();
        this.subscription = null;
      }

      if (this.session) {
        await this.session.close();
        this.session = null;
      }

      if (this.client) {
        await this.client.disconnect();
        this.client = null;
      }

      this.emit('disconnected');
      logger.info('Disconnected from OPC UA server');

    } catch (error) {
      logger.error('Error during disconnect:', error);
    }
  }

  /* async cleanup() {
    try {
      // Clear monitored items
      if (this.subscription && this.monitoredItems.size > 0) {
        for (const [nodeId, monitoredItem] of this.monitoredItems) {
          try {
            await monitoredItem.terminate();
          } catch (error) {
            logger.debug(`Error terminating monitored item for ${nodeId}:`, error);
          }
        }
        this.monitoredItems.clear();
      }

      // Terminate subscription (before closing session)
      if (this.subscription) {
        try {
          await this.subscription.terminate();
          this.subscription = null;
        } catch (error) {
          logger.debug(`Error terminating OPC UA subscription:`, error);
        }
      }

      // Close session
      if (this.session) {
        try {
          await this.session.close();
          this.session = null;
        } catch (error) {
          logger.debug(`Error closing OPC UA session:`, error);
        }
      }

      // Disconnect client
      if (this.client) {
        try {
          await this.client.disconnect();
        } catch (error) {
          logger.debug(`Error disconnecting OPC UA client:`, error);
        }
      }

      logger.debug(`OPC UA connector '${this.id}' cleanup completed`);

    } catch (error) {
      logger.error(`Error during OPC UA connector '${this.id}' cleanup:`, error);
    }
  } */
 
  /**
   * Browse and discover available nodes
   * Returns discovered nodes that can be used for mapping
   */
  /*async discoverNodes() {
    if (!this.session) {
      throw new Error('Cannot discover nodes: no active session');
    }

    try {
      logger.info(`Starting node discovery for OPC UA connector '${this.id}'`);

      const discoveredNodes = [];
      const rootNodeId = 'ns=0;i=85'; // Objects folder

      await this.browseNode(rootNodeId, discoveredNodes, 0, 3); // Max depth 3

      logger.info(`Discovered ${discoveredNodes.length} nodes for connector '${this.id}'`);

      // Emit discovery event for mapping tools
      this.emit('nodesDiscovered', {
        sourceId: this.id,
        protocol: 'OPC-UA',
        nodes: discoveredNodes
      });

      return discoveredNodes;

    } catch (error) {
      logger.error(`Failed to discover nodes for connector '${this.id}':`, error);
      throw error;
    }
  } */

  /**
   * Recursively browse OPC UA address space
   */
  /* async browseNode(nodeId, discoveredNodes, currentDepth, maxDepth) {
    if (currentDepth >= maxDepth) {
      return;
    }

    try {
      const browseResult = await this.session.browse(nodeId);

      for (const reference of browseResult.references) {
        const node = {
          nodeId: reference.nodeId.toString(),
          browseName: reference.browseName.name,
          displayName: reference.displayName?.text || reference.browseName.name,
          nodeClass: reference.nodeClass,
          depth: currentDepth + 1
        };

        // If it's a variable node, get additional info
        if (reference.nodeClass === 2) { // Variable
          try {
            const dataValue = await this.session.read({
              nodeId: reference.nodeId,
              attributeId: AttributeIds.DataType
            });
            node.dataType = dataValue.value?.value?.toString();
          } catch (error) {
            // Continue without datatype info
          }
        }

        discoveredNodes.push(node);

        // Continue browsing if it's an Object or has children
        if (reference.nodeClass === 1 || reference.isForward) {
          await this.browseNode(reference.nodeId, discoveredNodes, currentDepth + 1, maxDepth);
        }
      }
    } catch (error) {
      logger.debug(`Error browsing node ${nodeId}:`, error.message);
    }
  } */

  async browseNodes(rootNodeId) {
    try {
      logger.info(`🔍 Browsing OPC UA nodes from: ${rootNodeId}`);

      if (!this.session) {
        throw new Error('Session not established');
      }

      const browseResult = await this.session.browse(rootNodeId);

      const discoveredNodes = [];

      for (const reference of browseResult.references) {
        if (reference.nodeClass === 2) { // NodeClass.Variable
          const nodeIdStr = reference.nodeId.toString();
          const browseName = reference.browseName.name;

          discoveredNodes.push({
            nodeId: nodeIdStr,
            browseName: browseName,
            displayName: reference.displayName.text
          });

          logger.info(`  ✓ Discovered: ${browseName} (${nodeIdStr})`);
        }
      }

      logger.info(`🎯 Total nodes discovered: ${discoveredNodes.length}`);
      return discoveredNodes;

    } catch (error) {
      logger.error(`Failed to browse nodes from ${rootNodeId}:`, error);
      return [];
    }
  }
  /*async createSubscription() { 
    const { config } = this.config;
    const subscriptionOptions = {
      maxNotificationsPerPublish: config.subscriptionOptions?.maxNotificationsPerPublish || 10,
      publishingEnabled: config.subscriptionOptions?.publishingEnabled !== false,
      requestedLifetimeCount: config.subscriptionOptions?.requestedLifetimeCount || 60,
      requestedMaxKeepAliveCount: config.subscriptionOptions?.requestedMaxKeepAliveCount || 10,
      requestedPublishingInterval: config.subscriptionOptions?.requestedPublishingInterval || 1000,
      priority: config.subscriptionOptions?.priority || 10
    };

    try {
      // Use createSubscription2 (improved method from OPCUABinding.js example)
      this.subscription = await this.session.createSubscription2(subscriptionOptions);

      this.subscription.on('started', () => {
        logger.info(`OPC UA subscription started for connector '${this.id}'`);
      });

      this.subscription.on('keepalive', () => {
        logger.debug(`OPC UA subscription keepalive for connector '${this.id}'`);
      });

      this.subscription.on('terminated', () => {
        logger.info(`OPC UA subscription terminated for connector '${this.id}'`);
      });
    } catch (error) {
      logger.error(`Failed to create subscription for connector '${this.id}':`, error);
      throw error;
    }
  } */

  async createSubscription() {
    try {
      const { config } = this.config; // 🔥 IMPORTANTE

      if (!this.session) {
        logger.error('Cannot create subscription: session not established');
        return;
      }

      this.subscription = await ClientSubscription.create(this.session, {
        requestedPublishingInterval: this.config.samplingInterval || 1000,
        requestedLifetimeCount: 100,
        requestedMaxKeepAliveCount: 10,
        maxNotificationsPerPublish: 100,
        publishingEnabled: true,
        priority: 10
      });

      this.subscription.on('started', () => {
        logger.info(`OPC UA subscription started with interval ${this.config.samplingInterval}ms`);
      });

      this.subscription.on('terminated', () => {
        logger.warn('OPC UA subscription terminated');
        this.subscription = null;
      });

      logger.info('OPC UA subscription created successfully');

    } catch (error) {
      logger.error('Failed to create subscription:', error);
      this.subscription = null;
      throw error;
    }
  }

  async monitorNodes() {
    const { config } = this.config;

    for (const nodeId of config.nodes) {
      try {
        await this.monitorNode(nodeId);
      } catch (error) {
        logger.error(`Failed to monitor node '${nodeId}' on connector '${this.id}':`, error);
        // Continue with other nodes
      }
    }
  }

  /* async monitorNode(nodeId) {
    const monitoringParameters = {
      samplingInterval: 1000,
      discardOldest: true,
      queueSize: 10,
      filter: new DataChangeFilter({
        trigger: DataChangeTrigger.StatusValueTimestamp,
        deadbandType: DeadbandType.None,
        deadbandValue: 0.0
      })
    };

    const monitoredItem = ClientSubscription.prototype.monitor.call(
      this.subscription,
      {
        nodeId: nodeId,
        attributeId: AttributeIds.Value
      },
      monitoringParameters,
      TimestampsToReturn.Both,
      MonitoringMode.Reporting
    );

    monitoredItem.on('changed', (dataValue) => {
      this.handleNodeValueChange(nodeId, dataValue);
    });

    monitoredItem.on('err', (error) => {
      logger.error(`Monitoring error for node '${nodeId}' on connector '${this.id}':`, error);
    });

    this.monitoredItems.set(nodeId, monitoredItem);
    logger.debug(`Started monitoring node '${nodeId}' on connector '${this.id}'`);
  } */

  async monitorNode(nodeId) {
    try {
      const { config } = this.config; // 🔥 IMPORTANTE

      // Verifica che subscription esista
      if (!this.subscription) {
        logger.error(`Cannot monitor node '${nodeId}': subscription not initialized`);
        return null;
      }

      const monitoredItem = await this.subscription.monitor(
        {
          nodeId: nodeId,
          attributeId: AttributeIds.Value
        },
        {
          samplingInterval: this.config.samplingInterval || 1000,
          discardOldest: true,
          queueSize: 10
        }
      );

      if (monitoredItem && typeof monitoredItem.on === 'function') {
        monitoredItem.on('changed', (dataValue) => {
          this.handleDataChange(nodeId, dataValue);
        });

        logger.debug(`Monitoring node '${nodeId}'`);
      } else {
        logger.warn(`MonitoredItem for '${nodeId}' does not support events`);
      }

      return monitoredItem;

    } catch (error) {
      logger.error(`Failed to monitor node '${nodeId}':`, error);
      throw error;
    }
  }

  handleDataChange(nodeId, dataValue) {
    try {
      // Trova i metadati del nodo
      const nodeInfo = this.nodeMetadata?.find(n => n.nodeId === nodeId);
      const browseName = nodeInfo ? nodeInfo.browseName : nodeId;

      const data = {
        nodeId: nodeId,
        browseName: browseName,
        value: dataValue.value.value,
        dataType: dataValue.value.dataType,
        sourceTimestamp: dataValue.sourceTimestamp,
        serverTimestamp: dataValue.serverTimestamp,
        statusCode: dataValue.statusCode.name
      };

      logger.debug(`📥 Data from ${browseName}: ${data.value}`);

      // Emetti i dati raggruppati per browseName
      this.lastValues = this.lastValues || {};
      this.lastValues[browseName] = data.value;

      // Emetti tutti i valori correnti insieme
      this.emit('data', this.lastValues);

    } catch (error) {
      logger.error(`Error handling data change for ${nodeId}:`, error);
    }
  }

  startPolling() {
    const { config } = this.config;

    logger.info(`Starting polling mode with interval ${config.interval}ms`);

    this.pollingInterval = setInterval(async () => {
      try {
        const values = {};

        // 🔥 USA discoveredNodeIds invece di config.nodes
        const nodesToPoll = this.discoveredNodeIds || config.nodes || [];

        if (nodesToPoll.length === 0) {
          logger.warn('No nodes to poll');
          return;
        }

        for (const nodeId of nodesToPoll) {
          const dataValue = await this.session.readVariableValue(nodeId);
          const nodeInfo = this.nodeMetadata?.find(n => n.nodeId === nodeId);
          const browseName = nodeInfo ? nodeInfo.browseName : nodeId;

          values[browseName] = dataValue.value.value;
        }

        logger.debug('📥 Polled data:', values);
        this.emit('data', values);

      } catch (error) {
        logger.error('Polling error:', error);
      }
    }, config.interval || 1000);

    logger.info('Polling started successfully');
  }

  /* handleNodeValueChange(nodeId, dataValue) {
    try {
      // Check if status is good (from OPCUABinding.js example)
      if (dataValue.statusCode.value !== 0) {
        logger.error(`Error for node '${nodeId}' on connector '${this.id}': status code ${dataValue.statusCode.value}`);
        return;
      }

      // Handle UInt64 special case (from OPCUABinding.js example)
      let value = dataValue.value.value;
      if (dataValue.value.dataType === DataType.UInt64) {
        if (Array.isArray(value) && value.length === 2) {
          const msb = value[0];
          const lsb = value[1];
          value = ((msb << 32) | lsb) >>> 0;
        }
      }

      const data = {
        nodeId: nodeId,
        value: value,
        dataType: dataValue.value.dataType,
        statusCode: dataValue.statusCode.value,
        timestamp: dataValue.sourceTimestamp || dataValue.serverTimestamp || new Date(),
        quality: {
          good: dataValue.statusCode.isGood(),
          bad: dataValue.statusCode.isBad(),
          uncertain: dataValue.statusCode.isUncertain()
        }
      };

      this.onData(data);
      logger.debug(`Node '${nodeId}' value changed -> ${value}`);

    } catch (error) {
      logger.error(`Error handling node value change for '${nodeId}' on connector '${this.id}':`, error);
    }
  } */

  getSecurityMode(mode) {
    switch (mode?.toLowerCase()) {
      case 'none':
        return MessageSecurityMode.None;
      case 'sign':
        return MessageSecurityMode.Sign;
      case 'signandencrypt':
        return MessageSecurityMode.SignAndEncrypt;
      default:
        return MessageSecurityMode.None;
    }
  }

  getSecurityPolicy(policy) {
    switch (policy?.toLowerCase()) {
      case 'none':
        return SecurityPolicy.None;
      case 'basic128':
        return SecurityPolicy.Basic128;
      case 'basic128rsa15':
        return SecurityPolicy.Basic128Rsa15;
      case 'basic256':
        return SecurityPolicy.Basic256;
      case 'basic256sha256':
        return SecurityPolicy.Basic256Sha256;
      case 'aes128_sha256_rsaoaep':
        return SecurityPolicy.Aes128_Sha256_RsaOaep;
      case 'aes256_sha256_rsapss':
        return SecurityPolicy.Aes256_Sha256_RsaPss;
      default:
        return SecurityPolicy.None;
    }
  }

  async readNode(nodeId) {
    if (!this.session) {
      throw new Error('OPC UA session not available');
    }

    try {
      const dataValue = await this.session.read({
        nodeId: nodeId,
        attributeId: AttributeIds.Value
      });

      return {
        nodeId: nodeId,
        value: dataValue.value.value,
        dataType: dataValue.value.dataType,
        statusCode: dataValue.statusCode.value,
        timestamp: dataValue.sourceTimestamp || dataValue.serverTimestamp || new Date(),
        quality: {
          good: dataValue.statusCode.isGood(),
          bad: dataValue.statusCode.isBad(),
          uncertain: dataValue.statusCode.isUncertain()
        }
      };
    } catch (error) {
      logger.error(`Failed to read node '${nodeId}' on connector '${this.id}':`, error);
      throw error;
    }
  }

  async writeNode(nodeId, value, dataType) {
    if (!this.session) {
      throw new Error('OPC UA session not available');
    }

    try {
      const statusCode = await this.session.write({
        nodeId: nodeId,
        attributeId: AttributeIds.Value,
        value: {
          value: value,
          dataType: dataType
        }
      });

      logger.debug(`Wrote value to node '${nodeId}' on connector '${this.id}', status: ${statusCode.toString()}`);
      return statusCode.isGood();

    } catch (error) {
      logger.error(`Failed to write to node '${nodeId}' on connector '${this.id}':`, error);
      throw error;
    }
  }

  getStatus() {
    return {
      ...super.getStatus(),
      sessionActive: this.session !== null,
      subscriptionActive: this.subscription !== null,
      monitoredNodesCount: this.monitoredItems.size,
      endpoint: this.config.config.endpoint
    };
  }
}

/**
 * Application URI of the client: must match the one in its certificate. Fixed through
 * OPCUA_APPLICATION_URI when the host name changes (containers, Kubernetes pods).
 */
OpcUaConnector.applicationUri = () =>
  process.env.OPCUA_APPLICATION_URI || `urn:${os.hostname()}:Universal-Data-Connector`;

module.exports = OpcUaConnector;
