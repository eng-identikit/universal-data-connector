const Joi = require('joi');
const { compile, ExpressionError } = require('../expr/expression');
const { badRequest } = require('../errors');

const ENTITY_TYPE = Joi.string().pattern(/^[A-Za-z][A-Za-z0-9_]{0,63}$/);
const ENTITY_ID = Joi.string().pattern(/^[A-Za-z0-9_\-:.]{1,256}$/);
const ATTR_NAME = Joi.string().pattern(/^[A-Za-z_][A-Za-z0-9_]{0,63}$/);
// {tenant} {subscription} {entityId} {entityType} are expanded per message
const TOPIC = Joi.string().max(512).pattern(/^[^\s#+]*(\{[a-zA-Z]+\}[^\s#+]*)*$/);
const SUBJECT = Joi.string().max(512).pattern(/^[A-Za-z0-9_\-.{}>*]+$/);

const selector = Joi.object({
  types: Joi.array().items(ENTITY_TYPE).max(50).default([]),
  idPattern: Joi.string().max(256).allow('', null),
  ids: Joi.array().items(ENTITY_ID).max(1000).default([]),
  related: Joi.object({
    id: ENTITY_ID.required(),
    depth: Joi.number().integer().min(1).max(10).default(1),
    direction: Joi.string().valid('in', 'out').default('in')
  }).allow(null),
  bbox: Joi.array().ordered(
    Joi.number().min(-180).max(180), Joi.number().min(-90).max(90),
    Joi.number().min(-180).max(180), Joi.number().min(-90).max(90)
  ).length(4).allow(null)
}).default();

const trigger = Joi.object({
  on: Joi.array().items(Joi.string().valid('create', 'update', 'delete')).min(1).unique().default(['create', 'update', 'delete']),
  attrs: Joi.array().items(ATTR_NAME).max(100).unique().default([]),
  deadband: Joi.object().pattern(ATTR_NAME, Joi.number().min(0)).default({})
}).default();

const common = {
  attrs: Joi.array().items(ATTR_NAME).max(100).unique().default([]),
  format: Joi.string().valid('full', 'values').default('full'),
  perEntity: Joi.boolean().default(false)
};

const notify = Joi.alternatives().conditional('.channel', {
  switch: [
    {
      is: 'http',
      then: Joi.object({
        channel: Joi.string().valid('http').required(),
        url: Joi.string().uri({ scheme: ['http', 'https'] }).max(2048).required(),
        method: Joi.string().valid('POST', 'PUT', 'PATCH').default('POST'),
        headers: Joi.object().pattern(Joi.string().max(256), Joi.string().max(4096)).max(50).default({}),
        timeoutMs: Joi.number().integer().min(500).max(60000).default(10000),
        ...common
      })
    },
    {
      is: 'mqtt',
      then: Joi.object({
        channel: Joi.string().valid('mqtt').required(),
        url: Joi.string().uri({ scheme: ['mqtt', 'mqtts', 'ws', 'wss', 'tcp'] }).allow('', null),
        topic: TOPIC.required(),
        qos: Joi.number().valid(0, 1, 2).default(1),
        retain: Joi.boolean().default(false),
        username: Joi.string().max(256).allow('', null),
        password: Joi.string().max(1024).allow('', null),
        ...common
      })
    },
    {
      is: 'nats',
      then: Joi.object({
        channel: Joi.string().valid('nats').required(),
        url: Joi.string().uri({ scheme: ['nats', 'tls'] }).allow('', null),
        subject: SUBJECT.required(),
        ...common
      })
    },
    {
      is: 'websocket',
      then: Joi.object({ channel: Joi.string().valid('websocket').required(), ...common })
    }
  ],
  otherwise: Joi.any().forbidden().messages({ 'any.unknown': 'notify.channel must be one of http, mqtt, nats, websocket' })
});

const delivery = Joi.object({
  maxRetries: Joi.number().integer().min(0).max(100).default(10),
  backoffMs: Joi.number().integer().min(100).max(600000).default(1000)
}).default();

const base = {
  name: Joi.string().trim().min(1).max(200),
  description: Joi.string().max(2000).allow('', null),
  enabled: Joi.boolean(),
  select: selector,
  trigger,
  filter: Joi.string().max(2000).allow('', null),
  filterMode: Joi.string().valid('level', 'edge'),
  throttleMs: Joi.number().integer().min(0).max(86400000).allow(null),
  periodicMs: Joi.number().integer().min(1000).max(86400000).allow(null),
  notify,
  delivery,
  expiresAt: Joi.date().iso().allow(null)
};

const createSchema = Joi.object({
  ...base,
  name: base.name.required(),
  enabled: base.enabled.default(true),
  filterMode: base.filterMode.default('level'),
  notify: notify.required()
});

function validate(schema, input) {
  const { value, error } = schema.validate(input || {}, { abortEarly: false, stripUnknown: false });
  if (error) {
    throw badRequest(error.details.map(d => d.message).join('; '), { fields: error.details.map(d => d.path.join('.')) });
  }
  if (value.filter) {
    try {
      compile(value.filter);
    } catch (e) {
      if (e instanceof ExpressionError) throw badRequest(`filter: ${e.message}`, { field: 'filter', position: e.position });
      throw e;
    }
  }
  if (value.notify?.channel === 'http' && /[\r\n]/.test(Object.values(value.notify.headers || {}).join(''))) {
    throw badRequest('notify.headers must not contain line breaks');
  }
  const { channel, url } = value.notify || {};
  if ((channel === 'mqtt' || channel === 'nats') && !url && !defaultBrokers()[channel]) {
    throw badRequest(`notify.url is required: no default ${channel.toUpperCase()} broker is configured`, { field: 'notify.url' });
  }
  return value;
}

/** Channels that work without notify.url, from the environment (MQTT and NATS are optional services). */
const defaultBrokers = () => ({
  mqtt: !!(process.env.BROKER_MQTT_URL || process.env.MQTT_URL),
  nats: !!process.env.NATS_URL
});

/** Full subscription body for create. */
const validateCreate = (input) => validate(createSchema, input);

/** Full body after merging a patch over the stored subscription (validated as a whole). */
const validateMerged = (input) => validate(createSchema, input);

module.exports = { validateCreate, validateMerged, defaultBrokers };
