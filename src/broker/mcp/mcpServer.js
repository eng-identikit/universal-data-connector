const { McpServer, ResourceTemplate, ResourceNotFoundError } = require('@modelcontextprotocol/server');
const z = require('zod');
const { version } = require('../../../package.json');
const { valueOf, formatValue, shortTime, entityMarkdown, fitPage, respond, parseWhen } = require('./format');

/**
 * MCP server exposing the broker's data to an LLM, read-only: data model,
 * entity search and detail, relationships and attribute history.
 *
 * One instance per HTTP request (createMcpHandler), bound to the caller's
 * tenant context: every read goes through the same services as the REST API,
 * so tenants, roles and API key type restrictions apply unchanged.
 */

const READ_ONLY = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };
const FORMAT = z.enum(['markdown', 'json']).default('markdown')
  .describe("'markdown' for readable text (default), 'json' for structured data");
const ENTITY_ID = z.string().min(1).max(256).describe("Entity id, e.g. 'plant1:line3:press07' (udc_search_entities returns them)");
const TYPE = z.string().regex(/^[A-Za-z][A-Za-z0-9_]{0,63}$/);
// How many entities per type are sampled to describe the attributes of a type
const MODEL_SAMPLE = 50;

/** Error text the model can act on: what went wrong and what to call instead. */
function hint(error) {
  if (error.status === 404) return `${error.message}. Use udc_search_entities to find existing ids.`;
  if (error.status === 403) return `${error.message}. These credentials cannot read that data.`;
  return error.message;
}

/** Wraps a tool handler so errors come back as readable tool errors. */
const tool = (fn) => async (args, extra) => {
  try {
    return await fn(args, extra);
  } catch (error) {
    return { isError: true, content: [{ type: 'text', text: hint(error) }] };
  }
};

/** Attribute catalog per type from a sample of entities: name, kind, unit, example, relationship targets. */
async function dataModel(services, ctx, onlyTypes) {
  const types = (await services.entities.types(ctx)).filter(t => !onlyTypes?.length || onlyTypes.includes(t.type));
  const model = [];
  for (const { type, count } of types.slice(0, 100)) {
    const { entities } = await services.entities.list(ctx, { type, limit: String(MODEL_SAMPLE) });
    const attrs = {};
    let located = 0;
    for (const e of entities) {
      if (e.location) located++;
      for (const [name, a] of Object.entries(e.attrs || {})) {
        const entry = attrs[name] || (attrs[name] = { name, type: a.type, unit: a.unit || null, example: valueOf(a), seenIn: 0, targets: new Set() });
        entry.seenIn++;
        if (a.type === 'relationship' && a.target) entry.targets.add(a.target);
      }
    }
    model.push({
      type,
      count,
      sampled: entities.length,
      withLocation: located,
      attributes: Object.values(attrs).map(a => ({
        name: a.name,
        type: a.type,
        unit: a.unit,
        example: a.example,
        presentIn: `${a.seenIn}/${entities.length}`,
        ...(a.type === 'relationship' && { exampleTargets: [...a.targets].slice(0, 3) })
      }))
    });
  }
  return { tenant: ctx.tenant, types: model };
}

function dataModelMarkdown(model) {
  if (!model.types.length) return `Tenant '${model.tenant}' has no entities visible to these credentials.`;
  const lines = [`# Data model of tenant '${model.tenant}'`, ''];
  for (const t of model.types) {
    lines.push(`## ${t.type} — ${t.count} entities${t.withLocation ? `, ${t.withLocation}/${t.sampled} sampled with location` : ''}`);
    for (const a of t.attributes) {
      const what = a.type === 'relationship'
        ? `relationship → e.g. ${a.exampleTargets.join(', ') || '—'}`
        : `${a.type}${a.unit ? `, ${a.unit}` : ''}, e.g. ${formatValue(a.example)}`;
      lines.push(`- ${a.name}: ${what} (in ${a.presentIn} sampled)`);
    }
    lines.push('');
  }
  return lines.join('\n');
}

const INSTRUCTIONS = `Read-only access to the Universal Data Connector (UDC) context broker: the live state of
industrial devices and other entities, their relationships and their attribute history.

Data model: an entity has an id, a type and attributes. An attribute has a value (or, for a
relationship, the id of a target entity), and optionally a unit, a timestamp (ts) and a
quality (good, bad, uncertain). Entities may have a GeoJSON location.

Start with udc_describe_data_model to learn the entity types and their attributes, then
udc_search_entities to find entities, udc_get_entity for one entity in detail,
udc_get_related_entities to follow relationships (e.g. plant → line → machine), and
udc_get_attribute_history / udc_get_raw_samples for how values changed over time.
All data belongs to one tenant, chosen by the credentials used to connect.`;

/** Builds the MCP server for one request, bound to the caller's tenant context. */
function createUdcMcpServer(services, ctx) {
  const server = new McpServer(
    { name: 'udc-mcp-server', version },
    { instructions: `${INSTRUCTIONS}\nTenant: ${ctx.tenant}.` }
  );

  server.registerTool('udc_describe_data_model', {
    title: 'Describe the data model',
    description: `Lists the entity types of the tenant with their number of entities and, from a sample of up to ${MODEL_SAMPLE} entities per type, the attributes they have (name, kind, unit, an example value, relationship targets).
Call this first to learn what data exists and how attributes are named.`,
    inputSchema: z.strictObject({
      types: z.array(TYPE).max(20).optional().describe("Only these types, e.g. ['Press', 'Line']; all types when omitted"),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async ({ types, response_format }) => {
    const model = await dataModel(services, ctx, types);
    return respond(response_format, model, dataModelMarkdown(model));
  }));

  server.registerTool('udc_search_entities', {
    title: 'Search entities',
    description: `Finds entities by type, id pattern, relationship or position, with their current attribute values. Returns one page; when has_more is true call again with offset = next_offset.
Examples:
- all presses: types=['Press']
- everything in line 3: related_to='plant1:line3', direction='in', depth=3
- machines within 500 m of a point: near={lon: 9.19, lat: 45.46, max_distance_m: 500}
Use udc_get_entity for one entity whose id you know.`,
    inputSchema: z.strictObject({
      types: z.array(TYPE).max(20).optional().describe('Entity types to include'),
      id_pattern: z.string().max(256).optional().describe("Id pattern with * as wildcard, e.g. 'plant1:*'"),
      related_to: z.string().max(256).optional().describe('Entity id: only entities connected to it through relationships'),
      direction: z.enum(['in', 'out']).default('in')
        .describe("With related_to: 'in' = entities whose relationships lead to it (its parts), 'out' = entities it points to"),
      depth: z.number().int().min(1).max(10).default(1).describe('With related_to: how many relationship hops to follow'),
      near: z.strictObject({
        lon: z.number().min(-180).max(180),
        lat: z.number().min(-90).max(90),
        max_distance_m: z.number().positive().max(20000000).optional()
      }).optional().describe('Entities near a point, nearest first'),
      bbox: z.array(z.number()).length(4).optional().describe('Bounding box [minLon, minLat, maxLon, maxLat]'),
      attrs: z.array(z.string().max(64)).max(50).optional().describe('Only these attributes in the result (smaller answers)'),
      order_by: z.string().max(100).optional().describe("Sort fields: id, type, createdAt, updatedAt; prefix '-' for descending, e.g. '-updatedAt'"),
      limit: z.number().int().min(1).max(100).default(20).describe('Maximum number of entities'),
      offset: z.number().int().min(0).default(0).describe('Entities to skip, for pagination'),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async (a) => {
    const query = {
      limit: String(a.limit),
      offset: String(a.offset),
      ...(a.types?.length && { type: a.types.join(',') }),
      ...(a.id_pattern && { idPattern: a.id_pattern }),
      ...(a.related_to && { related: a.related_to, direction: a.direction, depth: String(a.depth) }),
      ...(a.near && { near: `${a.near.lon},${a.near.lat}`, ...(a.near.max_distance_m && { maxDistance: String(a.near.max_distance_m) }) }),
      ...(a.bbox && { bbox: a.bbox.join(',') }),
      ...(a.attrs?.length && { attrs: a.attrs.join(',') }),
      ...(a.order_by && { orderBy: a.order_by })
    };
    const { entities, total } = await services.entities.list(ctx, query);
    const { page, text } = fitPage(entities, (items) => {
      const next = a.offset + items.length;
      const page = { total, count: items.length, offset: a.offset, entities: items, has_more: total > next, ...(total > next && { next_offset: next }) };
      if (a.response_format === 'json') return { page, text: JSON.stringify(page, null, 2) };
      const lines = [`Found ${total} entities; showing ${items.length} from offset ${a.offset}.`, ''];
      lines.push(items.map(e => entityMarkdown(e)).join('\n\n'));
      if (page.has_more) lines.push('', `More results: call again with offset=${next}.`);
      return { page, text: lines.join('\n') };
    });
    if (!total) return { content: [{ type: 'text', text: 'No entities match. Check the type names with udc_describe_data_model, or widen the filters.' }] };
    return respond(a.response_format, page, text);
  }));

  server.registerTool('udc_get_entity', {
    title: 'Get an entity',
    description: `Returns one entity with all its attributes (value, unit, timestamp, quality), its location and which attributes have recorded history.
Use udc_get_related_entities to see what it is connected to, udc_get_attribute_history for past values.`,
    inputSchema: z.strictObject({
      entity_id: ENTITY_ID,
      attrs: z.array(z.string().max(64)).max(50).optional().describe('Only these attributes'),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async ({ entity_id, attrs, response_format }) => {
    const entity = await services.entities.get(ctx, entity_id, { attrs: attrs?.join(',') });
    const history = await services.history.attributes(ctx.tenant, entity_id);
    const data = { ...entity, history };
    const lines = [entityMarkdown(entity, { heading: '#' })];
    if (history.length) {
      lines.push('', '## Recorded history');
      for (const h of history) lines.push(`- ${h.attr}: ${h.samples} samples, ${shortTime(h.first)} → ${shortTime(h.last)}${h.numeric ? '' : ' (non-numeric)'}`);
    }
    return respond(response_format, data, lines.join('\n'));
  }));

  server.registerTool('udc_get_related_entities', {
    title: 'Get related entities',
    description: `Follows the relationships around an entity and returns the connected entities and the links between them (source -[attribute]-> target).
Example: from a plant, depth=2, direction='in' returns its lines and their machines.`,
    inputSchema: z.strictObject({
      entity_id: ENTITY_ID,
      direction: z.enum(['in', 'out', 'both']).default('both')
        .describe("'out' = what it points to, 'in' = what points to it, 'both'"),
      depth: z.number().int().min(1).max(5).default(2).describe('Relationship hops to follow'),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async ({ entity_id, direction, depth, response_format }) => {
    const graph = await services.entities.graph(ctx, entity_id, { direction, depth: String(depth) });
    const lines = [`# Relationships around ${entity_id}`, '', `## Entities (${graph.nodes.length})`];
    for (const n of graph.nodes) lines.push(`- ${n.id} (${n.type ?? 'missing entity'})`);
    lines.push('', `## Links (${graph.edges.length})`);
    for (const e of graph.edges) lines.push(`- ${e.source} -[${e.attr}]-> ${e.target}`);
    return respond(response_format, graph, lines.join('\n'));
  }));

  server.registerTool('udc_get_attribute_history', {
    title: 'Get attribute history',
    description: `Past values of an entity's attributes over a time range, downsampled into at most max_points buckets per attribute (average, minimum, maximum and last value per bucket), plus overall statistics.
from/to take ISO-8601 dates or durations back from now: from='24h' means the last 24 hours (the default).
Use udc_get_raw_samples for the individual readings of one attribute.`,
    inputSchema: z.strictObject({
      entity_id: ENTITY_ID,
      attrs: z.array(z.string().max(64)).max(20).optional().describe('Attributes to include; all recorded ones when omitted'),
      from: z.string().max(40).default('24h').describe("Start: ISO date or duration back from now ('30m', '24h', '7d')"),
      to: z.string().max(40).optional().describe('End: ISO date or duration back from now; now when omitted'),
      max_points: z.number().int().min(1).max(500).default(48).describe('Maximum buckets per attribute'),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async (a) => {
    await services.entities.get(ctx, a.entity_id);
    const result = await services.history.series(ctx.tenant, a.entity_id, {
      attrs: a.attrs?.join(','),
      from: parseWhen(a.from),
      to: parseWhen(a.to),
      maxPoints: String(a.max_points)
    });
    const names = Object.keys(result.series);
    if (!names.length) {
      return { content: [{ type: 'text', text: `No history for ${a.entity_id} between ${shortTime(result.from)} and ${shortTime(result.to)}. udc_get_entity lists the attributes with recorded history and their time range.` }] };
    }
    const lines = [`# History of ${a.entity_id}`, `${shortTime(result.from)} → ${shortTime(result.to)}, buckets of ${Math.round(result.bucketMs / 1000)} s`];
    for (const name of names) {
      const points = result.series[name];
      const numeric = points.filter(p => p.avg !== null);
      lines.push('', `## ${name}`);
      if (numeric.length) {
        const samples = points.reduce((s, p) => s + p.samples, 0);
        const min = Math.min(...numeric.map(p => p.min));
        const max = Math.max(...numeric.map(p => p.max));
        const avg = numeric.reduce((s, p) => s + p.avg * p.samples, 0) / numeric.reduce((s, p) => s + p.samples, 0);
        lines.push(`${samples} samples; min ${round(min)}, max ${round(max)}, average ${round(avg)}, last ${formatValue(points[points.length - 1].last)}`);
        lines.push('', '| time | avg | min | max |', '|---|---|---|---|');
        for (const p of points) lines.push(`| ${shortTime(p.t)} | ${round(p.avg)} | ${round(p.min)} | ${round(p.max)} |`);
      } else {
        lines.push('', '| time | value |', '|---|---|');
        for (const p of points) lines.push(`| ${shortTime(p.t)} | ${formatValue(p.last)} |`);
      }
    }
    return respond(a.response_format, result, lines.join('\n'));
  }));

  server.registerTool('udc_get_raw_samples', {
    title: 'Get raw samples',
    description: `Individual recorded readings of one attribute (time, value, quality), newest first.
Use udc_get_attribute_history instead for trends over long ranges.`,
    inputSchema: z.strictObject({
      entity_id: ENTITY_ID,
      attr: z.string().min(1).max(64).describe("Attribute name, e.g. 'temperature'"),
      from: z.string().max(40).default('24h').describe("Start: ISO date or duration back from now ('30m', '24h', '7d')"),
      to: z.string().max(40).optional().describe('End: ISO date or duration back from now; now when omitted'),
      limit: z.number().int().min(1).max(500).default(100).describe('Maximum number of samples'),
      response_format: FORMAT
    }),
    annotations: READ_ONLY
  }, tool(async (a) => {
    await services.entities.get(ctx, a.entity_id);
    const samples = await services.history.raw(ctx.tenant, a.entity_id, a.attr, { from: parseWhen(a.from), to: parseWhen(a.to), limit: String(a.limit) });
    if (!samples.length) return { content: [{ type: 'text', text: `No samples of ${a.attr} in that range. udc_get_entity lists the attributes with history.` }] };
    const { page, text } = fitPage(samples, (items) => {
      const page = { entity_id: a.entity_id, attr: a.attr, count: items.length, samples: items, truncated: items.length < samples.length };
      if (a.response_format === 'json') return { page, text: JSON.stringify(page, null, 2) };
      const lines = [`# ${a.entity_id} · ${a.attr} (${items.length} samples, newest first)`, '', '| time | value | quality |', '|---|---|---|'];
      for (const s of items) lines.push(`| ${shortTime(s.t)} | ${formatValue(s.value)} | ${s.quality ?? '—'} |`);
      if (page.truncated) lines.push('', 'Answer shortened: ask for a smaller limit or a shorter range.');
      return { page, text: lines.join('\n') };
    });
    return respond(a.response_format, page, text);
  }));

  // Resources: what a client can attach as context without a tool call
  server.registerResource('udc-data-model', 'udc://data-model', {
    title: 'Data model',
    description: 'Entity types of the tenant with their attributes (same content as udc_describe_data_model)',
    mimeType: 'application/json'
  }, async (uri) => ({
    contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(await dataModel(services, ctx)) }]
  }));

  server.registerResource('udc-entity', new ResourceTemplate('udc://entities/{id}', { list: undefined }), {
    title: 'Entity',
    description: 'One entity with its current attributes',
    mimeType: 'application/json'
  }, async (uri, { id }) => {
    let entity;
    try {
      entity = await services.entities.get(ctx, decodeURIComponent(String(id)));
    } catch (error) {
      if (error.status === 404) throw new ResourceNotFoundError(uri.href);
      throw error;
    }
    return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(entity) }] };
  });

  return server;
}

const round = (n) => (n === null || n === undefined ? '—' : Number(n.toFixed(3)));

/** Tool names and titles, for the UI page that explains how to connect. */
function describeTools(services) {
  const server = createUdcMcpServer(services, { tenant: '_' });
  // eslint-disable-next-line no-underscore-dangle
  const tools = server._registeredTools || {};
  return Object.entries(tools).map(([name, t]) => ({ name, title: t.title, description: t.description.split('\n')[0] }));
}

module.exports = { createUdcMcpServer, describeTools, INSTRUCTIONS };
