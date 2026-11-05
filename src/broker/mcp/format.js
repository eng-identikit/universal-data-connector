/**
 * Rendering helpers for the MCP tools: compact Markdown for the model,
 * JSON for programmatic use, and a size cap on every response.
 */

// Larger answers fill the model's context; tools return fewer items instead
const CHARACTER_LIMIT = Number(process.env.BROKER_MCP_CHARACTER_LIMIT) || 25000;

/** Plain value of an attribute: the target id for relationships. */
function valueOf(attr) {
  return attr.type === 'relationship' ? attr.target : attr.value;
}

function formatValue(value) {
  if (value === null || value === undefined) return '—';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

/** One attribute as a Markdown bullet: "temperature: 81.5 °C (quality bad, 2026-11-05 10:00:00Z)". */
function attrLine(name, attr) {
  if (attr.type === 'relationship') return `- ${name} → ${attr.target}`;
  const details = [];
  if (attr.quality && attr.quality !== 'good') details.push(`quality ${attr.quality}`);
  if (attr.ts) details.push(shortTime(attr.ts));
  if (attr.writable) details.push('writable');
  const unit = attr.unit ? ` ${attr.unit}` : '';
  return `- ${name}: ${formatValue(attr.value)}${unit}${details.length ? ` (${details.join(', ')})` : ''}`;
}

function shortTime(iso) {
  return iso ? iso.replace('T', ' ').replace(/\.\d+Z$/, 'Z') : '—';
}

/** Entity as Markdown: header with id and type, then its attributes. */
function entityMarkdown(e, { heading = '##' } = {}) {
  const lines = [`${heading} ${e.id} (${e.type})`];
  if (e.distance_m !== undefined) lines.push(`- distance: ${Math.round(e.distance_m)} m`);
  if (e.location) lines.push(`- location: ${JSON.stringify(e.location)}`);
  const names = Object.keys(e.attrs || {});
  if (!names.length) lines.push('- (no attributes)');
  for (const name of names) lines.push(attrLine(name, e.attrs[name]));
  lines.push(`- updated: ${shortTime(e.updatedAt)}`);
  return lines.join('\n');
}

/**
 * Builds a page of items and keeps its text under CHARACTER_LIMIT by returning
 * fewer items; next_offset then points at the first item left out.
 * render(items) → { page, text }
 */
function fitPage(items, render) {
  let shown = items;
  let result = render(shown);
  while (shown.length > 1 && result.text.length > CHARACTER_LIMIT) {
    shown = shown.slice(0, Math.ceil(shown.length / 2));
    result = render(shown);
  }
  return result;
}

/** Tool result in the requested format (JSON also carries structuredContent). */
function respond(format, data, markdown) {
  if (format === 'json') {
    const text = JSON.stringify(data, null, 2);
    return { content: [{ type: 'text', text: clip(text) }], structuredContent: data };
  }
  return { content: [{ type: 'text', text: clip(markdown) }] };
}

/** Last resort for a single item larger than the limit. */
function clip(text) {
  return text.length > CHARACTER_LIMIT
    ? `${text.slice(0, CHARACTER_LIMIT)}\n\n[Truncated at ${CHARACTER_LIMIT} characters: ask for fewer attributes or a shorter range.]`
    : text;
}

const RELATIVE_RE = /^(\d+)\s*(s|m|h|d|w)$/;
const UNIT_MS = { s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 };

/** ISO date, or a duration back from now such as "30m", "24h", "7d". */
function parseWhen(value, now = Date.now()) {
  if (value === undefined || value === null || value === '') return undefined;
  const rel = RELATIVE_RE.exec(String(value).trim());
  if (rel) return new Date(now - Number(rel[1]) * UNIT_MS[rel[2]]).toISOString();
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`'${value}' is not a date: use ISO-8601 (2026-11-05T08:00:00Z) or a duration back from now (30m, 24h, 7d)`);
  }
  return d.toISOString();
}

module.exports = { CHARACTER_LIMIT, valueOf, formatValue, attrLine, shortTime, entityMarkdown, fitPage, respond, parseWhen };
