/**
 * Filter expression language for subscriptions.
 *
 *   temperature > 80 && status == 'RUN'
 *   temperature.quality != 'good' || abs(temperature - prev.temperature) > 5
 *   @type in ['Press', 'Lathe'] && has('alarm')
 *
 * Identifiers:
 *   name            attribute value (target for relationships)
 *   name.field      attribute field: value, type, unit, ts, quality, source, target, meta.<key>
 *   prev.name[...]  same, on the previous state of the entity (null on create)
 *   @id @type @version
 * Operators: || && ! == != > >= < <= in contains matches + - * / %  (also: and, or, not)
 * Functions: abs round floor ceil min max len lower upper has startsWith endsWith now age
 *
 * Evaluation never throws: type mismatches make comparisons false and
 * arithmetic on non-numbers yields null.
 */

const { safeRegExp, UnsafeRegexError } = require('./safeRegex');

const MAX_LENGTH = 2000;
const MAX_DEPTH = 40;
const MAX_REGEX_LENGTH = 200;
// Longest string a 'matches' is applied to (longer strings never match)
const MAX_MATCH_INPUT = 1000;

// 'matches' runs in the change hot path: regexes are compiled by safeRegExp
// (linear-time engine, or rejected when they could backtrack exponentially).

class ExpressionError extends Error {
  constructor(message, position) {
    super(position === undefined ? message : `${message} at position ${position}`);
    this.position = position;
  }
}

// ─── Lexer ────────────────────────────────────────────────────────────────

const PUNCT = ['||', '&&', '==', '!=', '>=', '<=', '>', '<', '!', '+', '-', '*', '/', '%', '(', ')', '[', ']', ',', '.'];
const WORD_OPS = { and: '&&', or: '||', not: '!' };
const KEYWORDS = new Set(['true', 'false', 'null', 'in', 'contains', 'matches']);

function tokenize(src) {
  const tokens = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (/\s/.test(c)) { i++; continue; }

    if (/[0-9]/.test(c) || (c === '.' && /[0-9]/.test(src[i + 1]))) {
      const m = /^(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/.exec(src.slice(i));
      tokens.push({ t: 'num', v: Number(m[0]), pos: i });
      i += m[0].length;
      continue;
    }

    if (c === "'" || c === '"') {
      let j = i + 1;
      let s = '';
      while (j < src.length && src[j] !== c) {
        if (src[j] === '\\' && j + 1 < src.length) { s += src[j + 1]; j += 2; } else { s += src[j++]; }
      }
      if (j >= src.length) throw new ExpressionError('Unterminated string', i);
      tokens.push({ t: 'str', v: s, pos: i });
      i = j + 1;
      continue;
    }

    if (/[A-Za-z_@]/.test(c)) {
      const m = /^@?[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
      if (!m) throw new ExpressionError(`Unexpected '${c}'`, i);
      const word = m[0];
      if (Object.hasOwn(WORD_OPS, word)) tokens.push({ t: 'op', v: WORD_OPS[word], pos: i });
      else if (KEYWORDS.has(word)) tokens.push({ t: 'kw', v: word, pos: i });
      else tokens.push({ t: 'id', v: word, pos: i });
      i += word.length;
      continue;
    }

    const p = PUNCT.find(op => src.startsWith(op, i));
    if (!p) throw new ExpressionError(`Unexpected '${c}'`, i);
    tokens.push({ t: 'op', v: p, pos: i });
    i += p.length;
  }
  tokens.push({ t: 'eof', pos: src.length });
  return tokens;
}

// ─── Parser (recursive descent) → AST ─────────────────────────────────────

const FUNCTIONS = {
  abs: [1, 1], round: [1, 2], floor: [1, 1], ceil: [1, 1], min: [1, 20], max: [1, 20],
  len: [1, 1], lower: [1, 1], upper: [1, 1], has: [1, 1], startsWith: [2, 2], endsWith: [2, 2],
  now: [0, 0], age: [1, 1]
};
const ATTR_FIELDS = new Set(['value', 'type', 'unit', 'ts', 'quality', 'source', 'target', 'meta']);

function parse(src) {
  const tokens = tokenize(src);
  let pos = 0;
  let depth = 0;
  const peek = () => tokens[pos];
  const next = () => tokens[pos++];
  const isOp = (v) => peek().t === 'op' && peek().v === v;
  const isKw = (v) => peek().t === 'kw' && peek().v === v;
  const expect = (v) => {
    if (!isOp(v)) throw new ExpressionError(`Expected '${v}'`, peek().pos);
    next();
  };
  const enter = () => {
    if (++depth > MAX_DEPTH) throw new ExpressionError('Expression too deeply nested', peek().pos);
  };

  function binaryLevel(parseOperand, ops) {
    let left = parseOperand();
    while (peek().t === 'op' && ops.includes(peek().v)) {
      const op = next().v;
      left = { k: 'bin', op, left, right: parseOperand() };
    }
    return left;
  }

  const parseOr = () => binaryLevel(parseAnd, ['||']);
  const parseAnd = () => binaryLevel(parseNot, ['&&']);

  function parseNot() {
    if (isOp('!')) {
      next();
      enter();
      const node = { k: 'not', arg: parseNot() };
      depth--;
      return node;
    }
    return parseCmp();
  }

  function parseCmp() {
    const left = parseAdd();
    const t = peek();
    if (t.t === 'op' && ['==', '!=', '>', '>=', '<', '<='].includes(t.v)) {
      next();
      return { k: 'bin', op: t.v, left, right: parseAdd() };
    }
    if (t.t === 'kw' && ['in', 'contains', 'matches'].includes(t.v)) {
      next();
      const right = parseAdd();
      if (t.v === 'matches') {
        if (right.k !== 'lit' || typeof right.v !== 'string') throw new ExpressionError("'matches' needs a string literal regex", t.pos);
        if (right.v.length > MAX_REGEX_LENGTH) throw new ExpressionError('Regex too long', t.pos);
        right.re = compileRegex(right.v, t.pos);
      }
      return { k: 'bin', op: t.v, left, right };
    }
    return left;
  }

  const parseAdd = () => binaryLevel(parseMul, ['+', '-']);
  const parseMul = () => binaryLevel(parseUnary, ['*', '/', '%']);

  function parseUnary() {
    if (isOp('-')) {
      next();
      enter();
      const node = { k: 'neg', arg: parseUnary() };
      depth--;
      return node;
    }
    return parsePrimary();
  }

  function parsePrimary() {
    const t = next();
    if (t.t === 'num' || t.t === 'str') return { k: 'lit', v: t.v };
    if (t.t === 'kw' && t.v === 'true') return { k: 'lit', v: true };
    if (t.t === 'kw' && t.v === 'false') return { k: 'lit', v: false };
    if (t.t === 'kw' && t.v === 'null') return { k: 'lit', v: null };
    if (t.t === 'op' && t.v === '(') {
      enter();
      const e = parseOr();
      expect(')');
      depth--;
      return e;
    }
    if (t.t === 'op' && t.v === '[') {
      const items = [];
      if (!isOp(']')) {
        do { items.push(parseOr()); } while (isOp(',') && next());
      }
      expect(']');
      return { k: 'list', items };
    }
    if (t.t === 'id') {
      if (isOp('(')) return parseCall(t);
      return parsePath(t);
    }
    throw new ExpressionError(t.t === 'eof' ? 'Unexpected end of expression' : `Unexpected '${t.v}'`, t.pos);
  }

  function parseCall(t) {
    const spec = Object.hasOwn(FUNCTIONS, t.v) ? FUNCTIONS[t.v] : null;
    if (!spec) throw new ExpressionError(`Unknown function '${t.v}'`, t.pos);
    next();
    const args = [];
    if (!isOp(')')) {
      do { args.push(parseOr()); } while (isOp(',') && next());
    }
    expect(')');
    if (args.length < spec[0] || args.length > spec[1]) throw new ExpressionError(`Wrong number of arguments for ${t.v}()`, t.pos);
    if (t.v === 'has' && (args[0].k !== 'lit' || typeof args[0].v !== 'string')) {
      throw new ExpressionError("has() takes an attribute name as a string, e.g. has('alarm')", t.pos);
    }
    return { k: 'call', fn: t.v, args };
  }

  function parsePath(t) {
    if (t.v.startsWith('@')) {
      if (!['@id', '@type', '@version'].includes(t.v)) throw new ExpressionError(`Unknown entity field '${t.v}'`, t.pos);
      return { k: 'entity', field: t.v.slice(1) };
    }
    let prev = false;
    let name = t.v;
    if (name === 'prev' && isOp('.')) {
      next();
      const n = next();
      if (n.t !== 'id' || n.v.startsWith('@')) throw new ExpressionError("Expected attribute name after 'prev.'", n.pos);
      prev = true;
      name = n.v;
    }
    const path = [];
    while (isOp('.')) {
      next();
      const f = next();
      if (f.t !== 'id' && f.t !== 'kw') throw new ExpressionError("Expected field name after '.'", f.pos);
      path.push(f.v);
    }
    if (path.length && !ATTR_FIELDS.has(path[0])) {
      throw new ExpressionError(`Unknown attribute field '${path[0]}' (use ${[...ATTR_FIELDS].join(', ')})`, t.pos);
    }
    if (path.length > 1 && path[0] !== 'meta') throw new ExpressionError(`Field '${path[0]}' has no sub-fields`, t.pos);
    if (path.length > 2) throw new ExpressionError('meta supports one level: name.meta.key', t.pos);
    return { k: 'attr', prev, name, path };
  }

  const ast = parseOr();
  if (peek().t !== 'eof') throw new ExpressionError(`Unexpected '${peek().v}'`, peek().pos);
  return ast;
}

// ─── Evaluator ────────────────────────────────────────────────────────────

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
const sameType = (a, b) => typeof a === typeof b && a !== null && b !== null;

// Own properties only: names like 'constructor' must not reach Object.prototype
const own = (obj, key) => (obj && typeof obj === 'object' && Object.hasOwn(obj, key) ? obj[key] : undefined);

function attrValue(entity, name, path) {
  const attr = own(entity?.attrs, name);
  if (!attr) return null;
  if (!path.length) return attr.type === 'relationship' ? attr.target : (attr.value ?? null);
  if (path[0] === 'meta') return path.length === 2 ? (own(attr.meta, path[1]) ?? null) : (attr.meta ?? null);
  return own(attr, path[0]) ?? null;
}

function equals(a, b) {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
  return JSON.stringify(a) === JSON.stringify(b);
}

function evaluate(node, ctx) {
  switch (node.k) {
    case 'lit': return node.v;
    case 'list': return node.items.map(i => evaluate(i, ctx));
    case 'entity': {
      const e = ctx.entity;
      return e ? (e[node.field] ?? null) : null;
    }
    case 'attr': return attrValue(node.prev ? ctx.previous : ctx.entity, node.name, node.path);
    case 'not': return !truthy(evaluate(node.arg, ctx));
    case 'neg': {
      const v = evaluate(node.arg, ctx);
      return isNum(v) ? -v : null;
    }
    case 'call': return callFn(node, ctx);
    case 'bin': return binary(node, ctx);
    default: return null;
  }
}

function compileRegex(source, pos) {
  try {
    return safeRegExp(source);
  } catch (e) {
    if (e instanceof UnsafeRegexError) throw new ExpressionError(e.message, pos);
    throw new ExpressionError(`Invalid regex: ${e.message.replace(/^Invalid regular expression: /, '')}`, pos);
  }
}

function truthy(v) {
  return v !== null && v !== undefined && v !== false && v !== 0 && v !== '';
}

function binary(node, ctx) {
  const { op } = node;
  if (op === '&&') return truthy(evaluate(node.left, ctx)) && truthy(evaluate(node.right, ctx));
  if (op === '||') return truthy(evaluate(node.left, ctx)) || truthy(evaluate(node.right, ctx));

  const a = evaluate(node.left, ctx);
  const b = evaluate(node.right, ctx);
  switch (op) {
    case '==': return equals(a, b);
    case '!=': return !equals(a, b);
    case '>': return sameType(a, b) && (isNum(a) || typeof a === 'string') && a > b;
    case '>=': return sameType(a, b) && (isNum(a) || typeof a === 'string') && a >= b;
    case '<': return sameType(a, b) && (isNum(a) || typeof a === 'string') && a < b;
    case '<=': return sameType(a, b) && (isNum(a) || typeof a === 'string') && a <= b;
    case 'in': return Array.isArray(b) ? b.some(x => equals(x, a)) : (typeof b === 'string' && typeof a === 'string' && b.includes(a));
    case 'contains': return Array.isArray(a) ? a.some(x => equals(x, b)) : (typeof a === 'string' && typeof b === 'string' && a.includes(b));
    case 'matches': return typeof a === 'string' && a.length <= MAX_MATCH_INPUT && node.right.re.test(a);
    case '+':
      if (isNum(a) && isNum(b)) return a + b;
      if (typeof a === 'string' && typeof b === 'string') return a + b;
      return null;
    case '-': return isNum(a) && isNum(b) ? a - b : null;
    case '*': return isNum(a) && isNum(b) ? a * b : null;
    case '/': return isNum(a) && isNum(b) && b !== 0 ? a / b : null;
    case '%': return isNum(a) && isNum(b) && b !== 0 ? a % b : null;
    default: return null;
  }
}

function callFn(node, ctx) {
  if (node.fn === 'has') return !!own(ctx.entity?.attrs, node.args[0].v);
  if (node.fn === 'now') return ctx.now;
  const args = node.args.map(a => evaluate(a, ctx));
  const [x, y] = args;
  switch (node.fn) {
    case 'abs': return isNum(x) ? Math.abs(x) : null;
    case 'round': {
      if (!isNum(x)) return null;
      const f = 10 ** (isNum(y) ? Math.max(0, Math.min(10, Math.trunc(y))) : 0);
      return Math.round(x * f) / f;
    }
    case 'floor': return isNum(x) ? Math.floor(x) : null;
    case 'ceil': return isNum(x) ? Math.ceil(x) : null;
    case 'min': return args.every(isNum) ? Math.min(...args) : null;
    case 'max': return args.every(isNum) ? Math.max(...args) : null;
    case 'len': return typeof x === 'string' || Array.isArray(x) ? x.length : null;
    case 'lower': return typeof x === 'string' ? x.toLowerCase() : null;
    case 'upper': return typeof x === 'string' ? x.toUpperCase() : null;
    case 'startsWith': return typeof x === 'string' && typeof y === 'string' && x.startsWith(y);
    case 'endsWith': return typeof x === 'string' && typeof y === 'string' && x.endsWith(y);
    case 'age': {
      // Seconds elapsed since an ISO timestamp, e.g. age(temperature.ts) > 60
      const t = typeof x === 'string' ? Date.parse(x) : NaN;
      return Number.isNaN(t) ? null : (ctx.now - t) / 1000;
    }
    default: return null;
  }
}

function collectAttrs(node, out = new Set()) {
  if (!node || typeof node !== 'object') return out;
  if (node.k === 'attr') out.add(node.name);
  if (node.k === 'call' && node.fn === 'has') out.add(node.args[0].v);
  for (const key of ['left', 'right', 'arg']) collectAttrs(node[key], out);
  for (const list of [node.items, node.args]) (list || []).forEach(n => collectAttrs(n, out));
  return out;
}

/**
 * Compiles an expression. Throws ExpressionError on syntax errors.
 * Returns { source, attrs, test(entity, previous) → boolean }.
 */
function compile(source) {
  if (typeof source !== 'string' || !source.trim()) throw new ExpressionError('Expression is empty');
  if (source.length > MAX_LENGTH) throw new ExpressionError(`Expression too long (max ${MAX_LENGTH} chars)`);
  const ast = parse(source);
  return {
    source,
    attrs: [...collectAttrs(ast)],
    test(entity, previous = null) {
      try {
        return truthy(evaluate(ast, { entity, previous, now: Date.now() }));
      } catch {
        return false;
      }
    },
    evaluate(entity, previous = null) {
      return evaluate(ast, { entity, previous, now: Date.now() });
    }
  };
}

module.exports = { compile, ExpressionError, FUNCTIONS: Object.keys(FUNCTIONS) };
