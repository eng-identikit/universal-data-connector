/**
 * Regexes written by users (filters, transformers) run in the broker's event
 * loop, so they must not backtrack exponentially (ReDoS).
 *
 * safeRegExp() compiles for V8's linear-time engine (flag 'l') when it is
 * available and supports the pattern. Otherwise (backreferences, lookahead,
 * case-insensitive or counted repetitions the engine cannot run) it falls
 * back to the normal engine but rejects patterns with nested quantifiers or
 * backreferences, the shapes that backtrack exponentially.
 */

try {
  require('v8').setFlagsFromString('--enable-experimental-regexp-engine');
} catch { /* not available */ }

const LINEAR = (() => {
  try {
    new RegExp('a', 'l'); // eslint-disable-line no-new
    return true;
  } catch {
    return false;
  }
})();

class UnsafeRegexError extends Error {}

/** Star height > 1 (a quantified group containing a quantifier or alternation) or a backreference. */
function backtrackingRisk(source) {
  if (/\\[1-9]|\\k</.test(source)) return true;
  const groups = []; // per open group: contains a quantifier or '|'
  for (let i = 0; i < source.length; i++) {
    const c = source[i];
    if (c === '\\') { i++; continue; }
    if (c === '[') {
      while (i < source.length && source[i] !== ']') i += source[i] === '\\' ? 2 : 1;
      continue;
    }
    if (c === '(') groups.push(false);
    else if (c === ')') {
      const risky = groups.pop();
      const q = source[i + 1];
      if (risky && (q === '*' || q === '+' || q === '{')) return true;
      if (risky && groups.length) groups[groups.length - 1] = true;
    } else if ('*+{|'.includes(c) || (c === '?' && source[i - 1] !== '(')) {
      if (groups.length) groups[groups.length - 1] = true;
    }
  }
  return false;
}

/**
 * Compiles a user regex safely; throws SyntaxError for an invalid pattern and
 * UnsafeRegexError for one that could backtrack exponentially.
 * Accepts a RegExp too (its source and flags are used).
 */
function safeRegExp(pattern, flags) {
  const source = pattern instanceof RegExp ? pattern.source : String(pattern);
  const f = (flags ?? (pattern instanceof RegExp ? pattern.flags : '')).replace('l', '');
  if (LINEAR) {
    try {
      return new RegExp(source, `${f}l`);
    } catch (error) {
      if (!/linear time/.test(error.message)) throw error;
    }
  }
  if (backtrackingRisk(source)) throw new UnsafeRegexError('Regex not supported: nested quantifiers or backreferences');
  return new RegExp(source, f);
}

/** Constructor-compatible wrapper (new SafeRegExp(p, f)) for libraries that take a RegExp class. */
function SafeRegExp(pattern, flags) {
  return safeRegExp(pattern, flags);
}

module.exports = { safeRegExp, SafeRegExp, backtrackingRisk, UnsafeRegexError, LINEAR };
