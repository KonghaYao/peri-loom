import test from "node:test";
import assert from "node:assert/strict";
import { parseLosslessJSON } from "./dist/json.js";

test("integer lexical tokens become bigint only beyond the safe range", () => {
  assert.deepEqual(parseLosslessJSON("[9223372036854775807,-9007199254740993,18446744073709551615,9007199254740991,-9007199254740991,0,-0]"), [9223372036854775807n, -9007199254740993n, 18446744073709551615n, Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER, 0, -0]);
  const floats = parseLosslessJSON("[1.25,-0.125,9007199254740993.0,1e3,9.007199254740993e15,-2E-3]");
  assert.deepEqual(floats, [1.25, -0.125, Number("9007199254740993.0"), 1000, Number("9.007199254740993e15"), -0.002]);
  assert(floats.every((value) => typeof value === "number"));
});

test("quoted digits, escape sequences, nested blobs and marker-like SQL text stay unchanged", () => {
  const raw = String.raw`{"text":"9223372036854775807","escaped":"quote\" slash\\ newline\n \u0039\u0032","marker":"__peri_bigint_9223372036854775807","nested":[{"bytes":[0,127,255],"value":-9007199254740993}],"flags":[true,false,null]}`;
  const parsed = parseLosslessJSON(raw);
  assert.equal(parsed.text, "9223372036854775807");
  assert.equal(parsed.escaped, 'quote" slash\\ newline\n 92');
  assert.equal(parsed.marker, "__peri_bigint_9223372036854775807");
  assert.deepEqual(parsed.nested, [{ bytes: [0, 127, 255], value: -9007199254740993n }]);
  assert.deepEqual(parsed.flags, [true, false, null]);
  const rendered = JSON.stringify(parsed, (_key, value) => typeof value === "bigint" ? value.toString() : value);
  assert.equal(JSON.parse(rendered).nested[0].value, "-9007199254740993");
});

test("object construction preserves prototype-named data without changing prototypes", () => {
  const parsed = parseLosslessJSON('{"__proto__":{"polluted":true},"constructor":9223372036854775807}');
  assert.equal(Object.getPrototypeOf(parsed), Object.prototype);
  assert(Object.hasOwn(parsed, "__proto__"));
  assert.deepEqual(parsed.__proto__, { polluted: true });
  assert.equal(parsed.constructor, 9223372036854775807n);
  assert.equal({}.polluted, undefined);
});

test("malformed, duplicate and excessive-depth JSON fails closed", () => {
  for (const raw of [
    '{"ok":false,"ok":true}',
    String.raw`{"key":1,"\u006bey":2}`,
    '{"nested":{"epoch":9223372036854775807,"epoch":1}}',
    '{"array":[1,]}', '{"key":}', '{"key" 1}', '[01]', '[+1]', '[1.]', '[1e]',
    'true false', '{} {}', '[NaN]', '[Infinity]', '[1e400]', '"unterminated',
    String.raw`"bad\xescape"`, '"literal\nnewline"', '\u00a0{}',
    "[".repeat(66) + "0" + "]".repeat(66),
  ]) assert.throws(() => parseLosslessJSON(raw), undefined, raw);
});
