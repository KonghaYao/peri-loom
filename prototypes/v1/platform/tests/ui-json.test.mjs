import {test} from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
const source = await readFile(new URL("../web/json.js", import.meta.url), "utf8");
const {parseLosslessJSON, encodeLosslessJSON, displayJSON} = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

test("native integers and recovery sequences remain exact", () => {
  const text = '{"rows":[[9223372036854775807,-9007199254740993]],"sequence":18446744073709551615}';
  const parsed = parseLosslessJSON(text);
  assert.equal(parsed.rows[0][0], 9223372036854775807n);
  assert.equal(parsed.rows[0][1], -9007199254740993n);
  assert.equal(parsed.sequence, 18446744073709551615n);
  assert.equal(encodeLosslessJSON(parsed), text);
  assert.match(displayJSON(parsed), /18446744073709551615/);
});

test("strings floats and prototype keys remain data", () => {
  const parsed = parseLosslessJSON('{"__proto__":{"safe":true},"text":"\\\"9223372036854775807","float":1.25,"exp":1e3,"safe":42}');
  assert.equal(parsed.text, '"9223372036854775807');
  assert.equal(parsed.float, 1.25);
  assert.equal(parsed.exp, 1000);
  assert.equal(parsed.safe, 42);
  assert.equal({}.safe, undefined);
  assert.equal(parsed.__proto__.safe, true);
});

test("malformed duplicate deep and cyclic input fail closed", () => {
  for (const text of ['{"a":1,"a":2}', '[1,]', '01', '1e999', 'true false', '"unfinished', '['.repeat(66)+'0'+']'.repeat(66)]) {
    assert.throws(() => parseLosslessJSON(text));
  }
  const cyclic = {}; cyclic.self = cyclic;
  assert.throws(() => encodeLosslessJSON(cyclic));
  assert.throws(() => encodeLosslessJSON(NaN));
});
