import test from "node:test";
import assert from "node:assert/strict";
import {json, transport, stream, sessionID, reply, consoleRPC, wire} from "./ui-rpc-support.mjs";

const row = values => json.encodeLosslessJSON({type: "row", row: values}) + "\n";
const complete = (identifier, rows) => json.encodeLosslessJSON({type: "complete", request_id: identifier, rows}) + "\n";

for (const [name, response] of [
  ["empty EOF", () => ""],
  ["row-only EOF", () => row([1])],
  ["wrong complete ID", () => row([1]) + complete("wrong", 1)],
  ["wrong count", identifier => row([1]) + complete(identifier, 2)],
  ["negative count", identifier => complete(identifier, -1)],
  ["fractional count", identifier => complete(identifier, 0.5)],
  ["unsafe count", identifier => `{"type":"complete","request_id":"${identifier}","rows":9007199254740993}\n`],
  ["string count", identifier => complete(identifier, "0")],
  ["duplicate complete", identifier => complete(identifier, 0) + complete(identifier, 0)],
  ["row after complete", identifier => complete(identifier, 0) + row([1])],
  ["truncated tail", identifier => complete(identifier, 0) + '{"type":'],
  ["truncated complete", identifier => row([1]) + complete(identifier, 1).slice(0, -4)],
  ["blank trailing frame", identifier => complete(identifier, 0) + "\n"],
  ["row not array", identifier => '{"type":"row","row":1}\n' + complete(identifier, 1)],
  ["row extra field", identifier => '{"type":"row","row":[1],"extra":true}\n' + complete(identifier, 1)],
  ["complete extra field", identifier => `{"type":"complete","request_id":"${identifier}","rows":0,"extra":true}\n`],
  ["unknown event", identifier => '{"type":"begin"}\n' + complete(identifier, 0)],
  ["stream error event", identifier => row([1]) + `{"type":"error","request_id":"${identifier}","error":{"code":"sql_error","execution_completed":true,"may_have_committed":true}}\n`],
  ["duplicate row key", identifier => '{"type":"row","row":[1],"row":[2]}\n' + complete(identifier, 1)],
]) {
  test(`NDJSON ${name} is unknown and cannot continue old session`, async () => {
    const rpc = transport((_path, body) => stream(response(body.request_id)));
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), {code: "outcome_unknown"});
    for (const action of ["query", "execute", "close"]) await assert.rejects(
      rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "ROLLBACK"}), {code: "session_unusable"});
    assert.equal(rpc.requests.length, 1);
  });
}

for (const newline of [true, false]) {
  test(`valid unicode/bigint stream split at every byte, final newline=${newline}`, async () => {
    const expected = [["汉🙂", 9223372036854775807n, -9007199254740993n], [null, true, {bytes: [0, 255]}]];
    const rpc = transport((_path, body) => {
      const text = expected.map(row).join("") + complete(body.request_id, 2);
      const bytes = new TextEncoder().encode(newline ? text : text.slice(0, -1));
      return stream("", 200, Array.from(bytes, byte => new Uint8Array([byte])));
    });
    assert.deepEqual(await rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}, true), expected);
    assert.equal(rpc.requests.length, 1);
  });
}

test("zero-row complete is a valid stream", async () => {
  const rpc = transport((_path, body) => stream(complete(body.request_id, 0)));
  assert.deepEqual(await rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}, true), []);
});

for (const status of [401, 429, 500]) {
  test(`NDJSON HTTP ${status} never acknowledges a complete frame`, async () => {
    const rpc = transport((_path, body) => stream(complete(body.request_id, 0), status));
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), {code: "outcome_unknown"});
    assert.equal(rpc.requests.length, 1);
  });
}

test("NDJSON is not accepted for execute or unrequested query streaming", async () => {
  for (const action of ["execute", "query"]) {
    const rpc = transport((_path, body) => stream(complete(body.request_id, 0)));
    await assert.rejects(rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "COMMIT"}), {code: "outcome_unknown"});
    assert.equal(rpc.requests.length, 1);
  }
});

for (const [name, response, limits] of [
  ["row budget", identifier => row([1]) + row([2]) + complete(identifier, 2), {maxRows: 1}],
  ["reply budget", identifier => row(["x".repeat(180)]) + complete(identifier, 1), {maxReplyBytes: 128, maxFrameBytes: 128}],
  ["frame budget", identifier => row(["汉".repeat(30)]) + complete(identifier, 1), {maxFrameBytes: 64}],
  ["invalid UTF8", () => "", {}],
]) {
  test(`NDJSON ${name} is bounded and unknown`, async () => {
    const rpc = transport((_path, body) => name === "invalid UTF8" ? stream("", 200, [new Uint8Array([0xff])]) : stream(response(body.request_id)), limits);
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), {code: "outcome_unknown"});
    assert.equal(rpc.requests.length, 1);
  });
}

test("valid complete followed by reader failure is not a successful ACK", async () => {
  const rpc = transport((_path, body) => {
    let delivered = false;
    return {ok: true, headers: new Headers({"Content-Type": "application/x-ndjson"}), body: {getReader() {
      return {async read() {
        if (delivered) throw new Error("late transport failure");
        delivered = true;
        return {done: false, value: new TextEncoder().encode(complete(body.request_id, 0))};
      }, async cancel() {}, releaseLock() {}};
    }}};
  });
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), {code: "outcome_unknown"});
  assert.equal(rpc.requests.length, 1);
});

test("complete without EOF stalls only until timeout, then poisons locally", async () => {
  const rpc = transport((_path, body) => {
    let delivered = false;
    return {ok: true, headers: new Headers({"Content-Type": "application/x-ndjson"}), body: {getReader() {
      return {read() {
        if (delivered) return new Promise(() => {});
        delivered = true;
        return Promise.resolve({done: false, value: new TextEncoder().encode(complete(body.request_id, 0))});
      }, async cancel() {}, releaseLock() {}};
    }}};
  }, {timeoutMS: 5});
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), {code: "outcome_unknown"});
  await assert.rejects(rpc.call("/v1/sql/close", {session: sessionID}), {code: "session_unusable"});
  assert.equal(rpc.requests.length, 1);
});

test("UI does not display or return partial rows before verified complete plus EOF", async () => {
  let controller;
  let identifier;
  const rpc = consoleRPC(async (_path, options) => {
    identifier = wire(options.body).request_id;
    return new Response(new ReadableStream({start(incoming) {
      controller = incoming;
      incoming.enqueue(new TextEncoder().encode(row([1])));
    }}), {headers: {"Content-Type": "application/x-ndjson"}});
  });
  let finished = false;
  const pending = rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}, true).then(result => { finished = true; return result; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(finished, false);
  assert(!rpc.elements.has("output"));
  controller.enqueue(new TextEncoder().encode(complete(identifier, 1)));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(finished, false);
  controller.close();
  assert.deepEqual(await pending, [[1]]);
  assert.equal(finished, true);
  assert(rpc.elements.get("output").textContent.includes("1"));
});

for (const code of ["outcome_unknown", "session_moved"]) {
  test(`actual UI buttons keep ${code} session blocked without automatic close`, async () => {
    let requests = 0;
    const rpc = consoleRPC(async (path, options) => {
      requests++;
      const body = options.body ? wire(options.body) : null;
      if (path === "/v1/catalog") return reply("", {databases: {sample: {id: {}, state: "active"}}});
      if (path === "/v1/sql/open") return reply(body.request_id, {session: sessionID, readonly: false});
      return reply(body.request_id, null, {code}, 502);
    });
    await rpc.click("connect");
    rpc.elements.get("database").value = "sample";
    await rpc.click("open");
    rpc.element("sql").value = "COMMIT";
    await rpc.click("query");
    assert.equal(requests, 3);
    assert(rpc.elements.get("session-state").textContent.includes("不可继续"));
    for (const action of ["query", "execute", "close"]) await rpc.click(action);
    assert.equal(requests, 3);
    assert(rpc.elements.get("output").textContent.includes("session_unusable"));
  });
}
