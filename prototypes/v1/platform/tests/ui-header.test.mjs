import test from "node:test";
import assert from "node:assert/strict";
import {consoleRPC, credential, json, reply, sessionID} from "./ui-rpc-support.mjs";

test("console POST reuses one generated body ID; GET omits correlation header", async () => {
  const requests = [];
  const rpc = consoleRPC(async (path, options) => {
    requests.push({path, options});
    const body = options.body ? json.parseLosslessJSON(options.body) : null;
    assert.equal(options.headers.Authorization, `Bearer ${credential}`);
    assert.equal(options.headers["X-Request-ID"], body?.request_id);
    return reply(body?.request_id ?? "", path === "/v1/sql/query" ? [[1]] : {done: true});
  });
  await rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"});
  await rpc.call("/v1/databases/status", {database: "synthetic"});
  await rpc.call("/v1/catalog");
  assert.equal(rpc.generated(), 2);
  assert.equal(requests.length, 3);
  assert.equal(requests[2].options.method, "GET");
  assert.equal(requests[2].options.body, undefined);
  assert(!Object.hasOwn(requests[2].options.headers, "X-Request-ID"));
});

for (const status of [401, 429]) {
  test(`console correlated ${status} rejection is displayed without SQL replay`, async () => {
    let requests = 0;
    const code = status === 401 ? "unauthenticated" : "quota_exceeded";
    const rpc = consoleRPC(async (_path, options) => {
      requests++;
      const body = json.parseLosslessJSON(options.body);
      assert.equal(options.headers["X-Request-ID"], body.request_id);
      return new Response(JSON.stringify({version: 1, request_id: options.headers["X-Request-ID"], ok: false,
        result: null, error: {code}}), {status, headers: {"Content-Type": "application/json"}});
    });
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true), (error) => error.message === code);
    assert.equal(requests, 1);
    assert.equal(rpc.generated(), 1);
    assert(!rpc.elements.get("output").textContent.includes(credential));
  });
}
