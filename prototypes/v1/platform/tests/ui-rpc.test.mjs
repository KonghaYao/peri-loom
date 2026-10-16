import test from "node:test";
import assert from "node:assert/strict";
import {consoleRPC, reply, sessionID, envelope, json, transport} from "./ui-rpc-support.mjs";

test("SQL success with mismatched response ID is unknown, never acknowledged", async () => {
  let requests = 0;
  const rpc = consoleRPC(async () => {
    requests++;
    return reply("wrong-request", {executed: true});
  });
  await assert.rejects(rpc.call("/v1/sql/execute", {session: sessionID, sql: "COMMIT"}),
    (failure) => failure.code === "outcome_unknown" && failure.outcomeUnknown);
  assert.equal(requests, 1);
});

for (const [name, mutate] of [
  ["wrong version", response => response.version = 2],
  ["string version", response => response.version = "1"],
  ["wrong ID", response => response.request_id = "another"],
  ["missing ID", response => delete response.request_id],
  ["string ok", response => response.ok = "true"],
  ["missing result", response => delete response.result],
  ["missing error", response => delete response.error],
  ["extra field", response => response.extra = true],
  ["success with error", response => response.error = {code: "sql_error"}],
  ["false execution", response => response.result.executed = false],
]) {
  test(`JSON ${name} is unknown and poisons all old-session actions`, async () => {
    const rpc = transport((_path, body) => {
      const response = envelope(body.request_id, {executed: true});
      mutate(response);
      return new Response(json.encodeLosslessJSON(response), {headers: {"Content-Type": "application/json"}});
    });
    await assert.rejects(rpc.call("/v1/sql/execute", {session: sessionID, sql: "COMMIT"}), {code: "outcome_unknown"});
    for (const action of ["query", "execute", "close"]) {
      await assert.rejects(rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "ROLLBACK"}), {code: "session_unusable"});
    }
    assert.equal(rpc.requests.length, 1);
  });
}

for (const [action, result] of [
  ["open", {session: "not-an-identity", readonly: false}],
  ["open", {session: sessionID, readonly: "false"}],
  ["open", {session: sessionID, readonly: false, extra: true}],
  ["query", null], ["query", [1]], ["execute", {}], ["close", {closed: false}],
]) {
  test(`invalid ${action} result ${JSON.stringify(result)} is not acknowledged`, async () => {
    const rpc = transport((_path, body) => reply(body.request_id, result));
    await assert.rejects(rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "SELECT 1"}), {code: "outcome_unknown"});
    assert.equal(rpc.requests.length, 1);
  });
}

test("successful JSON open/query/execute/close validates shapes and exact i64", async () => {
  const rpc = transport((path, body) => reply(body.request_id,
    path.endsWith("open") ? {session: sessionID, readonly: false} : path.endsWith("query") ?
      [[9223372036854775807n, -9007199254740993n]] : path.endsWith("close") ? {closed: true} : {executed: true}));
  await rpc.call("/v1/sql/open", {database: {}});
  assert.deepEqual(await rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}), [[9223372036854775807n, -9007199254740993n]]);
  await rpc.call("/v1/sql/execute", {session: sessionID, sql: "ROLLBACK"});
  await rpc.call("/v1/sql/close", {session: sessionID});
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}), {code: "session_unusable"});
  assert.equal(rpc.requests.length, 4);
});

for (const status of [401, 429]) {
  test(`known correlated ${status} JSON fallback permits explicit later SQL without automatic dispatch`, async () => {
    const rpc = transport((_path, body) => rpc.requests.length === 1 ?
      reply(body.request_id, null, {code: status === 401 ? "unauthenticated" : "quota_exceeded"}, status) : reply(body.request_id, {executed: true}));
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true),
      failure => failure.code === (status === 401 ? "unauthenticated" : "quota_exceeded") && !failure.outcomeUnknown);
    assert.equal(rpc.requests.length, 1);
    await rpc.call("/v1/sql/execute", {session: sessionID, sql: "ROLLBACK"});
    assert.equal(rpc.requests.length, 2);
  });
}

test("flagged terminal SQL error retains session for user-issued rollback only", async () => {
  const rpc = transport((_path, body) => rpc.requests.length === 1 ?
    reply(body.request_id, null, {code: "sql_error", execution_completed: true, may_have_committed: true}, 400) : reply(body.request_id, {executed: true}));
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "bad SQL"}, true),
    failure => failure.code === "sql_error" && failure.executionCompleted && failure.mayHaveCommitted && !failure.outcomeUnknown);
  assert.equal(rpc.requests.length, 1);
  await rpc.call("/v1/sql/execute", {session: sessionID, sql: "ROLLBACK"});
  assert.equal(rpc.requests[1].body.sql, "ROLLBACK");
  assert.equal(rpc.requests.length, 2);
});

test("settled failure without commit risk is a definite answer", async () => {
  const rpc = transport((_path, body) => rpc.requests.length === 1 ?
    reply(body.request_id, null, {code: "sql_error", execution_completed: true, may_have_committed: false}, 400) : reply(body.request_id, {executed: true}));
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT bad"}, true),
    failure => failure.code === "sql_error" && failure.executionCompleted && !failure.mayHaveCommitted && !failure.outcomeUnknown);
  assert.equal(rpc.requests.length, 1);
  await rpc.call("/v1/sql/execute", {session: sessionID, sql: "ROLLBACK"});
  assert.equal(rpc.requests.length, 2);
});

for (const error of [
  {code: "sql_error"}, {code: "sql_error", execution_completed: true},
  {code: "sql_error", execution_completed: false, may_have_committed: true},
  {code: "sql_error", execution_completed: "true", may_have_committed: true},
  {code: "quota_exceeded", extra: true}, {code: 429},
]) {
  test(`malformed error ${JSON.stringify(error)} poisons without rollback/close`, async () => {
    const rpc = transport((_path, body) => reply(body.request_id, null, error, 400));
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}), {code: "outcome_unknown"});
    await assert.rejects(rpc.call("/v1/sql/execute", {session: sessionID, sql: "ROLLBACK"}), {code: "session_unusable"});
    assert.equal(rpc.requests.length, 1);
  });
}

// session_moved is a 409 refusal the gateway proved; only the platform's own
// unresolved answer is a 502. Both must poison the session.
for (const [code, status] of [["outcome_unknown", 502], ["session_moved", 409]]) {
  test(`${code} poisons execute/query/close with one actual dispatch`, async () => {
    const rpc = transport((_path, body) => reply(body.request_id, null, {code}, status));
    await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}), {code});
    for (const action of ["execute", "query", "close"]) await assert.rejects(
      rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "ROLLBACK"}), {code: "session_unusable"});
    assert.equal(rpc.requests.length, 1);
  });
}

test("uncertain open does not automatically open another opaque session", async () => {
  const rpc = transport(() => { throw new Error("network"); });
  await assert.rejects(rpc.call("/v1/sql/open", {database: {}}), {code: "outcome_unknown"});
  await assert.rejects(rpc.call("/v1/sql/open", {database: {}}), {code: "session_unusable"});
  assert.equal(rpc.requests.length, 1);
});

test("busy session rejects concurrent SQL and close until explicit operation finishes", async () => {
  let finish;
  const rpc = transport((_path, body) => new Promise(resolve => {
    finish = () => resolve(reply(body.request_id, {executed: true}));
  }));
  const pending = rpc.call("/v1/sql/execute", {session: sessionID, sql: "COMMIT"});
  for (const action of ["query", "execute", "close"]) await assert.rejects(
    rpc.call(`/v1/sql/${action}`, {session: sessionID, sql: "SELECT 1"}), {code: "session_busy"});
  assert.equal(rpc.requests.length, 1);
  finish();
  await pending;
});

test("dispatch timeout is unknown, aborts transport and does not auto-close", async () => {
  const rpc = transport(() => new Promise(() => {}), {timeoutMS: 5});
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}), {code: "outcome_unknown"});
  assert(rpc.requests[0].options.signal.aborted);
  assert.equal(rpc.requests[0].options.redirect, "error");
  await assert.rejects(rpc.call("/v1/sql/close", {session: sessionID}), {code: "session_unusable"});
  assert.equal(rpc.requests.length, 1);
});

for (const [name, response] of [
  ["error HTTP with success envelope", identifier => reply(identifier, {executed: true}, null, 401)],
  ["error envelope with nonnull result", identifier => reply(identifier, {executed: true}, {code: "quota_exceeded"}, 429)],
  ["invalid JSON", () => new Response('{"version":', {headers: {"Content-Type": "application/json"}})],
  ["duplicate fields", identifier => new Response(`{"version":1,"request_id":"${identifier}","ok":true,"ok":false,"result":null,"error":null}`,
    {headers: {"Content-Type": "application/json"}})],
  ["invalid UTF8", () => new Response(new Uint8Array([0xff]), {headers: {"Content-Type": "application/json"}})],
]) {
  test(`${name} is unknown`, async () => {
    const rpc = transport((_path, body) => response(body.request_id));
    await assert.rejects(rpc.call("/v1/sql/execute", {session: sessionID, sql: "COMMIT"}), {code: "outcome_unknown"});
    assert.equal(rpc.requests.length, 1);
  });
}

test("GET envelope version remains strict but has no invented outbound request ID", async () => {
  const rpc = transport(() => reply("", {projects: {}}));
  assert.deepEqual(await rpc.call("/v1/catalog"), {projects: {}});
  assert(!Object.hasOwn(rpc.requests[0].options.headers, "X-Request-ID"));
});

test("oversized JSON reply and local request are bounded without automatic dispatch", async () => {
  const rpc = transport((_path, body) => reply(body.request_id, [["payload".repeat(30)]]),
    {maxRequestBytes: 256, maxReplyBytes: 128, maxFrameBytes: 128});
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "x".repeat(300)}), {code: "request_limit"});
  assert.equal(rpc.requests.length, 0);
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}), {code: "outcome_unknown"});
  assert.equal(rpc.requests.length, 1);
});

test("NDJSON EOF without complete never acknowledges query rows", async () => {
  const rpc = consoleRPC(async () => new Response('{"type":"row","row":[1]}\n',
    {headers: {"Content-Type": "application/x-ndjson"}}));
  await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "COMMIT"}, true),
    (failure) => failure.code === "outcome_unknown" && failure.outcomeUnknown);
});
