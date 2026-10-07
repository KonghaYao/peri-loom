import test from "node:test";
import assert from "node:assert/strict";
import { Client, PeriError } from "./dist/client.js";

const credential = "synthetic-api-credential__________________";
const database = { namespace: { tenant: "tenant", database: "database" }, branch: "main" };
const identifier = "0123456789abcdef0123456789abcdef";
const originalFetch = globalThis.fetch;

test.afterEach(() => { globalThis.fetch = originalFetch; });

test("SQL and management responses retain exact untagged integer wire literals", async () => {
  const rows = String.raw`[[9223372036854775807,-9007199254740993,"9223372036854775807","quote\" slash\\ \u0039",{"bytes":[0,127,255],"nested":{"integer":-9007199254740993}},1.25,9007199254740993.0,9.007199254740993e15,true,false,null]]`;
  let calls = 0;
  globalThis.fetch = async (url, options) => {
    calls++;
    const body = JSON.parse(new TextDecoder().decode(options.body));
    if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
    const result = url.endsWith("/query") ? rows : '{"epoch":18446744073709551615,"nested":{"position":9223372036854775807}}';
    return new Response(`{"version":1,"request_id":"${body.request_id}","ok":true,"result":${result},"error":null}`);
  };
  const client = new Client("http://127.0.0.1:8080", credential);
  const session = await client.open(database);
  const result = await session.query("SELECT exact_native_values", "lossless-rows");
  assert.deepEqual(result, [[9223372036854775807n, -9007199254740993n, "9223372036854775807", 'quote" slash\\ 9', { bytes: [0, 127, 255], nested: { integer: -9007199254740993n } }, 1.25, Number("9007199254740993.0"), Number("9.007199254740993e15"), true, false, null]]);
  const status = await client.management("status", { database, request_id: "lossless-epoch" });
  assert.equal(status.epoch, 18446744073709551615n);
  assert.equal(status.nested.position, 9223372036854775807n);
  assert.equal(calls, 3);
});

test("terminal SQL failure permits explicit rollback on the same opaque session", async () => {
  const statements = [];
  globalThis.fetch = async (url, options) => {
    const body = JSON.parse(new TextDecoder().decode(options.body));
    if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
    assert.equal(body.session, identifier);
    statements.push(body.sql);
    if (body.sql === "invalid SQL") return new Response(JSON.stringify({ version: 1, request_id: body.request_id, ok: false, result: null, error: { code: "sql_error", execution_completed: true, may_have_committed: true } }));
    return reply(body.request_id, url.endsWith("/execute") ? { executed: true } : [[1]]);
  };
  const session = await new Client("http://127.0.0.1:8080", credential).open(database);
  await assert.rejects(session.execute("invalid SQL", "terminal"), (error) => error.code === "sql_error" && error.executionCompleted && error.mayHaveCommitted && !error.outcomeUnknown);
  await session.execute("ROLLBACK", "explicit-rollback");
  assert.deepEqual(await session.query("SELECT 1", "explicit-reuse"), [[1]]);
  assert.deepEqual(statements, ["invalid SQL", "ROLLBACK", "SELECT 1"]);
});

test("canonical management operations and serialization errors are sanitized", async () => {
  const paths = [];
  globalThis.fetch = async (url, options) => {
    const body = JSON.parse(new TextDecoder().decode(options.body));
    paths.push(new URL(url).pathname);
    return reply(body.request_id, { accepted: true });
  };
  const client = new Client("http://127.0.0.1:8080", credential);
  await client.management("organizations/create", { id: "local", name: "Local" });
  await client.management("projects/create", { id: "default", organization: "local", region: "local" });
  await client.management("databases/restore-point", { database, sequence: 3 });
  await client.management("pitr", { database, timestamp_ms: 1 });
  assert.deepEqual(paths, ["/v1/organizations/create", "/v1/projects/create", "/v1/databases/restore-point", "/v1/databases/pitr"]);
  await assert.rejects(client.management("not-a-server-operation", { database }), (error) => error.code === "invalid_operation");
  const body = { toJSON() { throw new Error(credential); } };
  await assert.rejects(client.call("/v1/sql/execute", "serialize", body), (error) => error.code === "invalid_request" && !String(error).includes(credential));
  assert.equal(paths.length, 4);
});

test("duplicate ACK keys fail closed and poison the session", async () => {
  let requests = 0;
  globalThis.fetch = async (url, options) => {
    requests++;
    const body = JSON.parse(new TextDecoder().decode(options.body));
    if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
    return new Response(`{"version":1,"request_id":"${body.request_id}","ok":false,"ok":true,"result":{"executed":true},"error":null}`);
  };
  const session = await new Client("http://127.0.0.1:8080", credential).open(database);
  await assert.rejects(session.execute("COMMIT", "duplicate"), (error) => error.outcomeUnknown);
  await assert.rejects(session.query("SELECT 1", "no-replay"), (error) => error.code === "session_unusable");
  assert.equal(requests, 2);
});

function reply(requestID, result) { return new Response(JSON.stringify({ version: 1, request_id: requestID, ok: true, result, error: null })); }

test("wire contract, management request ID, opaque sessions and integer cells", async () => {
  const paths = [];
  globalThis.fetch = async (url, options) => {
    assert.equal(options.redirect, "error");
    assert.equal(options.headers.Authorization, `Bearer ${credential}`);
    const body = JSON.parse(new TextDecoder().decode(options.body));
    assert.equal(options.headers["X-Request-ID"], body.request_id);
    paths.push(new URL(url).pathname);
    if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
    if (url.endsWith("/execute")) return reply(body.request_id, { executed: true });
    if (url.endsWith("/query")) return reply(body.request_id, [[1, "native"]]);
    if (url.endsWith("/close")) return reply(body.request_id, { closed: true });
    return reply(body.request_id, { created: true });
  };
  const client = new Client("http://127.0.0.1:8080", credential);
  const session = await client.open(database);
  await session.execute("BEGIN; COMMIT", "execute-1");
  assert.deepEqual(await session.query("INSERT INTO items VALUES(1) RETURNING id", "query-write"), [[1, "native"]]);
  await client.management("snapshot", { database, name: "snapshot", request_id: "snapshot-1" });
  await session.close();
  assert.equal(paths.length, 5);
  assert(!JSON.stringify(client).includes(credential));
});

for (const action of ["execute", "query"]) {
  test(`${action} lost ACK never retries or restores session`, async () => {
    let mutations = 0;
    globalThis.fetch = async (url, options) => {
      const body = JSON.parse(new TextDecoder().decode(options.body));
      if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
      mutations++;
      throw new Error(credential);
    };
    const client = new Client("http://127.0.0.1:8080", credential);
    const session = await client.open(database);
    await assert.rejects(session[action]("INSERT INTO items VALUES(1)", "lost-ack"), (error) => error instanceof PeriError && error.outcomeUnknown && error.requestID === "lost-ack" && !error.message.includes(credential));
    await assert.rejects(session.execute("INSERT INTO items VALUES(2)"), (error) => error.code === "session_unusable");
    assert.equal(mutations, 1);
  });
}

test("session_moved is explicit and never reconnects", async () => {
  let calls = 0;
  globalThis.fetch = async (url, options) => {
    calls++;
    const body = JSON.parse(new TextDecoder().decode(options.body));
    if (url.endsWith("/open")) return reply(body.request_id, { session: identifier, readonly: false });
    return new Response(JSON.stringify({ version: 1, request_id: body.request_id, ok: false, result: null, error: { code: "session_moved" } }), { status: 409 });
  };
  const session = await new Client("https://example.com", credential).open(database);
  await assert.rejects(session.query("SELECT 1"), (error) => error.code === "session_moved" && !error.outcomeUnknown);
  await assert.rejects(session.query("SELECT 1"), (error) => error.code === "session_unusable");
  assert.equal(calls, 2);
});

test("endpoint, request and response bounds with lossless integer results", async () => {
  for (const endpoint of ["http://localhost:8080", "http://example.com", "https://user:password@example.com", "https://example.com/?secret=x"]) assert.throws(() => new Client(endpoint, credential));
  globalThis.fetch = async (url, options) => {
    const body = JSON.parse(new TextDecoder().decode(options.body));
    return reply(body.request_id, { session: identifier, readonly: false });
  };
  await assert.rejects(new Client("https://example.com", credential, { maxRequestBytes: 1 }).open(database), (error) => error.code === "request_limit");
  await assert.rejects(new Client("https://example.com", credential, { maxResponseBytes: 4 }).open(database), (error) => error.outcomeUnknown);
  const client = new Client("https://example.com", credential);
  const session = await client.open(database);
  globalThis.fetch = async (_url, options) => {
    const body = JSON.parse(new TextDecoder().decode(options.body));
    return new Response(`{"version":1,"request_id":"${body.request_id}","ok":true,"result":[[9223372036854775807]],"error":null}`);
  };
  assert.deepEqual(await session.query("SELECT large_int"), [[9223372036854775807n]]);
});
