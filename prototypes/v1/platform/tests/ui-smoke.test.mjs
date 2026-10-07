import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {spawnSync} from "node:child_process";
import test from "node:test";

const helperURL = new URL("./ui-smoke.mjs", import.meta.url).href;
const sensitive = ["synthetic-project-key-secret", "synthetic-tenant-not-log", "synthetic-database-not-log", "synthetic-session-not-log", "SELECT private-secret"];
const fakeBrowser = String.raw`
const mode = process.env.FAKE_MODE;
let queryCalls = 0;
if (mode === "command_timeout") {
  const originalTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (callback, duration, ...extraArguments) => originalTimeout(callback, duration === 15000 ? 20 : duration, ...extraArguments);
}
globalThis.fetch = async (url, options) => {
  if (!String(url).startsWith("http://127.0.0.1:1/json/")) throw new Error("unexpected offline request");
  if (String(url).includes("/new?")) {
    if (mode === "target_failure") throw new Error("synthetic-project-key-secret");
    return {ok: true, json: async () => ({id: "fake-target", webSocketDebuggerUrl: "ws://127.0.0.1:1/fake"})};
  }
  console.log("FAKE_CDP release");
  if (mode === "release_failure" || mode === "query_and_release_failure") throw new Error("SELECT private-secret");
  return {ok: true};
};
if (mode === "dom_timeout" || mode === "query_and_release_failure" || mode === "close_ack_missing") {
  let ticks = 0;
  Date.now = () => (ticks += 10000);
}
class FakeSocket extends EventTarget {
  constructor() {
    super();
    queueMicrotask(() => this.dispatchEvent(new Event(mode === "socket_failure" ? "error" : "open")));
  }
  send(bytes) {
    const request = JSON.parse(bytes);
    if (mode === "command_timeout") return;
    if (mode === "malformed_cdp") {
      queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", {data: "null"})));
      return;
    }
    if (mode === "protocol_failure" && request.method === "Runtime.enable") {
      queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", {data: JSON.stringify({id: request.id, error: {message: "synthetic-project-key-secret"}})})));
      return;
    }
    let value = true;
    let exceptionDetails;
    const expression = request.params?.expression ?? "";
    if (expression.includes("getElementById('query').click()")) {
      queryCalls++;
      console.log("FAKE_SQL query");
      if (mode === "query_failure") exceptionDetails = {text: "synthetic-session-not-log SELECT private-secret"};
    }
    if (expression.includes("getElementById('close').click()")) console.log("FAKE_SQL close");
    if (expression.includes("getElementById('open').click()")) console.log("FAKE_SQL open");
    if (mode === "dom_timeout" && expression.includes("getElementById('connect') !== null")) value = false;
    if (mode === "query_and_release_failure" && queryCalls > 0 && expression.includes("#rows td")) value = false;
    if (mode === "close_ack_missing" && expression.includes("textContent === '无会话'")) value = false;
    if (expression === "localStorage.length") value = mode === "storage_failure" ? 1 : 0;
    if (expression === "sessionStorage.length") value = 0;
    const result = request.method === "Runtime.evaluate" ? {result: {value}, ...(exceptionDetails ? {exceptionDetails} : {})} : {};
    queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", {data: JSON.stringify({id: request.id, result})})));
  }
  close() {}
}
globalThis.WebSocket = mode === "preflight_failure" ? undefined : FakeSocket;
`;

function run(mode) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "peri-ui-stage-offline-"));
  try {
    const connection = path.join(directory, "fake.json");
    const config = {endpoint: "https://fake.invalid", project_api_key: sensitive[0], database: {namespace: {tenant: sensitive[1], database: sensitive[2]}, branch: "main"}};
    fs.writeFileSync(connection, mode === "config_failure" ? "invalid synthetic-project-key-secret" : JSON.stringify(config), {mode: 0o600});
    const entry = fakeBrowser + `\nawait import(${JSON.stringify(helperURL)});`;
    const result = spawnSync(process.execPath, ["--input-type=module", "--eval", entry], {env: {PATH: process.env.PATH, FAKE_MODE: mode, PERI_UI_CONNECTION_FILE: connection, PERI_UI_CDP_PORT: "1"}, encoding: "utf8", timeout: 2500, maxBuffer: 8192});
    assert.equal(result.error, undefined);
    assert.equal(result.signal, null);
    return result;
  } finally { fs.rmSync(directory, {recursive: true, force: true}); }
}

function diagnostic(result) {
  const lines = result.stderr.trim().split("\n");
  assert.equal(lines.length, 1);
  const record = JSON.parse(lines[0]);
  assert.deepEqual(Object.keys(record).sort(), ["category", "event", "stage"]);
  assert.equal(record.event, "ui_smoke_failed");
  for (const value of sensitive) assert.equal(`${result.stdout}${result.stderr}`.includes(value), false);
  return record;
}

test("actual UI helper reports preflight failure without raw exception", () => {
  const result = run("preflight_failure");
  assert.equal(result.status, 1);
  assert.deepEqual(diagnostic(result), {event: "ui_smoke_failed", stage: "preflight", category: "precondition"});
  assert.equal(result.stdout.includes("FAKE_SQL"), false);
});

test("actual UI helper malformed CDP envelope remains machine-readable and secret-free", () => {
  const result = run("malformed_cdp");
  assert.equal(result.status, 1);
  assert.deepEqual(diagnostic(result), {event: "ui_smoke_failed", stage: "cdp_runtime_enable", category: "cdp_protocol"});
  assert.equal(result.stdout.includes("FAKE_SQL"), false);
});

for (const [mode, stage, category] of [
  ["config_failure", "connection_file", "private_config"],
  ["target_failure", "cdp_target_open", "cdp_transport"],
  ["socket_failure", "cdp_socket_open", "cdp_transport"],
  ["protocol_failure", "cdp_runtime_enable", "cdp_protocol"],
  ["command_timeout", "cdp_runtime_enable", "cdp_timeout"],
  ["dom_timeout", "dom_ready", "dom_timeout"],
  ["query_failure", "query42_submit", "dom_evaluation"],
  ["query_and_release_failure", "query42_oracle", "dom_timeout"],
]) {
  test(`actual UI helper ${mode} preserves closed stage without SQL Close/reopen/retry`, () => {
    const result = run(mode);
    assert.equal(result.status, 1);
    assert.deepEqual(diagnostic(result), {event: "ui_smoke_failed", stage, category});
    assert.equal(result.stdout.includes("FAKE_SQL close"), false);
    assert((result.stdout.match(/FAKE_SQL open/g) ?? []).length <= 1);
    assert((result.stdout.match(/FAKE_SQL query/g) ?? []).length <= 1);
    assert.equal(result.stdout.includes("ui_smoke_complete"), false);
  });
}

test("actual UI helper success validates integer path, acknowledges one Close and checks refs/storage", () => {
  const result = run("success");
  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");
  assert.equal(result.stdout.match(/FAKE_SQL open/g)?.length, 1);
  assert.equal(result.stdout.match(/FAKE_SQL query/g)?.length, 4);
  assert.equal(result.stdout.match(/FAKE_SQL close/g)?.length, 1);
  assert.equal(result.stdout.match(/FAKE_CDP release/g)?.length, 1);
  assert.deepEqual(JSON.parse(result.stdout.trim().split("\n").at(-1)), {event: "ui_smoke_complete", stage: "complete", category: "ok", query_count: 4, close_ack: true, storage_empty: true});
  for (const value of sensitive) assert.equal(result.stdout.includes(value), false);
});

for (const [mode, stage, category] of [["close_ack_missing", "session_close_ack", "dom_timeout"], ["storage_failure", "storage_local", "oracle"], ["release_failure", "cdp_release", "cdp_transport"]]) {
  test(`actual UI helper ${mode} cannot report success or retry Close`, () => {
    const result = run(mode);
    assert.equal(result.status, 1);
    assert.deepEqual(diagnostic(result), {event: "ui_smoke_failed", stage, category});
    assert.equal(result.stdout.match(/FAKE_SQL close/g)?.length, 1);
    assert.equal(result.stdout.includes("ui_smoke_complete"), false);
  });
}
