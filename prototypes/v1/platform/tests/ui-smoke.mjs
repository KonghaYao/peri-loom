import fs from "node:fs";
import assert from "node:assert/strict";

let stage = "preflight";
const categories = new Set(["precondition", "private_config", "cdp_transport", "cdp_protocol", "cdp_timeout", "dom_timeout", "dom_evaluation", "oracle", "local_io", "helper_internal"]);

class SmokeFailure extends Error {
  constructor(category) {
    super("ui_smoke_closed_failure");
    this.category = category;
  }
}

async function step(nextStage, category, operation) {
  stage = nextStage;
  try { return await operation(); }
  catch (failure) { throw failure instanceof SmokeFailure ? failure : new SmokeFailure(category); }
}

async function smoke() {
  let target;
  let socket;
  let port;
  let primaryFailure;
  let failedStage;
  let sequence = 0;
  const pending = new Map();
  const rejectPending = category => {
    for (const callback of pending.values()) {
      clearTimeout(callback.timer);
      callback.reject(new SmokeFailure(category));
    }
    pending.clear();
  };
  function command(method, params = {}) {
    const identifier = ++sequence;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(identifier);
        reject(new SmokeFailure("cdp_timeout"));
      }, 15000);
      pending.set(identifier, {resolve, reject, timer});
      try { socket.send(JSON.stringify({id: identifier, method, params})); }
      catch { clearTimeout(timer); pending.delete(identifier); reject(new SmokeFailure("cdp_transport")); }
    });
  }
  async function evaluate(expression) {
    const response = await command("Runtime.evaluate", {expression, returnByValue: true, awaitPromise: true});
    if (!response?.result || response.exceptionDetails) throw new SmokeFailure("dom_evaluation");
    return response.result.value;
  }
  async function wait(expression) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      if (await evaluate(expression)) return;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new SmokeFailure("dom_timeout");
  }
  try {
    const connectionPath = process.env.PERI_UI_CONNECTION_FILE;
    port = process.env.PERI_UI_CDP_PORT;
    await step("preflight", "precondition", () => {
      assert(connectionPath && /^\d{1,5}$/.test(port ?? "") && Number(port) > 0 && Number(port) <= 65535 && typeof WebSocket === "function");
    });
    const connection = await step("connection_file", "private_config", () => {
      const info = fs.lstatSync(connectionPath);
      assert(info.isFile() && !info.isSymbolicLink() && (info.mode & 0o077) === 0 && info.size < 1024 * 1024);
      return JSON.parse(fs.readFileSync(connectionPath, "utf8"));
    });
    target = await step("cdp_target_open", "cdp_transport", async () => {
      const response = await fetch(`http://127.0.0.1:${port}/json/new?${encodeURIComponent(connection.endpoint)}`, {method: "PUT", signal: AbortSignal.timeout(15000)});
      if (!response.ok) throw new SmokeFailure("cdp_protocol");
      const opened = await response.json();
      if (typeof opened.id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(opened.id) || typeof opened.webSocketDebuggerUrl !== "string") throw new SmokeFailure("cdp_protocol");
      return opened;
    });
    await step("cdp_socket_open", "cdp_transport", async () => {
      socket = new WebSocket(target.webSocketDebuggerUrl);
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new SmokeFailure("cdp_timeout")), 15000);
        socket.addEventListener("open", () => { clearTimeout(timer); resolve(); }, {once: true});
        socket.addEventListener("error", () => { clearTimeout(timer); reject(new SmokeFailure("cdp_transport")); }, {once: true});
      });
    });
    socket.addEventListener("error", () => rejectPending("cdp_transport"));
    socket.addEventListener("close", () => rejectPending("cdp_transport"));
    socket.addEventListener("message", event => {
      let response;
      try { response = JSON.parse(String(event.data)); }
      catch { rejectPending("cdp_protocol"); return; }
      if (!response || typeof response !== "object" || Array.isArray(response)) { rejectPending("cdp_protocol"); return; }
      const callback = pending.get(response.id);
      if (!callback) return;
      clearTimeout(callback.timer);
      pending.delete(response.id);
      if (response.error) callback.reject(new SmokeFailure("cdp_protocol"));
      else callback.resolve(response.result);
    });
    await step("cdp_runtime_enable", "cdp_protocol", () => command("Runtime.enable"));
    await step("cdp_page_enable", "cdp_protocol", () => command("Page.enable"));
    await step("dom_ready", "dom_evaluation", () => wait("document.getElementById('connect') !== null"));
    await step("credential_submit", "dom_evaluation", () => evaluate(`document.getElementById('credential').value=${JSON.stringify(connection.project_api_key)};document.getElementById('connect').click();`));
    await step("catalog_ready", "dom_evaluation", () => wait("document.getElementById('connection').textContent.includes('authenticated')"));
    const databaseKey = await step("database_config", "private_config", () => `${connection.database.namespace.tenant}/${connection.database.namespace.database}/${connection.database.branch}`);
    await step("session_open_submit", "dom_evaluation", () => evaluate(`document.getElementById('database').value=${JSON.stringify(databaseKey)};document.getElementById('open').click();`));
    await step("session_open_ready", "dom_evaluation", () => wait("document.getElementById('session-state').textContent.includes('Writer')"));
    await step("query42_submit", "dom_evaluation", () => evaluate("document.getElementById('sql').value='SELECT 42';document.getElementById('query').click();"));
    await step("query42_oracle", "dom_evaluation", () => wait("document.querySelector('#rows td')?.textContent === '42'"));
    await step("i64_json_submit", "dom_evaluation", () => evaluate("document.getElementById('sql').value='SELECT 9223372036854775807';document.getElementById('query').click();"));
    await step("i64_json_oracle", "dom_evaluation", () => wait("document.querySelector('#rows td')?.textContent === '9223372036854775807'"));
    await step("i64_ndjson_submit", "dom_evaluation", () => evaluate("document.getElementById('stream').checked=true;document.getElementById('sql').value='SELECT -9007199254740993';document.getElementById('query').click();"));
    await step("i64_ndjson_oracle", "dom_evaluation", () => wait("document.querySelector('#rows td')?.textContent === '-9007199254740993'"));
    await step("ndjson43_submit", "dom_evaluation", () => evaluate("document.getElementById('stream').checked=true;document.getElementById('sql').value='SELECT 43';document.getElementById('query').click();"));
    await step("ndjson43_oracle", "dom_evaluation", () => wait("document.querySelector('#rows td')?.textContent === '43'"));
    await step("session_close_submit", "dom_evaluation", () => evaluate("document.getElementById('close').click();"));
    await step("session_close_ack", "dom_evaluation", () => wait("document.getElementById('session-state').textContent === '无会话'"));
    await step("refs_submit", "dom_evaluation", () => evaluate("document.querySelector('[data-tool=\"refs\"]').click();"));
    await step("refs_oracle", "dom_evaluation", () => wait("document.querySelector('#refs-graph svg') !== null"));
    await step("storage_local", "oracle", async () => assert.equal(await evaluate("localStorage.length"), 0));
    await step("storage_session", "oracle", async () => assert.equal(await evaluate("sessionStorage.length"), 0));
    if (process.env.PERI_UI_SCREENSHOT) await step("screenshot", "local_io", async () => {
      const image = await command("Page.captureScreenshot", {format: "png"});
      fs.writeFileSync(process.env.PERI_UI_SCREENSHOT, Buffer.from(image.data, "base64"), {mode: 0o600});
    });
  } catch (failure) {
    primaryFailure = failure;
    failedStage = stage;
  } finally {
    try {
      await step("cdp_release", "cdp_transport", async () => {
        rejectPending("cdp_transport");
        socket?.close();
        if (target) {
          const response = await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`, {signal: AbortSignal.timeout(5000)});
          if (!response.ok) throw new SmokeFailure("cdp_protocol");
        }
      });
    } catch (failure) {
      if (!primaryFailure) { primaryFailure = failure; failedStage = stage; }
    }
  }
  if (primaryFailure) { stage = failedStage; throw primaryFailure; }
  stage = "complete";
}

await smoke().then(() => {
  console.log(JSON.stringify({event: "ui_smoke_complete", stage: "complete", category: "ok", query_count: 4, close_ack: true, storage_empty: true}));
}).catch(failure => {
  const category = failure instanceof SmokeFailure && categories.has(failure.category) ? failure.category : "helper_internal";
  console.error(JSON.stringify({event: "ui_smoke_failed", stage, category}));
  process.exitCode = 1;
});
