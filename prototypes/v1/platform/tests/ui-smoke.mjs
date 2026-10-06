import fs from "node:fs";
import assert from "node:assert/strict";

const connectionPath = process.env.PERI_UI_CONNECTION_FILE;
const port = process.env.PERI_UI_CDP_PORT;
if (!connectionPath || !port || typeof WebSocket !== "function") throw new Error("private connection file, isolated CDP port and WebSocket-capable Node required");
const info = fs.lstatSync(connectionPath);
assert(info.isFile() && !info.isSymbolicLink() && (info.mode & 0o077) === 0 && info.size < 1024 * 1024);
const connection = JSON.parse(fs.readFileSync(connectionPath, "utf8"));
const target = await fetch(`http://127.0.0.1:${port}/json/new?${encodeURIComponent(connection.endpoint)}`, {method:"PUT"}).then(response => response.json());
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve,reject) => {socket.addEventListener("open",resolve,{once:true});socket.addEventListener("error",reject,{once:true});});
let sequence = 0;
const pending = new Map();
socket.addEventListener("message", event => {
  const response = JSON.parse(String(event.data));
  const callback = pending.get(response.id);
  if (!callback) return;
  pending.delete(response.id);
  if (response.error) callback.reject(new Error("browser protocol request failed"));
  else callback.resolve(response.result);
});
async function command(method, params = {}) {
  const identity = ++sequence;
  const response = new Promise((resolve,reject) => {pending.set(identity,{resolve,reject});setTimeout(() => {if (pending.delete(identity)) reject(new Error("browser command timed out"));},15000).unref();});
  socket.send(JSON.stringify({id:identity,method,params}));
  return response;
}
async function evaluate(expression) {
  const result = await command("Runtime.evaluate",{expression,returnByValue:true,awaitPromise:true});
  if (result.exceptionDetails) throw new Error("browser evaluation failed");
  return result.result?.value;
}
async function wait(expression) {
  const deadline = Date.now()+15000;
  while (Date.now()<deadline) {
    if (await evaluate(expression)) return;
    await new Promise(resolve => setTimeout(resolve,50));
  }
  throw new Error("browser state did not converge");
}
try {
  await command("Runtime.enable");
  await command("Page.enable");
  await wait(`document.getElementById('connect') !== null`);
  await evaluate(`document.getElementById('credential').value=${JSON.stringify(connection.project_api_key)};document.getElementById('connect').click();`);
  await wait(`document.getElementById('connection').textContent.includes('authenticated')`);
  const databaseKey = `${connection.database.namespace.tenant}/${connection.database.namespace.database}/${connection.database.branch}`;
  await evaluate(`document.getElementById('database').value=${JSON.stringify(databaseKey)};document.getElementById('open').click();`);
  await wait(`document.getElementById('session-state').textContent.includes('Writer')`);
  await evaluate(`document.getElementById('sql').value='SELECT 42';document.getElementById('query').click();`);
  await wait(`document.querySelector('#rows td')?.textContent === '42'`);
  await evaluate(`document.getElementById('sql').value='SELECT 9223372036854775807';document.getElementById('query').click();`);
  await wait(`document.querySelector('#rows td')?.textContent === '9223372036854775807'`);
  await evaluate(`document.getElementById('stream').checked=true;document.getElementById('sql').value='SELECT -9007199254740993';document.getElementById('query').click();`);
  await wait(`document.querySelector('#rows td')?.textContent === '-9007199254740993'`);
  await evaluate(`document.getElementById('stream').checked=true;document.getElementById('sql').value='SELECT 43';document.getElementById('query').click();`);
  await wait(`document.querySelector('#rows td')?.textContent === '43'`);
  await evaluate(`document.getElementById('close').click();`);
  await wait(`document.getElementById('session-state').textContent === '无会话'`);
  await evaluate(`document.querySelector('[data-tool="refs"]').click();`);
  await wait(`document.querySelector('#refs-graph svg') !== null`);
  assert.equal(await evaluate(`localStorage.length`),0);
  assert.equal(await evaluate(`sessionStorage.length`),0);
  if (process.env.PERI_UI_SCREENSHOT) {
    const image = await command("Page.captureScreenshot",{format:"png"});
    fs.writeFileSync(process.env.PERI_UI_SCREENSHOT,Buffer.from(image.data,"base64"),{mode:0o600});
  }
  console.log("PASS actual headless Chromium Admin UI: authenticated catalog, opaque SQL session, query42, exact int64 JSON/NDJSON, NDJSON43, real refs graph, clean close, no credential browser storage");
} finally {
  socket.close();
  await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`);
}
