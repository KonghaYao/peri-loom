import {parseLosslessJSON, encodeLosslessJSON, displayJSON} from "/json.js";
import {renderReferenceGraph} from "/refs.js";
const element = identity => document.getElementById(identity);
let session = null;
let catalogue = null;
let recoveryPoint = null;
const requestID = () => crypto.randomUUID().replaceAll("-", "");
function selectedDatabase() {
  const selected = element("database").value;
  if (!selected || !catalogue?.databases[selected]) throw new Error("请先选择数据库");
  return catalogue.databases[selected].id;
}
function display(value) { element("output").textContent = displayJSON(value); }
async function call(path, body, streaming = false) {
  const headers = {Authorization: `Bearer ${element("credential").value}`, "Content-Type": "application/json"};
  if (streaming) headers.Accept = "application/x-ndjson";
  const response = await fetch(path, {method: body ? "POST" : "GET", headers, body: body ? encodeLosslessJSON({...body, request_id: requestID()}) : undefined, cache: "no-store"});
  if (streaming && response.headers.get("content-type")?.includes("ndjson")) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let pending = "";
    const rows = [];
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      pending += decoder.decode(value, {stream: true});
      let boundary;
      while ((boundary = pending.indexOf("\n")) >= 0) {
        const line = pending.slice(0, boundary); pending = pending.slice(boundary + 1);
        if (!line) continue;
        const event = parseLosslessJSON(line);
        if (event.type === "row") rows.push(event.row);
        else display(event);
      }
    }
    renderRows(rows);
    return rows;
  }
  const envelope = parseLosslessJSON(await response.text());
  display(envelope);
  if (!response.ok || !envelope.ok) throw new Error(envelope.error?.code || `HTTP ${response.status}`);
  return envelope.result;
}
function renderRows(rows) {
  element("rows").replaceChildren();
  if (!Array.isArray(rows)) return;
  const table = document.createElement("table");
  for (const row of rows) {
    const record = document.createElement("tr");
    for (const value of row) {
      const cell = document.createElement("td");
      cell.textContent = value === null ? "NULL" : typeof value === "object" ? displayJSON(value) : String(value);
      record.appendChild(cell);
    }
    table.appendChild(record);
  }
  element("rows").appendChild(table);
}
async function refresh() {
  catalogue = await call("/v1/catalog");
  const old = element("database").value;
  element("database").replaceChildren();
  for (const [identity, database] of Object.entries(catalogue.databases)) {
    if (database.state === "deleted") continue;
    const option = document.createElement("option"); option.value = identity; option.textContent = `${identity} · ${database.state}`;
    element("database").appendChild(option);
  }
  if (catalogue.databases[old]) element("database").value = old;
  element("connection").textContent = "已连接 · authenticated";
}
function bind(identity, callback) {
  element(identity).addEventListener("click", async () => {
    const button = element(identity); button.disabled = true;
    try { await callback(); } catch (error) { element("output").textContent += `\n错误: ${error.message}（未自动重试）`; } finally { button.disabled = false; }
  });
}
bind("connect", refresh);
bind("open", async () => {
  if (session) throw new Error("请先关闭现有会话");
  session = await call("/v1/sql/open", {database: selectedDatabase()});
  element("session-state").textContent = session.readonly ? "原生 ReadOnly 会话" : "Writer 会话";
});
bind("close", async () => {
  if (!session) return;
  await call("/v1/sql/close", {session: session.session}); session = null; element("session-state").textContent = "无会话";
});
for (const action of ["query", "execute"]) bind(action, async () => {
  if (!session) throw new Error("请先打开 SQL 会话");
  const result = await call(`/v1/sql/${action}`, {session: session.session, sql: element("sql").value}, action === "query" && element("stream").checked);
  if (action === "query") renderRows(result);
});
document.querySelectorAll("[data-tool]").forEach(button => button.addEventListener("click", async () => {
  try { const result = await call(`/v1/databases/${button.dataset.tool}`, {database: selectedDatabase()}); if (button.dataset.tool === "schema") renderRows(result); if (button.dataset.tool === "refs") { let graph = element("refs-graph"); if (!graph) { graph = document.createElement("div"); graph.id = "refs-graph"; button.parentNode.after(graph); } renderReferenceGraph(graph, result); } } catch (error) { element("output").textContent += `\n${error.message}`; }
}));
document.querySelectorAll("[data-action]").forEach(button => button.addEventListener("click", async () => {
  try {
    const action = button.dataset.action;
    if (["delete", "restore"].includes(action) && !confirm("该操作会推进 fencing 并改变当前数据库状态。确定继续？")) return;
    const result = await call(`/v1/databases/${action}`, {database: selectedDatabase(), name: element("ref-name").value, snapshot: element("ref-name").value, worker_id: element("destination").value, sequence: BigInt(element("pitr-sequence").value || "0"), safety_window_ms: 1000});
    if (action === "pitr") recoveryPoint = result;
    if (["branch", "clone", "delete", "suspend", "resume"].includes(action)) await refresh();
  } catch (error) { element("output").textContent += `\n${error.message}（未自动重试）`; }
}));
bind("restore-pitr", async () => { if (!recoveryPoint) throw new Error("请先查找恢复点"); await call("/v1/databases/restore-point", {database:selectedDatabase(), sequence:recoveryPoint.sequence}); });
bind("create-org", () => call("/v1/organizations/create", {id:element("org").value,name:element("org").value}));
bind("create-project", () => call("/v1/projects/create", {id:element("project").value,organization:element("org").value,region:element("region").value}));
bind("create-db", async () => { await call("/v1/databases/create", {project:element("project").value,database:{namespace:{tenant:`${element("org").value}_${element("project").value}`,database:element("name").value},branch:"main"},role:"writer"}); await refresh(); });
bind("create-key", () => call("/v1/keys/create", {subject:"console",project:element("project").value,role:"admin",service_account:false}));
bind("token", () => call("/v1/tokens", {database:selectedDatabase(),role:"writer",expires_ms:Date.now()+120000}));
bind("metrics", () => call("/v1/metrics"));
