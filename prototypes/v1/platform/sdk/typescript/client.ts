import { parseLosslessJSON, type JSONValue } from "./json.js";

export type SqlValue = JSONValue;

export interface Namespace { tenant: string; database: string }
export interface DatabaseID { namespace: Namespace; branch: string }
export interface Options { timeoutMS?: number; maxRequestBytes?: number; maxResponseBytes?: number }
export interface ManagementRequest {
  database?: DatabaseID;
  request_id?: string;
  project?: string;
  role?: "writer" | "read_only";
  name?: string;
  snapshot?: string;
  worker_id?: string;
  sequence?: number;
  timestamp_ms?: number;
  safety_window_ms?: number;
  organization?: string;
  id?: string;
  region?: string;
  subject?: string;
  service_account?: boolean;
  expires_ms?: number;
  quotas?: Record<string, number>;
}

const label = /^[A-Za-z0-9_-]{1,64}$/;
const component = /^[A-Za-z0-9_-]{1,128}$/;
const codes = new Set(["outcome_unknown", "session_moved", "invalid_request", "invalid_session", "unauthenticated", "unauthorized", "forbidden", "quota_exceeded", "request_limit", "result_limit", "sql_limit", "session_busy", "upstream_rejected", "upstream_unavailable", "placement_unavailable", "metadata_unavailable", "audit_unavailable", "gateway_closing", "operation_incomplete", "token_binding_mismatch", "readonly_placement_required"]);
const operations = new Set(["create", "delete", "snapshot", "branch", "clone", "restore", "restore-point", "pitr", "suspend", "resume", "migrate", "status", "checkpoint", "schema", "gc", "gc-inspect", "refs", "compact", "drop-snapshot", "retain-after", "rewrap"]);

export class PeriError extends Error {
  constructor(public readonly code: string, public readonly requestID: string, public readonly outcomeUnknown = false, public readonly executionCompleted = false, public readonly mayHaveCommitted = false) {
    super(`peri: ${code} (request ${requestID})`);
    this.name = "PeriError";
  }
}

export function randomID(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function validDatabase(database: DatabaseID): boolean {
  return component.test(database.namespace.tenant) && component.test(database.namespace.database) && component.test(database.branch);
}

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export class Client {
  #endpoint: string;
  #credential: string;
  #timeout: number;
  #requestLimit: number;
  #responseLimit: number;

  constructor(endpoint: string, credential: string, options: Options = {}) {
    let parsed: URL;
    try { parsed = new URL(endpoint); } catch { throw new PeriError("invalid_endpoint", ""); }
    const host = parsed.hostname.replace(/^\[|\]$/g, "");
    const loopback = host === "::1" || /^127(?:\.\d{1,3}){3}$/.test(host);
    if (parsed.username || parsed.password || parsed.search || parsed.hash || (parsed.pathname !== "/" && parsed.pathname !== "") || (parsed.protocol !== "https:" && !(parsed.protocol === "http:" && loopback))) {
      throw new PeriError("invalid_endpoint", "");
    }
    if (!/^[\x21-\x7e]{32,4096}$/.test(credential)) throw new PeriError("invalid_credential", "");
    this.#endpoint = parsed.origin;
    this.#credential = credential;
    this.#timeout = options.timeoutMS ?? 30000;
    this.#requestLimit = options.maxRequestBytes ?? 1048576;
    this.#responseLimit = options.maxResponseBytes ?? 8388608;
    for (const limit of [this.#timeout, this.#requestLimit, this.#responseLimit]) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 33554432) throw new PeriError("invalid_limits", "");
    }
  }

  async call(path: string, requestID: string, body: unknown, signal?: AbortSignal): Promise<unknown> {
    const operation = path.replace(/^\/v1\//, "");
    if (!/^sql\/(open|execute|query|close)$/.test(operation) && !managementPath(operation)) throw new PeriError("invalid_operation", "");
    if (!label.test(requestID)) throw new PeriError("invalid_request_id", "");
    if (signal?.aborted) throw new PeriError("request_cancelled", requestID);
    let encoded: ReturnType<TextEncoder["encode"]>;
    try { encoded = new TextEncoder().encode(JSON.stringify(body)); } catch { throw new PeriError("invalid_request", requestID); }
    if (encoded.byteLength > this.#requestLimit) throw new PeriError("request_limit", requestID);
    const controller = new AbortController();
    const cancel = () => controller.abort();
    signal?.addEventListener("abort", cancel, { once: true });
    const timer = setTimeout(cancel, this.#timeout);
    try {
      const response = await fetch(this.#endpoint + path, {
        method: "POST", body: encoded, signal: controller.signal, redirect: "error", credentials: "omit", cache: "no-store", referrerPolicy: "no-referrer",
        headers: { "Content-Type": "application/json", "Accept": "application/json", "Authorization": `Bearer ${this.#credential}` },
      });
      if (!response.body) throw new PeriError("outcome_unknown", requestID, true);
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) break;
          size += chunk.value.byteLength;
          if (size > this.#responseLimit) { await reader.cancel(); throw new PeriError("outcome_unknown", requestID, true); }
          chunks.push(chunk.value);
        }
      } finally { reader.releaseLock(); }
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      const reply: unknown = parseLosslessJSON(text);
      if (!object(reply) || Object.keys(reply).sort().join(",") !== "error,ok,request_id,result,version" || reply.version !== 1 || reply.request_id !== requestID || typeof reply.ok !== "boolean") throw new PeriError("outcome_unknown", requestID, true);
      if (!reply.ok) {
        if (!object(reply.error) || typeof reply.error.code !== "string" || reply.result !== null || Object.keys(reply.error).some((key) => !["code", "execution_completed", "may_have_committed"].includes(key)) || [reply.error.execution_completed, reply.error.may_have_committed].some((flag) => flag !== undefined && typeof flag !== "boolean")) throw new PeriError("outcome_unknown", requestID, true);
        const code = reply.error.code === "sql_error" || codes.has(reply.error.code) ? reply.error.code : "remote_error";
        if (code === "sql_error" && (reply.error.execution_completed !== true || reply.error.may_have_committed !== true)) throw new PeriError("outcome_unknown", requestID, true);
        throw new PeriError(code, requestID, code === "outcome_unknown", reply.error.execution_completed === true, reply.error.may_have_committed === true);
      }
      if (!response.ok || reply.error !== null) throw new PeriError("outcome_unknown", requestID, true);
      return reply.result;
    } catch (error) {
      if (error instanceof PeriError) throw error;
      throw new PeriError("outcome_unknown", requestID, true);
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", cancel);
    }
  }

  async open(database: DatabaseID, signal?: AbortSignal): Promise<Session> {
    if (!validDatabase(database)) throw new PeriError("invalid_database", "");
    const requestID = randomID();
    const result = await this.call("/v1/sql/open", requestID, { database, request_id: requestID }, signal);
    if (!object(result) || Object.keys(result).sort().join(",") !== "readonly,session" || typeof result.session !== "string" || !/^[a-f0-9]{32}$/.test(result.session) || typeof result.readonly !== "boolean") throw new PeriError("outcome_unknown", requestID, true);
    return new Session(this, result.session, result.readonly);
  }

  async management(operation: string, request: ManagementRequest, signal?: AbortSignal): Promise<unknown> {
    const path = managementPath(operation);
    if (!path) throw new PeriError("invalid_operation", "");
    if (path.startsWith("databases/") || path === "tokens") {
      if (!request.database || !validDatabase(request.database)) throw new PeriError("invalid_database", "");
    }
    for (const value of [request.sequence, request.timestamp_ms, request.safety_window_ms, request.expires_ms]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) throw new PeriError("invalid_integer", "");
    }
    const requestID = request.request_id ?? randomID();
    return this.call(`/v1/${path}`, requestID, { ...request, request_id: requestID }, signal);
  }
}

function managementPath(operation: string): string | undefined {
  const databaseOperation = operation.replace(/^databases\//, "");
  if (operations.has(databaseOperation)) return `databases/${databaseOperation}`;
  if (["organizations/create", "projects/create", "keys/create", "keys/revoke", "tokens"].includes(operation)) return operation;
  return undefined;
}

export class Session {
  #client: Client;
  #identifier: string;
  #busy = false;
  #closed = false;
  #unusable = false;
  readonly readonly: boolean;

  constructor(client: Client, identifier: string, readonly: boolean) {
    this.#client = client;
    this.#identifier = identifier;
    this.readonly = readonly;
  }

  async #sql(action: "execute" | "query", sql: string, requestID: string, signal?: AbortSignal): Promise<unknown> {
    if (!label.test(requestID)) throw new PeriError("invalid_request_id", "");
    if (this.#busy) throw new PeriError("session_busy", requestID);
    if (this.#closed || this.#unusable) throw new PeriError("session_unusable", requestID);
    if (!sql) throw new PeriError("invalid_sql", requestID);
    this.#busy = true;
    try {
      const result = await this.#client.call(`/v1/sql/${action}`, requestID, { session: this.#identifier, sql, request_id: requestID }, signal);
      if (action === "execute" ? !object(result) || Object.keys(result).join(",") !== "executed" || result.executed !== true : !Array.isArray(result) || result.some((row: unknown) => !Array.isArray(row))) throw new PeriError("outcome_unknown", requestID, true);
      return result;
    } catch (error) {
      if (error instanceof PeriError && (error.outcomeUnknown || error.code === "session_moved")) this.#unusable = true;
      throw error;
    } finally { this.#busy = false; }
  }

  async execute(sql: string, requestID = randomID(), signal?: AbortSignal): Promise<void> { await this.#sql("execute", sql, requestID, signal); }
  async query(sql: string, requestID = randomID(), signal?: AbortSignal): Promise<SqlValue[][]> { return await this.#sql("query", sql, requestID, signal) as SqlValue[][]; }

  async close(signal?: AbortSignal): Promise<void> {
    if (this.#busy) throw new PeriError("session_busy", "");
    if (this.#closed) return;
    this.#busy = true;
    const requestID = randomID();
    try {
      const result = await this.#client.call("/v1/sql/close", requestID, { session: this.#identifier, request_id: requestID }, signal);
      if (!object(result) || Object.keys(result).join(",") !== "closed" || result.closed !== true) throw new PeriError("outcome_unknown", requestID, true);
      this.#closed = true;
    } catch (error) {
      if (error instanceof PeriError && (error.outcomeUnknown || error.code === "session_moved")) this.#unusable = true;
      throw error;
    } finally { this.#busy = false; }
  }
}
