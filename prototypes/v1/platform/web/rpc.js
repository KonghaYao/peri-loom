import {parseLosslessJSON, encodeLosslessJSON} from "/json.js";

export class ConsoleRPCError extends Error {
  constructor(code, requestID = "", outcomeUnknown = false, executionCompleted = false, mayHaveCommitted = false) {
    super(code);
    this.code = code;
    this.requestID = requestID;
    this.outcomeUnknown = outcomeUnknown;
    this.executionCompleted = executionCompleted;
    this.mayHaveCommitted = mayHaveCommitted;
  }
}

const object = value => typeof value === "object" && value !== null && !Array.isArray(value);
const keys = (value, expected) => object(value) && Object.keys(value).sort().join(",") === expected;
const rowsShape = rows => Array.isArray(rows) && rows.every(Array.isArray);
const identityShape = identity => typeof identity === "string" && /^[a-f0-9]{32}$/.test(identity);

function unknown(identifier) {
  return new ConsoleRPCError("outcome_unknown", identifier, true);
}

function envelopeResult(envelope, identifier, responseOK) {
  if (!keys(envelope, "error,ok,request_id,result,version") || envelope.version !== 1 ||
      typeof envelope.request_id !== "string" || (identifier !== null && envelope.request_id !== identifier) ||
      typeof envelope.ok !== "boolean") throw unknown(identifier ?? "");
  if (envelope.ok) {
    if (!responseOK || envelope.error !== null) throw unknown(identifier ?? "");
    return envelope.result;
  }
  const failure = envelope.error;
  if (envelope.result !== null || !object(failure) || typeof failure.code !== "string" ||
      !/^[a-z][a-z0-9_]{0,63}$/.test(failure.code) ||
      Object.keys(failure).some(key => !["code", "execution_completed", "may_have_committed"].includes(key)) ||
      [failure.execution_completed, failure.may_have_committed].some(flag => flag !== undefined && typeof flag !== "boolean")) {
    throw unknown(identifier ?? "");
  }
  if (failure.code === "sql_error" && (failure.execution_completed !== true || failure.may_have_committed !== true)) {
    throw unknown(identifier ?? "");
  }
  throw new ConsoleRPCError(failure.code, identifier ?? "", failure.code === "outcome_unknown",
    failure.execution_completed === true, failure.may_have_committed === true);
}

function validateResult(action, result, identifier, maxRows) {
  if (action === "open" && (!keys(result, "readonly,session") || !identityShape(result.session) || typeof result.readonly !== "boolean")) throw unknown(identifier);
  if (action === "execute" && (!keys(result, "executed") || result.executed !== true)) throw unknown(identifier);
  if (action === "close" && (!keys(result, "closed") || result.closed !== true)) throw unknown(identifier);
  if (action === "query" && (!rowsShape(result) || result.length > maxRows)) throw unknown(identifier);
}

async function abortable(promise, signal) {
  if (signal.aborted) throw new Error("aborted");
  let cancel;
  const interrupted = new Promise((resolve, reject) => {
    cancel = () => reject(new Error("aborted"));
    signal.addEventListener("abort", cancel, {once: true});
  });
  try {
    return await Promise.race([promise, interrupted]);
  } finally {
    signal.removeEventListener("abort", cancel);
  }
}

async function readReply(response, identifier, streaming, limits, signal) {
  const media = response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase();
  const ndjson = media === "application/x-ndjson";
  if (!response.body || (ndjson ? !streaming || !response.ok : media !== "application/json")) throw unknown(identifier ?? "");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", {fatal: true});
  let bytes = 0;
  let frameBytes = 0;
  let pending = "";
  let complete = false;
  let successful = false;
  const rows = [];
  const frame = line => {
    if (complete || !line) throw unknown(identifier);
    const event = parseLosslessJSON(line);
    if (keys(event, "row,type") && event.type === "row" && Array.isArray(event.row)) {
      if (rows.length >= limits.maxRows) throw unknown(identifier);
      rows.push(event.row);
    } else if (keys(event, "request_id,rows,type") && event.type === "complete" &&
        event.request_id === identifier && Number.isSafeInteger(event.rows) && event.rows >= 0 && event.rows === rows.length) {
      complete = true;
    } else throw unknown(identifier);
  };
  try {
    while (true) {
      const chunk = await abortable(reader.read(), signal);
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > limits.maxReplyBytes) throw unknown(identifier ?? "");
      if (ndjson) {
        for (const byte of chunk.value) {
          frameBytes = byte === 10 ? 0 : frameBytes + 1;
          if (frameBytes > limits.maxFrameBytes) throw unknown(identifier);
        }
      }
      pending += decoder.decode(chunk.value, {stream: true});
      if (ndjson) {
        let boundary;
        while ((boundary = pending.indexOf("\n")) >= 0) {
          const line = pending.slice(0, boundary);
          pending = pending.slice(boundary + 1);
          frame(line);
        }
      }
    }
    pending += decoder.decode();
    let result;
    if (ndjson) {
      if (pending) frame(pending);
      if (!complete) throw unknown(identifier);
      result = rows;
    } else result = envelopeResult(parseLosslessJSON(pending), identifier, response.ok);
    successful = true;
    return result;
  } finally {
    if (!successful) {
      try { void reader.cancel().catch(() => {}); } catch {}
    }
    reader.releaseLock();
  }
}

export function createConsoleRPC({fetch, getCredential, requestID, maxRequestBytes = 1 << 20,
  maxReplyBytes = 8 << 20, maxFrameBytes = 8 << 20, maxRows = 10000, timeoutMS = 30000}) {
  const limits = {maxRequestBytes, maxReplyBytes, maxFrameBytes, maxRows, timeoutMS};
  if (Object.values(limits).some(value => !Number.isSafeInteger(value) || value < 1) ||
      maxRequestBytes > 32 << 20 || maxReplyBytes > 32 << 20 || maxFrameBytes > maxReplyBytes || timeoutMS > 300000) {
    throw new ConsoleRPCError("invalid_limits");
  }
  const sessions = new Map();
  let opening = false;
  let uncertainOpen = false;
  return async function call(path, body, streaming = false) {
    const action = /^\/v1\/sql\/(open|execute|query|close)$/.exec(path)?.[1];
    const identifier = body ? requestID() : null;
    if (identifier !== null && (typeof identifier !== "string" || !/^[A-Za-z0-9_-]{1,64}$/.test(identifier))) throw new ConsoleRPCError("invalid_request_id");
    let state;
    if (action) {
      if (!object(body)) throw new ConsoleRPCError("invalid_request", identifier);
      if (action === "open") {
        if (uncertainOpen) throw new ConsoleRPCError("session_unusable", identifier);
        if (opening) throw new ConsoleRPCError("session_busy", identifier);
      } else {
        if (!identityShape(body.session)) throw new ConsoleRPCError("invalid_session", identifier);
        state = sessions.get(body.session);
        if (!state) {
          state = {busy: false, unusable: false};
          sessions.set(body.session, state);
        }
        if (state.unusable) throw new ConsoleRPCError("session_unusable", identifier);
        if (state.busy) throw new ConsoleRPCError("session_busy", identifier);
        if (action !== "close" && (typeof body.sql !== "string" || !body.sql)) throw new ConsoleRPCError("invalid_sql", identifier);
      }
    }
    let encoded;
    try { encoded = body ? encodeLosslessJSON({...body, request_id: identifier}) : undefined; }
    catch { throw new ConsoleRPCError("invalid_request", identifier ?? ""); }
    if (encoded && new TextEncoder().encode(encoded).byteLength > maxRequestBytes) throw new ConsoleRPCError("request_limit", identifier);
    const headers = {Authorization: `Bearer ${getCredential()}`, "Content-Type": "application/json", Accept: streaming ? "application/x-ndjson" : "application/json"};
    if (identifier !== null) headers["X-Request-ID"] = identifier;
    if (state) state.busy = true;
    if (action === "open") opening = true;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMS);
    try {
      const response = await abortable(fetch(path, {method: body ? "POST" : "GET", headers, body: encoded,
        cache: "no-store", redirect: "error", credentials: "omit", signal: controller.signal}), controller.signal);
      const result = await readReply(response, identifier, streaming && action === "query", limits, controller.signal);
      validateResult(action, result, identifier, maxRows);
      if (action === "open") sessions.set(result.session, {busy: false, unusable: false});
      if (action === "close") state.unusable = true;
      return result;
    } catch (failure) {
      const error = failure instanceof ConsoleRPCError ? failure : unknown(identifier ?? "");
      if (error.outcomeUnknown || error.code === "session_moved") {
        if (state) state.unusable = true;
        if (action === "open") uncertainOpen = true;
      }
      throw error;
    } finally {
      clearTimeout(timer);
      if (state) state.busy = false;
      if (action === "open") opening = false;
    }
  };
}
