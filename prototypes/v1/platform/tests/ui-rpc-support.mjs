import {readFile} from "node:fs/promises";
import {Script, createContext} from "node:vm";

const app = await readFile(new URL("../web/app.js", import.meta.url), "utf8");
const jsonSource = await readFile(new URL("../web/json.js", import.meta.url), "utf8");
const jsonURL = `data:text/javascript;base64,${Buffer.from(jsonSource).toString("base64")}`;
const contractSource = await readFile(new URL("../web/sqlcontract.js", import.meta.url), "utf8");
const contractURL = `data:text/javascript;base64,${Buffer.from(contractSource).toString("base64")}`;
export const json = await import(jsonURL);
export const contract = await import(contractURL);
let protocol = {};
try {
  const source = await readFile(new URL("../web/rpc.js", import.meta.url), "utf8");
  const rewritten = source.replace('"/json.js"', JSON.stringify(jsonURL)).replace('"/sqlcontract.js"', JSON.stringify(contractURL));
  protocol = await import(`data:text/javascript;base64,${Buffer.from(rewritten).toString("base64")}`);
} catch (failure) {
  if (failure.code !== "ENOENT") throw failure;
}

export const credential = "synthetic-console-credential__________________";
export const sessionID = "a".repeat(32);

export function consoleRPC(fetch) {
  const elements = new Map();
  let generated = 0;
  const context = createContext({
    ...json, ...protocol, fetch, TextDecoder, TextEncoder, AbortController, setTimeout, clearTimeout,
    crypto: {randomUUID() { generated++; return `console-header-${generated}`; }},
    document: {
      createElement() { return {appendChild() {}, textContent: ""}; },
      getElementById(identity) {
        if (!elements.has(identity)) elements.set(identity, {
          value: identity === "credential" ? credential : "", textContent: "", callbacks: {},
          addEventListener(event, callback) { this.callbacks[event] = callback; },
          replaceChildren() {}, appendChild() {},
        });
        return elements.get(identity);
      },
      querySelectorAll() { return []; },
    },
  });
  new Script(app.replace(/^import .*;\n/gm, "") + "\nglobalThis.rpc = call;").runInContext(context);
  return {call: context.rpc, generated: () => generated, elements,
    element: identity => context.document.getElementById(identity),
    click: (identity) => elements.get(identity).callbacks.click()};
}

export function envelope(identifier, result, error = null) {
  return {version: 1, request_id: identifier, ok: error === null, result, error};
}

export function reply(identifier, result, error = null, status = 200) {
  return new Response(json.encodeLosslessJSON(envelope(identifier, result, error)),
    {status, headers: {"Content-Type": "application/json"}});
}

export function wire(body) {
  return json.parseLosslessJSON(body);
}

export function createProtocol(options) {
  return protocol.createConsoleRPC(options);
}

export function transport(responder, limits = {}) {
  const requests = [];
  let generated = 0;
  const call = createProtocol({
    ...limits, getCredential: () => credential, requestID: () => `unit-${++generated}`,
    fetch: async (path, options) => {
      const body = options.body ? wire(options.body) : null;
      if (body && options.headers["X-Request-ID"] !== body.request_id) throw new Error("header mismatch");
      requests.push({path, options, body});
      return responder(path, body, options);
    },
  });
  return {call, requests};
}

export function stream(text, status = 200, chunks = null) {
  const bytes = new TextEncoder().encode(text);
  return new Response(new ReadableStream({start(controller) {
    for (const chunk of chunks ?? [bytes]) controller.enqueue(chunk);
    controller.close();
  }}), {status, headers: {"Content-Type": "application/x-ndjson"}});
}
