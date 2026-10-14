import { encodeLosslessJSON, parseLosslessJSON } from './features/lossless';

export type DatabaseID = { namespace: { tenant: string; database: string }; branch: string };
export type Database = { id: DatabaseID; project: string; state: string; role?: string; root_incarnation?: string; branch_incarnation?: number | bigint };
export type Project = { id: string; organization: string; tenant: string; region: string; quotas?: Record<string, number> };
export type Catalog = { revision: number | bigint; projects: Record<string, Project>; databases: Record<string, Database> };

export class APIError extends Error {
  constructor(public code: string, public requestID: string, public uncertain: boolean, public details: Record<string, unknown> = {}) {
    super(`${code}${requestID ? ` · request ${requestID}` : ''}${uncertain ? ' · outcome unknown; verify before any further action' : ''}${details.execution_completed === true ? ' · execution completed' : ''}${details.may_have_committed === true ? ' · may have committed' : ''}`);
  }
}

const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const unknown = (id: string) => new APIError('outcome_unknown', id, true);
const sessionID = (value: unknown): value is string => typeof value === 'string' && /^[a-f0-9]{32}$/.test(value);
const maxRows = 10000;

export function createAPI(getCredential: () => string) {
  const sessions = new Map<string, { busy: boolean; unusable: boolean }>();
  let opening = false;
  let uncertainOpen = false;
  let uncertainClose = false;
  return async function call<T = unknown>(path: string, body?: Record<string, unknown>, stream = false): Promise<T> {
    const action = /^\/v1\/sql\/(open|query|execute|close|statement|describe|cursor\/open|cursor\/fetch|cursor\/close)$/.exec(path)?.[1];
    const id = body ? crypto.randomUUID().replaceAll('-', '') : '';
    if (!getCredential().trim()) throw new APIError('credential_required', id, false);
    let session: { busy: boolean; unusable: boolean } | undefined;
    if (action === 'open' && (opening || uncertainOpen || uncertainClose)) throw new APIError('session_unusable', id, uncertainOpen || uncertainClose);
    if (action && action !== 'open') {
      if (!sessionID(body?.session)) throw new APIError('invalid_session', id, false);
      session = sessions.get(body.session);
      if (!session || session.unusable || session.busy) throw new APIError('session_unusable', id, !!session?.unusable);
    }
    let encoded: string | undefined;
    try { encoded = body ? encodeLosslessJSON({ ...body, request_id: id }) : undefined; }
    catch { throw new APIError('invalid_request', id, false); }
    if (encoded && new TextEncoder().encode(encoded).byteLength > 1048576) throw new APIError('request_limit', id, false);
    if (session) session.busy = true;
    if (action === 'open') opening = true;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 30000);
    try {
      const response = await fetch(path, { method: body ? 'POST' : 'GET', body: encoded, headers: {
        Authorization: `Bearer ${getCredential()}`, 'Content-Type': 'application/json', Accept: stream ? 'application/x-ndjson' : 'application/json',
        ...(body ? { 'X-Request-ID': id } : {}),
      }, cache: 'no-store', credentials: 'omit', redirect: 'error', signal: controller.signal });
      const media = response.headers.get('content-type')?.split(';')[0].trim().toLowerCase();
      if (!response.body || (media !== 'application/json' && !(media === 'application/x-ndjson' && stream && action === 'query' && response.ok))) throw unknown(id);
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8', { fatal: true });
      let pending = '';
      let bytes = 0;
      let frameBytes = 0;
      let complete = false;
      let consumed = false;
      const rows: unknown[][] = [];
      try {
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) break;
          bytes += chunk.value.byteLength;
          if (bytes > 8 * 1048576) throw unknown(id);
          if (media === 'application/x-ndjson') {
            for (const byte of chunk.value) {
              frameBytes = byte === 10 ? 0 : frameBytes + 1;
              if (frameBytes > 8 * 1048576) throw unknown(id);
            }
          }
          pending += decoder.decode(chunk.value, { stream: true });
          if (media === 'application/x-ndjson') {
            let boundary: number;
            while ((boundary = pending.indexOf('\n')) >= 0) {
              const line = pending.slice(0, boundary);
              pending = pending.slice(boundary + 1);
              if (!line || complete) throw unknown(id);
              const frame = parseLosslessJSON(line);
              if (!record(frame)) throw unknown(id);
              if (frame.type === 'row' && Object.keys(frame).sort().join(',') === 'row,type' && Array.isArray(frame.row) && rows.length < maxRows) rows.push(frame.row);
              else if (frame.type === 'complete' && Object.keys(frame).sort().join(',') === 'request_id,rows,type' && frame.request_id === id && frame.rows === rows.length) complete = true;
              else throw unknown(id);
            }
          }
        }
        pending += decoder.decode();
        if (media === 'application/x-ndjson') {
          if (pending || !complete) throw unknown(id);
          consumed = true;
          return rows as T;
        }
        const envelope = parseLosslessJSON(pending);
        consumed = true;
        if (!record(envelope) || Object.keys(envelope).sort().join(',') !== 'error,ok,request_id,result,version' || envelope.version !== 1 || envelope.request_id !== id || typeof envelope.ok !== 'boolean') throw unknown(id);
        if (!envelope.ok) {
          if (envelope.result !== null || !record(envelope.error) || typeof envelope.error.code !== 'string') throw unknown(id);
          const error = envelope.error as Record<string, unknown> & { code: string };
          if (!/^[a-z][a-z0-9_]{0,63}$/.test(error.code) || error.code === 'sql_error' && (error.execution_completed !== true || error.may_have_committed !== true)) throw unknown(id);
          throw new APIError(error.code, id, error.code === 'outcome_unknown' || error.code === 'session_moved', error);
        }
        if (!response.ok || envelope.error !== null) throw unknown(id);
        const result = envelope.result;
        if (action === 'open') {
          if (!record(result) || !sessionID(result.session) || typeof result.readonly !== 'boolean') throw unknown(id);
          sessions.set(result.session, { busy: false, unusable: false });
        }
        if (action === 'query' && (!Array.isArray(result) || result.length > maxRows || !result.every(Array.isArray))) throw unknown(id);
        if (action === 'execute' && (!record(result) || result.executed !== true)) throw unknown(id);
        if (action === 'close') {
          if (!record(result) || result.closed !== true) throw unknown(id);
          session!.unusable = true;
        }
        return result as T;
      } finally {
        if (!consumed) { try { await reader.cancel(); } catch {} }
        reader.releaseLock();
      }
    } catch (failure) {
      const error = failure instanceof APIError ? failure : unknown(id);
      if (error.uncertain || error.code === 'session_moved') {
        if (session) session.unusable = true;
        if (action === 'open') uncertainOpen = true;
        if (action === 'close') uncertainClose = true;
      }
      throw error;
    } finally {
      clearTimeout(timer);
      if (session) session.busy = false;
      if (action === 'open') opening = false;
    }
  };
}
