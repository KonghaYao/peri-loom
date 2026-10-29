import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';

export const now = () => performance.now();
export const hash = value => createHash('sha256').update(value).digest('hex');

// Typed, ordered encoding: preserves row order, integer representation and blobs.
// This runs after business timing, never on the response delivery path.
export function canonical(value) {
  if (value === null) return ['null'];
  if (typeof value === 'string' || typeof value === 'boolean') return [typeof value, value];
  if (typeof value === 'number') {
    assert.ok(Number.isFinite(value), 'non-finite result number');
    return ['number', Object.is(value, -0) ? '-0' : String(value)];
  }
  if (typeof value === 'bigint') return ['bigint', value.toString()];
  if (value instanceof ArrayBuffer) return ['bytes', Buffer.from(value).toString('base64')];
  if (ArrayBuffer.isView(value)) return ['bytes', Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('base64')];
  if (Array.isArray(value)) return ['array', value.map(canonical)];
  assert.ok(value && Object.getPrototypeOf(value) === Object.prototype, 'unsupported result value');
  return ['object', Object.keys(value).sort().map(key => [key, canonical(value[key])])];
}
export const resultHash = value => hash(JSON.stringify(canonical(value)));
export function stats(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const q = p => sorted.length ? sorted[Math.ceil(p * sorted.length) - 1] : null;
  return { count: sorted.length, mean_ms: sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : null,
    p50_ms: q(.5), p95_ms: q(.95), p99_ms: q(.99), max_ms: sorted.at(-1) ?? null };
}

export class Observer {
  constructor(url, { fetch = globalThis.fetch, timeoutMs = 30000 } = {}) {
    this.url = url.replace(/\/$/, '');
    this.fetchImpl = fetch;
    this.timeoutMs = timeoutMs;
    this.scope = null;
    this.actions = [];
    this.records = [];
    this.pending = [];
  }
  install() {
    this.originalFetch = globalThis.fetch;
    globalThis.fetch = (url, options) => this.fetch(url, options);
  }
  restore() { globalThis.fetch = this.originalFetch; }
  async call(action, fn) {
    assert.equal(this.scope, null, 'concurrent SDK calls unsupported');
    const scope = { action, started_ms: now(), requests: [], blocked_retries: 0, sdk_error: false };
    this.actions.push(scope);
    this.scope = scope;
    try {
      const result = await fn();
      // SDK cleanup may swallow the blocked fallback's exception. The logical
      // action must still fail immediately before any subsequent SDK action.
      assert.equal(scope.blocked_retries, 0, 'automatic SDK retry/fallback forbidden');
      return result;
    }
    catch (error) { scope.sdk_error = true; throw error; }
    finally {
      scope.sdk_resolved_ms = now();
      scope.sdk_duration_ms = scope.sdk_resolved_ms - scope.started_ms;
      this.scope = null;
    }
  }
  async fetch(url, options = {}) {
    const scope = this.scope;
    assert.ok(scope, 'unattributed SDK fetch');
    if (scope.requests.length) {
      scope.blocked_retries++;
      throw new Error('automatic SDK retry/fallback forbidden');
    }
    const target = String(url);
    assert.ok([this.url + '/v3/cursor', this.url + '/v3/pipeline'].includes(target), 'unexpected endpoint');
    const body = JSON.parse(options.body);
    const record = { request_id: randomUUID(), action: scope.action, path: new URL(target).pathname,
      started_ms: now(), request_body_sha256: hash(options.body),
      baton_in_sha256: body.baton ? hash(body.baton) : null,
      close_requested: Boolean(body.requests?.some(item => item.type === 'close')),
      outcome: 'unknown', close_acknowledged: false };
    this.records.push(record);
    scope.requests.push(record.request_id);
    try {
      const headers = new Headers(options.headers);
      headers.set('X-Request-ID', record.request_id);
      const timeout = AbortSignal.timeout(this.timeoutMs);
      const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
      const response = await this.fetchImpl(target, { ...options, headers, signal, redirect: 'error' });
      record.headers_received_ms = now();
      record.status = response.status;
      const pending = response.clone().text().then(text => {
        record.body_observed_ms = now();
        record.response_bytes = Buffer.byteLength(text);
        // Keep parsing and proof work outside the business clock. Body clone/tee
        // collection itself still overlaps SDK work and is disclosed overhead.
        return { record, text, httpOk: response.ok };
      }).catch(() => { record.outcome = 'unknown'; record.observation_error = true; });
      this.pending.push(pending);
      return response;
    } catch (error) {
      record.fetch_failed_ms = now();
      record.fetch_error = true;
      throw error;
    }
  }
  async settle() {
    const bodies = await Promise.all(this.pending);
    for (const body of bodies) {
      if (!body) continue;
      const { record, text, httpOk } = body;
      try {
        const lines = text.trim().split('\n').map(line => JSON.parse(line));
        const head = lines[0];
        if (!record.close_requested) {
          let activeStep = null, primaryComplete = false;
          for (const line of lines.slice(1)) {
            if (line.type === 'step_begin') activeStep = line.step;
            else if (line.type === 'step_end') {
              if (activeStep === 0) primaryComplete = true;
              activeStep = null;
            }
          }
          record.primary_step_complete = primaryComplete;
        }
        const successful = httpOk && !head.error
          && !lines.some(line => ['error', 'step_error'].includes(line.type))
          && !(head.results || []).some(item => item.type === 'error');
        record.baton_out_sha256 = head.baton ? hash(head.baton) : null;
        record.close_acknowledged = Boolean(record.close_requested && successful && !head.baton
          && head.results?.some(item => item.type === 'ok' && item.response?.type === 'close'));
        record.outcome = successful && (record.close_requested ? record.close_acknowledged
          : Boolean(head.baton) && record.primary_step_complete)
          ? 'success' : 'unknown';
      } catch { record.outcome = 'unknown'; record.observation_error = true; }
    }
    let baton = null;
    const failures = [];
    for (const action of this.actions) {
      if (action.sdk_error) failures.push('sdk_error');
      if (action.blocked_retries) failures.push('blocked_retry');
      if (action.requests.length !== 1) failures.push('request_count');
    }
    for (const record of this.records) {
      if (record.baton_in_sha256 !== baton) failures.push('baton_mismatch');
      if (record.outcome !== 'success') failures.push('http_unconfirmed');
      if (record.action === 'close' && !record.close_acknowledged) failures.push('close_unconfirmed');
      baton = record.baton_out_sha256 ?? null;
    }
    return { ok: failures.length === 0, failures: [...new Set(failures)], evidence_settled_ms: now() };
  }
}
