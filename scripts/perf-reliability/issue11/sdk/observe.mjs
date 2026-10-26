import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';

export const hash = value => createHash('sha256').update(value).digest('hex');
export const now = () => performance.now();

export function safeError(error, secrets = []) {
  const clean = value => {
    let text = String(value || '');
    for (const secret of secrets.filter(Boolean)) text = text.split(secret).join('<REDACTED>');
    return text.replace(/Bearer\s+\S+/gi, 'Bearer <REDACTED>')
      .replace(/https?:\/\/\S+/gi, '<URL>').slice(0, 500);
  };
  const chain = [];
  for (let item = error; item && chain.length < 4; item = item.cause) {
    chain.push({ name: clean(item.name), code: clean(item.code), message: clean(item.message) });
  }
  return chain;
}

// SDK close() swallows transport errors. Observe the actual response and never
// infer a server close acknowledgement from the resolved close() promise.
export class Observer {
  constructor(url, sink, originalFetch = globalThis.fetch, secrets = []) {
    this.url = url.replace(/\/$/, '');
    this.sink = sink;
    this.originalFetch = originalFetch;
    this.scope = null;
    this.batons = new Map();
    this.records = [];
    this.secrets = secrets;
  }

  install() {
    globalThis.fetch = (url, options) => this.fetch(url, options);
  }

  restore() { globalThis.fetch = this.originalFetch; }

  async call(session, action, operation, fn) {
    assert.equal(this.scope, null, 'concurrent SDK operations are unsupported');
    const scope = { session, action, operation, requests: [], pending: [] };
    this.scope = scope;
    const started = now();
    let result, failure;
    try { result = await fn(); } catch (error) { failure = error; }
    await Promise.allSettled(scope.pending);
    const completed = now();
    this.scope = null;
    if (!failure) {
      if (scope.requests.length !== 1) failure = new Error('expected exactly one SDK HTTP request');
      else if (scope.requests[0].outcome !== 'success') failure = new Error('HTTP result unconfirmed');
      else if (action === 'close' && !scope.requests[0].close_acknowledged) failure = new Error('close unconfirmed');
    }
    const evidence = {
      action, request_ids: scope.requests.map(record => record.request_id),
      started_ms: started, completed_ms: completed, duration_ms: completed - started,
      outcome: failure ? 'outcome_unknown' : 'success',
    };
    if (failure) {
      const error = new Error('SDK operation failed: ' + action);
      error.evidence = evidence;
      error.code = failure.code || failure.name || 'SDK_FAILURE';
      error.details = safeError(failure, this.secrets);
      throw error;
    }
    return { result, evidence };
  }

  async fetch(url, options = {}) {
    const scope = this.scope;
    assert.ok(scope, 'unattributed SDK fetch');
    // A retry is blocked before it reaches the network, including SDK-internal
    // fallback queries after a failed cursor. Unknown ends this run.
    assert.equal(scope.requests.length, 0, 'automatic SDK retry/fallback forbidden');
    const target = String(url);
    assert.ok([this.url + '/v3/cursor', this.url + '/v3/pipeline'].includes(target),
      'unexpected SDK protocol or redirected base URL');
    const body = JSON.parse(options.body);
    const steps = body.batch?.steps || body.requests || [];
    const inputBaton = body.baton || null;
    const previous = this.batons.get(scope.session) || null;
    assert.equal(inputBaton, previous, 'SDK session baton was not reused');
    const record = {
      schema: 2, request_id: randomUUID(), ...scope.operation,
      session_id: scope.session, action: scope.action, method: options.method,
      path: new URL(target).pathname, request_body_sha256: hash(options.body),
      started_ms: now(), baton_in_sha256: inputBaton ? hash(inputBaton) : null,
      sql: steps.map(step => step.stmt?.sql || step.sql || null).filter(Boolean),
      step_count: steps.length, outcome: 'outcome_unknown', close_acknowledged: false,
    };
    scope.requests.push(record);
    this.records.push(record);
    let response;
    try {
      const headers = new Headers(options.headers);
      headers.set('X-Request-ID', record.request_id);
      response = await this.originalFetch(target, { ...options, headers, redirect: 'error' });
      record.status = response.status;
    } catch (error) {
      record.completed_ms = now();
      record.error = error.code || error.name || 'FETCH_FAILURE';
      record.error_detail = safeError(error, this.secrets);
      this.sink(record);
      throw error;
    }
    const pending = response.clone().text().then(text => {
      record.completed_ms = now();
      record.response_bytes = Buffer.byteLength(text);
      const lines = text.trim().split('\n').map(line => JSON.parse(line));
      const head = lines[0];
      const closeRequested = body.requests?.some(item => item.type === 'close');
      const successful = response.ok && !head.error
        && !lines.some(line => ['error', 'step_error'].includes(line.type))
        && !(head.results || []).some(item => item.type === 'error');
      record.close_acknowledged = Boolean(closeRequested && successful
        && head.results?.some(item => item.type === 'ok' && item.response?.type === 'close')
        && !head.baton);
      const baton = head.baton || null;
      record.baton_out_sha256 = baton ? hash(baton) : null;
      record.outcome = successful && (closeRequested ? record.close_acknowledged : Boolean(baton))
        ? 'success' : 'outcome_unknown';
      if (successful) this.batons.set(scope.session, baton);
    }).catch(error => {
      record.completed_ms = now();
      record.error = error.code || error.name || 'INVALID_RESPONSE';
      record.error_detail = safeError(error, this.secrets);
    }).finally(() => this.sink(record));
    scope.pending.push(pending);
    return response;
  }
}
