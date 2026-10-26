import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, appendFileSync, readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { connect } from '@tursodatabase/serverless';
import { configuration, root, sql, payload, statistics } from './config.mjs';
import { Observer, hash, now, safeError } from './observe.mjs';

const config = configuration();
if (config.help) {
  console.log('node runner.mjs --connection private/connection.json --out NEW_DIR --scenario H1|H3|H4|H6 [--setup] [--samples 2000 --warmup 200] [--arm A|B --source-sha SHA]\nSet NODE_EXTRA_CA_CERTS before Node for fixture TLS. --url + --token-env is also supported; plaintext requires explicit --diagnostic-plaintext on loopback.');
  process.exit(0);
}
mkdirSync(config.out, { recursive: false, mode: 0o700 });
for (const name of ['operations.jsonl', 'requests.jsonl']) {
  writeFileSync(`${config.out}/${name}`, '', { mode: 0o600, flag: 'wx' });
}
const save = (name, value) => writeFileSync(`${config.out}/${name}`, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
const append = (name, value) => appendFileSync(`${config.out}/${name}`, JSON.stringify(value) + '\n', { mode: 0o600 });
const runId = randomUUID();
const operations = [];
const observer = new Observer(config.url, record => append('requests.jsonl', record), globalThis.fetch, [config.authToken]);
observer.install();
let context = { phase: 'setup', sample_index: 0, run_id: runId };
let sequence = 0;
let timedStart, timedEnd, failed = null;
let failureDetails = null;
const revisions = new Map();
const create = () => ({ id: randomUUID(), connection: connect({ url: config.url, authToken: config.authToken }) });
const invoke = (session, action, fn) => observer.call(session.id, action, context, fn);
const query = (session, statement, args = [], write = false) => invoke(session, write ? 'write' : 'read',
  () => session.connection[write ? 'run' : 'get'](statement, ...args));
const close = session => invoke(session, 'close', () => session.connection.close());
let persistent = null;
const startedAt = Date.now();
save('run.json', {
  schema: 2, run_id: runId, scenario: config.scenario, arm: config.arm,
  sdk: config.sdk, protocol: config.protocol, sdk_lock_sha256: config.lock_sha256,
  source_sha: config['source-sha'] || null, node: process.version, platform: process.platform,
  arch: process.arch, url: config.url, fixture_rows: config['fixture-rows'],
  samples_requested: config.samples, warmup_requested: config.warmup, started_at_unix_ms: startedAt,
  diagnostics: 'fetch response observation enabled in both arms',
  evidence_class: 'client-only diagnostic', formal_pass: false,
  note: 'SDK 1.4.1 uses v3; issue 11 SDK version is unknown. This is not a v2 reproduction or recovery proof.',
});
try {
  if (config.setup) {
    const setup = create();
    for (const file of ['00-schema.sql', `0${config['fixture-rows'] === 1000 ? 1 : 2}-fixture-${config['fixture-rows']}.sql`]) {
      for (const statement of readFileSync(root + 'sql/' + file, 'utf8').split(';').map(item => item.trim()).filter(Boolean)) {
        await query(setup, statement, [], true);
      }
    }
    await close(setup);
  }
  if (config.scenario !== 'H6') persistent = create();
  for (const phase of ['warmup', 'timed']) {
    const count = phase === 'warmup' ? config.warmup : config.samples;
    if (phase === 'timed') timedStart = now();
    for (let index = 0; index < count; index++, sequence++) {
      context = { phase, sample_index: index, run_id: runId };
      const op = { schema: 2, op_id: randomUUID(), scenario: config.scenario, ...context,
        issued_ms: now(), outcome: 'success', request_ids: [], retry_count: 0 };
      const session = persistent || create();
      const calls = [];
      try {
        if (config.scenario === 'H1') {
          const selected = await query(session, sql('10-select-one.sql'));
          calls.push(selected.evidence);
          assert.equal(Number(Object.values(selected.result || {})[0]), 1);
        } else if (config.scenario === 'H3') {
          const id = 1 + index % config['fixture-rows'];
          const changed = await query(session, sql('12-update.sql'), [id], true);
          calls.push(changed.evidence);
          assert.equal(Number(changed.result.changes), 1);
          const value = (revisions.get(id) || 0) + 1;
          revisions.set(id, value);
          op.write_effect = { request_id: changed.evidence.request_ids[0],
            kind: 'update', table: 'perf_kv', id, value, payload_sha256: hash(payload(id)) };
        } else {
          const id = 10_000_000 + sequence;
          const text = payload(id, true);
          const inserted = await query(session, sql('13-insert-readback.sql'), [id, id, text], true);
          calls.push(inserted.evidence);
          assert.equal(Number(inserted.result.changes), 1);
          op.write_effect = { request_id: inserted.evidence.request_ids[0],
            kind: 'insert', table: 'perf_events', id, value: id, payload_sha256: hash(text) };
          const selected = await query(session, 'SELECT value, payload FROM perf_events WHERE id = ?', [id]);
          calls.push(selected.evidence);
          assert.equal(Number(selected.result?.value), id);
          assert.equal(selected.result?.payload, text);
        }
        if (config.scenario === 'H6') calls.push((await close(session)).evidence);
      } catch (error) {
        if (error.evidence) calls.push(error.evidence);
        op.outcome = error.evidence ? 'outcome_unknown' : 'error';
        op.error = error.code || error.name || 'VERIFICATION_FAILURE';
        failed = op.error;
        failureDetails = error.details || safeError(error, [config.authToken]);
        op.error_detail = failureDetails;
      }
      op.completed_ms = now();
      op.duration_ms = op.completed_ms - op.issued_ms;
      op.actions = calls;
      op.request_ids = calls.flatMap(call => call.request_ids);
      op.verification = { ok: !failed, recovery: false };
      operations.push(op);
      append('operations.jsonl', op);
      if (failed) break;
    }
    if (phase === 'timed') timedEnd = now();
    if (failed) break;
  }
  // H3 measures a write only. Validate final deltas after the timed window;
  // this is ordinary readback, explicitly not a durable restart oracle.
  if (!failed && config.scenario === 'H3') {
    context = { phase: 'verification', sample_index: 0, run_id: runId };
    for (const [id, expected] of revisions) {
      const selected = await query(persistent, sql('11-point-read.sql'), [id]);
      assert.equal(Number(selected.result?.value), expected);
      assert.equal(selected.result?.payload, payload(id));
    }
  }
  if (!failed && persistent) {
    context = { phase: 'cleanup', sample_index: 0, run_id: runId };
    await close(persistent);
  }
} catch (error) {
  failed = error.code || error.name || 'RUN_FAILURE';
  failureDetails = error.details || safeError(error, [config.authToken]);
} finally {
  observer.restore();
  const timed = operations.filter(item => item.phase === 'timed');
  const success = timed.filter(item => item.outcome === 'success').length;
  const seconds = timedEnd && timedStart ? (timedEnd - timedStart) / 1000 : null;
  const summary = {
    schema: 2, run_id: runId, scenario: config.scenario, arm: config.arm,
    status: failed ? 'RUN_STOPPED' : 'RUN_COMPLETED', formal_pass: false,
    sdk: config.sdk, protocol: config.protocol, error: failed, error_detail: failureDetails,
    ...statistics(timed.map(item => item.duration_ms)), success,
    goodput_per_second: seconds ? success / seconds : null,
    http_requests_timed: observer.records.filter(item => item.phase === 'timed').length,
    close_acknowledgements: observer.records.filter(item => item.close_acknowledged).length,
    completed_at_unix_ms: Date.now(), recovery_proven: false,
  };
  save('summary.json', summary);
  const hashes = {};
  for (const name of ['run.json', 'requests.jsonl', 'operations.jsonl', 'summary.json']) {
    try { hashes[name] = hash(readFileSync(`${config.out}/${name}`)); } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  save('SHA256SUMS.json', hashes);
  console.log(JSON.stringify(summary, null, 2));
  process.exitCode = failed ? 1 : 0;
}
