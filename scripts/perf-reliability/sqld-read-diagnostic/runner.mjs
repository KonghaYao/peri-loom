import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, appendFileSync, readFileSync, existsSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '@tursodatabase/serverless';
import { Observer, resultHash, hash, now, stats } from './evidence.mjs';
import { publishFile, publishJSONL, storagePolicy } from './storage.mjs';

const { values: options } = parseArgs({ options: {
  manifest: { type: 'string' }, scenario: { type: 'string' }, out: { type: 'string' },
  connection: { type: 'string' }, url: { type: 'string' }, engine: { type: 'string', default: 'peri' },
  'token-env': { type: 'string', default: 'PERI_SQL_TOKEN' }, 'no-auth': { type: 'boolean', default: false },
  'diagnostic-plaintext': { type: 'boolean', default: false }, arm: { type: 'string', default: 'A' },
  'source-sha': { type: 'string' }, 'server-identity': { type: 'string' },
  samples: { type: 'string', default: '2000' }, warmup: { type: 'string', default: '50' },
  'pace-ms': { type: 'string', default: '0' }, 'timeout-ms': { type: 'string', default: '30000' },
  'setup-file': { type: 'string' }, 'allow-synthetic-fixture-writes': { type: 'boolean', default: false },
  help: { type: 'boolean', default: false },
} });
if (options.help) {
  console.log('runner.mjs --manifest FILE --scenario NAME --out NEW_DIR --engine peri|sqld --url URL [--token-env NAME | --no-auth] [--samples 2000 --warmup 50 --pace-ms 0]\n--connection FILE supports peri launcher fixtures. Loopback HTTP requires --diagnostic-plaintext. Optional isolated fixture setup: --setup-file setup.jsonl --allow-synthetic-fixture-writes (no scenario). This tool never declares formal acceptance.');
  process.exit(0);
}
assert.ok(options.manifest && options.out, 'manifest and output are required');
assert.ok(!existsSync(options.out), 'refusing to overwrite an existing run');
assert.ok(['peri', 'sqld'].includes(options.engine), 'unknown engine');
const connection = options.connection ? JSON.parse(readFileSync(options.connection, 'utf8')) : null;
let target = options.url;
if (!target && connection) target = `${connection.endpoint}/db/${connection.database.namespace.tenant}/${connection.database.namespace.database}/${connection.database.branch}`;
assert.ok(target, 'target URL is required');
target = target.replace(/\/$/, '');
const url = new URL(target);
assert.ok(!url.username && !url.password && !url.search && !url.hash, 'unsafe URL');
assert.ok(url.protocol === 'https:' || (options['diagnostic-plaintext'] && url.protocol === 'http:'
  && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)), 'verified TLS required');
assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED, '0', 'TLS verification cannot be disabled');
if (connection?.ca_file && url.protocol === 'https:') assert.equal(process.env.NODE_EXTRA_CA_CERTS, connection.ca_file);
if (options.engine === 'peri') assert.ok(/^\/db\/[^/]+\/[^/]+\/[^/]+$/.test(url.pathname), 'expected peri database path');
if (options['no-auth']) assert.equal(options.engine, 'sqld', 'only the explicitly unauthenticated sqld track permits no auth');
const token = options['no-auth'] ? undefined : connection?.project_api_key || process.env[options['token-env']];
assert.ok(options['no-auth'] || (typeof token === 'string' && token && !/[\r\n]/.test(token)), 'missing credential');
for (const key of ['samples', 'warmup', 'pace-ms', 'timeout-ms']) {
  options[key] = Number(options[key]);
  assert.ok(Number.isSafeInteger(options[key]) && options[key] >= (['samples', 'timeout-ms'].includes(key) ? 1 : 0), 'invalid ' + key);
}
const manifestBytes = readFileSync(options.manifest);
const manifest = JSON.parse(manifestBytes);
assert.equal(manifest.schema, 'sqld-read-diagnostic/1');
assert.ok(['synthetic-proxy', 'user-fixture'].includes(manifest.dataset_origin));
assert.match(manifest.dataset_sha256, /^[0-9a-f]{64}$/);
assert.ok(Array.isArray(manifest.scenarios) && manifest.scenarios.length);
assert.equal(new Set(manifest.scenarios.map(item => item.name)).size, manifest.scenarios.length, 'duplicate scenario names');
const setupMode = Boolean(options['setup-file']);
const scenario = manifest.scenarios.find(item => item.name === options.scenario);
let commands;
if (setupMode) {
  assert.ok(options['allow-synthetic-fixture-writes'] && manifest.dataset_origin === 'synthetic-proxy');
  assert.ok(!options.scenario, 'setup and measurement are separate runs');
  const bytes = readFileSync(options['setup-file']);
  assert.equal(hash(bytes), manifest.setup_sha256, 'setup digest mismatch');
  commands = bytes.toString().trim().split('\n').map(JSON.parse);
  assert.ok(commands.length && commands.every(item => /^(CREATE TABLE diag_read_r7 \(|INSERT INTO diag_read_r7 \(id,value,content\) VALUES )/.test(item.sql)
    && Array.isArray(item.args)), 'only isolated generated fixture writes are supported');
} else {
  assert.ok(scenario && typeof scenario.sql === 'string' && Array.isArray(scenario.args), 'unknown or malformed scenario');
  assert.ok(Number.isSafeInteger(scenario.expected_rows) && scenario.expected_rows >= 0);
  assert.match(scenario.expected_result_sha256, /^[0-9a-f]{64}$/);
  // The SQL text is user-authored configuration, not a security classifier.
  // A read-only database credential is required for untrusted user manifests.
}
mkdirSync(options.out, { recursive: false, mode: 0o700 });
const save = (name, data) => publishFile(options.out, name, JSON.stringify(data, null, 2) + '\n');
const append = (name, data) => appendFileSync(`${options.out}/${name}`, JSON.stringify(data) + '\n', { mode: 0o600 });
for (const name of ['operations.partial.jsonl', 'requests.partial.jsonl']) writeFileSync(`${options.out}/${name}`, '', { flag: 'wx', mode: 0o600 });
const runId = randomUUID(), startedAt = Date.now();
save('run.json', { schema: 'sqld-read-diagnostic/1', run_id: runId, engine: options.engine, arm: options.arm,
  source_sha: options['source-sha'] ?? null,
  server_identity: options['server-identity'] ? JSON.parse(readFileSync(options['server-identity'])) : null,
  sdk: '@tursodatabase/serverless@1.4.1', sdk_lock_sha256: hash(readFileSync(new URL('./package-lock.json', import.meta.url))),
  node: process.version, arch: process.arch, platform: process.platform, target,
  manifest_sha256: hash(manifestBytes), dataset_origin: manifest.dataset_origin,
  dataset_ref: manifest.dataset_ref, dataset_sha256: manifest.dataset_sha256,
  scenario: setupMode ? 'isolated_fixture_setup' : scenario.name,
  sql_sha256: setupMode ? null : hash(scenario.sql), expected_rows: scenario?.expected_rows,
  samples_requested: setupMode ? 0 : options.samples, warmup_requested: setupMode ? 0 : options.warmup,
  pace_ms: options['pace-ms'], timeout_ms: options['timeout-ms'], auth: token ? 'token' : 'explicit-none',
  tls_verified: url.protocol === 'https:', started_at_unix_ms: startedAt,
  evidence_class: 'SDK client diagnostic; not formal acceptance', formal_pass: false, recovery_proven: false,
  storage_policy: storagePolicy,
  clock: 'issued before logical connect to last necessary SDK action resolution; observer settlement, digest and file writes excluded',
  observer: 'response clone/tee and body collection overlap SDK work; parsing, proof and digest follow the clock; common diagnostic overhead in both arms',
});
const operations = [];
const requestObservations = [];
let stopped = false;
const phases = setupMode ? [['setup', commands.length]] : [['warmup', options.warmup], ['timed', options.samples]];
for (const [phase, count] of phases) {
  for (let index = 0; index < count; index++) {
    const op = { run_id: runId, operation_id: randomUUID(), phase, sample_index: index,
      scenario: setupMode ? 'isolated_fixture_setup' : scenario.name, retry_count: 0, outcome: 'unknown' };
    const observer = new Observer(target, { timeoutMs: options['timeout-ms'] });
    observer.install();
    let result, sdkFailed = false;
    op.issued_ms = now();
    try {
      const client = connect({ url: target, ...(token ? { authToken: token } : {}) });
      op.connect_returned_ms = now();
      const command = setupMode ? commands[index] : scenario;
      result = await observer.call(setupMode ? 'setup_write' : 'read', () => client[setupMode ? 'run' : 'all'](command.sql, ...command.args));
      await observer.call('close', () => client.close());
    } catch { sdkFailed = true; }
    op.sdk_completed_ms = observer.actions.at(-1)?.sdk_resolved_ms ?? now();
    op.duration_ms = op.sdk_completed_ms - op.issued_ms;
    const settled = await observer.settle();
    observer.restore();
    op.evidence_settled_ms = settled.evidence_settled_ms;
    op.observer_wait_ms = settled.evidence_settled_ms - op.sdk_completed_ms;
    op.actions = observer.actions;
    op.request_ids = observer.records.map(record => record.request_id);
    op.blocked_retry_count = observer.actions.reduce((sum, action) => sum + action.blocked_retries, 0);
    op.evidence_failures = settled.failures;
    const closeConfirmed = observer.records.some(record => record.close_acknowledged);
    op.close_acknowledged = closeConfirmed;
    if (!sdkFailed && settled.ok && closeConfirmed) {
      try {
        if (!setupMode) {
          assert.ok(Array.isArray(result));
          op.result_rows = result.length;
          op.result_sha256 = resultHash(result);
          assert.equal(op.result_rows, scenario.expected_rows);
          assert.equal(op.result_sha256, scenario.expected_result_sha256);
        }
        op.outcome = 'success';
      } catch { op.outcome = 'verification_failed'; }
    }
    op.validation_completed_ms = now();
    for (const record of observer.records) {
      const observation = { ...record, run_id: runId, operation_id: op.operation_id, phase, sample_index: index };
      requestObservations.push(observation);
      append('requests.partial.jsonl', observation);
    }
    append('operations.partial.jsonl', op);
    operations.push(op);
    if (op.outcome !== 'success') { stopped = true; break; }
    const remaining = options['pace-ms'] - (now() - op.issued_ms);
    if (remaining > 0) await delay(remaining);
  }
  if (stopped) break;
}
const timed = operations.filter(item => item.phase === 'timed');
const successful = timed.filter(item => item.outcome === 'success');
// Publish the full original observation ledger after all business clocks end.
// Separate partial append files remain useful if interrupted. Final raw filenames
// have never existed before this publication and must never be replaced.
assert.deepEqual(operations.flatMap(item => item.request_ids), requestObservations.map(item => item.request_id));
const publishedRaw = {
  'operations.jsonl': publishJSONL(options.out, 'operations.jsonl', operations),
  'requests.jsonl': publishJSONL(options.out, 'requests.jsonl', requestObservations),
};
const summary = { schema: 'sqld-read-diagnostic/1', run_id: runId,
  status: stopped ? 'RUN_STOPPED' : 'RUN_COMPLETED', formal_pass: false, recovery_proven: false,
  dataset_origin: manifest.dataset_origin, scenario: setupMode ? 'isolated_fixture_setup' : scenario.name,
  storage_policy: storagePolicy, published_raw: publishedRaw,
  all_timed: stats(timed.map(item => item.duration_ms)), successful_timed: stats(successful.map(item => item.duration_ms)),
  failures_all_phases: operations.filter(item => item.outcome !== 'success').length,
  unknown_all_phases: operations.filter(item => item.outcome === 'unknown').length,
  blocked_retries_all_phases: operations.reduce((sum, item) => sum + item.blocked_retry_count, 0),
  all_operations: operations.length, timed_requests: timed.reduce((sum, item) => sum + item.request_ids.length, 0),
  timed_close_acknowledgements: timed.filter(item => item.close_acknowledged).length, completed_at_unix_ms: Date.now() };
save('summary.json', summary);
const publishedSums = Object.fromEntries(['run.json', 'operations.jsonl', 'requests.jsonl', 'summary.json']
  .map(name => [name, hash(readFileSync(`${options.out}/${name}`))]));
for (const [name, ledger] of Object.entries(publishedRaw)) assert.equal(publishedSums[name], ledger.sha256, 'raw ledger changed before manifest publication');
save('SHA256SUMS.json', publishedSums);
console.log(JSON.stringify(summary));
process.exitCode = stopped ? 1 : 0;
