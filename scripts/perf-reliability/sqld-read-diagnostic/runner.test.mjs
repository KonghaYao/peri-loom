import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { mkdtempSync, readFileSync, writeFileSync, rmSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { Observer, hash, resultHash, stats } from './evidence.mjs';
import { publishFile, publishJSONL } from './storage.mjs';

const runner = fileURLToPath(new URL('./runner.mjs', import.meta.url));
const fixtureGenerator = fileURLToPath(new URL('./fixture.mjs', import.meta.url));
async function childResult(command, args, env = process.env) {
  const child = spawn(process.execPath, [command, ...args], { env });
  let stdout = '', stderr = '';
  child.stdout.on('data', data => { stdout += data; });
  child.stderr.on('data', data => { stderr += data; });
  const exit = await new Promise((resolve, reject) => { child.on('close', resolve); child.on('error', reject); });
  return { exit, stdout, stderr };
}

// Runs the pinned, unmodified SDK against a protocol fixture. These are
// correctness tests of the diagnostic runner, not server performance evidence.
async function fixture(mode = 'normal', engine = 'sqld') {
  const requests = [];
  const rows = mode === 'truncated-empty' ? [] : [{ id: 1, value: 'one' }, { id: 2, value: 'two' }];
  const directory = mkdtempSync(join(tmpdir(), 'sqld-read-test-'));
  const manifestPath = join(directory, 'manifest.json');
  const out = join(directory, 'run');
  writeFileSync(manifestPath, JSON.stringify({ schema: 'sqld-read-diagnostic/1', dataset_origin: 'synthetic-proxy',
    dataset_ref: 'mock-2-rows', dataset_sha256: resultHash(rows), scenarios: [{ name: 'proxy_mock',
      sql: 'SELECT id,value FROM diag_read_r7 ORDER BY id', args: [], expected_rows: rows.length,
      expected_result_sha256: resultHash(rows) }] }));
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks));
    requests.push({ path: request.url, body });
    response.setHeader('content-type', 'application/json');
    if (request.url.endsWith('/pipeline')) {
      response.statusCode = mode === 'close-fails' ? 503 : 200;
      response.end(JSON.stringify(mode === 'close-fails' ? { error: 'close unavailable' }
        : mode === 'close-unconfirmed' ? { baton: null, results: [] }
          : { baton: null, results: [{ type: 'ok', response: { type: 'close' } }] }));
      return;
    }
    if (mode === 'read-unknown') {
      response.statusCode = 504;
      response.end(JSON.stringify({ error: 'unavailable' }));
      return;
    }
    const selected = mode === 'wrong-value' ? [{ id: 1, value: 'WRONG' }, rows[1]]
      : mode === 'wrong-order' ? [...rows].reverse() : mode === 'missing-row' ? rows.slice(0, 1) : rows;
    const lines = [{ baton: 'mock-baton', base_url: null }];
    for (const [index, step] of body.batch.steps.entries()) {
      if (mode === 'truncated-empty') break;
      if (mode === 'probe-error' && index === 1) {
        lines.push({ type: 'step_error', step: 1, error: { message: 'probe failed', code: 'SQLITE_ERROR' } });
        continue;
      }
      const data = step.stmt.sql.startsWith('SELECT id,value') ? selected : [];
      lines.push({ type: 'step_begin', step: index, cols: data.length ? [{ name: 'id' }, { name: 'value' }] : [] });
      for (const row of data) lines.push({ type: 'row', row: [
        { type: 'integer', value: String(row.id) }, { type: 'text', value: row.value },
      ] });
      lines.push({ type: 'step_end', affected_row_count: 0, last_insert_rowid: null });
    }
    response.end(lines.map(line => JSON.stringify(line)).join('\n') + '\n');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}${engine === 'peri' ? '/db/test/test/main' : ''}`;
    const result = await childResult(runner, ['--manifest', manifestPath, '--scenario', 'proxy_mock',
      '--url', endpoint, '--engine', engine, ...(engine === 'sqld' ? ['--no-auth'] : []),
      '--diagnostic-plaintext', '--out', out, '--samples', '2', '--warmup', '1'],
    { ...process.env, PERI_SQL_TOKEN: 'synthetic-test-only' });
    const summary = JSON.parse(readFileSync(join(out, 'summary.json')));
    const operations = readFileSync(join(out, 'operations.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
    const raw = readFileSync(join(out, 'requests.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
    const sums = JSON.parse(readFileSync(join(out, 'SHA256SUMS.json')));
    for (const [name, sha] of Object.entries(sums)) assert.equal(hash(readFileSync(join(out, name))), sha);
    for (const [name, records] of [['operations.jsonl', operations], ['requests.jsonl', raw]]) {
      const bytes = readFileSync(join(out, name));
      assert.equal(summary.published_raw[name].records, records.length);
      assert.equal(summary.published_raw[name].bytes, bytes.length);
      assert.equal(summary.published_raw[name].sha256, sums[name]);
      assert.deepEqual(readFileSync(join(out, name.replace('.jsonl', '.partial.jsonl'))), bytes);
    }
    assert.ok(!readdirSync(out).some(name => name.includes('.tmp-')));
    assert.ok(!JSON.stringify({ ...result, operations, raw }).includes('synthetic-test-only'), 'credential leaked');
    return { ...result, summary, operations, raw, requests };
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

for (const engine of ['sqld', 'peri']) {
  test(`real SDK ${engine} verifies full results, baton, raw counts and close`, async () => {
    const result = await fixture('normal', engine);
    assert.equal(result.exit, 0, result.stderr);
    assert.equal(result.summary.all_timed.count, 2);
    assert.equal(result.summary.successful_timed.count, 2);
    assert.equal(result.summary.timed_requests, 4);
    assert.equal(result.summary.timed_close_acknowledgements, 2);
    assert.equal(result.summary.formal_pass, false);
    assert.equal(result.summary.recovery_proven, false);
    assert.equal(result.requests.length, 6);
    assert.ok(result.operations.every(item => item.retry_count === 0 && item.blocked_retry_count === 0));
    for (const operation of result.operations) {
      assert.ok(operation.issued_ms <= operation.connect_returned_ms);
      assert.equal(operation.sdk_completed_ms, operation.actions.at(-1).sdk_resolved_ms);
      assert.ok(operation.sdk_completed_ms <= operation.evidence_settled_ms);
      assert.ok(operation.evidence_settled_ms <= operation.validation_completed_ms);
      assert.equal(operation.duration_ms, operation.sdk_completed_ms - operation.issued_ms);
      assert.equal(operation.result_rows, 2);
    }
  });
}

for (const mode of ['close-fails', 'close-unconfirmed']) {
  test(`${mode} is not hidden by SDK close swallowing`, async () => {
    const result = await fixture(mode);
    assert.equal(result.exit, 1);
    assert.equal(result.summary.status, 'RUN_STOPPED');
    assert.equal(result.summary.unknown_all_phases, 1);
    assert.equal(result.operations[0].close_acknowledged, false);
    assert.ok(result.operations[0].evidence_failures.includes('close_unconfirmed'));
    assert.equal(result.requests.length, 2);
  });
}

for (const mode of ['wrong-value', 'wrong-order', 'missing-row']) {
  test(`${mode} fails whole-result verification even after a close ACK`, async () => {
    const result = await fixture(mode);
    assert.equal(result.exit, 1);
    assert.equal(result.operations[0].outcome, 'verification_failed');
    assert.equal(result.operations[0].close_acknowledged, true);
    assert.equal(result.requests.length, 2);
  });
}

test('HTTP unknown stops without readback, retry or close', async () => {
  const result = await fixture('read-unknown');
  assert.equal(result.exit, 1);
  assert.equal(result.requests.length, 1);
  assert.equal(result.operations[0].outcome, 'unknown');
});

test('empty-result digest cannot hide a truncated cursor without a completed SQL step', async () => {
  const result = await fixture('truncated-empty');
  assert.equal(result.exit, 1);
  assert.equal(result.operations[0].outcome, 'unknown');
  assert.equal(result.raw[0].primary_step_complete, false);
  assert.equal(result.summary.successful_timed.count, 0);
});

test('SDK autocommit fallback is blocked before a second network request', async () => {
  const result = await fixture('probe-error');
  assert.equal(result.exit, 1);
  assert.equal(result.requests.length, 1);
  assert.equal(result.summary.blocked_retries_all_phases, 1);
  assert.ok(result.operations[0].evidence_failures.includes('blocked_retry'));
});

test('SDK action clock ends before body collection settles; parsing is deferred', async () => {
  let finishBody;
  const observer = new Observer('https://test.invalid', { fetch: async () => ({ status: 200, ok: true,
    clone: () => ({ text: () => new Promise(resolve => { finishBody = resolve; }) }),
  }) });
  await observer.call('read', () => observer.fetch('https://test.invalid/v3/cursor', { body: '{"baton":null}' }));
  const completed = observer.actions[0].sdk_resolved_ms;
  assert.equal(observer.records[0].body_observed_ms, undefined);
  finishBody('{ malformed JSON');
  await Promise.resolve();
  assert.equal(observer.records[0].observation_error, undefined, 'parser ran before settle');
  const settled = await observer.settle();
  assert.equal(settled.ok, false);
  assert.equal(observer.records[0].observation_error, true);
  assert.ok(observer.records[0].body_observed_ms >= completed);
  assert.ok(settled.evidence_settled_ms >= observer.records[0].body_observed_ms);
});

test('typed whole-result digests retain order, numbers, nulls and blobs', () => {
  assert.notEqual(resultHash([{ n: 1 }]), resultHash([{ n: '1' }]));
  assert.notEqual(resultHash([{ n: 1 }]), resultHash([{ n: 1n }]));
  assert.notEqual(resultHash([{ n: null }]), resultHash([{ n: '' }]));
  assert.notEqual(resultHash([1, 2]), resultHash([2, 1]));
  assert.equal(resultHash({ a: 1, b: 2 }), resultHash({ b: 2, a: 1 }));
  assert.equal(resultHash(Buffer.from([1, 2])), resultHash(Uint8Array.from([1, 2])));
  assert.equal(stats(Array.from({ length: 200 }, (_, index) => index + 1)).p99_ms, 198);
});

test('fixture generator has deterministic hashes and refuses overwrites', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'sqld-fixture-test-'));
  try {
    const results = [];
    for (const name of ['one', 'two']) {
      const out = join(directory, name);
      const result = await childResult(fixtureGenerator, ['--out', out, '--rows', '20', '--content-bytes', '32']);
      assert.equal(result.exit, 0, result.stderr);
      const manifest = JSON.parse(readFileSync(join(out, 'manifest.json')));
      assert.equal(manifest.dataset_origin, 'synthetic-proxy');
      assert.equal(manifest.setup_sha256, hash(readFileSync(join(out, 'setup.jsonl'))));
      assert.deepEqual(manifest.scenarios.map(item => item.expected_rows), [0, 1, 20, 20, 20]);
      results.push(manifest);
    }
    assert.deepEqual(results[0], results[1]);
    const duplicate = await childResult(fixtureGenerator, ['--out', join(directory, 'one'), '--rows', '20']);
    assert.notEqual(duplicate.exit, 0);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('final publication preserves every original observation independently of a truncated partial file', () => {
  const directory = mkdtempSync(join(tmpdir(), 'sqld-publish-test-'));
  try {
    const observations = [{ request_id: 'first', outcome: 'success' }, { request_id: 'last', outcome: 'unknown' }];
    const original = JSON.stringify(observations);
    const partial = JSON.stringify(observations[0]) + '\n';
    writeFileSync(join(directory, 'requests.partial.jsonl'), partial);
    const ledger = publishJSONL(directory, 'requests.jsonl', observations);
    const bytes = readFileSync(join(directory, 'requests.jsonl'));
    assert.deepEqual(bytes.toString().trim().split('\n').map(JSON.parse), observations);
    assert.equal(JSON.stringify(observations), original, 'original observations were mutated');
    assert.equal(ledger.records, 2);
    assert.equal(ledger.bytes, bytes.length);
    assert.equal(ledger.sha256, hash(bytes));
    assert.equal(readFileSync(join(directory, 'requests.partial.jsonl'), 'utf8'), partial);
    assert.deepEqual(readdirSync(directory).sort(), ['requests.jsonl', 'requests.partial.jsonl']);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('final raw filenames cannot be published twice or replace an append version', () => {
  const directory = mkdtempSync(join(tmpdir(), 'sqld-immutable-test-'));
  try {
    const original = [{ request_id: 'original' }];
    publishJSONL(directory, 'requests.jsonl', original);
    assert.throws(() => publishJSONL(directory, 'requests.jsonl', [{ request_id: 'replacement' }]), /refusing to overwrite/);
    assert.equal(readFileSync(join(directory, 'requests.jsonl'), 'utf8'), JSON.stringify(original[0]) + '\n');
    assert.deepEqual(readdirSync(directory), ['requests.jsonl']);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('atomic metadata publication refuses overwriting an existing evidence file', () => {
  const directory = mkdtempSync(join(tmpdir(), 'sqld-metadata-test-'));
  try {
    publishFile(directory, 'run.json', '{"original":true}\n');
    assert.throws(() => publishFile(directory, 'run.json', '{"original":false}\n'), /refusing to overwrite/);
    assert.equal(readFileSync(join(directory, 'run.json'), 'utf8'), '{"original":true}\n');
    assert.deepEqual(readdirSync(directory), ['run.json']);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
