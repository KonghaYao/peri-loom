import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { safeError } from './observe.mjs';

// A protocol fixture, not a performance benchmark. The real pinned SDK runs
// against HTTP so close swallowing, baton reuse, and hidden retries are tested.
async function fixture(mode, scenario) {
  const requests = [], rows = new Map();
  let baton = 0;
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks));
    requests.push({ path: request.url, body });
    response.setHeader('content-type', 'application/json');
    if (request.url.endsWith('/pipeline')) {
      response.statusCode = mode === 'close-fails' ? 503 : 200;
      response.end(JSON.stringify(mode === 'close-fails' ? { error: 'close unavailable' }
        : { baton: null, results: [{ type: 'ok', response: { type: 'close' } }] }));
      return;
    }
    if (mode === 'write-unknown' && body.batch.steps[0].stmt.sql.startsWith('INSERT')) {
      response.statusCode = 504;
      response.end(JSON.stringify({ error: 'unknown after execute' }));
      return;
    }
    const lines = [{ baton: body.baton || `baton-${++baton}`, base_url: null }];
    for (const [index, step] of body.batch.steps.entries()) {
      const sql = step.stmt.sql;
      const args = (step.stmt.args || []).map(arg => arg.type === 'integer' ? Number(arg.value) : arg.value);
      let selected = [], columns = [], changed = 0;
      if (sql.startsWith('INSERT')) { rows.set(args[0], { value: args[1], payload: args[2] }); changed = 1; }
      else if (sql.startsWith('UPDATE')) {
        const previous = rows.get(args[0]) || { value: 0, payload: 'r' + String(args[0]).padStart(8, '0') + ':' + '0'.repeat(246) };
        rows.set(args[0], { ...previous, value: previous.value + 1 }); changed = 1;
      } else if (sql.includes('WHERE id = ?')) {
        const row = rows.get(args[0]); columns = ['value', 'payload'];
        selected = row ? [[row.value, row.payload]] : [];
      } else if (sql.startsWith('SELECT 1')) { columns = ['1']; selected = [[1]]; }
      lines.push({ type: 'step_begin', step: index, cols: columns.map(name => ({ name })) });
      for (const row of selected) lines.push({ type: 'row', row: row.map(value => ({
        type: typeof value === 'number' ? 'integer' : 'text', value: String(value),
      })) });
      lines.push({ type: 'step_end', affected_row_count: changed, last_insert_rowid: changed ? '1' : null });
    }
    response.end(lines.map(line => JSON.stringify(line)).join('\n') + '\n');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const directory = mkdtempSync(join(tmpdir(), 'issue11-sdk-test-'));
  const out = join(directory, 'evidence');
  const command = fileURLToPath(new URL('./runner.mjs', import.meta.url));
  let stderr = '';
  try {
    const child = spawn(process.execPath, [command, '--url',
      `http://127.0.0.1:${server.address().port}/db/test/test/main`, '--diagnostic-plaintext',
      '--out', out, '--scenario', scenario, '--samples', '2', '--warmup', '1'],
    { env: { ...process.env, PERI_SQL_TOKEN: 'synthetic-test-only' } });
    child.stdout.resume();
    child.stderr.on('data', data => { stderr += data; });
    const exit = await new Promise(resolve => child.on('close', resolve));
    const summary = JSON.parse(readFileSync(join(out, 'summary.json'), 'utf8'));
    const operations = readFileSync(join(out, 'operations.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
    assert.ok(!stderr.includes('synthetic-test-only'), 'credential leaked');
    return { exit, summary, requests, operations, stderr };
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

for (const scenario of ['H1', 'H3', 'H4', 'H6']) {
  test(`real SDK ${scenario} preserves HTTP shape and confirms close`, async () => {
    const result = await fixture('normal', scenario);
    assert.equal(result.exit, 0, result.stderr);
    assert.equal(result.summary.success, 2);
    assert.equal(result.summary.formal_pass, false);
    const count = scenario === 'H4' ? 4 : scenario === 'H6' ? 6 : 2;
    assert.equal(result.summary.http_requests_timed, count);
    assert.equal(result.summary.close_acknowledgements, scenario === 'H6' ? 3 : 1);
    assert.ok(result.requests.every(item => item.path.includes('/v3/')));
    assert.ok(result.operations.every(item => item.retry_count === 0));
  });
}

test('a swallowed SDK close failure is still a failed churn sample', async () => {
  const result = await fixture('close-fails', 'H6');
  assert.equal(result.exit, 1);
  assert.equal(result.summary.status, 'RUN_STOPPED');
  assert.equal(result.summary.close_acknowledgements, 0);
  assert.equal(result.requests.length, 3);
});

test('unknown write stops the SDK run without readback, retry, or close', async () => {
  const result = await fixture('write-unknown', 'H4');
  assert.equal(result.exit, 1);
  assert.equal(result.requests.length, 1);
  assert.equal(result.operations[0].outcome, 'outcome_unknown');
});

test('diagnostic cause chain retains useful codes without credentials or URLs', () => {
  const cause = new Error('connect ECONNREFUSED https://host/db/test?token=private-value');
  cause.code = 'ECONNREFUSED';
  const error = new TypeError('fetch failed with Bearer private-value', { cause });
  const details = safeError(error, ['private-value']);
  assert.equal(details[0].name, 'TypeError');
  assert.equal(details[1].code, 'ECONNREFUSED');
  assert.ok(!JSON.stringify(details).includes('private-value'));
  assert.ok(!JSON.stringify(details).includes('https://'));
});
