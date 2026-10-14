import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@libsql/client';
import { openWs } from '@libsql/hrana-client';

const platform = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const root = resolve(platform, '../../..');
const sleep = milliseconds => new Promise(resolveWait => setTimeout(resolveWait, milliseconds));

async function live() {
  const connection = JSON.parse(await readFile(process.argv[3], 'utf8'));
  const topology = JSON.parse(await readFile(process.argv[4], 'utf8'));
  assert.equal(topology.launcher_pid, Number(process.argv[5]), 'only crash workers owned by this launcher');
  assert.equal(process.ppid, Number(process.argv[6]), 'test must belong to the launcher owner');
  assert.match(connection.endpoint, /^https:\/\/127\.0\.0\.1:\d+$/);
  assert.equal(process.env.NODE_EXTRA_CA_CERTS, connection.ca_file);
  assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED, '0');
  const table = `hrana_crash_${randomBytes(6).toString('hex')}`;
  const call = async (path, body) => {
    const response = await fetch(`${connection.endpoint}${path}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${connection.project_api_key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ request_id: randomBytes(12).toString('hex'), ...body }),
      signal: AbortSignal.timeout(8000),
    });
    const reply = await response.json();
    if (response.status !== 200 || reply.ok !== true) throw new Error(`${path}: HTTP ${response.status} ${reply.error?.code ?? 'invalid_reply'}`);
    return reply.result;
  };
  const database = connection.database;
  const statement = (session, sql) => call('/v1/sql/statement', { session, sql, args: [], named_args: [] });
  const session = (await call('/v1/sql/open', { database })).session;
  await statement(session, `CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  const opened = await call('/v1/sql/cursor/open', {
    session,
    sql: `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`,
    args: [], named_args: [], write_cursor: true,
  });
  assert.deepEqual(opened.cols.map(column => column.name), ['id']);
  const first = await call('/v1/sql/cursor/fetch', { session, max_rows: 1, write_cursor: true });
  assert.equal(first.done, false);
  assert.equal(first.rows.length, 1);
  const placement = await call('/v1/databases/status', { database });
  const worker = placement.placement.desired_worker;
  assert.ok(['worker-a', 'worker-b'].includes(worker));
  const workerPid = topology.processes[worker].pid;
  assert.ok(Number.isSafeInteger(workerPid) && workerPid > 0);
  process.kill(workerPid, 'SIGKILL');
  await sleep(300);

  let failedFetch;
  try {
    await call('/v1/sql/cursor/fetch', { session, max_rows: 1, write_cursor: true });
  } catch (failure) {
    failedFetch = failure;
  }
  assert.ok(failedFetch, 'a crashed worker must not resume an unfinished cursor');

  let count;
  let lastFailure;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const freshSession = (await call('/v1/sql/open', { database })).session;
      const result = await statement(freshSession, `SELECT COUNT(*) FROM ${table}`);
      count = Number(result.rows[0][0]);
      await call('/v1/sql/close', { session: freshSession });
      break;
    } catch (failure) {
      lastFailure = failure;
      await sleep(1000);
    }
  }
  if (count === undefined) {
    throw new Error(`crashed cursor terminated; post-crash read unavailable after two attempts (${lastFailure.message}); no write was retried`);
  }
  assert.equal(count, 0, 'unfinished write committed after worker crash');
  const { tenant, database: name } = database.namespace;
  const path = `/db/${tenant}/${name}/${database.branch}/`;
  const sdk = createClient({ url: `${connection.endpoint}${path}`, authToken: connection.project_api_key });
  try {
    assert.equal((await sdk.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 0);
    await sdk.execute(`INSERT INTO ${table}(id) VALUES(12001)`);
    assert.equal((await sdk.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 1);
  } finally {
    sdk.close();
  }
  const ws = openWs(`${connection.endpoint.replace('https:', 'wss:')}${path}`, connection.project_api_key, 3);
  try {
    assert.equal(await ws.getVersion(), 3);
    const recoveredStream = ws.openStream();
    try {
      assert.equal((await recoveredStream.query(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 1);
    } finally {
      recoveredStream.closeGracefully();
    }
  } finally {
    ws.close();
  }
  console.log(JSON.stringify({ tls: 'verified', worker_crash: 'SIGKILL', active_write_cursor: 'terminated', uncommitted_rows: count, new_sdk_https_write: true, new_sdk_wss_read: true, blind_write_retry: false }));
}

async function main() {
  if (process.argv[2] === '--live') return live();
  const rust = process.env.PERI_RUST_BINARY || resolve(root, 'prototypes/v1/target/debug/peri-loom-v1');
  const platformBinary = process.env.PERI_PLATFORM_BINARY;
  if (!platformBinary) throw new Error('PERI_PLATFORM_BINARY must point to a built Go gateway');
  const directory = await mkdtemp(resolve(tmpdir(), 'peri-hrana-crash-'));
  const listener = createServer();
  await new Promise((accept, reject) => listener.listen(0, '127.0.0.1', error => error ? reject(error) : accept()));
  const port = listener.address().port;
  await new Promise((accept, reject) => listener.close(error => error ? reject(error) : accept()));
  const launcher = spawn('python3', [resolve(platform, 'dev.py'), '--run-dir', directory, '--listen', `127.0.0.1:${port}`, '--tls', '--no-build'], {
    cwd: root,
    env: { ...process.env, PERI_RUST_BINARY: rust, PERI_PLATFORM_BINARY: platformBinary, PERI_HRANA_EXPERIMENTAL: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  launcher.stdout.on('data', chunk => { output += chunk; });
  launcher.stderr.on('data', () => {});
  try {
    const deadline = Date.now() + 45000;
    while (!output.includes('"ready": true') && Date.now() < deadline && launcher.exitCode === null) await sleep(150);
    assert.ok(output.includes('"ready": true'), 'private launcher failed to become ready');
    const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--live', resolve(directory, 'connection.json'), resolve(directory, 'topology.json'), String(launcher.pid), String(process.pid)], {
      cwd: root,
      env: { ...process.env, NODE_EXTRA_CA_CERTS: JSON.parse(await readFile(resolve(directory, 'connection.json'), 'utf8')).ca_file },
      encoding: 'utf8', timeout: 45000,
    });
    if (child.status !== 0) throw new Error(child.stderr.trim() || `live check exited ${child.status}`);
    process.stdout.write(child.stdout);
    const restart = spawnSync('python3', [resolve(platform, 'dev.py'), '--run-dir', directory, '--listen', `127.0.0.1:${port}`, '--tls', '--no-build'], {
      cwd: root,
      env: { ...process.env, PERI_RUST_BINARY: rust, PERI_PLATFORM_BINARY: platformBinary },
      encoding: 'utf8', timeout: 10000,
    });
    assert.notEqual(restart.status, 0, 'unsafe reuse of crashed worker incarnation was accepted');
    assert.match(restart.stderr, /owned worker incarnation|incomplete|cannot be relaunched/);
    console.log(JSON.stringify({ same_fixture_restart: 'refused; fresh worker incarnation required', safety_gate: true }));
  } finally {
    if (launcher.exitCode === null) launcher.kill('SIGTERM');
    await Promise.race([new Promise(accept => launcher.once('exit', accept)), sleep(15000)]);
    if (launcher.exitCode === null) launcher.kill('SIGKILL');
    await rm(directory, { recursive: true, force: true });
  }
}

main().catch(failure => { console.error(failure.message); process.exitCode = 1; });
