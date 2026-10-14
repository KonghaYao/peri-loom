import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const table = `hrana_abort_write_${randomBytes(4).toString('hex')}`;
const call = async (path, body) => {
  const response = await fetch(`${connection.endpoint}${path}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${connection.project_api_key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const reply = await response.json();
  assert.equal(response.status, 200, `${path}: ${reply.error?.code ?? 'invalid response'}`);
  assert.equal(reply.ok, true);
  return reply.result;
};

const session = (await call('/v1/sql/open', { database: connection.database })).session;
try {
  await call('/v1/sql/statement', { session, sql: `CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`, args: [], named_args: [] });
  const opened = await call('/v1/sql/cursor/open', { session, sql: `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`, args: [], named_args: [], write_cursor: true });
  assert.deepEqual(opened.cols.map(column => column.name), ['id']);
  const page = await call('/v1/sql/cursor/fetch', { session, max_rows: 1, write_cursor: true });
  assert.equal(page.done, false);
  assert.equal(page.rows.length, 1);
  assert.equal(page.affected_row_count, null);
  assert.equal(page.last_insert_rowid, null);
  assert.equal((await call('/v1/sql/cursor/close', { session, write_cursor: true })).closed, true);
  const counted = await call('/v1/sql/statement', { session, sql: `SELECT COUNT(*) AS count FROM ${table}`, args: [], named_args: [] });
  assert.equal(Number(counted.rows[0][0]), 0);
  console.log(JSON.stringify({ transport: 'verified native V1 HTTPS', write_cursor_abort: true, rollback: true }));
} finally {
  await call('/v1/sql/close', { session });
}
