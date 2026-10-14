import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';
import { openHttp } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
const client = createClient({ url, authToken: connection.project_api_key });
const http = openHttp(url, connection.project_api_key, fetch, 3);
const stream = http.openStream();
const table = `hrana_http_abort_${randomBytes(4).toString('hex')}`;
let cursor;
try {
  await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  assert.equal(await http.getVersion(), 3);
  const sql = `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`;
  cursor = await stream._openCursor({ steps: [{ stmt: { sql, args: [], namedArgs: [], wantRows: true }, condition: undefined }] });
  assert.equal((await cursor.next()).type, 'step_begin');
  assert.equal((await cursor.next()).type, 'row');
  cursor.close();
  assert.equal(cursor.closed, true);
  const rows = await client.execute(`SELECT COUNT(*) AS count FROM ${table}`);
  assert.equal(rows.rows[0].count, 0);
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client@0.7.0', transport: 'https', version: 3, partial_write_cursor_abort: true, committed_rows: 0 }));
} finally {
  cursor?.close();
  stream.closeGracefully();
  http.close();
  client.close();
}
