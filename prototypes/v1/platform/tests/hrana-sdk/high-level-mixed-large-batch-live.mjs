import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const client = createClient({ url, authToken: connection.project_api_key });
  const table = `hrana_high_batch_${randomBytes(4).toString('hex')}`;
  try {
    await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    const results = await client.batch([
      `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`,
      `SELECT id FROM ${table} ORDER BY id`,
    ], 'write');
    assert.equal(results[0].rowsAffected, 12000);
    assert.equal(results[1].rows.length, 12000);
    assert.equal(results[1].rows[0].id, 1);
    assert.equal(results[1].rows.at(-1).id, 12000);
    await assert.rejects(client.batch([
      `INSERT INTO ${table} VALUES(12001)`,
      `INSERT INTO ${table} VALUES(1)`,
    ], 'write'), error => error.code === 'sql_error');
    assert.equal((await client.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 12000);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, atomic_large_batch: true, read_rows: results[1].rows.length, failed_batch_rollback: true }));
  } finally {
    client.close();
  }
}
