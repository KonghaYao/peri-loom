import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
const table = `hrana_high_large_${randomBytes(4).toString('hex')}`;
const http = createClient({ url: `${connection.endpoint}${path}`, authToken: connection.project_api_key });
try {
  await http.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  await http.execute(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`);
  assert.equal((await http.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 12000);
} finally {
  http.close();
}

let failed = false;
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const client = createClient({ url, authToken: connection.project_api_key });
  try {
    const result = await client.execute(`SELECT id FROM ${table} ORDER BY id`);
    assert.equal(result.rows.length, 12000);
    assert.equal(result.rows[0].id, 1);
    assert.equal(result.rows.at(-1).id, 12000);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, execute_large_select: true, rows: result.rows.length }));
  } catch (error) {
    failed = true;
    console.error(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, execute_large_select: false, error_code: error.code ?? error.name }));
  } finally {
    client.close();
  }
}
assert.equal(failed, false, 'high-level execute must return all 12,000 rows on both transports');
