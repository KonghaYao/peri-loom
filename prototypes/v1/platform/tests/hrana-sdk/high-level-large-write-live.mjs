import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
const failures = [];
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const table = `hrana_high_write_${randomBytes(4).toString('hex')}`;
  const client = createClient({ url, authToken: connection.project_api_key });
  try {
    await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    const result = await client.execute(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`);
    assert.equal(result.rows.length, 12000);
    assert.equal(result.rowsAffected, 12000);
    assert.equal(Number(result.lastInsertRowid), 12000);
    assert.equal(result.rows[0].id, 1);
    assert.equal(result.rows.at(-1).id, 12000);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, execute_large_write_returning: true, rows: result.rows.length }));
  } catch (error) {
    const reader = createClient({ url, authToken: connection.project_api_key });
    let observedRows = 'unavailable';
    try {
      observedRows = (await reader.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count;
    } catch {
      observedRows = 'unknown';
    } finally {
      reader.close();
    }
    failures.push(transport);
    console.error(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, execute_large_write_returning: false, error_code: error.code ?? error.name, post_error_rows: observedRows }));
  } finally {
    client.close();
  }
}
assert.deepEqual(failures, [], 'high-level execute must safely return the full write result');
