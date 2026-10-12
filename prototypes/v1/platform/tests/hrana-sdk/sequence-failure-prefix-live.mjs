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
  const table = `hrana_sequence_error_${randomBytes(4).toString('hex')}`;
  try {
    await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    await assert.rejects(
      client.executeMultiple(`INSERT INTO ${table} VALUES(1); INSERT INTO ${table} VALUES(1); INSERT INTO ${table} VALUES(2);`),
      error => error.code === 'sql_error',
    );
  } finally {
    client.close();
  }
  const reader = createClient({ url, authToken: connection.project_api_key });
  try {
    const rows = (await reader.execute(`SELECT id FROM ${table} ORDER BY id`)).rows;
    assert.deepEqual(rows.map(row => row.id), [1]);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, sequence_failure_stops_suffix: true, committed_prefix: true, post_error_rows: rows.length }));
  } finally {
    reader.close();
  }
}
