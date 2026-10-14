import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connectionFile = process.argv[2];
if (!connectionFile) throw new Error('private TLS fixture connection.json required');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');

const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const client = createClient({ url, authToken: connection.project_api_key, intMode: 'bigint' });
  try {
    const result = await client.execute('SELECT type AS schema_type, name AS schema_name FROM sqlite_schema WHERE 0');
    assert.deepEqual(result.rows, []);
    assert.deepEqual(result.columns, ['schema_type', 'schema_name']);
    assert.deepEqual(result.columnTypes, ['TEXT', 'TEXT']);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, empty_result_metadata: true }));
  } finally {
    client.close();
  }
}
