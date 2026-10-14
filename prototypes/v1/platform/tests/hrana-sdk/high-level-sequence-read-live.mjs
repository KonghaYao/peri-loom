import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connectionFile = process.argv[2];
if (!connectionFile) throw new Error('private TLS fixture connection.json required');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');

const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
const statements = ['SELECT 40 + 2 AS answer', "SELECT 'read-only' AS marker"];

for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const client = createClient({ url, authToken: connection.project_api_key, intMode: 'bigint' });
  try {
    assert.equal(await client.executeMultiple(`${statements.join('; ')};`), undefined);
    const results = await client.batch(statements.map(sql => ({ sql })), 'read');
    assert.equal(results.length, 2);
    assert.deepEqual(results[0].columns, ['answer']);
    assert.equal(results[0].rows[0].answer, 42n);
    assert.deepEqual(results[1].columns, ['marker']);
    assert.equal(results[1].rows[0].marker, 'read-only');
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, read_only_sequence: true, read_batch_results: true }));
  } finally {
    client.close();
  }
}
