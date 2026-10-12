import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file) throw new Error('verified TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
const client = createClient({ url, authToken: connection.project_api_key, intMode: 'bigint' });
try {
  const exact = await client.execute('SELECT 9223372036854775807 AS exact');
  assert.equal(exact.rows[0].exact, 9223372036854775807n);
  const bound = await client.execute({ sql: 'SELECT ? AS value', args: ["it's safe"] });
  assert.equal(bound.rows[0].value, "it's safe");
  const derived = await client.execute('SELECT 42');
  assert.equal(derived.columns[0], '42');
  assert.equal(derived.rows[0][0], 42n);
  await assert.rejects(client.execute('SELECT missing AS result FROM unknown_table'), (error) => error.code === 'sql_error');
  const unauthenticated = createClient({ url, authToken: 'not-a-project-key' });
  try {
    await assert.rejects(unauthenticated.execute('SELECT 1 AS one'));
  } finally {
    unauthenticated.close();
  }
  console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport: 'v2/pipeline JSON fallback', bigint_read: 'PASS', native_positional_binding: 'PASS', native_column_metadata: 'PASS', sql_error: 'PASS', auth_denied: 'PASS' }));
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  client.close();
}
