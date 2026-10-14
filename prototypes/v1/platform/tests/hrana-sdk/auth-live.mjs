import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file) throw new Error('verified TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
async function management(path, input) {
  const requestID = randomBytes(16).toString('hex');
  const response = await fetch(connection.endpoint + path, {
    method: 'POST',
    headers: { Authorization: `Bearer ${connection.project_api_key}`, 'Content-Type': 'application/json', 'X-Request-ID': requestID },
    body: JSON.stringify({ ...input, request_id: requestID }),
  });
  const reply = await response.json();
  assert.equal(response.status, 200);
  assert.equal(reply.request_id, requestID);
  assert.equal(reply.ok, true);
  return reply.result;
}

const created = await management('/v1/keys/create', { project: 'default', subject: `hrana_test_${randomBytes(4).toString('hex')}`, role: 'sql', service_account: false });
assert.equal(created.shown_once, true);
assert.equal(created.role, 'sql');
const client = createClient({ url, authToken: created.credential, intMode: 'bigint' });
try {
  const before = await client.execute('SELECT 42 AS value');
  assert.equal(before.rows[0].value, 42n);
  const revoked = await management('/v1/keys/revoke', { id: created.key });
  assert.equal(revoked.revoked, true);
  await assert.rejects(client.execute('SELECT 43 AS value'));
  console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', bearer_auth: true, revoked_key_denied: true }));
} finally {
  client.close();
}
