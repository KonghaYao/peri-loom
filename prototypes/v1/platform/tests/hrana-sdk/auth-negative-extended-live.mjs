import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

async function main() {
  if (!process.argv[2]) throw new Error('private TLS fixture connection.json required');
  const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
  if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) ||
      process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file ||
      process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');

  const { tenant, database } = connection.database.namespace;
  const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
  const sql = 'SELECT 42 AS value';

  async function execute(token, transport) {
    const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
    const client = createClient({ url, authToken: token, intMode: 'bigint' });
    try {
      return await client.execute(sql);
    } finally {
      client.close();
    }
  }

  async function denied(token, label, transport) {
    let failure;
    try {
      await execute(token, transport);
    } catch (error) {
      failure = error;
    }
    assert.ok(failure, `${label}: SQL unexpectedly succeeded`);
    assert.match(`${failure.code ?? ''} ${failure.message ?? ''}`, /\b403\b|\b401\b|forbidden|unauthenticated/i, `${label}: expected an authentication/authorization denial`);
  }

  async function management(path, input) {
    const requestID = randomBytes(16).toString('hex');
    const response = await fetch(connection.endpoint + path, {
      method: 'POST',
      headers: { Authorization: `Bearer ${connection.project_api_key}`, 'Content-Type': 'application/json', 'X-Request-ID': requestID },
      body: JSON.stringify({ ...input, request_id: requestID }),
      signal: AbortSignal.timeout(8000),
    });
    const reply = await response.json();
    assert.equal(response.status, 200, `${path}: management request failed`);
    assert.equal(reply.request_id, requestID);
    assert.equal(reply.ok, true);
    return reply.result;
  }

  for (const transport of ['https', 'wss']) {
    assert.equal((await execute(connection.project_api_key, transport)).rows[0].value, 42n);
    await denied(randomBytes(32).toString('hex'), 'invalid project key', transport);
    await denied(connection.global_api_key, 'master key', transport);
  }

  const created = await management('/v1/keys/create', {
    project: 'default', subject: `hrana_negative_${randomBytes(4).toString('hex')}`, role: 'sql', service_account: false,
  });
  assert.equal(created.shown_once, true);
  for (const transport of ['https', 'wss']) {
    assert.equal((await execute(created.credential, transport)).rows[0].value, 42n);
  }
  const revoked = await management('/v1/keys/revoke', { id: created.key });
  assert.equal(revoked.revoked, true);
  for (const transport of ['https', 'wss']) {
    await denied(created.credential, 'revoked project key', transport);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, invalid_project_key_denied: true, master_key_denied: true, revoked_project_key_denied: true }));
  }
}

main().catch(() => { console.error('Hrana SDK negative auth check failed (details withheld to protect credentials)'); process.exitCode = 1; });
