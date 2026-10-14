import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openHttp, openWs } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
for (const [transport, version] of [['https', 2], ['wss', 3]]) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}${path}`;
  const client = transport === 'wss' ? openWs(url, connection.project_api_key, version) : openHttp(url, connection.project_api_key, fetch, version);
  const table = `hrana_discard_large_${randomBytes(4).toString('hex')}`;
  let stream;
  try {
    assert.equal(await client.getVersion(), version);
    stream = client.openStream();
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    await stream.run(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`);
    const discarded = await stream.run(`SELECT id FROM ${table} ORDER BY id`);
    assert.equal(discarded.affectedRowCount, 0);
    assert.equal((await stream.query(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 12000);
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport, version, discard_large_read: true, rows_evaluated: 12000 }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}
