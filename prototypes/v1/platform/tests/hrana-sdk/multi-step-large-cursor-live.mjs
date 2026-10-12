import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openWs } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint.replace('https:', 'wss:')}/db/${tenant}/${database}/${connection.database.branch}/`;
const table = `hrana_multi_${randomBytes(4).toString('hex')}`;
const client = openWs(url, connection.project_api_key, 3);
let stream;
try {
  assert.equal(await client.getVersion(), 3);
  stream = client.openStream();
  await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  await stream.run(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`);
  const batch = stream.batch(true);
  const counted = batch.step().query(`SELECT COUNT(*) AS count FROM ${table}`);
  const selected = batch.step().query(`SELECT id FROM ${table} ORDER BY id`);
  await batch.execute();
  assert.equal(Number((await counted).rows[0].count), 12000);
  const rows = (await selected).rows;
  assert.equal(rows.length, 12000);
  assert.equal(Number(rows[0].id), 1);
  assert.equal(Number(rows.at(-1).id), 12000);
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport: 'wss', multi_step_streaming: true, rows: rows.length }));
} finally {
  stream?.closeGracefully();
  client.close();
}
