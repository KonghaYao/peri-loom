import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openHttp, openWs } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', transport === 'wss' ? 'wss:' : 'https:')}/db/${tenant}/${database}/${connection.database.branch}/`;
  const observedFetch = async request => {
    const response = await fetch(request);
    if (!response.ok) console.error(JSON.stringify({ path: new URL(request.url).pathname, status: response.status, detail: (await response.clone().text()).slice(0, 160) }));
    return response;
  };
  const client = transport === 'wss' ? openWs(url, connection.project_api_key, 3) : openHttp(url, connection.project_api_key, observedFetch, 3);
  const table = `hrana_large_write_${randomBytes(4).toString('hex')}`;
  let stream;
  try {
    assert.equal(await client.getVersion(), 3);
    stream = client.openStream();
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    const batch = stream.batch(true);
    const returned = batch.step().query(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`);
    await batch.execute();
    const rows = (await returned).rows;
    assert.equal(rows.length, 12000);
    assert.equal(Number(rows[0].id), 1);
    assert.equal(Number(rows.at(-1).id), 12000);
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport, streamed_write_returning: true, rows: rows.length }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}
