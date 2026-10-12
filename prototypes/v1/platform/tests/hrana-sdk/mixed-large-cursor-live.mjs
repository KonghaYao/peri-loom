import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { BatchCond, openHttp, openWs } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
for (const transport of ['https', 'wss']) {
  const url = `${connection.endpoint.replace('https:', transport === 'wss' ? 'wss:' : 'https:')}/db/${tenant}/${database}/${connection.database.branch}/`;
  const client = transport === 'wss' ? openWs(url, connection.project_api_key, 3) : openHttp(url, connection.project_api_key, fetch, 3);
  const table = `hrana_mixed_${randomBytes(4).toString('hex')}`;
  let stream;
  try {
    assert.equal(await client.getVersion(), 3);
    stream = client.openStream();
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    await stream.run(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`);
    const batch = stream.batch(true);
    const insertedStep = batch.step();
    const inserted = insertedStep.run(`INSERT INTO ${table}(id) VALUES(12001)`);
    const selected = batch.step().condition(BatchCond.ok(insertedStep)).query(`SELECT id FROM ${table} ORDER BY id`);
    await batch.execute();
    assert.equal((await inserted).affectedRowCount, 1);
    assert.equal((await inserted).lastInsertRowid, 12001n);
    const rows = (await selected).rows;
    assert.equal(rows.length, 12001);
    assert.equal(Number(rows.at(-1).id), 12001);
    assert.equal(Number((await stream.query(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count), 12001);
    const returningBatch = stream.batch(true);
    const returning = returningBatch.step().query(`INSERT INTO ${table}(id) VALUES(12002) RETURNING id`);
    await returningBatch.execute();
    assert.equal(Number((await returning).rows[0].id), 12002);
    assert.equal((await returning).lastInsertRowid, 12002n);
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport, mixed_write_large_read_cursor: true, rows: rows.length }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}
