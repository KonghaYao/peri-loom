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
  const table = `hrana_cond_large_${randomBytes(4).toString('hex')}`;
  let stream;
  try {
    assert.equal(await client.getVersion(), 3);
    stream = client.openStream();
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    await stream.run(`WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row`);
    const batch = stream.batch(true);
    const countedStep = batch.step();
    const counted = countedStep.query(`SELECT COUNT(*) AS count FROM ${table}`);
    const selected = batch.step().condition(BatchCond.ok(countedStep)).query(`SELECT id FROM ${table} ORDER BY id`);
    const skipped = batch.step().condition(BatchCond.error(countedStep)).query(`SELECT id FROM ${table} ORDER BY id`);
    await batch.execute();
    assert.equal(Number((await counted).rows[0].count), 12000);
    assert.equal(await skipped, undefined);
    const rows = (await selected).rows;
    assert.equal(rows.length, 12000);
    assert.equal(Number(rows[0].id), 1);
    assert.equal(Number(rows.at(-1).id), 12000);
    const recovery = stream.batch(true);
    const invalidStep = recovery.step();
    const invalid = invalidStep.query(`SELECT missing_id FROM ${table}`);
    void invalid.catch(() => {});
    const recovered = recovery.step().condition(BatchCond.error(invalidStep)).query(`SELECT id FROM ${table} ORDER BY id`);
    await recovery.execute();
    await assert.rejects(invalid);
    assert.equal((await recovered).rows.length, 12000);
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport, conditional_large_read_cursor: true, rows: rows.length }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}
