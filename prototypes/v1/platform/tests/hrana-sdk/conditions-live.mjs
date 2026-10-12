import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { BatchCond, openHttp } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;

for (const version of [2, 3]) {
  const paths = [];
  const observedFetch = async (request) => {
    paths.push(new URL(request.url).pathname);
    if (version === 3 && new URL(request.url).pathname.endsWith('/v2/pipeline')) throw new Error('v3 fell back to v2');
    return fetch(request);
  };
  const client = openHttp(url, connection.project_api_key, observedFetch, version);
  const table = `hrana_cond_${version}_${randomBytes(4).toString('hex')}`;
  let stream;
  try {
    assert.equal(await client.getVersion(), version);
    stream = client.openStream();
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
    const readDescription = await stream.describe(`SELECT id FROM ${table} WHERE id=:lookup`);
    assert.deepEqual(readDescription.paramNames, [':lookup']);
    assert.deepEqual(readDescription.columns.map(column => column.name), ['id']);
    assert.equal(readDescription.isExplain, false);
    assert.equal(readDescription.isReadonly, true);
    const writeDescription = await stream.describe(`INSERT INTO ${table}(id) VALUES(91)`);
    assert.equal(writeDescription.isReadonly, false);
    assert.equal((await stream.query(`SELECT id FROM ${table}`)).rows.length, 0, 'describe executed a write');
    const batch = stream.batch(version === 3);
    const first = batch.step();
    const firstResult = first.run(`INSERT INTO ${table}(id) VALUES(1)`);
    const second = batch.step();
    second.condition(BatchCond.ok(first)).condition(BatchCond.not(BatchCond.error(first)));
    const secondResult = second.run(`INSERT INTO ${table}(id) VALUES(2)`);
    const third = batch.step();
    third.condition(BatchCond.or(batch, [BatchCond.error(first), BatchCond.ok(second)]));
    const thirdResult = third.run(`INSERT INTO ${table}(id) VALUES(3)`);
    const skipped = batch.step();
    skipped.condition(BatchCond.and(batch, [BatchCond.error(first), BatchCond.ok(second)]));
    const skippedResult = skipped.run(`INSERT INTO ${table}(id) VALUES(4)`);
    await batch.execute();
    assert.equal((await firstResult).affectedRowCount, 1);
    assert.equal((await secondResult).affectedRowCount, 1);
    assert.equal((await thirdResult).affectedRowCount, 1);
    assert.equal(await skippedResult, undefined);
    const rows = await stream.query(`SELECT id FROM ${table} ORDER BY id`);
    assert.deepEqual(rows.rows.map(row => Number(row.id)), [1, 2, 3]);
    if (version === 3) {
      assert.equal(await stream.getAutocommit(), true);
      await stream.run('BEGIN');
      assert.equal(await stream.getAutocommit(), false);
      const conditional = stream.batch(true);
      const skippedOutsideTransaction = conditional.step().condition(BatchCond.isAutocommit(conditional)).run(`INSERT INTO ${table}(id) VALUES(4)`);
      const executedInsideTransaction = conditional.step().condition(BatchCond.not(BatchCond.isAutocommit(conditional))).run(`INSERT INTO ${table}(id) VALUES(5)`);
      await conditional.execute();
      assert.equal(await skippedOutsideTransaction, undefined);
      assert.equal((await executedInsideTransaction).affectedRowCount, 1);
      await stream.run('ROLLBACK');
      assert.equal(await stream.getAutocommit(), true);
      const afterRollback = await stream.query(`SELECT id FROM ${table} ORDER BY id`);
      assert.deepEqual(afterRollback.rows.map(row => Number(row.id)), [1, 2, 3]);
    }
    assert(paths.some(path => path.endsWith(version === 3 ? '/v3-protobuf/cursor' : '/v2/pipeline')));
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', version, native_describe_without_execution: true, and_or_nested: true, skipped_sql_not_dispatched: true, native_autocommit: version === 3 }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}
