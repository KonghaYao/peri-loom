import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { createClient } from '@libsql/client';

const [connectionFile] = process.argv.slice(2);
if (!connectionFile) throw new Error('pass private fixture connection.json');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || !process.env.NODE_EXTRA_CA_CERTS || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file) throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
const table = `hrana_sdk_${randomBytes(4).toString('hex')}`;
let stage = 'create client';
const client = createClient({ url, authToken: connection.project_api_key, intMode: 'bigint' });
try {
  stage = 'DDL';
  const created = await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, title TEXT)`);
  assert.equal(created.rowsAffected, 0);
  stage = 'bound insertion';
  const inserted = await client.execute({ sql: `INSERT INTO ${table}(id,title) VALUES(?,?)`, args: [9223372036854775807n, "it's bound"] });
  assert.equal(inserted.rowsAffected, 1);
  assert.equal(inserted.lastInsertRowid, 9223372036854775807n);
  stage = 'read exact bigint';
  const read = await client.execute(`SELECT id AS id, title AS title FROM ${table}`);
  assert.equal(read.rows[0].id, 9223372036854775807n);
  assert.equal(read.rows[0].title, "it's bound");
  stage = 'native column metadata and named binding';
  const named = await client.execute({ sql: `SELECT * FROM ${table} WHERE id=:lookup`, args: { lookup: 9223372036854775807n } });
  assert.deepEqual(named.columns, ['id', 'title']);
  assert.equal(named.rows[0].title, "it's bound");
  const values = await client.execute({ sql: 'SELECT ? AS binary, ? AS optional, ? AS decimal', args: [new Uint8Array([0, 128, 255]), null, 1.5] });
  assert.deepEqual(new Uint8Array(values.rows[0].binary), new Uint8Array([0, 128, 255]));
  assert.equal(values.rows[0].optional, null);
  assert.equal(values.rows[0].decimal, 1.5);
  stage = 'transaction rollback';
  const transaction = await client.transaction('write');
  await transaction.execute({ sql: `INSERT INTO ${table}(id,title) VALUES(?,?)`, args: [2, 'rolled'] });
  await transaction.rollback();
  const rows = await client.execute(`SELECT id AS id FROM ${table}`);
  assert.equal(rows.rows.length, 1);
  stage = 'transaction commit';
  const committed = await client.transaction('write');
  await committed.execute({ sql: `INSERT INTO ${table}(id,title) VALUES(?,?)`, args: [3, 'committed'] });
  await committed.commit();
  const after = await client.execute(`SELECT id AS id FROM ${table}`);
  assert.equal(after.rows.length, 2);
  stage = 'batch write';
  const batched = await client.batch([
    { sql: `INSERT INTO ${table}(id,title) VALUES(?,?)`, args: [4, 'batched-a'] },
    { sql: `INSERT INTO ${table}(id,title) VALUES(?,?)`, args: [5, 'batched-b'] },
  ], 'write');
  assert.equal(batched.length, 2);
  assert.equal(batched[0].rowsAffected, 1);
  assert.equal(batched[1].rowsAffected, 1);
  const all = await client.execute(`SELECT id FROM ${table} ORDER BY id`);
  assert.deepEqual(all.rows.map(row => row.id), [3n, 4n, 5n, 9223372036854775807n]);
  stage = 'multi-statement execution';
  await client.executeMultiple(`INSERT INTO ${table}(id,title) VALUES(6,'sequence-a'); INSERT INTO ${table}(id,title) VALUES(7,'sequence-b');`);
  const sequenced = await client.execute(`SELECT id FROM ${table} WHERE id IN (6,7) ORDER BY id`);
  assert.deepEqual(sequenced.rows.map(row => row.id), [6n, 7n]);
  stage = 'failed batch rollback';
  await assert.rejects(client.batch([
    { sql: `INSERT INTO ${table}(id,title) VALUES(8,'rolled-batch')` },
    { sql: `INSERT INTO ${table}(id,title) VALUES(3,'duplicate')` },
  ], 'write'));
  const rolledBatch = await client.execute(`SELECT id FROM ${table} WHERE id=8`);
  assert.equal(rolledBatch.rows.length, 0);
  stage = 'repeat insert rowid within one session';
  const repeat = await client.transaction('write');
  try {
    const firstInsert = await repeat.execute(`INSERT INTO ${table}(id,title) VALUES(9,'temporary')`);
    assert.equal(firstInsert.lastInsertRowid, 9n);
    await repeat.execute(`DELETE FROM ${table} WHERE id=9`);
    const secondInsert = await repeat.execute(`INSERT INTO ${table}(id,title) VALUES(9,'again')`);
    assert.equal(secondInsert.lastInsertRowid, 9n);
  } finally {
    await repeat.rollback();
  }
  stage = 'SQL error';
  await assert.rejects(client.execute(`INSERT INTO ${table}(id,title) VALUES(3,'duplicate')`), (error) => typeof error.code === 'string' && error.code.length > 0);
  console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport: 'v2/pipeline JSON fallback', read: true, write: true, positional_binding: true, named_binding: true, blob: true, bigint: true, transaction: true, batch: true, sequence: true, failed_batch_rollback: true, error: true }));
} catch (error) {
  console.error(`Hrana SDK FAIL stage=${stage}: ${error?.stack ?? error}`);
  process.exitCode = 1;
} finally {
  client.close();
}
