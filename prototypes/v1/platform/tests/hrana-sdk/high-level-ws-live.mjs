import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
for (const transport of ['wss', 'libsql']) {
  const url = `${connection.endpoint.replace('https:', `${transport}:`)}/db/${tenant}/${database}/${connection.database.branch}/`;
  const table = `hrana_high_${randomBytes(4).toString('hex')}`;
  const client = createClient({ url, authToken: connection.project_api_key, intMode: 'bigint' });
  try {
    await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, value TEXT)`);
    const inserted = await client.execute({ sql: `INSERT INTO ${table}(id,value) VALUES(?,?)`, args: [1, "it's bound"] });
    assert.equal(inserted.rowsAffected, 1);
    const selected = await client.execute({ sql: `SELECT id,value FROM ${table} WHERE id=:lookup`, args: { lookup: 1 } });
    assert.deepEqual(selected.columns, ['id', 'value']);
    assert.equal(selected.rows[0].id, 1n);
    assert.equal(selected.rows[0].value, "it's bound");
    const transaction = await client.transaction('write');
    await transaction.execute(`INSERT INTO ${table} VALUES(2, 'rollback')`);
    await transaction.rollback();
    assert.equal((await client.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 1n);
    const committed = await client.transaction('write');
    await committed.execute(`INSERT INTO ${table} VALUES(2, 'committed')`);
    await committed.commit();
    const batched = await client.batch([
      { sql: `INSERT INTO ${table} VALUES(3, 'batch')` },
      { sql: `INSERT INTO ${table} VALUES(4, 'batch')` },
    ], 'write');
    assert.equal(batched.length, 2);
    await client.executeMultiple(`INSERT INTO ${table} VALUES(5, 'sequence'); INSERT INTO ${table} VALUES(6, 'sequence');`);
    assert.deepEqual((await client.execute(`SELECT id FROM ${table} ORDER BY id`)).rows.map(row => row.id), [1n, 2n, 3n, 4n, 5n, 6n]);
    await assert.rejects(client.execute(`INSERT INTO ${table} VALUES(1, 'duplicate')`), error => error.code === 'sql_error');
    assert.equal((await client.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 6n);
    console.log(JSON.stringify({ sdk: '@libsql/client@0.15.15', transport, read: true, write: true, named_binding: true, rollback: true, commit: true, batch: true, sequence: true, known_sql_error: true }));
  } finally {
    client.close();
  }
}
