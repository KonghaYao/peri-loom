import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openWs, WsClient, WebSocket } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint.replace('https:', 'wss:')}/db/${tenant}/${database}/${connection.database.branch}/`;

for (const version of [2, 3]) {
  const client = openWs(url, connection.project_api_key, version);
  let stream;
  try {
    assert.equal(await client.getVersion(), version);
    stream = client.openStream();
    const table = `hrana_ws_${version}_${randomBytes(4).toString('hex')}`;
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, value TEXT)`);
    await stream.run([`INSERT INTO ${table} VALUES(?, ?)`, [1, "it's native"]]);
    assert.deepEqual((await stream.query([`SELECT value FROM ${table} WHERE id=?`, [1]])).rows.map(row => row.value), ["it's native"]);
    const described = await stream.describe(`SELECT id FROM ${table} WHERE id=:lookup`);
    assert.deepEqual(described.paramNames, [':lookup']);
    assert.equal(described.isReadonly, true);
    const cached = client.storeSql(`SELECT value FROM ${table} WHERE id=?`);
    try {
      assert.equal((await stream.query([cached, [1]])).rows[0].value, "it's native");
    } finally {
      cached.close();
    }
    assert.equal(cached.closed, true);
    assert.throws(() => stream.query([cached, [1]]));
    const renewed = client.storeSql(`SELECT value FROM ${table} WHERE id=?`);
    try {
      assert.equal((await stream.query([renewed, [1]])).rows[0].value, "it's native");
    } finally {
      renewed.close();
    }
    await assert.rejects(stream.run(`INSERT INTO ${table}(id,value) VALUES(1,'duplicate')`), error => typeof error.code === 'string');
    assert.equal((await stream.query(`SELECT COUNT(*) AS total FROM ${table}`)).rows[0].total, 1);
    if (version === 3) {
      assert.equal(await stream.getAutocommit(), true);
      const cursorWrite = stream.batch(true);
      const inserted = cursorWrite.step().run([`INSERT INTO ${table} VALUES(?, ?)`, [2, 'cursor']]);
      await cursorWrite.execute();
      assert.equal((await inserted).affectedRowCount, 1);
      const cursorRead = stream.batch(true);
      const selected = cursorRead.step().query(`SELECT id, value FROM ${table} ORDER BY id`);
      await cursorRead.execute();
      assert.deepEqual((await selected).rows.map(row => row.value), ["it's native", 'cursor']);
    }
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport: 'wss', version, execute: true, native_bind: true, describe: true, cached_sql: true, known_sql_error: true, cursor: version === 3 }));
  } finally {
    stream?.closeGracefully();
    client.close();
  }
}

const legacy = new WsClient(new WebSocket(url, ['hrana1']), connection.project_api_key);
try {
  assert.equal(await legacy.getVersion(), 1);
  const stream = legacy.openStream();
  try {
    assert.equal((await stream.query('SELECT 1 AS value')).rows[0].value, 1);
    const table = `hrana_ws_legacy_${randomBytes(4).toString('hex')}`;
    await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, value TEXT)`);
    await stream.run([`INSERT INTO ${table}(id,value) VALUES(?,?)`, [1, 'legacy']]);
    const batch = stream.batch();
    const inserted = batch.step().run(`INSERT INTO ${table} VALUES(2, 'batch')`);
    await batch.execute();
    assert.equal((await inserted).affectedRowCount, 1);
    await stream.run('BEGIN');
    await stream.run(`INSERT INTO ${table} VALUES(3, 'rollback')`);
    await stream.run('ROLLBACK');
    assert.deepEqual((await stream.query(`SELECT id FROM ${table} ORDER BY id`)).rows.map(row => Number(row.id)), [1, 2]);
  } finally {
    stream.closeGracefully();
  }
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport: 'wss', version: 1, read: true, write: true, bound_args: true, batch: true, rollback: true }));
} finally {
  legacy.close();
}
