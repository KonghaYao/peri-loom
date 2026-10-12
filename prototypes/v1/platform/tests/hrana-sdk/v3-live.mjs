import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openHttp, Stmt } from '@libsql/hrana-client';

const [connectionFile] = process.argv.slice(2);
if (!connectionFile) throw new Error('private TLS fixture connection.json required');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
const table = `hrana_v3_${randomBytes(4).toString('hex')}`;
const paths = [];
const observedFetch = async (request) => {
  const path = new URL(request.url).pathname;
  paths.push(path);
  if (path.endsWith('/v2/pipeline')) throw new Error('SDK fell back to v2');
  return fetch(request);
};
let stage = 'v3 negotiation';
const hrana = openHttp(url, connection.project_api_key, observedFetch, 3);
hrana.intMode = 'bigint';
let stream;
try {
  assert.equal(await hrana.getVersion(), 3, 'real v3-protobuf handshake required');
  stream = hrana.openStream();
  stage = 'write and typed bigint binding';
  await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, title TEXT, payload BLOB)`);
  const inserted = await stream.run(new Stmt(`INSERT INTO ${table}(id,title,payload) VALUES(?,?,?)`).bindIndexes([9223372036854775807n, 'v3', new Uint8Array([0, 128, 255])]));
  assert.equal(inserted.affectedRowCount, 1);
  assert.equal(inserted.lastInsertRowid, 9223372036854775807n);
  stage = 'named binding, native columns, blob';
  const read = await stream.query(new Stmt(`SELECT * FROM ${table} WHERE id=:lookup`).bindName('lookup', 9223372036854775807n));
  assert.deepEqual(read.columnNames, ['id', 'title', 'payload']);
  assert.equal(read.rows[0].id, 9223372036854775807n);
  assert.deepEqual(new Uint8Array(read.rows[0].payload), new Uint8Array([0, 128, 255]));
  stage = 'real Hrana v3 protobuf cursor';
  const cursorWrite = stream.batch(true);
  const cursorInserted = cursorWrite.step().run(new Stmt(`INSERT INTO ${table}(id,title) VALUES(?,?)`).bindIndexes([7n, 'cursor']));
  await cursorWrite.execute();
  assert.equal((await cursorInserted).lastInsertRowid, 7n);
  const batch = stream.batch(true);
  const result = batch.step().query(new Stmt(`SELECT id AS id, title AS title FROM ${table} WHERE id=?`).bindIndexes([9223372036854775807n]));
  await batch.execute();
  const selected = await result;
  assert.equal(selected.rows[0].id, 9223372036854775807n);
  assert.equal(selected.rows[0].title, 'v3');
  stage = 'known SQL error';
  await assert.rejects(stream.run(`INSERT INTO ${table}(id,title) VALUES(9223372036854775807,'duplicate')`), (error) => typeof error.code === 'string' && error.code.length > 0);
  assert(paths.some((path) => path.endsWith('/v3-protobuf')));
  assert(paths.some((path) => path.endsWith('/v3-protobuf/pipeline')));
  assert(paths.some((path) => path.endsWith('/v3-protobuf/cursor')));
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client@0.7.0 (forced v3; @libsql/client@0.15.15 defaults to v2)', protocol: 'v3-protobuf', negotiation: true, pipeline: true, cursor: true, bigint: true, typed_args: true, named_args: true, blob: true, error: true }));
} catch (error) {
  console.error(`Hrana v3 SDK FAIL stage=${stage}: ${error?.stack ?? error}`);
  process.exitCode = 1;
} finally {
  stream?.closeGracefully();
  hrana.close();
}
