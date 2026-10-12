import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { openHttp, Stmt } from '@libsql/hrana-client';

const [connectionFile] = process.argv.slice(2);
if (!connectionFile) throw new Error('private TLS fixture connection.json required');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
const observedFetch = (request) => {
  if (new URL(request.url).pathname.endsWith('/v2/pipeline')) throw new Error('v3 fell back to v2');
  return fetch(request);
};
const client = openHttp(url, connection.project_api_key, observedFetch, 3);
let stream;
try {
  assert.equal(await client.getVersion(), 3);
  stream = client.openStream();
  const result = await stream.query(new Stmt('SELECT ? AS optional, ? AS decimal, ? AS label').bindIndexes([null, 1.25, '雪']));
  assert.deepEqual(result.columnNames, ['optional', 'decimal', 'label']);
  assert.deepEqual([result.rows[0].optional, result.rows[0].decimal, result.rows[0].label], [null, 1.25, '雪']);
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client@0.7.0', transport: 'https', protocol: 'v3-protobuf', read_only_bindings: true }));
} finally {
  stream?.closeGracefully();
  client.close();
}
