import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { openHttp } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;

for (const version of [2, 3]) {
  const client = openHttp(url, connection.project_api_key, fetch, version);
  const stream = client.openStream();
  try {
    assert.equal(await client.getVersion(), version);
    const cached = stream.storeSql('SELECT ? AS value');
    try {
      assert.equal((await stream.query([cached, [42]])).rows[0].value, 42);
      assert.equal((await stream.describe(cached)).isReadonly, true);
      assert.equal((await stream.query([cached, [43]])).rows[0].value, 43);
    } finally {
      cached.close();
    }
    assert.equal(cached.closed, true);
    assert.throws(() => stream.query([cached, [44]]));
    assert.equal((await stream.query('SELECT 45 AS value')).rows[0].value, 45);
    console.log(JSON.stringify({ sdk: '@libsql/hrana-client', transport: 'https', version, cached_sql: true, close_sql: true }));
  } finally {
    stream.closeGracefully();
    client.close();
  }
}
