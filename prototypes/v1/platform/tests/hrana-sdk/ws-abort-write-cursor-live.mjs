import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';
import { WebSocket } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const path = `/db/${tenant}/${database}/${connection.database.branch}/`;
const client = createClient({ url: `${connection.endpoint}${path}`, authToken: connection.project_api_key });
const table = `hrana_ws_abort_${randomBytes(4).toString('hex')}`;
let socket;
try {
  await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  socket = new WebSocket(`${connection.endpoint.replace('https:', 'wss:')}${path}`, ['hrana3']);
  await once(socket, 'open', { signal: AbortSignal.timeout(10000) });
  assert.equal(socket.protocol, 'hrana3');
  async function send(message) {
    const incoming = once(socket, 'message', { signal: AbortSignal.timeout(20000) });
    socket.send(JSON.stringify(message));
    const [payload] = await incoming;
    return JSON.parse(payload.toString());
  }
  assert.equal((await send({ type: 'hello', jwt: connection.project_api_key })).type, 'hello_ok');
  let requestID = 0;
  async function request(input) {
    const reply = await send({ type: 'request', request_id: ++requestID, request: input });
    assert.equal(reply.request_id, requestID);
    assert.equal(reply.type, 'response_ok');
    return reply.response;
  }
  assert.equal((await request({ type: 'open_stream', stream_id: 1 })).type, 'open_stream');
  const sql = `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`;
  const batch = { steps: [{ stmt: { sql, args: [], named_args: [], want_rows: true } }] };
  assert.equal((await request({ type: 'open_cursor', stream_id: 1, cursor_id: 2, batch })).type, 'open_cursor');
  const page = await request({ type: 'fetch_cursor', cursor_id: 2, max_count: 2 });
  assert.equal(page.type, 'fetch_cursor');
  assert.equal(page.done, false);
  assert.deepEqual(page.entries.map(entry => entry.type), ['step_begin', 'row']);
  assert.equal((await request({ type: 'close_cursor', cursor_id: 2 })).type, 'close_cursor');
  assert.equal((await request({ type: 'close_stream', stream_id: 1 })).type, 'close_stream');
  assert.equal((await client.execute(`SELECT COUNT(*) AS count FROM ${table}`)).rows[0].count, 0);
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client WebSocket', transport: 'wss', version: 3, partial_write_cursor_abort: true, committed_rows: 0 }));
} finally {
  socket?.close();
  client.close();
}
