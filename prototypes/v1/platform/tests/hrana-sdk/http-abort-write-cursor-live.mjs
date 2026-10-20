// E1 known-gap：HttpCursor.close() 仅调用 reader.cancel()，没有服务器 aborted 回执。
// hrana_http_cursor.go 的连续 fetch 继承请求 context；取消可能丢失已派发的末页回复。
// Rust session/lane.rs 的 fetch_write_cursor 会收束末页写入，close_write_cursor
// 对已完成 cursor 也返回成功，所以后续 close/回读不能证明在途写入已取消。
// WS/native 对照用例先收到 done=false，再显式 close 并等待确认，没有并行 fetch。
// 本用例原断言保持不变；偶发通过不代表协议取消证明。矩阵默认明确跳过 E1，
// PERI_HRANA_INCLUDE_KNOWN_GAPS=1 可在独立 fixture 运行，失败仍以非零状态退出。
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createClient } from '@libsql/client';
import { openHttp } from '@libsql/hrana-client';

const connection = JSON.parse(await readFile(process.argv[2], 'utf8'));
if (!/^https:\/\/127\.0\.0\.1:\d+$/.test(connection.endpoint) || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');
const { tenant, database } = connection.database.namespace;
const url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}/`;
// 只记录失败回复的状态与公开错误码；不记录请求头、凭据或完整响应。
const observedFetch = async (...args) => {
  const response = await fetch(...args);
  if (!response.ok) {
    const body = await response.clone().json().catch(() => null);
    console.error(JSON.stringify({ case: 'http-abort-write-cursor-live', status: response.status, code: body?.error?.code, cleanup_status: body?.cleanup_error?.status, cleanup_code: body?.cleanup_error?.error?.code }));
  }
  return response;
};
const client = createClient({ url, authToken: connection.project_api_key, fetch: observedFetch });
const http = openHttp(url, connection.project_api_key, fetch, 3);
const stream = http.openStream();
const table = `hrana_http_abort_${randomBytes(4).toString('hex')}`;
let cursor;
try {
  await client.execute(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY)`);
  assert.equal(await http.getVersion(), 3);
  const sql = `WITH RECURSIVE values_row(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM values_row WHERE id<12000) INSERT INTO ${table} SELECT id FROM values_row RETURNING id`;
  cursor = await stream._openCursor({ steps: [{ stmt: { sql, args: [], namedArgs: [], wantRows: true }, condition: undefined }] });
  assert.equal((await cursor.next()).type, 'step_begin');
  assert.equal((await cursor.next()).type, 'row');
  cursor.close();
  assert.equal(cursor.closed, true);
  const rows = await client.execute(`SELECT COUNT(*) AS count FROM ${table}`);
  assert.equal(rows.rows[0].count, 0);
  console.log(JSON.stringify({ sdk: '@libsql/hrana-client@0.7.0', transport: 'https', version: 3, partial_write_cursor_abort: true, committed_rows: 0 }));
} finally {
  cursor?.close();
  stream.closeGracefully();
  http.close();
  client.close();
}
