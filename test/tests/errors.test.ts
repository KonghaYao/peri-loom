/**
 * ============================================================================
 *  tests/errors.test.ts —— 错误路径：非法 SQL 必须得到**结构化错误**
 * ============================================================================
 *  为什么这条不能只看"抛错了没有"：SQL 错误在协议里有专属的承载方式
 *  （cursor 流里的 step_error / entry:error；pipeline 里的 error 结果），
 *  客户端据此构造 DatabaseError，带上可读 message 和机器可读 code。
 *
 *  不合格的失败长这样，而且很容易被"反正抛错了"糊弄过去：
 *    · 裸的 "HTTP error! status: 500"   → 服务端把 SQL 错误升级成了传输层错误，
 *                                         错误码/原因全部丢失，客户端无法区分
 *                                         "SQL 写错了"和"服务挂了"
 *    · SyntaxError（JSON 解析崩溃）      → 响应体不是合法 JSON / NDJSON
 *  所以这里既断言"有 message"，也断言"不是裸 HTTP 状态码、是 SDK 的结构化错误"。
 *
 *  最后一条：出一次 SQL 错误不能把整条 stream 废掉（否则用户报错后连接即死）。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import { DatabaseError, type Connection } from '@tursodatabase/serverless';
import { preflight, reasonOf } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

/** 捕获一次调用抛出的错误；没抛错直接判失败（比 assert.rejects 更容易给出可读原因）。 */
async function capture(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (error) {
    return error;
  }
  assert.fail('期望抛出结构化错误，但调用正常返回了');
}

describe('错误路径：结构化错误', () => {
  const table = tableName('err');
  let conn!: Connection;

  before(async () => {
    const config = await preflight();
    conn = openConnection(config);
    await conn.run(createTableSql(table));
  });

  after(async () => {
    try {
      await conn.run(`DROP TABLE IF EXISTS ${table}`);
    } catch {
      /* DROP 失败就随它去（例如库正好被别的会话占着）：随机表名保证下次运行不受影响，
         代价只是可能在这张库上留下一张 t_* 表，不影响任何断言结论 */
    }
    await closeConnections();
  });

  test('查询不存在的表：得到带 message/code 的 DatabaseError，而不是裸 HTTP 状态码', async () => {
    const missing = `${table}_no_such`;
    const error = await capture(() => conn.all(`SELECT * FROM ${missing}`));

    assert.ok(
      error instanceof DatabaseError,
      `应抛出 SDK 的 DatabaseError（结构化错误），实际 ${reasonOf(error)}`,
    );
    const message = error.message.trim();
    assert.ok(message.length > 0, '错误没有 message，不是结构化错误');
    assert.doesNotMatch(
      message,
      /^HTTP error! status: \d+$/i,
      `拿到的是裸的传输层错误，SQL 失败原因丢失：${message}`,
    );
    assert.match(
      message,
      /no such table/i,
      `错误 message 应当说明是表不存在，实际 ${JSON.stringify(message)}`,
    );
    assert.match(
      String(error.code ?? ''),
      /^[A-Z][A-Z0-9_]*$/,
      `错误应带机器可读的 code，实际 ${JSON.stringify(error.code)}`,
    );
  });

  test('语法错误：message 指出语法问题，且带 code', async () => {
    const error = await capture(() => conn.all('SELEC 1'));

    assert.ok(error instanceof DatabaseError, `应抛出 DatabaseError，实际 ${reasonOf(error)}`);
    const message = error.message.trim();
    assert.ok(message.length > 0, '语法错误没有 message');
    assert.doesNotMatch(message, /^HTTP error! status: \d+$/i, `语法错误被降级成 HTTP 状态码：${message}`);
    assert.match(message, /syntax error/i, `message 应指出语法错误，实际 ${JSON.stringify(message)}`);
    assert.match(
      String(error.code ?? ''),
      /^[A-Z][A-Z0-9_]*$/,
      `语法错误应带机器可读的 code，实际 ${JSON.stringify(error.code)}`,
    );
  });

  test('约束冲突：NOT NULL 与 UNIQUE 都返回结构化错误', async () => {
    const notNull = await capture(() => conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [null, 'k 为 null']));
    assert.ok(notNull instanceof DatabaseError, `NOT NULL 冲突应抛 DatabaseError，实际 ${reasonOf(notNull)}`);
    assert.match(
      notNull.message,
      /not null constraint failed/i,
      `NOT NULL 冲突的 message 不对：${JSON.stringify(notNull.message)}`,
    );

    const id = Math.floor(Math.random() * 1_000_000) + 1;
    await conn.run(`INSERT INTO ${table}(id, k, t) VALUES (?, ?, ?)`, [id, caseKey('uniq-1'), 'first']);
    const unique = await capture(() =>
      conn.run(`INSERT INTO ${table}(id, k, t) VALUES (?, ?, ?)`, [id, caseKey('uniq-2'), 'second']),
    );
    assert.ok(unique instanceof DatabaseError, `UNIQUE 冲突应抛 DatabaseError，实际 ${reasonOf(unique)}`);
    assert.match(
      unique.message,
      /unique constraint failed/i,
      `UNIQUE 冲突的 message 不对：${JSON.stringify(unique.message)}`,
    );
  });

  test('出错之后连接仍然可用（一次 SQL 错误不得废掉整条 stream）', async () => {
    await capture(() => conn.all(`SELECT * FROM ${table}_still_missing`));
    await capture(() => conn.run('THIS IS NOT SQL'));

    const row = await conn.get('SELECT 1 AS ok');
    assert.equal(row?.ok, 1, `报错后连接应仍可查询，实际读到 ${JSON.stringify(row)}`);
    assert.equal(conn.inTransaction, false, 'SQL 报错后连接不应残留事务状态');
  });
});
