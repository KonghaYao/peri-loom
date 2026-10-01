/**
 * ============================================================================
 *  tests/batch.test.ts —— 多语句入口：batch() / exec() / compat executeMultiple()
 * ============================================================================
 *  为什么这三条要分开验：它们在协议上是**三种不同的请求形状**——
 *    · batch([...])            → 一次 /v3/cursor 里多个 step，逐条给结果
 *    · batch([...], 'immediate') → 同一批里追加被 condition 门控的
 *                                  BEGIN IMMEDIATE / COMMIT / ROLLBACK 步骤（原子批）
 *    · exec(sql)               → /v3/pipeline 的 {type:"sequence"}，服务端自己按分号切
 *    · compat.executeMultiple() → compat 层的多语句入口，同样是 sequence 路径
 *  只实现其中一条、另一条没实现，这里会立刻暴露，而不是等到某个用户写多语句时才发现。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection } from '@tursodatabase/serverless';
import { createClient } from '@tursodatabase/serverless/compat';
import { preflight, type PlatformConfig } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

describe('多语句入口：batch / exec / executeMultiple', () => {
  const table = tableName('batch');
  let config!: PlatformConfig;
  let conn!: Connection;

  before(async () => {
    config = await preflight();
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

  test('batch() 按数组顺序执行，并逐条返回结果与影响行数', async () => {
    const key = caseKey('batch');
    const results = await conn.batch([
      { sql: `INSERT INTO ${table}(k, t) VALUES (?, ?)`, args: [key, 'v1'] },
      { sql: `UPDATE ${table} SET t = ? WHERE k = ?`, args: ['v2', key] },
      { sql: `SELECT t FROM ${table} WHERE k = ?`, args: [key] },
    ]);

    assert.equal(results.length, 3, `batch 应逐条返回 3 个结果，实际 ${results.length} 个`);
    assert.equal(results[0].rowsAffected, 1, `第 1 条 INSERT 影响行数不对：${results[0].rowsAffected}`);
    assert.equal(results[1].rowsAffected, 1, `第 2 条 UPDATE 影响行数不对：${results[1].rowsAffected}`);
    assert.deepEqual(results[2].columns, ['t'], `第 3 条 SELECT 的列名不对：${JSON.stringify(results[2].columns)}`);
    assert.equal(results[2].rows.length, 1, `第 3 条 SELECT 应返回 1 行，实际 ${results[2].rows.length} 行`);
    assert.equal(
      results[2].rows[0].t,
      'v2',
      `batch 内应按顺序执行：SELECT 应读到 UPDATE 之后的值 'v2'，实际 ${JSON.stringify(results[2].rows[0].t)}`,
    );
  });

  test('batch() 支持纯 SQL 字符串与 {sql,args} 混用（含命名参数）', async () => {
    const key = caseKey('batch-mixed');
    const results = await conn.batch([
      `INSERT INTO ${table}(k, t) VALUES ('${key}', 'literal')`,
      { sql: `UPDATE ${table} SET t = :t WHERE k = :k`, args: { t: 'named', k: key } },
    ]);

    assert.equal(results.length, 2, `batch 应返回 2 个结果，实际 ${results.length} 个`);
    assert.equal(results[1].rowsAffected, 1, `命名参数那条 UPDATE 未生效：${results[1].rowsAffected}`);

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.t, 'named', `命名参数在 batch 里的取值不对：${JSON.stringify(row?.t)}`);
  });

  test("batch(..., 'immediate') 原子批：整批成功后数据全部落库", async () => {
    const keyA = caseKey('atomic-a');
    const keyB = caseKey('atomic-b');
    const results = await conn.batch(
      [
        { sql: `INSERT INTO ${table}(k, t) VALUES (?, ?)`, args: [keyA, 'A'] },
        { sql: `INSERT INTO ${table}(k, t) VALUES (?, ?)`, args: [keyB, 'B'] },
      ],
      'immediate',
    );

    assert.equal(results.length, 2, `原子批应返回 2 个结果，实际 ${results.length} 个`);
    assert.equal(results[0].rowsAffected, 1, '原子批第 1 条 INSERT 影响行数不对');
    assert.equal(results[1].rowsAffected, 1, '原子批第 2 条 INSERT 影响行数不对');

    const rows = await conn.all(`SELECT k, t FROM ${table} WHERE k IN (?, ?) ORDER BY k`, [keyA, keyB]);
    assert.equal(rows.length, 2, `原子批提交后应有 2 行落库，实际 ${rows.length} 行`);
  });

  test('exec() 把多语句按分号顺序全部执行', async () => {
    const key = caseKey('exec');
    await conn.exec(
      `INSERT INTO ${table}(k, t) VALUES ('${key}', 'exec-1');` +
        ` UPDATE ${table} SET t = 'exec-2' WHERE k = '${key}';`,
    );

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.ok(row != null, 'exec() 里的 INSERT 没有生效（sequence 路径没执行第一条？）');
    assert.equal(
      row.t,
      'exec-2',
      `exec() 应按顺序执行完所有语句：期望 'exec-2'，实际 ${JSON.stringify(row.t)}`,
    );
  });

  test('compat 的 executeMultiple() 走通同一套多语句语义', async () => {
    const key = caseKey('execute-multiple');
    const client = createClient({ url: config.dbUrl, authToken: config.token });
    try {
      await client.executeMultiple(
        `INSERT INTO ${table}(k, t) VALUES ('${key}', 'em-1');` +
          ` UPDATE ${table} SET t = 'em-2' WHERE k = '${key}';`,
      );
    } finally {
      client.close();
    }

    // 用主连接（另一条 stream）回读，确认是真的落库了
    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.ok(row != null, 'executeMultiple() 里的 INSERT 没有生效');
    assert.equal(
      row.t,
      'em-2',
      `executeMultiple() 应按顺序执行完所有语句：期望 'em-2'，实际 ${JSON.stringify(row.t)}`,
    );
  });
});
