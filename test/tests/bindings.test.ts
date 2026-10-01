/**
 * ============================================================================
 *  tests/bindings.test.ts —— 参数绑定：位置参数与命名参数
 * ============================================================================
 *  为什么要单独验：绑定值在协议里是 `{type,value}` 结构，与 SQL 文本分开下发。
 *  服务端必须把它们按顺序填进 `?`，或按名字填进 `:name` / `@name` / `$name`。
 *  这一层如果错了，最常见的表现不是报错，而是**静默写入错误的值**——
 *  所以每条都用读回来的真实值做断言。
 *
 *  额外验一条安全性质：绑定值原样往返，不会因为内容里带引号/分号而改变语义
 *  （如果服务端把参数拼进 SQL 文本，这里会立刻炸表）。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection } from '@tursodatabase/serverless';
import { preflight } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

describe('参数绑定：位置参数与命名参数', () => {
  const table = tableName('bind');
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

  test('位置参数按顺序填充 ?，写入的值逐列可读回', async () => {
    const key = caseKey('pos');
    await conn.run(`INSERT INTO ${table}(k, n, r, t) VALUES (?, ?, ?, ?)`, [key, 7, 1.25, '第四位']);

    const row = await conn.get(`SELECT k, n, r, t FROM ${table} WHERE k = ?`, [key]);
    assert.ok(row != null, '按位置参数写入的行读不回来');
    assert.equal(row.n, 7, `第 2 个位置参数应为 7，实际 ${JSON.stringify(row.n)}`);
    assert.equal(row.r, 1.25, `第 3 个位置参数应为 1.25，实际 ${JSON.stringify(row.r)}`);
    assert.equal(row.t, '第四位', `第 4 个位置参数应为 '第四位'，实际 ${JSON.stringify(row.t)}`);
  });

  test('位置参数也可以展开传（run(sql, a, b) 与 run(sql, [a, b]) 等价）', async () => {
    const asArray = await conn.get('SELECT ? AS a, ? AS b', ['数组形式', 11]);
    const asSpread = await conn.get('SELECT ? AS a, ? AS b', '展开形式', 22);

    assert.deepEqual(
      { a: asArray.a, b: asArray.b },
      { a: '数组形式', b: 11 },
      '数组形式的位置参数绑定结果不对',
    );
    assert.deepEqual(
      { a: asSpread.a, b: asSpread.b },
      { a: '展开形式', b: 22 },
      '展开形式的位置参数绑定结果不对',
    );
  });

  test('命名参数 :name / @name / $name 三种前缀都支持', async () => {
    for (const prefix of [':', '@', '$']) {
      const expected = `${prefix}value-${prefix}`;
      const row = await conn.get(`SELECT ${prefix}v AS v`, { v: expected });
      assert.equal(
        row?.v,
        expected,
        `命名参数 ${prefix}v 绑定失败：期望 ${JSON.stringify(expected)}，实际 ${JSON.stringify(row?.v)}`,
      );
    }
  });

  test('命名参数在写入路径同样生效（run + 回读）', async () => {
    const key = caseKey('named');
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (:k, :t)`, { k: key, t: '命名参数写入' });

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = :k`, { k: key });
    assert.equal(row?.t, '命名参数写入', `命名参数写入的值读回来不对：${JSON.stringify(row?.t)}`);
  });

  test('一个语句里混用多个命名参数，各自独立取值', async () => {
    const row = await conn.get('SELECT :a AS a, :b AS b, :a AS a_again', { a: 'A', b: 'B' });
    assert.deepEqual(
      { a: row?.a, b: row?.b, again: row?.a_again },
      { a: 'A', b: 'B', again: 'A' },
      '多个命名参数没有各自独立取值',
    );
  });

  test('绑定 null 写入后读回仍是 null', async () => {
    const key = caseKey('null');
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, null]);

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.ok(row != null, '写入了 null 的行读不回来');
    assert.equal(row.t, null, `null 绑定后应读回 null，实际 ${JSON.stringify(row.t)}`);
  });

  test('绑定值按数据下发，不会因为内容含引号/分号而改变语义', async () => {
    const key = caseKey('quoted');
    const tricky = `O'Brien; DROP TABLE ${table}; -- 逗号, 反斜杠\\`;
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, tricky]);

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.t, tricky, `含引号/分号的文本往返后不一致：${JSON.stringify(row?.t)}`);

    const stillThere = await conn.all(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
      [table],
    );
    assert.equal(stillThere.length, 1, '绑定值里的 DROP TABLE 竟然生效了：参数没有按数据下发');
  });
});
