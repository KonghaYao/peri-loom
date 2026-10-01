/**
 * ============================================================================
 *  tests/basic.test.ts —— 基本契约：all / get / run 的返回值与列名，以及 DDL
 * ============================================================================
 *  为什么先验这一层：SDK 的这两个入口走的是**不同的服务端路径**——
 *    · all()/get()/run() → /v3/cursor（NDJSON 流：step_begin.cols + row + step_end）
 *    · 列名来自 step_begin 的 cols；值来自 row 的 {type,value} 编码
 *  只要 cols 丢了，返回的行就没有列名可访问；只要值编码错了，数字会变成字符串。
 *  所以这里断言的是**具体的值与列名**，不是"没抛异常"。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection } from '@tursodatabase/serverless';
import { preflight } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

describe('基本契约：all / get / run 与 DDL', () => {
  const table = tableName('basic');
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

  test('all() 返回的行可按列名取值，且列名顺序与 SELECT 一致', async () => {
    const rows = await conn.all('SELECT 1 AS n, ? AS s, 2.5 AS f', '文本');

    assert.equal(rows.length, 1, `期望 1 行，实际 ${rows.length} 行`);
    // 列名既要在（否则访问不到），顺序也要对（step_begin.cols 的下发顺序）
    assert.deepEqual(Object.keys(rows[0]), ['n', 's', 'f'], '列名或列顺序与 SELECT 不一致');
    assert.equal(rows[0].n, 1, `整数列应为数字 1，实际 ${JSON.stringify(rows[0].n)}`);
    assert.equal(rows[0].s, '文本', `文本列绑定值不对，实际 ${JSON.stringify(rows[0].s)}`);
    assert.equal(rows[0].f, 2.5, `浮点列应为 2.5，实际 ${JSON.stringify(rows[0].f)}`);
  });

  test('all() 多行结果保序，且逐行都能按列名取值', async () => {
    const keys = [caseKey('all-a'), caseKey('all-b'), caseKey('all-c')];
    for (const [index, key] of keys.entries()) {
      await conn.run(`INSERT INTO ${table}(k, n) VALUES (?, ?)`, [key, index]);
    }

    const rows = await conn.all(
      `SELECT k, n FROM ${table} WHERE k IN (?, ?, ?) ORDER BY n`,
      keys[0],
      keys[1],
      keys[2],
    );

    assert.equal(rows.length, 3, `期望 3 行，实际 ${rows.length} 行`);
    assert.deepEqual(
      rows.map((row) => row.n),
      [0, 1, 2],
      'ORDER BY 后的行顺序不对（或值没按列名还原）',
    );
    assert.deepEqual(
      rows.map((row) => row.k),
      keys,
      '行内容与写入值不一致',
    );
  });

  test('get() 命中返回单行，未命中返回 undefined（而不是抛错或空对象）', async () => {
    const key = caseKey('get');
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'only-one']);

    const hit = await conn.get(`SELECT k, t FROM ${table} WHERE k = ?`, key);
    assert.ok(hit != null, '刚插入的行读不回来');
    assert.equal(hit.t, 'only-one', `get() 返回值不对：${JSON.stringify(hit.t)}`);

    const miss = await conn.get(`SELECT k FROM ${table} WHERE k = ?`, caseKey('get-missing'));
    assert.equal(miss, undefined, `未命中应返回 undefined，实际 ${JSON.stringify(miss)}`);
  });

  test('run() 的 changes / lastInsertRowid 是真实计数', async () => {
    const first = await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [caseKey('run-1'), 'x']);
    assert.equal(first.changes, 1, `INSERT 应报告影响 1 行，实际 ${JSON.stringify(first.changes)}`);
    assert.ok(
      Number.isSafeInteger(first.lastInsertRowid) && first.lastInsertRowid > 0,
      `lastInsertRowid 应是一个真实的自增 id，实际 ${JSON.stringify(first.lastInsertRowid)}`,
    );

    const second = await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [caseKey('run-2'), 'y']);
    assert.ok(
      second.lastInsertRowid > first.lastInsertRowid,
      `第二次 INSERT 的 lastInsertRowid 应递增：${second.lastInsertRowid} 未大于 ${first.lastInsertRowid}`,
    );

    const none = await conn.run(`UPDATE ${table} SET t = 'z' WHERE k = ?`, caseKey('run-none'));
    assert.equal(none.changes, 0, `未命中的 UPDATE 应报告 0 行，实际 ${JSON.stringify(none.changes)}`);
  });

  test('DDL：CREATE / ALTER / DROP 都真实生效', async () => {
    const ddlTable = tableName('ddl');

    await conn.run(createTableSql(ddlTable));
    const created = await conn.all(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
      ddlTable,
    );
    assert.equal(created.length, 1, `CREATE TABLE 后 sqlite_master 查不到 ${ddlTable}`);
    assert.equal(created[0].name, ddlTable, 'sqlite_master 返回的表名不对');

    await conn.run(`ALTER TABLE ${ddlTable} ADD COLUMN extra TEXT`);
    const extraKey = caseKey('ddl');
    await conn.run(`INSERT INTO ${ddlTable}(k, extra) VALUES (?, ?)`, [extraKey, '已经加上的列']);
    const row = await conn.get(`SELECT extra FROM ${ddlTable} WHERE k = ?`, extraKey);
    assert.equal(row?.extra, '已经加上的列', 'ALTER TABLE 加的新列没有真正生效');

    await conn.run(`DROP TABLE ${ddlTable}`);
    const dropped = await conn.all(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
      ddlTable,
    );
    assert.equal(dropped.length, 0, `DROP TABLE 后 sqlite_master 仍能查到 ${ddlTable}`);
  });
});
