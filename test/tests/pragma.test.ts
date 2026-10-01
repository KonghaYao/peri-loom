/**
 * ============================================================================
 *  tests/pragma.test.ts —— PRAGMA 走普通 execute 路径，但结果形状不同
 * ============================================================================
 *  为什么值得单独验：PRAGMA 的返回**没有"用户列名"那一层**——
 *    · 结果行是**数组**（row[0]、row[1]…），不是按列名索引的对象；
 *    · 列名只出现在结果的 columns 元数据里；
 *    · 且这些列名/行数完全由服务端下发，客户端无从编造。
 *  所以这里同时断言"列名元数据"和"行里的真实值"，两条都对才算 PRAGMA 通。
 *
 *  user_version 是少数**可写**的 PRAGMA：写进去再读回来、并换一条连接读，
 *  才能证明它真的落到了库上，而不是被 SDK 或服务端缓存住了。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection } from '@tursodatabase/serverless';
import { preflight, type PlatformConfig } from '../src/config.ts';
import { closeConnections, createTableSql, openConnection, TABLE_COLUMNS, tableName } from '../src/harness.ts';

describe('PRAGMA', () => {
  const table = tableName('pragma');
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

  test('pragma(table_info) 下发列名元数据，且逐列信息与建表语句一致', async () => {
    const info = await conn.pragma(`table_info(${table})`);

    assert.ok(Array.isArray(info?.rows), `pragma 结果应带 rows 数组，实际 ${JSON.stringify(info?.rows)}`);
    assert.deepEqual(
      info.columns,
      ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
      `table_info 的列名元数据不对：${JSON.stringify(info.columns)}`,
    );

    // 数组行：第 1 列是 name，第 2 列是声明类型，第 3 列是 notnull
    const byName = new Map<string, unknown[]>(
      info.rows.map((row: unknown[]): [string, unknown[]] => [String(row[1]), row]),
    );
    assert.deepEqual(
      [...byName.keys()],
      [...TABLE_COLUMNS],
      `table_info 返回的列集合与建表语句不一致：${JSON.stringify([...byName.keys()])}`,
    );
    assert.equal(byName.get('k')?.[2], 'TEXT', '列 k 的声明类型丢了（应下发 decltype）');
    assert.equal(byName.get('n')?.[2], 'INTEGER', '列 n 的声明类型不对');
    assert.equal(byName.get('r')?.[2], 'REAL', '列 r 的声明类型不对');
    assert.equal(byName.get('b')?.[2], 'BLOB', '列 b 的声明类型不对');
    assert.equal(byName.get('k')?.[3], 1, 'NOT NULL 列 k 的 notnull 标记应为 1');
    assert.equal(byName.get('t')?.[3], 0, '可空列 t 的 notnull 标记应为 0');
  });

  test('pragma(user_version) 可写可读，且换一条连接仍能读到', async () => {
    // 随机值：既证明"读到的就是刚写的"，又不依赖库的历史状态（可重复运行）
    const marker = Math.floor(Math.random() * 1_000_000) + 1;
    await conn.exec(`PRAGMA user_version = ${marker}`);

    const sameConn = await conn.pragma('user_version');
    assert.equal(typeof sameConn.rows[0][0], 'number', 'user_version 应以数字下发');
    assert.equal(sameConn.rows[0][0], marker, `同连接回读的 user_version 不对：${sameConn.rows[0][0]}`);

    // 换一条独立连接（独立 stream）再读：证明值真的落在库上，不是会话级缓存
    const other = openConnection(config);
    try {
      const otherConn = await other.pragma('user_version');
      assert.equal(
        otherConn.rows[0][0],
        marker,
        `独立连接读到的 user_version 不一致：${otherConn.rows[0][0]} ≠ ${marker}（写入没有真正落库？）`,
      );
    } finally {
      await other.close();
    }
  });
});
