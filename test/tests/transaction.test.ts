/**
 * ============================================================================
 *  tests/transaction.test.ts —— 事务：提交落库 / 回滚不落库 / autocommit 真实性
 * ============================================================================
 *  为什么必须用**独立连接**做旁证：
 *  事务跑在服务端的一条 stream（会话）上，同会话天然看得见自己的未提交写入。
 *  如果在同一条连接里"自证"提交成功，即使服务端根本没提交、甚至根本没开事务，
 *  用例也会绿。所以：写入用专用连接，判定用另一条连接。
 *
 *  为什么 autocommit 这条最值得验：
 *  SDK 在每个 cursor 请求末尾追加一个被 `condition:{type:"is_autocommit"}` 门控的
 *  探针步骤（`SELECT 1`）。服务端必须在该步骤真正轮到执行时，按**当时**的
 *  autocommit 状态决定跑还是跳：
 *      跑 → SDK 认为"不在事务里"；跳 → SDK 认为"在事务里"。
 *  一旦服务端谎报（在事务里却执行了探针），SDK 会以为还在 autocommit，于是
 *  Connection.batch(stmts, 'write') 会在已开启的事务里**再发一次 BEGIN IMMEDIATE**，
 *  服务端回 "cannot start a transaction within a transaction" —— 用户侧表现为
 *  "事务里调 batch 就炸"。这里把这个链条钉死。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection, Transaction } from '@tursodatabase/serverless';
import { preflight, type PlatformConfig } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

describe('事务与 autocommit 真实性', () => {
  const table = tableName('tx');
  let config!: PlatformConfig;
  /** 建表/清理用，不参与事务。 */
  let setup!: Connection;
  /** 旁证连接：只用来读"另一条连接提交后的结果"。 */
  let verify!: Connection;

  before(async () => {
    config = await preflight();
    setup = openConnection(config);
    verify = openConnection(config);
    await setup.run(createTableSql(table));
  });

  after(async () => {
    try {
      await setup.run(`DROP TABLE IF EXISTS ${table}`);
    } catch {
      /* DROP 失败就随它去（例如库正好被别的会话占着）：随机表名保证下次运行不受影响，
         代价只是可能在这张库上留下一张 t_* 表，不影响任何断言结论 */
    }
    await closeConnections();
  });

  test('事务提交后数据真的落库（独立连接验证）', async () => {
    const key = caseKey('commit');
    const tx = openConnection(config);
    try {
      await tx.run('BEGIN IMMEDIATE');
      await tx.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'committed']);

      // 先证明写入确实发生在事务内（否则"提交成功"这个结论没有意义）
      const inside = await tx.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
      assert.equal(
        inside?.t,
        'committed',
        `事务内读不到自己刚写入的值：BEGIN/INSERT 没有生效（读到 ${JSON.stringify(inside)}）`,
      );

      await tx.run('COMMIT');
    } finally {
      await tx.close();
    }

    const row = await verify.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(
      row?.t,
      'committed',
      `COMMIT 后另一条连接读不到该行：提交没有真正落库（读到 ${JSON.stringify(row)}）`,
    );
  });

  test('事务回滚后数据不落库（独立连接验证）', async () => {
    const key = caseKey('rollback');
    const tx = openConnection(config);
    try {
      await tx.run('BEGIN IMMEDIATE');
      await tx.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'rolled-back']);

      // 先证明写入真的发生了：否则"回滚成功"可能只是 INSERT 从没成功过
      const inside = await tx.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
      assert.equal(
        inside?.t,
        'rolled-back',
        `事务内应能看见待回滚的写入，实际 ${JSON.stringify(inside)}（INSERT 就没成功，回滚用例无意义）`,
      );

      await tx.run('ROLLBACK');
    } finally {
      await tx.close();
    }

    const row = await verify.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(
      row,
      undefined,
      `ROLLBACK 后另一条连接仍能读到该行：回滚没有生效，脏数据已落库（读到 ${JSON.stringify(row)}）`,
    );
  });

  test('autocommit 真实性：事务内继续执行语句不得触发嵌套 BEGIN', async () => {
    const key = caseKey('autocommit');
    const tx = openConnection(config);
    let began = false;

    try {
      await tx.run('BEGIN IMMEDIATE');
      began = true;

      assert.equal(
        tx.inTransaction,
        true,
        'BEGIN 之后 inTransaction 仍为 false：服务端谎报 autocommit=true——' +
          'cursor 请求里被 condition:{type:"is_autocommit"} 门控的探针步骤本应被跳过，服务端却执行了它',
      );

      // 事务内继续走 cursor 路径：这是最容易暴露探针谎报的地方
      await tx.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'tx-1']);
      assert.equal(
        tx.inTransaction,
        true,
        '事务内执行语句后 inTransaction 变成 false：autocommit 状态被服务端谎报',
      );

      // 事务内再走 batch(..., 'write')：SDK 只有在"确实在事务里"时才不重复包 BEGIN。
      // 若服务端谎报 autocommit，这里会变成嵌套 BEGIN IMMEDIATE 而直接报错。
      await tx.batch([{ sql: `UPDATE ${table} SET t = ? WHERE k = ?`, args: ['tx-2', key] }], 'write');

      const inner = await tx.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
      assert.equal(
        inner?.t,
        'tx-2',
        `事务内应读到未提交的最新值 'tx-2'，实际 ${JSON.stringify(inner?.t)}`,
      );

      await tx.run('COMMIT');
      began = false;
      assert.equal(tx.inTransaction, false, 'COMMIT 之后 inTransaction 应为 false（回到 autocommit）');
    } catch (error) {
      if (began) {
        try {
          await tx.run('ROLLBACK');
        } catch {
          /* 连接已经废了就随它去 */
        }
      }
      if (/cannot start a transaction within a transaction/i.test(String((error as Error)?.message ?? ''))) {
        assert.fail(
          '服务端谎报 autocommit=true：SDK 判定连接不在事务中，于是在已开启的事务里又发了 BEGIN IMMEDIATE。' +
            '正确行为是：cursor 请求里 condition 为 is_autocommit 的探针步骤应当被跳过而不是执行',
        );
      }
      throw error;
    } finally {
      await tx.close();
    }

    const row = await verify.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.t, 'tx-2', `事务提交后独立连接读到的值不对：${JSON.stringify(row?.t)}`);
  });

  test('transactionAsync()：回调正常结束即提交，回调抛错即回滚', async () => {
    // 提交路径
    const committedKey = caseKey('ta-commit');
    const writeCommitted = setup.transactionAsync(async (tx: Transaction, key: string) => {
      await tx.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'ta-committed']);
    });
    await writeCommitted(committedKey);

    const committed = await verify.get(`SELECT t FROM ${table} WHERE k = ?`, [committedKey]);
    assert.equal(
      committed?.t,
      'ta-committed',
      `transactionAsync 正常返回后另一条连接读不到数据：没有提交（读到 ${JSON.stringify(committed)}）`,
    );

    // 回滚路径：回调抛错 → SDK 发 ROLLBACK 并把错误抛出来
    const rolledBackKey = caseKey('ta-rollback');
    const writeAndThrow = setup.transactionAsync(async (tx: Transaction, key: string) => {
      await tx.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, 'ta-rolled-back']);
      throw new Error('用例主动抛错，触发回滚');
    });
    await assert.rejects(
      () => writeAndThrow(rolledBackKey),
      (error: unknown) => {
        assert.match(
          String((error as Error)?.message ?? ''),
          /用例主动抛错/,
          'transactionAsync 应把回调抛出的错误原样抛出',
        );
        return true;
      },
      'transactionAsync 的回调抛错后，调用方应当收到错误',
    );

    const rolledBack = await verify.get(`SELECT t FROM ${table} WHERE k = ?`, [rolledBackKey]);
    assert.equal(
      rolledBack,
      undefined,
      `transactionAsync 回调抛错后数据仍可读到：回滚没有生效（读到 ${JSON.stringify(rolledBack)}）`,
    );
  });
});
