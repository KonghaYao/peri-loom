/**
 * ============================================================================
 *  tests/values.test.ts —— 值往返：null / i64 / 浮点 / 中文 / blob
 * ============================================================================
 *  为什么单列一组：值在协议里是 `{type, value|base64}` 的标签联合，
 *  服务端要按 SQLite 的真实类型打标签，客户端再按标签解码。任何一处偷懒都会
 *  **静默改变数据**，而不是报错：
 *    · 整数被打成 float  → 超过 2^53 的整数丢精度（用户拿到的是错的 id）
 *    · blob 走了 text    → 二进制里的 0x00 / 0xff 被改写
 *    · text 不是 UTF-8   → 中文/emoji 变问号
 *  所以每条都断言"写进去什么，读回来必须一模一样"，而不是"能读出来"。
 *
 *  i64 那一组特别说明：SDK 默认用 parseInt 解码整数，2^53+1 会退化成不精确的
 *  number。要精确读回必须开 safeIntegers（默认 safeIntegers=false 是 SDK 的
 *  既定行为，不是服务端的问题）——所以用例里专门开一条连接来验。
 *
 *  前置：入口 /readyz 可达 + 凭据（见 src/config.ts）；运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import type { Connection } from '@tursodatabase/serverless';
import { preflight } from '../src/config.ts';
import { caseKey, closeConnections, createTableSql, openConnection, tableName } from '../src/harness.ts';

describe('值往返', () => {
  const table = tableName('values');
  /** 默认模式连接：整数按 SDK 默认解码成 number。 */
  let conn!: Connection;
  /** 精确模式连接：整数解码成 bigint，才能验 i64 边界。 */
  let safe!: Connection;

  const I64 = 9_007_199_254_740_993n; // 2^53 + 1：超出 IEEE754 安全整数范围
  const NEG_I64 = -9_007_199_254_740_993n;
  const F64 = 0.1 + 0.2; // 典型的二进制浮点值，JSON 往返必须原样
  const TEXT = '中文-文本-🚀-emoji';
  const BLOB = Uint8Array.from([0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff]);

  before(async () => {
    const config = await preflight();
    conn = openConnection(config);
    safe = openConnection(config);
    safe.defaultSafeIntegers(true);
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

  test('null 往返仍是 null（不是空串、不是 0）', async () => {
    const key = caseKey('null');
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, null]);

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.ok(row != null, '写入了 null 的行读不回来');
    assert.equal(row.t, null, `null 往返失败：实际 ${JSON.stringify(row.t)}`);
  });

  test('中文与 emoji 往返逐字符一致（UTF-8 边界）', async () => {
    const key = caseKey('text');
    await conn.run(`INSERT INTO ${table}(k, t) VALUES (?, ?)`, [key, TEXT]);

    const row = await conn.get(`SELECT t FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.t, TEXT, `文本往返失败：实际 ${JSON.stringify(row?.t)}`);
    assert.equal(row?.t.length, TEXT.length, '文本长度变了：多字节字符被改写');
  });

  test('浮点往返保持 double 精度', async () => {
    const key = caseKey('float');
    await conn.run(`INSERT INTO ${table}(k, r) VALUES (?, ?)`, [key, F64]);

    const row = await conn.get(`SELECT r, typeof(r) AS ty FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.r, F64, `浮点往返失败：期望 ${F64}，实际 ${JSON.stringify(row?.r)}`);
    assert.equal(row?.ty, 'real', `浮点在库里的类型应为 real，实际 ${JSON.stringify(row?.ty)}`);
  });

  test('blob 逐字节往返（含 0x00 与 0xff）', async () => {
    const key = caseKey('blob');
    await conn.run(`INSERT INTO ${table}(k, b) VALUES (?, ?)`, [key, BLOB]);

    const row = await conn.get(`SELECT b FROM ${table} WHERE k = ?`, [key]);
    const got = Buffer.from(row?.b ?? []);
    assert.equal(
      got.toString('hex'),
      Buffer.from(BLOB).toString('hex'),
      `blob 往返失败：期望 0x${Buffer.from(BLOB).toString('hex')}，实际 0x${got.toString('hex')}`,
    );
  });

  test('i64 超出 f64 安全范围仍能精确往返（safeIntegers 连接）', async () => {
    const key = caseKey('i64');
    await conn.run(`INSERT INTO ${table}(k, n) VALUES (?, ?), (?, ?)`, [key, I64, `${key}-neg`, NEG_I64]);

    const positive = await safe.get(`SELECT n, typeof(n) AS ty FROM ${table} WHERE k = ?`, [key]);
    assert.equal(
      positive?.n,
      I64,
      `i64 往返失败：期望 ${I64}n，实际 ${String(positive?.n)}（typeof=${typeof positive?.n}）；` +
        '超范围整数必须以 {type:"integer", value:"…"} 字符串形式下发，被当成 double 就必然丢精度',
    );
    assert.equal(typeof positive?.n, 'bigint', 'safeIntegers 模式下整数应解码为 bigint');
    assert.equal(positive?.ty, 'integer', `i64 在库里的类型应为 integer，实际 ${JSON.stringify(positive?.ty)}`);

    const negative = await safe.get(`SELECT n FROM ${table} WHERE k = ?`, [`${key}-neg`]);
    assert.equal(negative?.n, NEG_I64, `负 i64 往返失败：期望 ${NEG_I64}n，实际 ${String(negative?.n)}`);
  });

  test('默认模式下整数以 number 下发（对照：精确读回必须开 safeIntegers）', async () => {
    const key = caseKey('default-int');
    await conn.run(`INSERT INTO ${table}(k, n) VALUES (?, ?)`, [key, 42]);

    const row = await conn.get(`SELECT n FROM ${table} WHERE k = ?`, [key]);
    assert.equal(row?.n, 42, `小整数往返失败：实际 ${JSON.stringify(row?.n)}`);
    assert.equal(
      typeof row?.n,
      'number',
      'SDK 默认把整数解码成 number（因此 2^53 以上需要 safeIntegers 才能精确读回）',
    );
  });
});
