/**
 * ============================================================================
 *  src/harness.ts —— 连接与数据夹具
 * ============================================================================
 *  为什么这样组织：
 *
 *  · `node --test` 默认**每个测试文件一个子进程**，所以"一条连接 + 一张表"的
 *    生命周期就放在文件级 before/after 里；文件之间天然隔离，互不连坐。
 *
 *  · 表名带随机后缀：同一套用例可以反复跑（不会因为上次残留的行而假绿/假红），
 *    并发执行的文件也不会撞表。
 *
 *  · **文件之间串行跑**（npm test 里的 --test-concurrency=1）：这些文件打的是
 *    同一个库，而平台对同一个库的写入是串行的——并行时后面到的请求会拿到
 *    "Database is busy"。串行不是掩盖问题，而是承认"共享一个库"这个前提；
 *    文件内部本来就互相独立，串行只是去掉无意义的锁争用。
 *
 *  · 已知的平台侧瞬时错误（实测：背靠背启动两次测试时约 1/3 概率出现）：
 *      "调用 Worker worker-1 失败: Timeout expired" —— worker 冷启动/唤醒超时
 *      （平台 WAKEUP_TIMEOUT_MS 默认 3000ms）
 *      "Database is busy"                          —— 库上有别的会话在写
 *    这两类不是用例的断言问题，**隔几秒重跑即可**；这里刻意不做自动重试：
 *    重试会把"平台真的变慢了/真的开始串行失败"也一起吞掉，得不偿失。
 *
 *  · 用例之间不共享数据：每条用例写自己的 key，因此单条失败不会污染其它用例的
 *    前置状态。事务类用例一律用**专用连接**，避免把共享连接留在事务里连坐后面。
 * ============================================================================
 */

import { randomUUID } from 'node:crypto';
import { connect, type Connection } from '@tursodatabase/serverless';
import type { PlatformConfig } from './config.ts';

/** 本进程开过的连接：after 里统一关掉，避免测试进程挂着不退出。 */
const openConnections = new Set<Connection>();

/**
 * 打开一条独立连接并登记。
 *
 * 每条连接在服务端是一条独立 stream（独立会话），所以它天然是"旁证"：
 * 用它去读另一条连接提交/回滚的结果，才不会被同会话的未提交视图骗过。
 */
export function openConnection(config: PlatformConfig): Connection {
  const conn = connect({
    // 只传数据库根 URL：端点路径（/v3/pipeline、/v3/cursor）由 SDK 自己拼。
    url: config.dbUrl,
    authToken: config.token,
    defaultQueryTimeout: config.queryTimeoutMs,
  });
  openConnections.add(conn);
  return conn;
}

/** 关闭本进程打开过的所有连接（关不掉的忽略：进程马上要退了）。 */
export async function closeConnections(): Promise<void> {
  for (const conn of openConnections) {
    try {
      await conn.close();
    } catch {
      /* 忽略：连接可能已被服务端回收 */
    }
  }
  openConnections.clear();
}

/**
 * 生成带随机后缀的表名。
 *
 * 用 uuid 截断而不是时间戳：同一毫秒内并发启动的测试文件不会撞名，
 * 且名字短、只含 [0-9a-f]，拼进 SQL 不需要引号。
 */
export function tableName(prefix: string): string {
  return `t_${prefix}_${randomUUID().replaceAll('-', '').slice(0, 10)}`;
}

/**
 * 夹具表结构。
 *
 * 列型覆盖值往返需要的全部类型；`k TEXT NOT NULL` 是**故意**的：
 * 约束冲突用例需要一个真的会失败的写入。
 */
export function createTableSql(table: string): string {
  return `CREATE TABLE ${table} (
  id INTEGER PRIMARY KEY,
  k  TEXT NOT NULL,
  n  INTEGER,
  r  REAL,
  b  BLOB,
  t  TEXT
)`;
}

/** 夹具表的列名，顺序与 createTableSql 一致。 */
export const TABLE_COLUMNS = ['id', 'k', 'n', 'r', 'b', 't'] as const;

/** 每条用例自己生成 key，保证用例之间不共享数据。 */
export function caseKey(name: string): string {
  return `${name}-${randomUUID().slice(0, 8)}`;
}
