#!/usr/bin/env node
/**
 * ============================================================================
 *  scripts/hrana-v3-sdk-check.mjs
 * ============================================================================
 *  用官方 tursodb SDK（npm 包 @tursodatabase/serverless）打本平台的 Hrana v3
 *  端点，做一次可重复运行的端到端验收：每条用例独立 PASS / FAIL，结尾给汇总。
 *
 *  ── 为什么值得这样验 ────────────────────────────────────────────────────
 *  这个 SDK 是"真实客户端"，它对服务端报文形状有硬性要求，而且这些要求无法靠
 *  curl 手工构造看出来：
 *    · /v3/cursor   收 NDJSON 流（step_begin / row / step_end / step_error）
 *    · /v3/pipeline 收单个 JSON（results[]，含 get_autocommit / sequence）
 *    · 每个 cursor 请求都会**追加一个被 condition 门控的 autocommit 探针步骤**
 *      （`{stmt:{sql:"SELECT 1",...}, condition:{type:"is_autocommit"}}`）。
 *      服务端必须在该步骤真正轮到执行时，按当时的 is_autocommit 决定跑还是跳。
 *      跑 -> SDK 认为"不在事务里"；跳 -> SDK 认为"在事务里"。
 *      这一条谎报，SDK 就会在已开启的事务里再发一次 BEGIN。
 *  只要下面 14 条全绿，说明平台的 v3 实现与上游协议是对齐的，而不只是"能通"。
 *
 *  ── 准备 SDK（本脚本零第三方依赖，不负责装包）──────────────────────────
 *      mkdir -p /tmp/tursodb-probe && cd /tmp/tursodb-probe
 *      npm i @tursodatabase/serverless@1.4.0
 *  默认从这里加载：
 *      /tmp/tursodb-probe/node_modules/@tursodatabase/serverless/dist/index.js
 *  装在别处时用环境变量指过来（也可以只指到包目录，脚本会自动补 dist/index.js）：
 *      TURSODB_SDK_PATH=/path/to/node_modules/@tursodatabase/serverless/dist/index.js
 *
 *  ── 配置 ────────────────────────────────────────────────────────────────
 *      BASE_URL            默认 http://127.0.0.1:8090（web 容器 / nginx 反代入口）
 *      DB_ID               目标数据库 id，默认 01a0f4e6-ad03-7601-8125-936e0a3a1785
 *      DB_PLATFORM_TOKEN   平台 token。未设置时读 /tmp/.peri-token；
 *                          若该变量指向一个存在的文件，则按文件读取，否则按字面量。
 *      QUERY_TIMEOUT_MS    单条查询超时，默认 20000（避免服务端挂死拖住脚本）
 *
 *  ── URL 拼接（踩过的坑，改脚本时务必保留）─────────────────────────────
 *  SDK 的 normalizeUrl 只把 libsql:// / turso:// 前缀换成 https://，http:// 原样
 *  穿过；随后它用朴素字符串拼接 `${url}/v3/cursor`。因此：
 *      ✅ http://127.0.0.1:8090/db/<db_id>/   尾部斜杠会被 normalizeUrl 去掉，无害
 *      ❌ http://127.0.0.1:8090/db/<id>?tls=0   query string 会被当成路径的一部分吃掉
 *      ❌ http://127.0.0.1:8090/db/<id>/v2/pipeline   路径必须停在数据库根
 *
 *  ── 安全 ────────────────────────────────────────────────────────────────
 *  脚本绝不打印 token。所有输出统一过 redact()，它会把 token 原文以及
 *  "Bearer xxx" 形态的字符串替换成 <REDACTED>。
 *
 *  ── 退出码 ──────────────────────────────────────────────────────────────
 *      0  全部用例通过
 *      1  有用例失败，或前置检查失败（连不上 / 读不到 token / 找不到 SDK）
 * ============================================================================
 */

import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

/* ────────────────────────────── 配置 ────────────────────────────── */

const CONFIG = {
  baseUrl: (process.env.BASE_URL || 'http://127.0.0.1:8090').replace(/\/+$/, ''),
  dbId: process.env.DB_ID || '01a0f4e6-ad03-7601-8125-936e0a3a1785',
  tokenEnv: (process.env.DB_PLATFORM_TOKEN || '').trim(),
  defaultTokenFile: '/tmp/.peri-token',
  sdkPath:
    process.env.TURSODB_SDK_PATH ||
    '/tmp/tursodb-probe/node_modules/@tursodatabase/serverless/dist/index.js',
  queryTimeoutMs: Number(process.env.QUERY_TIMEOUT_MS || 20000),
};

// 数据库根 URL：端点路径（/v3/pipeline、/v3/cursor）由 SDK 自己拼，这里绝不要带 query。
const DB_URL = `${CONFIG.baseUrl}/db/${CONFIG.dbId}`;

// 带随机后缀的表名：避免污染别人/上次跑留下的数据，也让脚本可以重复运行。
const RAND = (Math.random().toString(36).slice(2, 8) + Date.now().toString(36).slice(-4)).replace(
  /[^a-z0-9]/g,
  '',
);
const TABLE = `hrana_v3_check_${RAND}`;

const CREATE_TABLE_SQL = `CREATE TABLE IF NOT EXISTS ${TABLE} (
  id INTEGER PRIMARY KEY,
  k  TEXT,
  s  TEXT,
  i  INTEGER,
  r  REAL,
  b  BLOB,
  n  TEXT
)`;

/* ────────────────────────── 脱敏与格式化 ────────────────────────── */

let TOKEN = '';

/** 一切对外输出都必须先过这里：抹掉 token 原文与 Bearer 形态的凭据。 */
function redact(value) {
  let out = typeof value === 'string' ? value : String(value);
  if (TOKEN && TOKEN.length >= 8) out = out.split(TOKEN).join('<REDACTED>');
  out = out.replace(/Bearer\s+[A-Za-z0-9\-._~+/=]+/gi, 'Bearer <REDACTED>');
  return out;
}

/** 安全的人类可读序列化：BigInt / Uint8Array / undefined 都不会炸。 */
function show(value) {
  if (typeof value === 'bigint') return `${value}n`;
  if (value instanceof Uint8Array) return `Uint8Array(0x${Buffer.from(value).toString('hex')})`;
  if (value === undefined) return 'undefined';
  if (typeof value === 'string') return JSON.stringify(value);
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

/** 断言失败：只表达"哪条契约没满足"，不掺 SDK 内部细节。 */
class CheckError extends Error {
  constructor(message) {
    super(message);
    this.name = 'CheckError';
  }
}

function assert(condition, message) {
  if (!condition) throw new CheckError(message);
}

/** 把任意抛出物压成一行可读原因（含网络错误的 cause 链）。 */
function formatReason(error) {
  if (error instanceof CheckError) return redact(error.message);
  if (!(error instanceof Error)) return redact(`非 Error 抛出：${show(error)}`);
  const bits = [`${error.name || 'Error'}: ${error.message ?? ''}`];
  if (error.code !== undefined) bits.push(`code=${show(error.code)}`);
  if (error.rawCode !== undefined) bits.push(`rawCode=${show(error.rawCode)}`);
  const cause = error.cause;
  if (cause && typeof cause === 'object') {
    const c = `${cause.code ?? cause.name ?? ''}${cause.message ? `: ${cause.message}` : ''}`.trim();
    if (c) bits.push(`cause=${c}`);
  }
  return redact(bits.join(' | '));
}

/**
 * 取列值。SDK 在不同入口返回的行形状不同，别假设：
 *   all()/get()/batch() -> 以列名为 key 的普通对象
 *   pragma()            -> 数组 + 非枚举的同名列属性
 */
function pick(row, name, index = 0) {
  if (row == null) return undefined;
  const boxed = Object(row);
  if (name in boxed) return boxed[name];
  if (index in boxed) return boxed[index];
  return undefined;
}

/* ────────────────────────── SDK 加载 ────────────────────────── */

const sdk = { connect: null, compat: null, resolvedPath: null, packageVersion: null };

function resolveSdkEntry(p) {
  const candidates = /\.(m|c)?js$/.test(p)
    ? [p]
    : [path.join(p, 'dist', 'index.js'), path.join(p, 'index.js')];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

async function loadSdk() {
  const entry = resolveSdkEntry(CONFIG.sdkPath);
  if (!entry) {
    throw new Error(
      `找不到 SDK 入口：${CONFIG.sdkPath}\n` +
        `  请先准备：mkdir -p /tmp/tursodb-probe && cd /tmp/tursodb-probe && npm i @tursodatabase/serverless@1.4.0\n` +
        `  或用 TURSODB_SDK_PATH 指向 <包目录>/dist/index.js`,
    );
  }
  const mod = await import(pathToFileURL(entry).href);
  if (typeof mod.connect !== 'function') {
    throw new Error(`SDK 入口未导出 connect()：${entry}`);
  }
  sdk.connect = mod.connect;
  sdk.resolvedPath = entry;

  try {
    const pkg = JSON.parse(readFileSync(path.join(path.dirname(entry), '..', 'package.json'), 'utf8'));
    sdk.packageVersion = pkg.version ?? null;
  } catch {
    /* 版本号只是展示用，读不到就算了 */
  }

  // compat 子入口（libSQL 兼容层）提供 commit()/rollback()/executeMultiple()，
  // 是协议里另一条独立路径，值得单独覆盖。加载失败不致命，用到时再报。
  try {
    sdk.compat = await import(pathToFileURL(path.join(path.dirname(entry), 'compat', 'index.js')).href);
  } catch {
    sdk.compat = null;
  }
  return entry;
}

function requireCompat() {
  if (!sdk.compat || typeof sdk.compat.createClient !== 'function') {
    throw new CheckError(
      'SDK 的 compat 子入口不可用（期望 <包>/dist/compat/index.js 导出 createClient）；' +
        '请确认安装的是 @tursodatabase/serverless@1.4.0',
    );
  }
  return sdk.compat;
}

/* ────────────────────────── 连接管理 ────────────────────────── */

const openConnections = new Set();

function openConn() {
  const conn = sdk.connect({
    url: DB_URL,
    authToken: TOKEN,
    defaultQueryTimeout: CONFIG.queryTimeoutMs,
  });
  openConnections.add(conn);
  return conn;
}

async function closeAllConnections() {
  for (const conn of openConnections) {
    try {
      await conn.close();
    } catch {
      /* 关闭失败无所谓，进程要退了 */
    }
  }
  openConnections.clear();
}

/* ────────────────────── 共享状态（跨用例） ────────────────────── */

let conn = null; // 主连接：绝大多数用例共用
let verifyConn = null; // 独立连接：用于"数据是否真的落库"的旁证，绕开任何客户端缓存
let tableReady = null; // 建表 memo

/**
 * 保证表存在。刻意**不缓存失败的 Promise**：万一 DDL 因瞬时问题挂了，
 * 后面的用例还会再试一次，而不是被同一个 rejection 连坐。
 */
async function ensureTable() {
  if (tableReady) return tableReady;
  tableReady = conn.run(CREATE_TABLE_SQL);
  try {
    return await tableReady;
  } catch (error) {
    tableReady = null;
    throw error;
  }
}

function getVerifyConn() {
  if (!verifyConn) verifyConn = openConn();
  return verifyConn;
}

/* ══════════════════════════ 用例 ══════════════════════════ */

/**
 * 用例 1 —— all() 的基本契约：值的类型/内容正确，且列名能作为 key 取到。
 * 验的是：cursor 流的 step_begin.cols 被正确解析 + 行的值编码能还原。
 * 如果列名丢了，返回的行就没有 answer 键；如果值编码错了，42 会变成字符串。
 */
async function case01_allValueAndColumnName() {
  const rows = await conn.all('SELECT 42 AS answer');
  assert(Array.isArray(rows), `all() 应返回数组，实际 ${typeof rows}`);
  assert(rows.length === 1, `应返回 1 行，实际 ${rows.length} 行`);
  const value = pick(rows[0], 'answer');
  assert(
    value === 42,
    `列名 answer 应可取到数字 42，实际 ${show(value)}（typeof=${typeof value}）`,
  );
}

/**
 * 用例 2 —— run() 执行 DDL。
 * 验的是：非查询语句也能走通完整链路（step_begin/step_end 而非只认 SELECT），
 * 以及建完表后表真的可查询。这是后面所有读写用例的地基。
 */
async function case02_runDdl() {
  const result = await ensureTable();
  assert(result && typeof result === 'object', `run(CREATE TABLE) 应返回结果对象，实际 ${show(result)}`);
  const probe = await conn.get(`SELECT count(*) AS c FROM ${TABLE}`);
  assert(probe != null, `建表后查询 ${TABLE} 应返回一行，实际 ${show(probe)}`);
  const count = pick(probe, 'c');
  assert(
    typeof count === 'number' && count >= 0,
    `count(*) 应是数字，实际 ${show(count)}（typeof=${typeof count}）`,
  );
}

/**
 * 用例 3 —— run(sql, [args]) 位置参数绑定。
 * 验的是：args[] 被编码成 [{type:'text',...}] 后服务端真的把占位符替换掉了。
 * 若绑定被忽略，要么直接报"参数个数不对"，要么插进去的是 NULL —— 两种都被抓。
 * 顺带验 run() 的返回值契约：changes 与 lastInsertRowid。
 */
async function case03_runPositionalBinding() {
  await ensureTable();
  const key = `pos-${RAND}`;
  const payload = `hello-${RAND}`;
  const result = await conn.run(`INSERT INTO ${TABLE}(k, s) VALUES (?, ?)`, [key, payload]);
  assert(result && typeof result === 'object', `run(INSERT) 应返回结果对象，实际 ${show(result)}`);
  assert(result.changes === 1, `INSERT 应影响 1 行，实际 changes=${show(result.changes)}（服务端 step_end.affected_row_count 是否正确？）`);
  assert(
    typeof result.lastInsertRowid === 'number' && result.lastInsertRowid > 0,
    `run() 应返回正整数 lastInsertRowid，实际 ${show(result.lastInsertRowid)}（服务端 step_end.last_insert_rowid 是否正确？）`,
  );
  const back = await conn.get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
  assert(back != null, `按绑定参数查不回刚插入的行，说明位置参数没有生效`);
  const got = pick(back, 's');
  assert(got === payload, `位置参数绑定往返不一致：期望 ${show(payload)}，实际 ${show(got)}`);
}

/**
 * 用例 4 —— 命名参数绑定（该 SDK 的写法：普通对象 + SQL 里的 :name）。
 * 验的是：服务端能解析 named_args[]（[{name,value}]）并按名字绑定。
 * 这条路径和位置参数是两套独立编码，只测 ? 覆盖不到。
 */
async function case04_runNamedBinding() {
  await ensureTable();
  const key = `named-${RAND}`;
  const payload = `命名参数-${RAND}`;
  const result = await conn.run(`INSERT INTO ${TABLE}(k, s) VALUES (:key, :payload)`, {
    key,
    payload,
  });
  assert(result && result.changes === 1, `命名参数 INSERT 应影响 1 行，实际 changes=${show(result?.changes)}`);
  const back = await conn.get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
  assert(back != null, `按命名参数写入的行查不回来，说明命名参数没有生效`);
  const got = pick(back, 's');
  assert(got === payload, `命名参数绑定往返不一致：期望 ${show(payload)}，实际 ${show(got)}`);
}

/**
 * 用例 5 —— get() 的单行语义（命中 + 未命中）。
 * 命中验列名映射；未命中必须得到 undefined 而不是空对象/抛错/塞个 null 行，
 * 否则上层 `if (!row)` 之类的判断会集体失效。
 * 这里全部用常量查询，刻意不依赖表，保证与其它用例解耦。
 */
async function case05_getSingleRow() {
  const row = await conn.get('SELECT 7 AS one, 8 AS two');
  assert(row != null, `get() 命中时应返回一行，实际 ${show(row)}`);
  assert(pick(row, 'one') === 7, `列 one 应为 7，实际 ${show(pick(row, 'one'))}`);
  assert(pick(row, 'two') === 8, `列 two 应为 8，实际 ${show(pick(row, 'two'))}`);

  const miss = await conn.get('SELECT 1 AS x WHERE 0');
  assert(
    miss == null,
    `get() 无结果时应返回 undefined，实际 ${show(miss)}（空结果集不该造出一行）`,
  );
}

/**
 * 用例 6 —— batch() 多语句混合读写，一次性发送。
 * 验的是：cursor 的 batch.steps 支持多条语句，且每条 step 的产出能按 step 序号
 * 正确归位（响应里的 step 字段不能丢/串位），否则 rowsAffected 与行数据会错配。
 */
async function case06_batchMixed() {
  await ensureTable();
  const key = `batch-${RAND}`;
  const results = await conn.batch([
    { sql: `INSERT INTO ${TABLE}(k, s) VALUES (?, ?)`, args: [key, 'batch-v1'] },
    { sql: `UPDATE ${TABLE} SET s = ? WHERE k = ?`, args: ['batch-v2', key] },
    { sql: `SELECT s FROM ${TABLE} WHERE k = ?`, args: [key] },
  ]);
  assert(Array.isArray(results), `batch() 应返回数组，实际 ${typeof results}`);
  assert(results.length === 3, `batch() 应按语句返回 3 个结果，实际 ${results.length} 个`);
  assert(
    results[0].rowsAffected === 1,
    `batch 第 1 条 INSERT 应影响 1 行，实际 ${show(results[0].rowsAffected)}`,
  );
  assert(
    results[1].rowsAffected === 1,
    `batch 第 2 条 UPDATE 应影响 1 行，实际 ${show(results[1].rowsAffected)}`,
  );
  const rows = results[2].rows ?? [];
  assert(rows.length === 1, `batch 第 3 条 SELECT 应返回 1 行，实际 ${rows.length} 行`);
  const got = pick(rows[0], 's');
  assert(got === 'batch-v2', `batch 内读写顺序有误：期望 'batch-v2'，实际 ${show(got)}`);
}

/**
 * 用例 7 —— SDK 的多语句入口（主入口 exec() + compat 入口 executeMultiple()）。
 * 这类入口在协议上是 /v3/pipeline 的 `{type:"sequence"}`，和逐条 execute 是
 * 两条不同的服务端代码路径：它要把一段 SQL 按分号切开并全部执行。
 * 只实现 execute 而没实现 sequence，这里会立刻暴露。
 */
async function case07_multiStatementEntrypoints() {
  await ensureTable();

  const keyExec = `multi-${RAND}`;
  await conn.exec(
    `INSERT INTO ${TABLE}(k, s) VALUES ('${keyExec}', 'exec-1');` +
      ` UPDATE ${TABLE} SET s = 'exec-2' WHERE k = '${keyExec}';`,
  );
  const viaExec = await conn.get(`SELECT s FROM ${TABLE} WHERE k = ?`, [keyExec]);
  assert(viaExec != null, `exec() 多语句里的 INSERT 没有生效`);
  const execValue = pick(viaExec, 's');
  assert(execValue === 'exec-2', `exec() 多条语句应按顺序全部执行：期望 'exec-2'，实际 ${show(execValue)}`);

  const compat = requireCompat();
  const client = compat.createClient({ url: DB_URL, authToken: TOKEN });
  try {
    const keyEm = `multicompat-${RAND}`;
    await client.executeMultiple(
      `INSERT INTO ${TABLE}(k, s) VALUES ('${keyEm}', 'em-1');` +
        ` UPDATE ${TABLE} SET s = 'em-2' WHERE k = '${keyEm}';`,
    );
    const viaEm = await conn.get(`SELECT s FROM ${TABLE} WHERE k = ?`, [keyEm]);
    assert(viaEm != null, `executeMultiple() 多语句里的 INSERT 没有生效`);
    const emValue = pick(viaEm, 's');
    assert(emValue === 'em-2', `executeMultiple() 应按顺序全部执行：期望 'em-2'，实际 ${show(emValue)}`);
  } finally {
    try {
      client.close();
    } catch {
      /* ignore */
    }
  }
}

/**
 * 用例 8 —— pragma()。
 * PRAGMA 走的是普通 execute 路径，但返回的行没有"用户列名"这一层，
 * 而且 table_info 这类 PRAGMA 自带参数、要回多列多行，能顺带验证
 * step_begin.cols 的列名与列类型是否都下发了。
 */
async function case08_pragma() {
  await ensureTable();

  const tableInfo = await conn.pragma(`table_info(${TABLE})`);
  assert(tableInfo && Array.isArray(tableInfo.rows), `pragma(table_info) 应返回 rows 数组，实际 ${show(tableInfo?.rows)}`);
  assert(tableInfo.rows.length === 7, `表应有 7 列，pragma 返回 ${tableInfo.rows.length} 行`);
  const names = tableInfo.rows.map((row) => pick(row, 'name', 1));
  for (const expected of ['id', 'k', 's', 'i', 'r', 'b', 'n']) {
    assert(names.includes(expected), `pragma(table_info) 缺少列 ${expected}，实际拿到 [${names.join(', ')}]`);
  }

  const userVersion = await conn.pragma('user_version');
  assert(
    userVersion && Array.isArray(userVersion.rows) && userVersion.rows.length >= 1,
    `pragma(user_version) 应至少返回 1 行，实际 ${show(userVersion?.rows)}`,
  );
  const version = pick(userVersion.rows[0], 'user_version');
  assert(typeof version === 'number', `user_version 应是数字，实际 ${show(version)}（typeof=${typeof version}）`);
}

/**
 * 用例 9 —— 事务提交：开事务 -> 插入 -> commit() -> 数据真的落库。
 * 事务在 compat 层独占一个 session（服务端一条独立 stream），所以提交后必须
 * 换一条**全新连接**去读，否则可能读到同一 session 的未提交视图而假绿。
 */
async function case09_transactionCommit() {
  await ensureTable();
  const compat = requireCompat();
  const key = `commit-${RAND}`;
  const client = compat.createClient({ url: DB_URL, authToken: TOKEN });
  try {
    const tx = await client.transaction('write');
    await tx.execute({ sql: `INSERT INTO ${TABLE}(k, s) VALUES (?, ?)`, args: [key, 'committed'] });
    const inside = await tx.execute({ sql: `SELECT s FROM ${TABLE} WHERE k = ?`, args: [key] });
    assert(
      inside.rows.length === 1,
      `事务内应能看到自己未提交的写入（BEGIN 未生效？），实际 ${inside.rows.length} 行`,
    );
    await tx.commit();
  } finally {
    try {
      client.close();
    } catch {
      /* ignore */
    }
  }

  const row = await getVerifyConn().get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
  assert(row != null, `commit() 后新连接读不到该行：提交没有真正落库`);
  const got = pick(row, 's');
  assert(got === 'committed', `commit() 落库的值不对：期望 'committed'，实际 ${show(got)}`);
}

/**
 * 用例 10 —— 事务回滚：开事务 -> 插入 -> rollback() -> 数据不落库。
 * 关键是先断言"事务内看得见"，再断言"事务外看不见"：
 * 只有两边都成立，才证明是 ROLLBACK 起了作用，而不是 INSERT 根本没执行。
 */
async function case10_transactionRollback() {
  await ensureTable();
  const compat = requireCompat();
  const key = `rollback-${RAND}`;
  const client = compat.createClient({ url: DB_URL, authToken: TOKEN });
  try {
    const tx = await client.transaction('write');
    await tx.execute({ sql: `INSERT INTO ${TABLE}(k, s) VALUES (?, ?)`, args: [key, 'rolled-back'] });
    const inside = await tx.execute({ sql: `SELECT s FROM ${TABLE} WHERE k = ?`, args: [key] });
    assert(
      inside.rows.length === 1,
      `事务内应能看到待回滚的写入，实际 ${inside.rows.length} 行（INSERT 就没成功，回滚用例无意义）`,
    );
    await tx.rollback();
  } finally {
    try {
      client.close();
    } catch {
      /* ignore */
    }
  }

  const row = await getVerifyConn().get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
  assert(row == null, `rollback() 后数据仍可读到：回滚没有生效，脏数据已落库（读到 ${show(row)}）`);
}

/**
 * 用例 11 —— autocommit 真实性（最关键的一条，联合验证服务端链路）。
 *
 * 背景：SDK 在每个 cursor 请求末尾追加一个被 `condition:{type:"is_autocommit"}`
 * 门控的探针步骤。服务端**必须在该步骤轮到执行时**按当时的 autocommit 状态决定
 * 跑还是跳。同时 Connection.batch(stmts, 'write') 会先看 session.inTransaction：
 * 已在事务里就**不再**包 BEGIN/COMMIT（让 batch 并入当前事务）。
 *
 * 所以服务端只要谎报 autocommit=true：
 *   a) BEGIN 之后 conn.inTransaction 仍是 false —— 被第 1 个断言抓住；
 *   b) batch(..., 'write') 会在已开启的事务里再发一次 BEGIN IMMEDIATE ——
 *      服务端回 "cannot start a transaction within a transaction"，被下面的显式
 *      捕获转成可读的 FAIL 原因（而不是一坨栈）。
 *
 * 用一条**专用连接**跑，跑完就还，避免把主连接留在事务里连坐后面的用例。
 */
async function case11_autocommitTruthfulness() {
  await ensureTable();
  const txConn = openConn();
  const key = `autocommit-${RAND}`;
  let began = false;

  try {
    await txConn.run('BEGIN IMMEDIATE');
    began = true;

    assert(
      txConn.inTransaction === true,
      'BEGIN 之后 conn.inTransaction 仍为 false：服务端谎报了 autocommit=true。' +
        'cursor 请求里被 condition:{type:"is_autocommit"} 门控的探针步骤本应被跳过，' +
        '服务端却执行了它（或根本没做条件判断），SDK 据此认为连接不在事务中',
    );

    // 事务内写：这条请求同样带 autocommit 探针，是最容易暴露谎报的地方。
    await txConn.run(`INSERT INTO ${TABLE}(k, s) VALUES (?, ?)`, [key, 'tx-1']);

    // 事务内再走 batch(..., 'write')：SDK 只有在"确实在事务里"时才不重复发 BEGIN。
    await txConn.batch([{ sql: `UPDATE ${TABLE} SET s = ? WHERE k = ?`, args: ['tx-2', key] }], 'write');

    const inner = await txConn.get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
    assert(inner != null, '事务内读不到自己刚写的数据');
    const innerValue = pick(inner, 's');
    assert(innerValue === 'tx-2', `事务内应看到未提交的最新值 'tx-2'，实际 ${show(innerValue)}`);

    await txConn.run('COMMIT');
    began = false;
  } catch (error) {
    if (began) {
      try {
        await txConn.run('ROLLBACK');
      } catch {
        /* 连接已经废了就随它去 */
      }
    }
    if (/cannot start a transaction within a transaction/i.test(String(error?.message ?? ''))) {
      throw new CheckError(
        '服务端谎报 autocommit=true：SDK 判定连接不在事务中，于是在已开启的事务里又发了一条 ' +
          'BEGIN IMMEDIATE，服务端回以 "cannot start a transaction within a transaction"。' +
          '正确行为是：cursor 请求里 condition 为 is_autocommit 的探针步骤，应当跳过而不是执行',
      );
    }
    throw error;
  }

  const row = await getVerifyConn().get(`SELECT s FROM ${TABLE} WHERE k = ?`, [key]);
  assert(row != null, 'COMMIT 后新连接读不到该行：事务内的写入没有真正提交');
  const got = pick(row, 's');
  assert(got === 'tx-2', `提交落库的值不对：期望 'tx-2'，实际 ${show(got)}`);
}

/**
 * 用例 12 —— 值往返：null / i64 边界 / 浮点 / 中文文本 / blob。
 * 这是值编解码的对照实验：写进去什么，读回来必须一模一样。
 * i64 用 9007199254740993（2^53+1）—— 它超出 IEEE754 安全整数范围，
 * 只有服务端老老实实按 `{type:"integer", value:"9007199254740993"}` 传字符串、
 * 客户端又开了 safeIntegers 时才能原样拿回；一旦被当成 float/double 就必然丢精度。
 */
async function case12_valueRoundtrip() {
  await ensureTable();
  const key = `types-${RAND}`;
  const I64 = 9007199254740993n; // 2^53 + 1
  const F64 = 3.5;
  const TEXT = `中文-文本-🚀-${RAND}`;
  const BLOB = Uint8Array.from([0x00, 0x01, 0x7f, 0x80, 0xff]);

  await conn.run(`INSERT INTO ${TABLE}(k, s, i, r, b, n) VALUES (?, ?, ?, ?, ?, ?)`, [
    key,
    TEXT,
    I64,
    F64,
    BLOB,
    null,
  ]);

  // 用开启 safeIntegers 的独立连接读回，整数才会解码成 BigInt 而不丢精度。
  const safeConn = openConn();
  safeConn.defaultSafeIntegers(true);
  try {
    const row = await safeConn.get(`SELECT s, i, r, b, n FROM ${TABLE} WHERE k = ?`, [key]);
    assert(row != null, '值往返：刚插入的行读不回来');

    const nullValue = pick(row, 'n');
    assert(nullValue === null, `null 往返失败：期望 null，实际 ${show(nullValue)}`);

    const intValue = pick(row, 'i');
    assert(
      typeof intValue === 'bigint' && intValue === I64,
      `i64 边界往返失败：期望 ${I64}n，实际 ${show(intValue)}（typeof=${typeof intValue}）` +
        '；服务端应下发 {type:"integer", value:"9007199254740993"} 而不是 float',
    );

    const floatValue = pick(row, 'r');
    assert(floatValue === F64, `浮点往返失败：期望 ${F64}，实际 ${show(floatValue)}`);

    const textValue = pick(row, 's');
    assert(textValue === TEXT, `文本往返失败：期望 ${show(TEXT)}，实际 ${show(textValue)}`);

    const blobValue = pick(row, 'b');
    const gotHex = Buffer.from(blobValue ?? []).toString('hex');
    const wantHex = Buffer.from(BLOB).toString('hex');
    assert(gotHex === wantHex, `blob 往返失败：期望 0x${wantHex}，实际 0x${gotHex}`);
  } finally {
    try {
      await safeConn.close();
    } catch {
      /* ignore */
    }
    openConnections.delete(safeConn);
  }
}

/**
 * 用例 13 —— 错误路径：一条非法 SQL 必须得到**结构化错误**。
 * 合格的失败长这样：DatabaseError，message 是可读的 SQL 原因（如 "no such table: xxx"）。
 * 不合格的两种典型：
 *   · 裸的 "HTTP error! status: 500"  —— 服务端把 SQL 错误升级成了 HTTP 级错误
 *   · 客户端 JSON 解析崩溃（SyntaxError）—— 响应体不是合法 JSON / NDJSON
 * 最后再确认连接在报错后仍然可用：出一次错就废掉整条 stream 是不可接受的行为。
 */
async function case13_errorPath() {
  const errorConn = openConn();
  const missing = `${TABLE}_no_such_${RAND}`;
  let thrown = null;
  try {
    await errorConn.all(`SELECT * FROM ${missing}`);
  } catch (error) {
    thrown = error;
  }

  assert(thrown !== null, '非法 SQL 应当抛错，但调用正常返回了');
  assert(thrown instanceof Error, `抛出的应当是 Error，实际 ${typeof thrown}`);

  const message = String(thrown.message ?? '');
  assert(message.trim().length > 0, '错误没有 message，不是结构化错误');

  assert(
    !/^HTTP error! status: \d+$/.test(message.trim()),
    `错误只是裸的 HTTP 状态（"${message}"）：服务端把 SQL 级错误升级成了 HTTP 错误，` +
      '应当以 step_error / results[].type="error" 的形式回结构化错误体',
  );

  assert(
    !/is not valid JSON|Unexpected token|Unexpected end of JSON|JSON\.parse|Bad control character/i.test(
      message,
    ),
    `客户端在解析响应时崩溃（"${message}"）：响应体不是合法 JSON / NDJSON`,
  );

  assert(
    /no such table|does not exist|not found|unknown table/i.test(message),
    `错误信息不是可读的 SQL 原因："${message}"（期望形如 "no such table: ${missing}"）`,
  );

  const after = await errorConn.get('SELECT 1 AS ok');
  assert(
    after != null && pick(after, 'ok') === 1,
    '报错之后同一条连接就不可用了：stream 被错误毒死，后续查询应当照常工作',
  );
}

/**
 * 用例 14 —— prepare() 的 describe 元数据。
 * prepare() 只应编译语句；返回的列名与列类型来自 Hrana describe，而不是执行结果。
 */
async function case14_prepareDescribe() {
  await ensureTable();
  const statement = await conn.prepare(`SELECT i AS answer FROM ${TABLE} LIMIT 1`);
  const columns = statement.columns();
  assert(Array.isArray(columns) && columns.length === 1, `prepare() 应返回 1 列，实际 ${show(columns)}`);
  assert(columns[0].name === 'answer', `prepare() 列名错误：实际 ${show(columns[0].name)}`);
  assert(columns[0].type === 'INTEGER', `prepare() 列类型错误：实际 ${show(columns[0].type)}`);
}



const CASES = [
  ['1. all() 取值与列名', case01_allValueAndColumnName],
  ['2. run() DDL 建表', case02_runDdl],
  ['3. run() 位置参数绑定', case03_runPositionalBinding],
  ['4. run() 命名参数绑定', case04_runNamedBinding],
  ['5. get() 单行读取', case05_getSingleRow],
  ['6. batch() 多语句混合读写', case06_batchMixed],
  ['7. exec()/executeMultiple() 多语句入口', case07_multiStatementEntrypoints],
  ['8. pragma() 读取', case08_pragma],
  ['9. 事务提交 commit() 后落库', case09_transactionCommit],
  ['10. 事务回滚 rollback() 后不落库', case10_transactionRollback],
  ['11. autocommit 真实性（事务内不重复 BEGIN）', case11_autocommitTruthfulness],
  ['12. 值往返 null/i64/浮点/中文/blob', case12_valueRoundtrip],
  ['13. 错误路径返回结构化错误', case13_errorPath],
  ['14. prepare() describe 列元数据', case14_prepareDescribe],
];

function printReason(reason) {
  const lines = String(reason).split('\n');
  console.log(`      └─ ${lines[0]}`);
  for (const line of lines.slice(1)) console.log(`         ${line}`);
}

async function preflight() {
  console.log('── 前置检查 ─────────────────────────────────────────────');

  const resolved = resolveToken();
  if (resolved.error) throw new Error(resolved.error);
  TOKEN = resolved.token;

  let readyStatus = null;
  try {
    const response = await fetch(`${CONFIG.baseUrl}/readyz`, {
      signal: AbortSignal.timeout(5000),
    });
    readyStatus = response.status;
  } catch (error) {
    const cause = error?.cause?.code ?? error?.cause?.message ?? error?.message ?? error;
    throw new Error(
      `连不上平台入口 ${CONFIG.baseUrl}/readyz（${cause}）。\n` +
        `  请确认 web 容器 / nginx 反代已启动，或用 BASE_URL 指向正确入口。`,
    );
  }
  if (readyStatus < 200 || readyStatus >= 300) {
    throw new Error(`平台入口 ${CONFIG.baseUrl}/readyz 返回 HTTP ${readyStatus}，服务未就绪。`);
  }

  await loadSdk();
  if (!Number.isFinite(CONFIG.queryTimeoutMs) || CONFIG.queryTimeoutMs <= 0) {
    throw new Error(`QUERY_TIMEOUT_MS 非法：${process.env.QUERY_TIMEOUT_MS}`);
  }

  const version = sdk.packageVersion ? `@${sdk.packageVersion}` : '';
  console.log(`  ✓ 平台入口    ${CONFIG.baseUrl}/readyz → HTTP ${readyStatus}`);
  console.log(`  ✓ 数据库 URL  ${DB_URL}`);
  console.log(`  ✓ 平台 token  <REDACTED>（来自 ${resolved.source}）`);
  console.log(`  ✓ tursodb SDK ${version} ${sdk.resolvedPath}`);
  console.log(`  ✓ 本次表名    ${TABLE}`);
  console.log('');
}

function resolveToken() {
  if (CONFIG.tokenEnv) {
    if (existsSync(CONFIG.tokenEnv)) {
      const token = readFileSync(CONFIG.tokenEnv, 'utf8').trim();
      if (!token) return { token: '', source: CONFIG.tokenEnv, error: `token 文件为空：${CONFIG.tokenEnv}` };
      return { token, source: `文件 ${CONFIG.tokenEnv}` };
    }
    return { token: CONFIG.tokenEnv, source: '环境变量 DB_PLATFORM_TOKEN' };
  }
  const file = CONFIG.defaultTokenFile;
  if (!existsSync(file)) {
    return {
      token: '',
      source: file,
      error:
        `未设置 DB_PLATFORM_TOKEN，且默认 token 文件不存在：${file}\n` +
        `  请设置 DB_PLATFORM_TOKEN=<token>，或把 token 写到 ${file}。`,
    };
  }
  const token = readFileSync(file, 'utf8').trim();
  if (!token) return { token: '', source: file, error: `token 文件为空：${file}` };
  return { token, source: `文件 ${file}` };
}

async function main() {
  await preflight();

  console.log('── 用例 ─────────────────────────────────────────────────');
  let passed = 0;
  let failed = 0;

  conn = openConn();

  for (const [name, fn] of CASES) {
    try {
      await fn();
      passed += 1;
      console.log(`PASS  ${name}`);
    } catch (error) {
      failed += 1;
      console.log(`FAIL  ${name}`);
      printReason(formatReason(error));
    }
  }

  await closeAllConnections();

  console.log('─────────────────────────────────────────────────────────');
  console.log(`${passed} passed / ${failed} failed`);
  return failed === 0 ? 0 : 1;
}

let exitCode = 1;
try {
  exitCode = await main();
} catch (error) {
  console.error('');
  console.error('── 前置检查失败（用例未开始）─────────────────────────────');
  console.error(formatReason(error));
} finally {
  await closeAllConnections().catch(() => {});
}

process.exitCode = exitCode;
// 显式 exit 前先把 stdout 冲干净，否则管道下结尾几行可能被截断。
if (process.stdout.writableLength > 0) {
  await new Promise((resolve) => process.stdout.write('', resolve));
}
process.exit(exitCode);
