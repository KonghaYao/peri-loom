/**
 * ============================================================================
 *  src/config.ts —— 运行时接线：入口地址、目标库、凭据、脱敏、前置检查
 * ============================================================================
 *  为什么把这些收敛成单独一层：
 *
 *  1. **数据库 URL 只能在这里拼。** SDK 的 normalizeUrl 只把 `libsql://` /
 *     `turso://` 前缀换成 `https://`，`http://` 原样穿过；随后它用
 *     `` `${url}/v3/cursor` `` 做**朴素字符串拼接**。所以数据库根 URL 一旦带上
 *     query（例如 `?tls=0`），query 会被当成路径的一部分吃掉 → 请求必然 404。
 *     拼接收敛到一处，测试代码里就没人有机会踩这个坑。
 *
 *  2. **凭据有优先级，且绝不能出现在输出里。**
 *       DB_PLATFORM_TOKEN（token 字面量；若它指向一个存在的文件则按文件读）
 *         → /tmp/.peri-token（平台既有脚本约定的兜底位置）
 *         → 都没有时给人话，而不是让每条用例各自抛 401 / ECONNREFUSED。
 *     redact() 是唯一的出口过滤器：凡是可能带凭据的文本（尤其是错误原因）都先过它。
 *
 *  3. **前置问题前置报错。** /readyz 不通、token 过期这类环境问题应该在 before
 *     钩子里变成一句可读原因，而不是让几十条用例各抛一遍同样的栈。
 * ============================================================================
 */

import { readFileSync } from 'node:fs';

/** 平台对外入口：web 容器 nginx 反代，`/db/*` 已转发。 */
const DEFAULT_BASE_URL = 'http://127.0.0.1:8090';
/** 默认目标库 id。 */
const DEFAULT_DB_ID = '01a0f4e6-ad03-7601-8125-936e0a3a1785';
/** 没有环境变量时的兜底凭据文件。 */
const DEFAULT_TOKEN_FILE = '/tmp/.peri-token';

export interface PlatformConfig {
  /** 入口根地址，例如 http://127.0.0.1:8090。 */
  baseUrl: string;
  /** 目标库 id。 */
  dbId: string;
  /** 凭据（Bearer）。只应传给 SDK，不应出现在任何输出里。 */
  token: string;
  /** 数据库根 URL，端点路径由 SDK 自己拼。 */
  dbUrl: string;
  /** 单条查询超时，避免服务端挂死拖住整个测试进程。 */
  queryTimeoutMs: number;
}

/**
 * 前置条件不满足（缺凭据 / 入口不可达 / 服务未就绪）。
 *
 * 刻意把 stack 压成一行：这类错误只需要"原因 + 怎么办"，栈帧对排查没帮助，
 * 反而会把真正有用的那句话埋掉。
 */
export class PreflightError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PreflightError';
    this.stack = `${this.name}: ${message}`;
  }
}

/** 已解析到的凭据原文，供 redact() 做精确擦除。 */
let loadedToken = '';

/** JWT 形状兜底：即使凭据来源未知（例如日志里混进别人的 token），也一并抹掉。 */
const JWT_SHAPED = /\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\b/g;
/** 数据库 API Token 形状兜底，覆盖未经 loadConfig 的日志路径。 */
const API_TOKEN_SHAPED = /\bdbp_[0-9a-f]{64}\b/g;
/** `Bearer <token>` 形态：打印请求头时只允许留下 `Bearer <REDACTED>`。 */
const BEARER_SHAPED = /(Bearer\s+)\S+/gi;

/**
 * 唯一的输出脱敏出口。
 *
 * 三件事：擦掉已知凭据原文 → 擦掉任何 JWT 形状的串 → 把 `Bearer xxx` 收敛成
 * `Bearer <REDACTED>`。顺序有意义：先按原文精确擦除，再按形状兜底。
 */
export function redact(value: unknown): string {
  let text = typeof value === 'string' ? value : String(value);
  if (loadedToken) text = text.split(loadedToken).join('<REDACTED>');
  text = text.replace(JWT_SHAPED, '<REDACTED>');
  text = text.replace(API_TOKEN_SHAPED, '<REDACTED>');
  text = text.replace(BEARER_SHAPED, '$1<REDACTED>');
  return text;
}

/**
 * 把任意抛出物压成一行可读原因（含 Error.cause 链），并且一定过脱敏。
 *
 * 为什么不用 error.stack：SDK 的网络错误栈又长又重复，且里面可能夹带请求信息；
 * 排查这些用例真正需要的是 name / message / code / cause 四个字段。
 */
export function reasonOf(error: unknown): string {
  if (!(error instanceof Error)) return redact(`非 Error 抛出：${String(error)}`);
  const bits = [`${error.name || 'Error'}: ${error.message ?? ''}`];
  const coded = error as { code?: unknown; rawCode?: unknown; cause?: unknown };
  if (coded.code !== undefined) bits.push(`code=${String(coded.code)}`);
  if (coded.rawCode !== undefined) bits.push(`rawCode=${String(coded.rawCode)}`);
  const cause = coded.cause;
  if (cause instanceof Error) {
    // 网络错误的真正原因往往在 cause 上（例如 ECONNREFUSED / UND_ERR_SOCKET）
    const causeCode = (cause as { code?: unknown }).code ?? cause.name;
    bits.push(`cause=${String(causeCode)}: ${cause.message}`);
  } else if (cause !== undefined) {
    bits.push(`cause=${String(cause)}`);
  }
  return redact(bits.join(' | '));
}

/** 看起来像路径（而不是 token 字面量）：token 用 base64url/不透明串，不会含路径分隔符。 */
const LOOKS_LIKE_PATH = /[/\\]/;

/**
 * 读一个"装着 token 的文件"；不存在 / 不可读 / 是目录 / 内容是空白，都算"这里没有凭据"。
 *
 * 刻意不用 existsSync：它和随后的 read 之间有 TOCTOU 窗口，而且会把"目录"当成
 * 存在的凭据文件，最后抛出 EISDIR 或者把路径本身当 token 发出去 —— 用户看到的
 * 只会是一个莫名其妙的 401。
 */
function readTokenFile(path: string): string | null {
  try {
    const raw = readFileSync(path, 'utf8').trim();
    return raw.length > 0 ? raw : null;
  } catch {
    return null;
  }
}

/**
 * 解析凭据；解析不到返回 null（不抛）。
 *
 * `DB_PLATFORM_TOKEN` 的语义与平台既有脚本保持一致：它若指向一个存在的文件，
 * 就按文件内容读，否则当作 token 字面量。这样 CI 里既可以直接传值，
 * 也可以传 secret 文件路径。
 */
export function tryResolveToken(): string | null {
  const fromEnv = (process.env.DB_PLATFORM_TOKEN ?? '').trim();
  const token = fromEnv
    ? readTokenFile(fromEnv) ?? (LOOKS_LIKE_PATH.test(fromEnv) ? null : fromEnv)
    : readTokenFile(DEFAULT_TOKEN_FILE);
  if (token) loadedToken = token;
  return token;
}

/** 解析凭据；解析不到就抛可读的 PreflightError。 */
export function resolveToken(): string {
  const token = tryResolveToken();
  if (!token) {
    throw new PreflightError(
      '缺少平台凭据：请设置 DB_PLATFORM_TOKEN（token 字面量，或指向含 token 的文件路径），' +
        `或把 token 写入 ${DEFAULT_TOKEN_FILE}。\n` +
        '  提示：请在目标数据库详情申请本库 API Token（dbp_ 前缀），管理登录 JWT 不能用于 SDK。',
    );
  }
  return token;
}

/** 组装全部运行时配置（会在缺凭据时抛 PreflightError）。 */
export function loadConfig(): PlatformConfig {
  const baseUrl = (process.env.BASE_URL ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
  const dbId = process.env.DB_ID ?? DEFAULT_DB_ID;
  const token = resolveToken();
  const queryTimeoutMs = Number(process.env.QUERY_TIMEOUT_MS ?? 20_000);
  return {
    baseUrl,
    dbId,
    token,
    // ⚠️ 这里绝不允许追加 query：SDK 会把它当成 /v3/cursor 路径的一部分（见文件头第 1 条）。
    dbUrl: `${baseUrl}/db/${dbId}`,
    queryTimeoutMs,
  };
}

/**
 * 前置检查：凭据可解析 + 入口 /readyz 返回 200。
 *
 * 失败时抛的是 PreflightError（只有一行原因，没有栈）。测试文件在 before 里调用它，
 * 于是"环境没起来"会表现为一条清晰的失败原因，而不是几十条重复的 401 栈。
 */
export async function preflight(): Promise<PlatformConfig> {
  const config = loadConfig();

  let status: number;
  try {
    const response = await fetch(`${config.baseUrl}/readyz`, {
      signal: AbortSignal.timeout(5_000),
    });
    status = response.status;
  } catch (error) {
    throw new PreflightError(
      `连不上平台入口 ${config.baseUrl}/readyz —— ${reasonOf(error)}\n` +
        '  请先确认 web 容器与 db-server 已就绪（docker compose ps），' +
        '或用 BASE_URL 指向别的入口。',
    );
  }

  if (status !== 200) {
    throw new PreflightError(
      `平台入口 ${config.baseUrl}/readyz 返回 ${status}，期望 200：服务尚未就绪。`,
    );
  }
  return config;
}
