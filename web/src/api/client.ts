/**
 * 手写运行时 API client（不依赖 orval 生成代码）。
 *
 * 约定：
 *   - 同 Origin 相对路径：管理面 /api/v1，数据面 /data/v1（dev 由 vite proxy、prod 由 nginx 转发）；
 *   - 鉴权：Authorization: Bearer <token>，token 存 localStorage；
 *   - 统一错误体 { error: { code, message, request_id, retryable } } -> ApiError；
 *   - orval（npm run gen:api）生成的 src/api/generated 仅作类型参考，运行时不引用。
 */
import type {
  ApiErrorBody,
  AuditLog,
  BatchRequest,
  BatchResponse,
  CreateDatabaseRequest,
  CreateSavedQueryRequest,
  CreateTokenRequest,
  Database,
  DatabaseListQuery,
  LoginRequest,
  LoginResponse,
  MoveDatabaseRequest,
  Operation,
  OperationAccepted,
  Page,
  PageQuery,
  PreferenceView,
  QueryRequest,
  QueryResult,
  QueryStreamChunk,
  RestoreDatabaseRequest,
  SavedQuery,
  SessionOpened,
  SlowQuery,
  Snapshot,
  TokenCreated,
  TokenInfo,
  Worker,
} from './types';
import { messageForCode, retryableByStatus } from './errors';

// ---------------------------------------------------------------- 常量

const MGMT_BASE = '/api/v1';
const DATA_BASE = '/data/v1';

const TOKEN_STORAGE_KEY = 'peri-loom.api_token';
const USER_STORAGE_KEY = 'peri-loom.user';

/** 认证失效事件：AuthProvider 监听后跳转登录页 */
export const UNAUTHORIZED_EVENT = 'peri-loom:unauthorized';

// ---------------------------------------------------------------- Token 存储

export const tokenStore = {
  get(): string | null {
    try {
      return localStorage.getItem(TOKEN_STORAGE_KEY);
    } catch {
      return null;
    }
  },
  set(token: string): void {
    try {
      localStorage.setItem(TOKEN_STORAGE_KEY, token);
    } catch {
      /* localStorage 不可用时静默降级（仅当前会话有效） */
    }
  },
  clear(): void {
    try {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
      localStorage.removeItem(USER_STORAGE_KEY);
    } catch {
      /* ignore */
    }
  },
  getUser(): string | null {
    try {
      return localStorage.getItem(USER_STORAGE_KEY);
    } catch {
      return null;
    }
  },
  setUser(user: string): void {
    try {
      localStorage.setItem(USER_STORAGE_KEY, user);
    } catch {
      /* ignore */
    }
  },
};

// ---------------------------------------------------------------- 错误

export class ApiError extends Error {
  readonly code: string;
  readonly status: number;
  readonly requestId?: string;
  readonly retryable: boolean;
  readonly details?: unknown;
  /** 已按 code 翻译的中文提示 */
  readonly friendlyMessage: string;

  constructor(init: {
    code: string;
    status: number;
    message: string;
    requestId?: string;
    retryable?: boolean;
    details?: unknown;
  }) {
    super(init.message);
    this.name = 'ApiError';
    this.code = init.code;
    this.status = init.status;
    this.requestId = init.requestId;
    this.retryable = init.retryable ?? retryableByStatus(init.status);
    this.details = init.details;
    this.friendlyMessage = messageForCode(init.code, init.status);
  }
}

/** 把任意异常（网络异常 / 中断 / ApiError）统一成 ApiError */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  if (err instanceof DOMException && err.name === 'AbortError') {
    return new ApiError({
      code: 'REQUEST_ABORTED',
      status: 0,
      message: '请求已取消',
      retryable: false,
    });
  }
  if (err instanceof TypeError) {
    // fetch 网络层失败（后端不可达 / DNS / CORS）
    return new ApiError({
      code: 'NETWORK_ERROR',
      status: 0,
      message: err.message || '网络异常',
      retryable: true,
    });
  }
  return new ApiError({
    code: 'UNKNOWN_ERROR',
    status: 0,
    message: err instanceof Error ? err.message : String(err),
    retryable: false,
  });
}

/** 是否为「后端未实现」类错误（用于登录降级等场景） */
export function isUnimplementedError(err: unknown): boolean {
  if (!(err instanceof ApiError)) return false;
  return (
    err.status === 404 ||
    err.status === 405 ||
    err.status === 501 ||
    // 后端 ErrorCode 实际取值是 NOT_IMPLEMENTED（见 crates/domain/src/error.rs）
    err.code === 'NOT_IMPLEMENTED' ||
    err.code === 'UNIMPLEMENTED' ||
    err.code === 'NOT_FOUND'
  );
}

// ---------------------------------------------------------------- 底层 request

type QueryValue = string | number | boolean | undefined | null;

export interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: unknown;
  query?: Record<string, QueryValue>;
  signal?: AbortSignal;
  headers?: Record<string, string>;
  /** 跳过 Authorization 头（如登录接口） */
  anonymous?: boolean;
  /** 允许把 HTTP 错误交给调用方处理（默认抛 ApiError） */
  raw?: boolean;
}

function buildQuery(query?: Record<string, QueryValue>): string {
  if (!query) return '';
  const sp = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === '') continue;
    sp.set(key, String(value));
  }
  const s = sp.toString();
  return s ? `?${s}` : '';
}

async function parseErrorBody(res: Response): Promise<ApiError> {
  let code = `HTTP_${res.status}`;
  let message = res.statusText || `HTTP ${res.status}`;
  let requestId: string | undefined;
  let retryable: boolean | undefined;
  let details: unknown;

  const text = await res.text().catch(() => '');
  if (text) {
    try {
      const parsed = JSON.parse(text) as Partial<ApiErrorBody> & Record<string, unknown>;
      const body = parsed.error ?? (parsed as unknown as ApiErrorBody['error']);
      if (body && typeof body === 'object' && 'code' in body) {
        code = String(body.code);
        message = String(body.message ?? message);
        requestId = body.request_id;
        retryable = body.retryable;
        // 契约字段是单数 detail（ErrorBody.detail）
        details = body.detail;
      } else {
        // 非信封（如 axum 的 4xx 纯文本反序列化错误）
        message = text.slice(0, 500);
      }
    } catch {
      message = text.slice(0, 500);
    }
  }

  const err = new ApiError({ code, status: res.status, message, requestId, retryable, details });
  if (res.status === 401) {
    // 认证失效：清 token 并广播，由 AuthProvider 跳转登录页
    tokenStore.clear();
    try {
      window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT));
    } catch {
      /* ignore */
    }
  }
  return err;
}

async function request<T>(base: string, path: string, options: RequestOptions = {}): Promise<T> {
  const { method = 'GET', body, query, signal, headers = {}, anonymous } = options;
  const url = `${base}${path}${buildQuery(query)}`;

  const finalHeaders: Record<string, string> = { Accept: 'application/json', ...headers };
  if (body !== undefined) finalHeaders['Content-Type'] = 'application/json';
  if (!anonymous) {
    const token = tokenStore.get();
    if (token) finalHeaders.Authorization = `Bearer ${token}`;
  }

  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: finalHeaders,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
      credentials: 'same-origin',
    });
  } catch (err) {
    throw toApiError(err);
  }

  if (!res.ok) throw await parseErrorBody(res);
  if (res.status === 204) return undefined as T;

  const text = await res.text();
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    // 后端返回非 JSON：按原样字符串返回，避免整体崩掉
    return text as unknown as T;
  }
}

const mgmt = <T,>(path: string, options?: RequestOptions) => request<T>(MGMT_BASE, path, options);
const data = <T,>(path: string, options?: RequestOptions) => request<T>(DATA_BASE, path, options);

// ---------------------------------------------------------------- 管理面 API

export const api = {
  /** 登录（后端 OIDC/JWT；未实现时前端降级为粘贴 Token，见 pages/LoginPage.tsx） */
  async login(req: LoginRequest): Promise<string> {
    const res = await mgmt<LoginResponse>('/auth/login', {
      method: 'POST',
      body: req,
      anonymous: true,
    });
    const token = res.access_token;
    if (!token) {
      throw new ApiError({
        code: 'INVALID_RESPONSE',
        status: 200,
        message: '登录响应未包含 access_token',
        retryable: false,
      });
    }
    return token;
  },

  databases: {
    /** 创建（202 {operation_id, state}） */
    create: (req: CreateDatabaseRequest) =>
      mgmt<OperationAccepted>('/databases', { method: 'POST', body: req }),
    list: (query: DatabaseListQuery = {}) => mgmt<Page<Database>>('/databases', { query: { ...query } }),
    get: (dbId: string) => mgmt<Database>(`/databases/${encodeURIComponent(dbId)}`),
    remove: (dbId: string) => mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}`, { method: 'DELETE' }),
    start: (dbId: string) => mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/start`, { method: 'POST' }),
    stop: (dbId: string) => mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/stop`, { method: 'POST' }),
    restart: (dbId: string) =>
      mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/restart`, { method: 'POST' }),
    move: (dbId: string, req: MoveDatabaseRequest = {}) =>
      mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/move`, { method: 'POST', body: req }),
    snapshot: (dbId: string) =>
      mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/snapshot`, { method: 'POST' }),
    backup: (dbId: string) =>
      mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/backup`, { method: 'POST' }),
    restore: (dbId: string, req: RestoreDatabaseRequest = {}) =>
      mgmt<OperationAccepted>(`/databases/${encodeURIComponent(dbId)}/restore`, { method: 'POST', body: req }),
  },

  operations: {
    list: (query: PageQuery = {}) => mgmt<Page<Operation>>('/operations', { query: { ...query } }),
    get: (operationId: string) => mgmt<Operation>(`/operations/${encodeURIComponent(operationId)}`),
  },

  workers: {
    /** 裸数组（非分页容器） */
    list: () => mgmt<Worker[]>('/workers'),
    get: (workerId: string) => mgmt<Worker>(`/workers/${encodeURIComponent(workerId)}`),
    drain: (workerId: string) =>
      mgmt<OperationAccepted>(`/workers/${encodeURIComponent(workerId)}/drain`, { method: 'POST' }),
  },

  snapshots: {
    /** database_id 是必填查询参数，缺失会 400 */
    list: (databaseId: string, limit?: number) =>
      mgmt<Snapshot[]>('/snapshots', { query: { database_id: databaseId, limit } }),
  },

  audit: {
    list: (query: PageQuery = {}) => mgmt<Page<AuditLog>>('/audit', { query: { ...query } }),
  },

  tokens: {
    create: (req: CreateTokenRequest) => mgmt<TokenCreated>('/tokens', { method: 'POST', body: req }),
    list: () => mgmt<TokenInfo[]>('/tokens'),
    /** 204 No Content */
    revoke: (tokenId: string) => mgmt<void>(`/tokens/${encodeURIComponent(tokenId)}`, { method: 'DELETE' }),
  },

  preferences: {
    /**
     * 404 表示未设置，返回 null。
     * 后端返回 PreferenceView `{ key, value }`，这里只取 value 交给调用方。
     */
    async get<T = unknown>(key: string): Promise<T | null> {
      try {
        const view = await mgmt<PreferenceView<T>>(`/panel/preferences/${encodeURIComponent(key)}`);
        return view?.value ?? null;
      } catch (err) {
        const e = toApiError(err);
        if (e.status === 404 || e.code === 'NOT_FOUND') return null;
        throw e;
      }
    },
    /** 请求体必须是 { value } */
    async put<T>(key: string, value: T): Promise<T> {
      const view = await mgmt<PreferenceView<T>>(`/panel/preferences/${encodeURIComponent(key)}`, {
        method: 'PUT',
        body: { value },
      });
      return view?.value ?? value;
    },
  },

  savedQueries: {
    /** 裸数组；后端只接受 limit 查询参数 */
    list: (limit = 100) => mgmt<SavedQuery[]>('/saved-queries', { query: { limit } }),
    create: (req: CreateSavedQueryRequest) => mgmt<SavedQuery>('/saved-queries', { method: 'POST', body: req }),
    /** 204 No Content */
    remove: (id: string) => mgmt<void>(`/saved-queries/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  },

  slowQueries: {
    /** database_id 是必填查询参数，缺失会 400 */
    list: (databaseId: string, limit = 50) =>
      mgmt<SlowQuery[]>('/slow-queries', { query: { database_id: databaseId, limit } }),
  },

  openapi: {
    get: () => mgmt<Record<string, unknown>>('/openapi.json'),
  },
};

// ---------------------------------------------------------------- 数据面 API

/** 普通 JSON 查询（结果较大时请使用 streamQuery） */
export function executeQuery(dbId: string, req: QueryRequest, signal?: AbortSignal): Promise<QueryResult> {
  return data<QueryResult>(`/databases/${encodeURIComponent(dbId)}/query`, {
    method: 'POST',
    body: req,
    signal,
  });
}

/** 批量执行（响应是 { results: ResultSetView[] }） */
export function executeBatch(dbId: string, req: BatchRequest, signal?: AbortSignal): Promise<BatchResponse> {
  return data<BatchResponse>(`/databases/${encodeURIComponent(dbId)}/batch`, {
    method: 'POST',
    body: req,
    signal,
  });
}

/**
 * NDJSON 流式查询：Accept: application/x-ndjson。
 * 逐行解析（ReadableStream + TextDecoder），yield 每个 chunk（后端三型帧）：
 *   {"columns":[{"name","type_name","nullable"}],"type":"header"}
 *   {"type":"row","values":[...]}
 *   {"affected_rows":0,"wal_lsn":0,"elapsed_micros":330,"type":"trailer"}
 * 调用方边收边渲染，避免一次性加载完整结果集（架构「HTTP backpressure 逐行输出」）。
 */
export async function* streamQuery(
  dbId: string,
  req: QueryRequest,
  signal?: AbortSignal,
): AsyncGenerator<QueryStreamChunk, void, undefined> {
  const token = tokenStore.get();
  const res = await fetch(`${DATA_BASE}/databases/${encodeURIComponent(dbId)}/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(req),
    signal,
    credentials: 'same-origin',
  }).catch((err: unknown) => {
    throw toApiError(err);
  });

  if (!res.ok) throw await parseErrorBody(res);
  if (!res.body) {
    throw new ApiError({
      code: 'STREAM_UNAVAILABLE',
      status: res.status,
      message: '响应不支持流式读取',
      retryable: false,
    });
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  const parseLine = (line: string): QueryStreamChunk | null => {
    const trimmed = line.trim();
    if (!trimmed) return null;
    try {
      return JSON.parse(trimmed) as QueryStreamChunk;
    } catch {
      throw new ApiError({
        code: 'STREAM_PARSE_ERROR',
        status: res.status,
        message: `NDJSON 分片解析失败：${trimmed.slice(0, 200)}`,
        retryable: false,
      });
    }
  };

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx = buffer.indexOf('\n');
      while (idx >= 0) {
        const chunk = parseLine(buffer.slice(0, idx));
        buffer = buffer.slice(idx + 1);
        if (chunk) yield chunk;
        idx = buffer.indexOf('\n');
      }
    }
    buffer += decoder.decode();
    const tail = parseLine(buffer);
    if (tail) yield tail;
  } finally {
    reader.releaseLock();
  }
}

/** 显式会话（BEGIN/COMMIT/ROLLBACK 由 SQL 语句驱动） */
export const sessions = {
  open: (dbId: string) =>
    data<SessionOpened>(`/databases/${encodeURIComponent(dbId)}/sessions`, { method: 'POST' }),
  query: (sessionId: string, req: QueryRequest, signal?: AbortSignal) =>
    data<QueryResult>(`/sessions/${encodeURIComponent(sessionId)}/query`, { method: 'POST', body: req, signal }),
  close: (sessionId: string) =>
    data<void>(`/sessions/${encodeURIComponent(sessionId)}`, { method: 'DELETE' }),
};

// ---------------------------------------------------------------- 响应归一化工具

/** /workers、/tokens、/snapshots 等接口返回裸数组，分页接口返回 { items }，这里统一 */
export function toItems<T>(res: Page<T> | T[] | undefined | null): T[] {
  if (!res) return [];
  if (Array.isArray(res)) return res;
  return Array.isArray(res.items) ? res.items : [];
}

/**
 * 后端分页响应**没有 total**（只有 items/limit/offset），所以这里返回的是
 * 「当前已加载条目数」，用于展示；分页控件请配合 hasMorePage 使用。
 */
export function toTotal<T>(res: Page<T> | T[] | undefined | null, fallback = 0): number {
  if (!res) return fallback;
  return toItems(res).length;
}

/** 是否可能还有下一页：本页取满 limit 就认为还有（后端不提供 total） */
export function hasMorePage<T>(res: Page<T> | T[] | undefined | null): boolean {
  if (!res || Array.isArray(res)) return false;
  return toItems(res).length >= (res.limit || 0) && (res.limit || 0) > 0;
}
