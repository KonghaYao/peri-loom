/**
 * 后端契约类型：与 db-server 暴露的 openapi.json（`GET /api/v1/openapi.json`）逐字段对齐。
 *
 * 字段名一律以后端为准（主键统一叫 `id`，不做别名猜测）：
 *   - 管理面 `/api/v1`，数据面 `/data/v1`，同 Origin 由 nginx / vite 代理；
 *   - 列表响应：管理面分页接口是 `{ items, limit, offset }`（**没有 total**），
 *     非分页接口（/workers、/tokens、/saved-queries、/snapshots、/slow-queries）直接返回裸数组；
 *   - 错误体：`{ error: { code, message, request_id, retryable, route_retry_count, detail? } }`。
 */

// ---------------------------------------------------------------- 通用

/** 统一错误体（ErrorEnvelope）：错误明细字段是单数 `detail` */
export interface ApiErrorBody {
  error: {
    code: string;
    message: string;
    request_id: string;
    retryable: boolean;
    route_retry_count: number;
    detail?: unknown;
  };
}

/** 分页容器：后端只回 items + limit + offset，总量需由调用方按 offset/limit 推算 */
export interface Page<T> {
  items: T[];
  limit: number;
  offset: number;
}

export interface PageQuery {
  limit?: number;
  offset?: number;
}

/** 裸数组或分页容器（/workers、/tokens 等接口返回裸数组） */
export type PageOrArray<T> = Page<T> | T[];

// ---------------------------------------------------------------- 操作（Operation）

export type OperationState = 'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED';

/**
 * 202 响应体：长操作受理结果。
 * `operation_id` 为 null 表示幂等无操作（目标状态已达成，没有产生新操作）。
 */
export interface OperationAccepted {
  operation_id: string | null;
  state: string;
  replayed: boolean;
  database_id?: string | null;
}

/** 长操作视图（OperationView）：主键是 `id`，类型字段是 `kind` */
export interface Operation {
  id: string;
  /** 操作类型，如 CREATE_DB / START_DB / MOVE_DB / BACKUP_DB ... */
  kind: string;
  state: OperationState;
  /** 0-100 */
  progress: number;
  result: unknown;
  created_at: string;
  updated_at: string;
  database_id?: string | null;
  worker_id?: string | null;
  tenant_id?: string | null;
  requested_by?: string | null;
  idempotency_key?: string | null;
  error_code?: string | null;
  error_message?: string | null;
  finished_at?: string | null;
}

export const TERMINAL_OPERATION_STATES: readonly OperationState[] = [
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
];

export function isTerminalOperation(state?: string): boolean {
  return !!state && (TERMINAL_OPERATION_STATES as readonly string[]).includes(state);
}

export const OPERATION_STATE_META: Record<string, { label: string; color: string }> = {
  PENDING: { label: '排队中', color: 'default' },
  RUNNING: { label: '执行中', color: 'processing' },
  SUCCEEDED: { label: '成功', color: 'success' },
  FAILED: { label: '失败', color: 'error' },
  CANCELLED: { label: '已取消', color: 'warning' },
};

// ---------------------------------------------------------------- 数据库

/** 生命周期状态：COLD / STARTING / WARM / HOT / DRAINING / STOPPING / FAILED */
export type DatabaseState = string;

export const DATABASE_STATE_META: Record<string, { label: string; color: string }> = {
  COLD: { label: '冷（已停止）', color: 'default' },
  STARTING: { label: '启动中', color: 'processing' },
  WARM: { label: '温（热备）', color: 'cyan' },
  HOT: { label: '热（运行中）', color: 'success' },
  DRAINING: { label: '排空中', color: 'warning' },
  STOPPING: { label: '停止中', color: 'processing' },
  FAILED: { label: '失败', color: 'error' },
};

/** Dashboard 状态分布统计使用的状态（后端不认识的状态会返回 400） */
export const DATABASE_STATE_CANDIDATES: string[] = [
  'COLD',
  'STARTING',
  'WARM',
  'HOT',
  'DRAINING',
  'STOPPING',
  'FAILED',
];

/** 资源预算（BudgetView） */
export interface BudgetView {
  cpu_milli: number;
  memory_mib: number;
  fd_limit: number;
  disk_mib: number;
  iops_limit: number;
}

/** 数据库视图（DatabaseView）：主键是 `id` */
export interface Database {
  id: string;
  tenant_id: string;
  name: string;
  state: DatabaseState;
  owner_worker_id?: string | null;
  owner_epoch: number;
  priority: number;
  evictable: boolean;
  wakeup_in_progress: boolean;
  storage_region: string;
  storage_prefix: string;
  engine_version: string;
  budget: BudgetView;
  created_at: string;
  updated_at: string;
  deleted_at?: string | null;
  lease_expires_at?: string | null;
  last_snapshot_id?: string | null;
  last_snapshot_lsn?: number | null;
}

/** 列表过滤参数：`state` 只接受生命周期状态；`tenant_id` 必须是合法 UUID，否则 400 */
export interface DatabaseListQuery extends PageQuery {
  state?: string;
  tenant_id?: string;
}

/** 创建数据库（CreateDatabaseRequest）：只有 name 必填 */
export interface CreateDatabaseRequest {
  name: string;
  tenant_id?: string | null;
  storage_region?: string | null;
  cpu_milli?: number | null;
  memory_mib?: number | null;
  disk_mib?: number | null;
  fd_limit?: number | null;
  iops_limit?: number | null;
  priority?: number | null;
  evictable?: boolean | null;
  labels?: unknown;
}

export interface MoveDatabaseRequest {
  target_worker_id?: string | null;
}

export interface RestoreDatabaseRequest {
  snapshot_id?: string | null;
}

// ---------------------------------------------------------------- Worker

/** Worker 资源用量（UsageView） */
export interface WorkerUsage {
  cpu_milli_used: number;
  memory_mib_used: number;
  fd_used: number;
  disk_mib_used: number;
  process_slots_used: number;
  iops_used: number;
}

/** Worker 容量（CapacityView） */
export interface WorkerCapacity {
  cpu_milli_total: number;
  memory_mib_total: number;
  fd_total: number;
  disk_mib_total: number;
  process_slots_total: number;
  iops_total: number;
}

/** Worker 视图（WorkerView）：主键是 `id`，本节点数据库列表是 `running_databases` */
export interface Worker {
  id: string;
  endpoint: string;
  control_endpoint?: string | null;
  data_endpoint?: string | null;
  state: string;
  region: string;
  zone: string;
  version: string;
  missed_heartbeats: number;
  reserved_for_failover: boolean;
  capacity: WorkerCapacity;
  usage: WorkerUsage;
  running_databases?: Database[];
  last_heartbeat_at?: string | null;
}

export const WORKER_STATE_META: Record<string, { label: string; color: string }> = {
  ACTIVE: { label: '在线', color: 'success' },
  DRAINING: { label: '排空中', color: 'warning' },
  DRAINED: { label: '已排空', color: 'default' },
  SUSPECT: { label: '可疑', color: 'warning' },
  UNHEALTHY: { label: '异常', color: 'error' },
  OFFLINE: { label: '离线', color: 'default' },
  DEAD: { label: '失联', color: 'error' },
};

// ---------------------------------------------------------------- 快照 / 审计 / 慢查询

/** 快照视图（SnapshotView）：主键是 `id` */
export interface Snapshot {
  id: string;
  database_id: string;
  state: string;
  size_bytes: number;
  base_lsn: number;
  checksum: string;
  compression: string;
  engine_version: string;
  object_key: string;
  owner_epoch: number;
  created_at: string;
  verified_at?: string | null;
}

/** 审计视图（AuditView）：`id` 是自增整数，操作者是 `actor_name` */
export interface AuditLog {
  id: number;
  actor_name: string;
  action: string;
  target_type: string;
  target_id: string;
  result: string;
  detail: unknown;
  created_at: string;
  actor_id?: string | null;
  tenant_id?: string | null;
  database_id?: string | null;
  error_code?: string | null;
  request_id?: string | null;
  source_ip?: string | null;
}

/** 慢查询视图（SlowQueryView）：时长为微秒，语句字段是 `sql_text` */
export interface SlowQuery {
  id: number;
  database_id: string;
  sql_text: string;
  fingerprint: string;
  duration_micros: number;
  rows_returned: number;
  created_at: string;
  error_code?: string | null;
  session_id?: string | null;
  worker_id?: string | null;
}

/** 慢查询 / 快照列表都要求 database_id 必填 */
export interface DatabaseScopedQuery {
  database_id: string;
  limit?: number;
}

// ---------------------------------------------------------------- Token / 偏好 / Saved SQL

/** Token 视图（TokenView）：主键是 `id`，权限列表是 `permissions` */
export interface TokenInfo {
  id: string;
  name: string;
  permissions: string[];
  created_at: string;
  tenant_id?: string | null;
  database_id?: string | null;
  expires_at?: string | null;
  last_used_at?: string | null;
  revoked_at?: string | null;
}

/** 创建 Token 响应（TokenCreated）：明文 token 只返回一次 */
export interface TokenCreated {
  id: string;
  name: string;
  token: string;
  permissions: string[];
  created_at: string;
  expires_at?: string | null;
}

/** 创建 Token 请求（CreateTokenRequest）：过期时间用绝对时间戳 `expires_at` */
export interface CreateTokenRequest {
  name: string;
  permissions?: string[];
  expires_at?: string | null;
  tenant_id?: string | null;
}

export interface SavedQuery {
  id: string;
  name: string;
  sql: string;
  tags: string[];
  user_id: string;
  created_at: string;
  updated_at: string;
  database_id?: string | null;
  description?: string | null;
}

export interface CreateSavedQueryRequest {
  name: string;
  sql: string;
  database_id?: string | null;
  description?: string | null;
  tags?: string[];
}

export interface LoginRequest {
  username: string;
  password: string;
}

/** 登录响应：平台自签 JWT */
export interface LoginResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
}

/** 偏好视图（PreferenceView）：PUT 时必须包成 { value }，GET 返回的是整个对象 */
export interface PreferenceView<T = unknown> {
  key: string;
  value: T;
}

// ---------------------------------------------------------------- 数据面（Data API）

export interface QueryRequest {
  sql: string;
  params?: unknown[];
}

/** 列描述（ColumnView） */
export interface ColumnView {
  name: string;
  type_name: string;
  nullable: boolean;
}

/** 结果集（ResultSetView） */
export interface ResultSetView {
  columns: ColumnView[];
  rows: RowData[];
  affected_rows: number;
  truncated: boolean;
}

/** 单条查询响应（QueryResponse）：比 ResultSetView 多出 elapsed_micros / request_id / wal_lsn */
export interface QueryResult extends ResultSetView {
  wal_lsn: number;
  elapsed_micros: number;
  request_id: string;
}

/** 批量执行响应（BatchResponse）：结果是 `results` 数组 */
export interface BatchResponse {
  results: ResultSetView[];
  wal_lsn: number;
  elapsed_micros: number;
  request_id: string;
}

/** 行数据：按 columns 顺序的数组 */
export type RowData = unknown[];

export interface BatchStatement {
  sql: string;
  params?: unknown[];
}

export interface BatchRequest {
  statements: BatchStatement[];
  atomic?: boolean | null;
}

/** 会话创建响应（SessionOpened） */
export interface SessionOpened {
  session_id: string;
  database_id: string;
  worker_id: string;
  expires_at_unix_ms: number;
  request_id: string;
}

/**
 * NDJSON 流式 chunk（与后端逐行输出的三型帧一致）：
 *   {"columns":[...],"type":"header"}
 *   {"type":"row","values":[...]}
 *   {"affected_rows":0,"elapsed_micros":330,"type":"trailer","wal_lsn":0}
 */
export interface StreamHeaderChunk {
  type: 'header';
  columns: ColumnView[];
}
export interface StreamRowChunk {
  type: 'row';
  values: unknown[];
}
export interface StreamTrailerChunk {
  type: 'trailer';
  affected_rows: number;
  wal_lsn: number;
  elapsed_micros: number;
}

export type QueryStreamChunk = StreamHeaderChunk | StreamRowChunk | StreamTrailerChunk;

export function isHeaderChunk(c: QueryStreamChunk): c is StreamHeaderChunk {
  return (c as { type?: string }).type === 'header' || Array.isArray((c as Partial<StreamHeaderChunk>).columns);
}
export function isRowChunk(c: QueryStreamChunk): c is StreamRowChunk {
  return (c as { type?: string }).type === 'row' || Array.isArray((c as Partial<StreamRowChunk>).values);
}
export function isTrailerChunk(c: QueryStreamChunk): c is StreamTrailerChunk {
  return (c as { type?: string }).type === 'trailer' || typeof (c as Partial<StreamTrailerChunk>).affected_rows === 'number';
}

/** 列名提取：列是 ColumnView 对象，表格只需要名字 */
export function columnNames(columns?: Array<ColumnView | string> | null): string[] {
  if (!columns) return [];
  return columns.map((c) => (typeof c === 'string' ? c : c.name));
}
