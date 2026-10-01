/**
 * 错误码 -> 中文可读提示。
 * 后端统一错误体 { error: { code, message, request_id, retryable } }：
 *   - code 用于定位可读文案；
 *   - retryable=true 时前端展示「重试」按钮（见 components/ErrorAlert.tsx）；
 *   - request_id 便于对照服务端日志。
 */

/** 已知错误码的中文文案 */
export const ERROR_CODE_MESSAGES: Record<string, string> = {
  // 通用
  INVALID_ARGUMENT: '请求参数不合法',
  INVALID_REQUEST: '请求格式不合法',
  BAD_REQUEST: '请求格式不合法',
  UNAUTHENTICATED: '未认证或凭证缺失',
  UNAUTHORIZED: '认证失败，请重新登录',
  FORBIDDEN: '当前身份无权执行该操作',
  PERMISSION_DENIED: '当前身份无权执行该操作',
  NOT_FOUND: '请求的资源不存在',
  CONFLICT: '资源状态冲突，请刷新后重试',
  PRECONDITION_FAILED: '前置条件不满足',
  RATE_LIMITED: '请求过于频繁，请稍后重试',
  TOO_MANY_REQUESTS: '请求过于频繁，请稍后重试',
  TIMEOUT: '服务端处理超时',
  DEADLINE_EXCEEDED: '服务端处理超时',
  UNIMPLEMENTED: '后端尚未实现该接口',
  NOT_IMPLEMENTED: '后端尚未实现该接口',
  INTERNAL: '服务端内部错误',
  INTERNAL_ERROR: '服务端内部错误',
  SERVICE_UNAVAILABLE: '服务暂不可用，请稍后重试',
  UNAVAILABLE: '服务暂不可用，请稍后重试',
  NETWORK_ERROR: '网络异常，无法连接服务端',
  // 通用（后端 ErrorCode 实际取值）
  CANCELLED: '请求已被取消',
  CONSTRAINT_VIOLATION: '违反数据约束（UNIQUE / NOT NULL / 外键等）',
  IDEMPOTENCY_CONFLICT: '幂等键冲突：相同幂等键的参数与首次请求不一致',
  RESOURCE_EXHAUSTED: '资源已耗尽',
  QUOTA_EXCEEDED: '已超出配额',

  // Token
  TOKEN_INVALID: 'Token 无效',
  TOKEN_EXPIRED: 'Token 已过期，请重新登录',
  TOKEN_REVOKED: 'Token 已被吊销',
  TOKEN_NOT_FOUND: 'Token 不存在',

  // 数据库
  DB_NOT_FOUND: '数据库不存在',
  DATABASE_NOT_FOUND: '数据库不存在',
  DB_ALREADY_EXISTS: '同名数据库已存在',
  DB_NOT_RUNNING: '数据库当前未运行',
  DB_ALREADY_RUNNING: '数据库已在运行中',
  DB_STATE_INVALID: '数据库当前状态不允许该操作',
  DB_HAS_OPEN_SESSIONS: '数据库仍有打开的会话',
  DB_DELETING: '数据库正在删除中',
  DB_QUOTA_EXCEEDED: '已超出数据库配额',
  // Ownership / Epoch / Fencing（架构 §10）——
  // 这些码在「接管中 / 租约过期 / 路由缓存过期」时出现，必须让 DBA 看到真实原因，
  // 落到状态码兜底会显示成「资源状态冲突」，无法定位。
  NOT_OWNER: '当前 Worker 不是该数据库的 Owner（fencing 拒绝本次写入）',
  EPOCH_MISMATCH: 'Owner Epoch 已过期（存在更新的 Owner，请刷新后重试）',
  ROUTE_STALE: '路由信息已过期（stale route，可安全重试）',
  DATABASE_NOT_READY: '数据库尚未就绪（冷启动 / 恢复中）',
  WAKEUP_TIMEOUT: '冷启动等待超时：数据库未在期限内就绪，请重试或检查 Worker 健康',
  // Worker / 调度（后端实际取值）
  WORKER_UNAVAILABLE: '承载该数据库的 Worker 不可用',
  ADMISSION_DENIED: '准入被拒：资源预算或硬上限不足',

  // Worker / 调度
  WORKER_NOT_FOUND: 'Worker 不存在',
  WORKER_OFFLINE: 'Worker 已离线',
  WORKER_DRAINING: 'Worker 正在排空中，无法承载新数据库',
  WORKER_FULL: 'Worker 资源已满',
  NO_CAPACITY: '没有满足条件的 Worker 可调度',
  CAPACITY_EXCEEDED: '超出容量上限',
  SCHEDULER_UNAVAILABLE: '调度器不可用',
  MOVE_TARGET_INVALID: '迁移目标 Worker 不合法',

  // 操作 / 快照 / 备份
  OPERATION_NOT_FOUND: '操作记录不存在',
  OPERATION_ALREADY_FINISHED: '操作已结束，无法再次取消',
  OPERATION_CANCELLED: '操作已被取消',
  SNAPSHOT_NOT_FOUND: '快照不存在',
  SNAPSHOT_FAILED: '快照失败',
  BACKUP_FAILED: '备份失败',
  RESTORE_FAILED: '恢复失败',
  RESTORE_IN_PROGRESS: '恢复正在进行中',
  NO_SNAPSHOT_AVAILABLE: '没有可用快照',

  // SQL / 数据面
  SQL_SYNTAX_ERROR: 'SQL 语法错误',
  SQL_ERROR: 'SQL 执行失败',
  SQL_PARSE_ERROR: 'SQL 解析失败',
  SQL_NOT_SUPPORTED: '该 SQL 不被支持',
  QUERY_TOO_LARGE: '结果集过大，请增加 LIMIT',
  SESSION_NOT_FOUND: '会话不存在或已过期',
  SESSION_EXPIRED: '会话已过期',
  SESSION_BUSY: '会话正忙，请等待当前语句结束',
  TRANSACTION_NOT_ACTIVE: '当前没有活动事务',
  TRANSACTION_ALREADY_ACTIVE: '事务已经开启',
  WRITE_CONFLICT: '写入冲突，请重试',
  WAL_UNAVAILABLE: 'WAL 服务不可用',
  BATCH_PARTIAL_FAILURE: '批量执行部分失败（成功与失败并存，请逐条核对）',
  RESULT_TOO_LARGE: '结果集过大，请增加 LIMIT',
  // 会话 / 事务失效（架构 §12）——failover 后原连接上下文不可恢复
  SESSION_LOST: '会话已丢失（failover 后无法恢复原连接上下文，请重新建立会话）',
  SESSION_IDLE_TIMEOUT: '会话空闲超时已关闭，请重新建立会话',
  TRANSACTION_LOST: '事务已丢失（failover 后无法恢复，请重新执行）',
  TRANSACTION_STATE_INVALID: '事务状态非法（例如未 BEGIN 就 COMMIT）',
  TRANSACTION_MAX_LIFETIME_EXCEEDED: '事务超过最大生命周期已被终止，请拆分重试',
  // WAL 持久化（架构 §11）——
  // WAL_NOT_DURABLE / WAL_APPEND_REJECTED 语义是「本次未取得 durable 确认」，
  // 而非「一定没写进去」（quorum 成功但 ACK 丢失时事务可能已落地）。
  // 严禁提示用户直接重放整个写事务，否则会出现「报失败但已生效 + 重试再生效」的双写。
  WAL_NOT_DURABLE: 'WAL 未确认持久化：本次未取得 durable 确认，数据可能已生效，不要直接重放写事务',
  WAL_APPEND_REJECTED: 'WAL 拒绝该次写入（fencing / 配额 / 背压）：需先重新获取 Owner 或更换幂等键',
  WAL_NOT_LEADER: '当前节点不是 WAL leader（可换端点重试，不改变请求语义）',
  // 对象存储 / 快照
  // 注意：后端也用该码包装 Remote WAL 的所有权/追加失败（例如 fencing 拒绝 epoch 回退），
  // 所以文案不能只写「对象存储」，具体原因以同屏展示的服务端 message 为准。
  STORAGE_UNAVAILABLE: '存储依赖不可用（对象存储 / Remote WAL）：需由管理面携带 Idempotency-Key 显式重试',
  SNAPSHOT_UNAVAILABLE: '快照不可用（不存在 / 未完成 / 已损坏）',
  CHECKSUM_MISMATCH: '校验和不匹配：数据可能已损坏',

  // 偏好
  PREFERENCE_NOT_FOUND: '偏好未设置',
};

/** HTTP 状态码兜底文案 */
const STATUS_MESSAGES: Record<number, string> = {
  400: '请求参数不合法',
  401: '认证失败，请重新登录',
  403: '无权执行该操作',
  404: '后端未实现该接口或资源不存在',
  405: '后端不支持该请求方法',
  408: '请求超时',
  409: '资源状态冲突',
  // 410：会话 / 事务曾经存在但已不可恢复（failover、空闲超时）
  410: '会话或事务已失效，请重新建立',
  // 413：结果集超过服务端内联上限
  413: '结果集过大，请增加 LIMIT',
  422: '参数校验失败',
  429: '请求过于频繁',
  // 499：请求被取消（客户端断开或显式 Cancel）
  499: '请求已被取消',
  500: '服务端内部错误',
  501: '后端尚未实现该接口',
  502: '网关错误，后端不可达',
  503: '服务暂不可用',
  504: '网关超时',
};

export function messageForCode(code: string | undefined, httpStatus?: number): string {
  if (code && ERROR_CODE_MESSAGES[code]) return ERROR_CODE_MESSAGES[code];
  if (httpStatus && STATUS_MESSAGES[httpStatus]) return STATUS_MESSAGES[httpStatus];
  return '请求失败';
}

/** 依据 HTTP 状态码推断是否可重试（服务端显式 retryable 优先） */
export function retryableByStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}
