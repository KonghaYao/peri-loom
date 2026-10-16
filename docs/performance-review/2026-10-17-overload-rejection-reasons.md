# W7 第一步：准入拒绝原因显式化与 goodput 统计（Go 平台侧）

日期：2026-10-17。工作树 `overload-admission-20261017`，分支 `perf/overload-admission-20261017`，基线 `1e9ce3b`。
范围：`prototypes/v1/platform` 的 Go 网关准入与统计导出。**未改 Rust 侧、未改表结构、未加依赖、未加 HTTP 端点。**

状态：**分类与统计已实现并自测通过；未做任何过载、容量或公平性测量，不宣称任何性能结论。**

## 1. 结论（做了什么，没做什么）

- 把原来只有一条 `429 quota_exceeded` 的准入拒绝，按其**真正作出决定的那一层**命名为 12 个稳定字符串常量
  （`rejectionReason`，`admission.go:8-58`）。原因在拒绝点产生，不在统一入口事后猜。
- 原因**不进响应体**：对外 `code` 与 body 字段一字未改（理由见第 5 节）。
  原因写入进程日志与既有 `/v1/metrics` 导出（`Metrics()`，`metrics.go:64`）。
- 新增分开统计：每原因计数、`admitted_requests_total`、`successful_requests_total`、
  `rejected_requests_total`，以及**只含成功请求**的 `successful_latency_ns_total`。
  既有 `action_latency_ns_total` 语义不变（仍含全部已作答请求），新增 `action_latency_semantics` 说明。
- **没有引入任何队列**。准入仍是"立即拒绝、不排队"，第 6 节给出三条上限的现状与常量。

## 2. 原因枚举表

| 原因值 | 产生点（本提交工作树） | 对外 code（未改） | 可见性 |
|---|---|---|---|
| `session_limit` | `admission.go:187` | 429 `quota_exceeded` | 计数/日志 |
| `concurrency_limit` | `admission.go:184`（active 满）、`admission.go:194`（为 writer 预留的车道满） | 429 `quota_exceeded` | 计数/日志 |
| `rate_limit` | `admission.go:211` | 429 `quota_exceeded` | 计数/日志 |
| `writer_busy` | `admission.go:191`（本库 owner 车道忙）；`gateway_sessions.go:495`（worker 证明 `sql_conflict` 且未进入执行） | 429 `quota_exceeded` / 409 `sql_conflict` | 计数/日志 |
| `classify_limit` | `admission.go:145` | 429 `quota_exceeded` | 计数/日志 |
| `close_lane_busy` | `admission.go:179` | 429 `quota_exceeded` | 计数/日志 |
| `quarantine` | `gateway_operations.go:13`（`ErrDatabaseQuarantined`） | 409 `database_isolated` | 计数/日志 |
| `memory_budget` | `gateway_sessions.go:348/373/399/408`（平台结果预算）；`gateway_sessions.go:490`（worker 结果预算） | 413 `result_limit` | 计数/日志 |
| `sql_bytes` | `gateway_http.go:204/287/293`（wire 上限）、`gateway_http.go:309`（`MaxSQLBytes`）；`gateway_sessions.go:492`（worker 语句字节预算） | 413 `request_limit` / 413 `sql_limit` | 计数/日志 |
| `deadline` | `gateway_sessions.go:487`：**仅当 worker 证明"未进入执行"** | 504 `sql_timeout` | 计数/日志 |
| `operation_capacity` | `gateway_operations.go:19`（`ErrOperationCapacity`，4096 条未决记录预算） | 503 `operation_registration_unavailable` | 计数/日志 |
| `operation_queue_full` | `gateway_operations.go:22`（`ErrOperationQueueFull`，持久追加队列 256 深度） | 503 `operation_registration_unavailable` | 计数/日志 |

拒绝点到 HTTP 应答的组装点：`gateway_http.go:271`、`gateway_http.go:277`、`gateway_sessions.go:141`
（三条 429 路径）、`gateway_sessions.go:472`（worker 已证明的拒绝）。

**为什么没有 `saturated` / `worker_saturated` 之类的值**：worker 自身的准入文案
（`sql_failure.go:57-77`：`writer admission queue full`、`worker execution lane saturated`、
`worker connection limit reached`、`worker admission quarantine` 等）目前被归并成
`sql_unavailable` 一个平台码。在 worker 给出自己的 cause class 之前，平台无法把它们拆成稳定原因，
因此本次**不命名**，如实留白（`sql_failure.go:16-19` 已记录该移除条件）。

## 3. 统计口径

导出通道：既有 `Gateway.Metrics()`（`metrics.go:64`），经既有 `/v1/metrics`（`management.go:263`，仅 admin）对外。
新增字段：

| 字段 | 口径 |
|---|---|
| `rejection_reasons_total` | 按原因计数；**只列已观测到的原因**（≤12 键） |
| `rejection_reason_values` | 封闭枚举全量名单，供消费者区分"已定义未观测"与"未定义" |
| `admitted_requests_total` | 该请求**持有准入 permit**（`gateway_http.go:164` 用 `permit != nil` 判定） |
| `successful_requests_total` | 最终 `code == ""` |
| `rejected_requests_total` | 带具名原因的拒绝，等于 `rejection_reasons_total` 之和 |
| `successful_latency_ns_total` | 按 action 的成功请求延迟累加，**拒绝请求绝不进入** |
| `rejection_reason_semantics` 等 4 个语义字符串 | 说明上面各口径 |

口径要点（必须与数字一起读）：

1. **`admitted` 与 `rejected` 不互斥**。`memory_budget`、`deadline`、worker 证明的 `writer_busy`
   发生在取得 permit 之后：请求被准入、随后被拒。只报其中一半都会歪曲事实，故两者都计。
2. **`deadline` 只在"证明未进入执行"时才算拒绝**。执行之后的 deadline 可能已提交，
   按研究文档"已可能提交后 deadline 不构成取消证明"，它**不计入拒绝**（用例
   `deadline after execution is not a refusal` 钉住）。
3. **`memory_budget` 可能在执行后发生**（`result_limit` 带 `execution_completed:true`）：
   被拒的是结果物化，不是语句执行，**不得据此推断语句未执行**。
4. **`outcome_unknown` 不是拒绝**。被隔离库上的会话收到 502 `outcome_unknown`，
   其 permit 仍被 retained；把它计成拒绝会把"未知"当成"被拒"，故不命名原因。
5. **有界性**：label 只有三个封闭集合——原因（12）、action（`gatewayAction`，11）、错误码（`sqlcontract` 词表）；
   数据库 ID、会话 ID、请求 ID、operation ID **从不成为 label**（高基数只出现在采样 trace）。

## 4. 有界性说明（条数 / 字节 / 等待）

准入路径**不排队**：每条车道满即拒（`return nil, <reason>`），没有缓冲区，
因此"用无界缓冲掩盖过载"在当前实现里不存在。三条上限的现状：

| 层 | 条数上限（常量） | 字节上限 | 最大等待 | 拒绝原因 |
|---|---|---|---|---|
| 租户并发车道 | `Quotas.MaxConcurrent`（默认 4） | 不适用 | 0（立即拒） | `concurrency_limit` |
| 租户会话席位 | `Quotas.MaxSessions`（默认 16） | 不适用 | 0 | `session_limit` |
| 令牌桶 | — | — | 0 | `rate_limit` |
| 分类车道 | `min(MaxConcurrent,128)+2`（`admission.go:144`） | 不适用 | 0 | `classify_limit` |
| 会话关闭车道 | 1（每租户） | 不适用 | 0 | `close_lane_busy` |
| 单库 writer | 1（owner 车道） | 不适用 | 0 | `writer_busy` |
| 请求字节 | — | `sqlWireLimit = 32 MiB`（`sql_upstream.go:23`）、`Quotas.MaxSQLBytes`（默认 65536） | 不适用 | `sql_bytes` |
| 结果字节 | — | `Quotas.MaxResultBytes`（默认 4 MiB） | 不适用 | `memory_budget` |
| 持久未决记录 | `operationCapacity = 4096`（`operation_state.go:10`） | 每条记录结构定长（无用户数据，只存摘要/ID） | 0 | `operation_capacity` |
| 持久追加队列 | `operationJournalQueueMaximum = 256`（`operation_commit.go:22`） | 单条 ≤ `operationJournalMaximumRecordBytes = 1 MiB`；合批 ≤ `operationJournalBatchMaximum = 32` 条 / `operationJournalBatchBytes = 1 MiB` | 提交者只等自己的 barrier，无额外等待窗口 | `operation_queue_full` |
| 整请求 | — | 请求体 ≤ 上述 wire 上限 | `RequestTimeout`（默认 30s，`gateway_http.go:92`；读 deadline 10s，写 deadline = timeout） | 超时后由上游/死线路径作答，**不是**准入拒绝 |

**结论**：条数与字节上限均已存在且有具名拒绝原因；等待上限在准入层为 0（不排队），
整请求层面的等待由 `RequestTimeout` 界定。**本次没有新增队列，也没有把拒绝改成排队**——
那会改变默认配置下的准入/拒绝结果（研究文档要求的行为中性）。

## 5. 为什么原因不进响应体（协议兼容）

`platform/web/rpc.js:36` 对 failure 对象做**严格键白名单**：只允许
`code`/`message`/`execution_completed`/`may_have_committed`，出现任何其它键即判为畸形回复并
**按 `outcome_unknown` 处理（会话从此不可用）**；`tests/ui-rpc.test.mjs:104` 正好钉住了
`{code:"quota_exceeded", extra:true}` 必须被当成畸形。因此给拒绝体加字段会把一次 429 变成
一次 unknown + 会话中毒。据此本次选择：

- 老 `code` 字符串一个不改（`quota_exceeded`/`sql_limit`/`result_limit`/`database_isolated`/
  `sql_timeout`/`sql_conflict`/`operation_registration_unavailable` 全部保留原值原状态码）；
- 更细的原因只出现在**既有**内部导出与日志；
- 变量 `TestGatewayRefusalReplyKeepsPublishedFailureFields` 把这一决定钉成回归（拒绝体只允许 `code`）。

若将来确实要让客户端看见细因，正确顺序是先改该白名单与其契约测试，并在 SDK/浏览器契约里同步；不在本次范围。

## 6. 用例与变异验证

新增用例（`admission_rejection_test.go`）：

| 用例 | 断言 |
|---|---|
| `TestAdmissionRefusalReasonsPerLane`（7 子例） | 六条车道 + 预留车道的拒绝原因各自正确，未满车道的准入返回 `none` |
| `TestGatewayRefusalReasonsAtTheRefusalSite`（9 子例） | 每层经 HTTP 端到端：状态码与原语义不变（429/413/409/504）且计数恰好命中该原因；含"执行后 deadline 不算拒绝"与"隔离库的 502 不算拒绝" |
| `TestOperationCapacityRefusalIsNamed` | 持久记录预算 / 追加队列 / 隔离 / 重放四条映射 |
| `TestRefusedRequestStaysOutOfSuccessfulLatency` | 一个慢拒绝（30 ms 后结果预算拒绝）+ 一个快成功：`successful_latency_ns_total["query"] == 0` 而 `action_latency_ns_total["query"] ≥ 25 ms`，并断言 admitted/success/rejected 三个计数 |
| `TestRejectionReasonVocabularyIsClosed` | 原因去重、符合 label 字符集、导出名单与封闭枚举一致、计数键 ⊆ 枚举 |
| `TestGatewayRefusalReplyKeepsPublishedFailureFields` | 拒绝体仅含 `code` |

变异验证（**故意改坏→确认报错→改回**，逐条实跑）：

| 变异 | 期望失败用例 | 实际输出 |
|---|---|---|
| 让所有请求进入 goodput 账户 | `TestRefusedRequestStaysOutOfSuccessfulLatency` | `a refused request entered the successful latency account: 31835708 ns` |
| `session_limit` 改报 `concurrency_limit` | `...PerLane/session_limit`、`...AtTheRefusalSite/session_limit` | `session seats: "concurrency_limit"` / `refusal account: map[concurrency_limit:1], want exactly one session_limit` |
| 去掉 `deadline` 的"已证明未执行"前置条件 | `.../deadline_after_execution_is_not_a_refusal` | `settled deadline became a refusal: map[deadline:1]` |
| 拒绝体加 `rejection_reason` 字段 | `TestGatewayRefusalReplyKeepsPublishedFailureFields` | `refusal body carries "rejection_reason", which the published clients reject` |

## 7. 未测量 / 不承诺

- **不宣称容量、公平性或收益 PASS**。本次没有跑过载实验（E7）、没有测 goodput 恢复、
  没有测拒绝洪峰对审计/日志的压力（研究文档 W7 明确要求后两项）。
- **未实现**：有界公平队列、为已进入执行的显式事务预留提交/回滚/清理资源、
  dispatch 前的 deadline 快速拒绝。这些都会改变默认配置下的准入结果，需单独决策。
- worker 侧独立内存/会话限制仍是不可分辨的单一 `sql_unavailable`（第 2 节末）。
- 管理面 `/v1/databases/*` 的 `quota_exceeded`（数据库数配额）与 Hrana 适配器的流水线预算
  **不在本次命名范围**：它们各自独立，且没有接入 `Gateway.Metrics()`。
- 计数口径均为**单网关进程**（沿用既有 `quota_scope`），跨进程/跨副本不聚合。
- 本文的"拒绝率/goodput"只在单进程计数器语义下成立，未与服务端吞吐或客户端体验做交叉校准。
