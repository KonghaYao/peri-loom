# E01 连接复用实验（实施方案 A）

日期：2026-10-16。基线提交：`80d3fcdb`（skill 与实测报告）、`b6d28512`（调优方案）。
状态：**实现与协议测试已完成并验证；真实 A/B 实测被环境阻塞，未取得平台侧数据**。

## 1. 实现（默认关闭，不改变既有语义）

| 项 | 内容 |
| --- | --- |
| 开关 | `run_platform_queries.py --connection-mode per-request|reuse`，**默认 `per-request`** |
| reuse 语义 | 每个 worker 线程（= 每个会话）独占一条持久连接；必须读完整响应后才复用；结束显式 `close_connections()` |
| 安全边界 | 保持 CA 校验、Authorization 不落日志、禁重定向与代理；失活/断连/半响应一律 `outcome_unknown` 且**不重发已派发请求**；只有"尚未派发的下一次操作"可新建连接（会话按契约停用后必须显式新开会话） |
| 真实计数 | 报告 `transport.observed.{connections_created, requests_sent}`：reuse 为真实计数；per-request 走 urllib 路径**不计数**，如实 null，不用请求数推断 |

## 2. 协议测试证据（mock，全量 52 项通过）

| 用例 | 断言 | 结果 |
| --- | --- | --- |
| reuse 单 worker 5 次查询 | connections_created=1、服务端接受连接=1、requests=6 | 通过 |
| reuse 4 worker 并发 | 连接=4，无跨 worker 共享同一流 | 通过 |
| per-request 模式 | 连接随请求增长（≥4） | 通过 |
| 服务端断连 | 该请求 outcome_unknown 且未重发（服务端计数 +1）；会话停用；新会话才新建连接 | 通过 |
| 半响应 | outcome_unknown，未重发 | 通过 |
| 清理 | close_connections() 显式且幂等 | 通过 |

## 3. 真实 A/B 实测：环境阻塞（证据）

计划：新栈 + 100k fixture（同 manifest/SQL），C1 主对照，AB BA AB 交替 ≥3 轮。

实际：**三次新建本地完整栈均在 dev.py 就绪检查阶段被平台拒绝**，无法取得端点：

| 尝试 | 端口 | run 目录 | 结果 |
| --- | --- | --- | --- |
| 1 | 29444 | `~/.peri-e01/live2-20261016T101256Z` | `project stopped: request rejected: operation_incomplete` |
| 2 | 29444 | `~/.peri-e01/live2-20261016T101701Z` | 同上 |
| 3 | 29445 | `~/.peri-e01/live3-<stamp>` | 同上（控制目录内 `launcher3.log`） |

- 同一命令行当日更早两次运行（端口 19443 / 29443）**可以就绪**，故非参数问题；表现为平台侧数据库状态操作持续 `operation_incomplete`（与本轮此前 `peri create` 现象一致）。
- 未改平台代码/配置绕过，未调超时或配额；按"少量定向排查后如实报告"处理，未继续新建 fixture。
- 三个 run 目录保留为证据、未删除；无进程残留（29444/29445 无监听）。

## 4. 噪声与并行工作披露

- 测量窗口内主机有并行工作流进程（此前观测 30+ 个），且本轮期间父线程启动额外 worktree 调研（Gateway 固定成本、Rust 提交路径）。共享主机结果一律按探索性对待。
- 本次未取得平台侧数值，故不存在被噪声污染的比值；**AB 交替复测证据待环境恢复后补齐**。

## 5. 平台源码/构建摘要（供 worktree 对齐，只读）

- 仓库 HEAD 含本任务三个提交；工作树另有**他人 52 个未提交路径**（`prototypes/v1/**`）。
- turso 引擎 submodule：`24a3a4609683b73935888f8bc30b47727d100aa8`（未移动）。
- 平台二进制（私有构建，本轮复用未重建）：`control-20261016T082844Z/peri-platform`，sha256 前 16 位 `869b75df…`；Rust 数据面 `prototypes/v1/target/debug/peri-loom-v1`。
- 因工作树被并行修改，平台侧源码 revision **未冻结**；如需冻结基线请以二进制摘要 + HEAD + 时间戳为准。

## 6. 未测边界与下一步

- 未测：reuse vs per-request 的真实吞吐/p99 差异、C16 补充、570k/8M 放大、首请求 vs 稳态对比。
- 下一步（环境恢复后，同一 owner）：起新栈 → 100k 导入与回读 → AB BA AB 交替 C1 各 3 轮 → 中位吞吐提升 ≥20% 或关键 p99 显著改善且超出噪声才升 570k；否则记录"无突破"。
- 本开关默认关闭：**未默认启用**，无收益时不改变既有行为。

## 7. 范围与口径修正（2026-10-16，按 v1.1.4 基线与限额语义）

### 7.1 版本标注（不做混合版本对比）

- 本报告此前的平台观测（100k/570k 查询、8M 导入中止）来自当时工作树 HEAD `1faafd05` +
  私有构建二进制 `869b75df…`；**最新 main / v1.1.4 `997abbd` 未被测量**，
  不得声称"最新 main 已测"，也不把两版数据混成一条对比。
- 连接复用实现属本技能代码，与平台版本无关；其协议测试为 mock 级，不对应任何平台版本。

### 7.2 并发语义纠正（撤回旧表述）

- 准入由 quota 控制：`admission.go:42` 校验 `MaxSessions`/`MaxConcurrent`/`RequestsPerSecond`
  等为正值，`:104` 在 `active >= MaxConcurrent` 或 `sessions >= MaxSessions` 时拒绝。
- **配额中的 sessions 是会话上限，不等于执行并发**。因此：
  - **正常容量档 = C1 / C2 / C4**；
  - **C16 / C100 属过载档**（本实验在 C16/C100 观测到 83–85% 拒绝）。
- 此前"C16 可准入 16 并发执行"的说法**撤回**；旧表中 C16/C100 行只作为过载行为观测，
  不构成并发执行能力或吞吐对比。
- 工具默认仍为 100（用户要求保留），但工具与报告必须**标注为过载档默认**；
  容量结论只能在 C1/C2/C4 上给出。未修改平台 quota。

### 7.3 传输层事实（分工与分层）

- 外部 Go SDK `sdk/go/client.go:61` `transport.DisableKeepAlives = true` → **外部连接不复用**，
  这正是本实验 `--connection-mode reuse` 要验证的客户端侧变量。
- Gateway→Worker 已复用：`sql_upstream.go:105-107`（`MaxIdleConns 64`、`MaxIdleConnsPerHost 16`、
  `IdleConnTimeout 5s`）。
- 另注：native 会话 idle 为 5 **分钟**（`gateway_native_idle.go:11`），与 5s 连接 idle 属不同层，
  不并入本实验结论。
- 我负责：外部 SDK/压测客户端的连接复用与**连接/TLS/请求分段计时**；
  内部两层、WAL 与 Gateway 队列由其他 owner 负责，我不处理其文件。

### 7.4 待补测试与 SDK 补丁策略

- 必须新增 **Go transport 隐式重试风险测试**：标准库对幂等请求可能自动重发；
  需证明它不会掩盖"SQL 绝不自动重发、断连即 Unknown"的边界。
- SDK 文件当前**无并行改动**（`git status -- prototypes/v1/platform/sdk` 为空）；
  即便如此，仍按指示**在独立 worktree 从核实基线建立补丁**，不覆盖主树；
  若主树出现并行改动，先给出精确冲突文件与行段再决策。
- 本轮**未写 SDK 补丁**（先前的三次建栈阻塞占满预算），下阶段在 worktree 内实现并测试。

### 7.5 时隙与残留

- 本轮结束时**无自有进程残留**，也未启动任何性能任务（三次建栈均失败，
  `operation_incomplete`，见第 3 节）→ **性能时隙已释放**，重型任务可安排，
  但仍建议与其他 owner 错峰。

## 8. 修正与补充（2026-10-16 二轮）

### 8.1 归因措辞修正（此前不准确）

- 第 3 节所述三次就绪失败（`project stopped: request rejected: operation_incomplete`）**只是待证假设**，
  不是已归因结论；需 operation journal/controller reconcile 的 owner 确认收敛条件。此前写成"归属"已作废。

### 8.2 版本关系（不得混合对比）

- 既有平台测量来自旧工作树 `1faafd05` + 私有二进制 `869b75df…`；SDK 补丁与测试建在**干净基线
  `997abbd`（v1.1.4）** 的独立 worktree；两者不可混比，也不得称"最新 main 已测"。

### 8.3 Go 重试路径（go1.23.4 实际源码）

- `net/http/transport.go:756 func (pc *persistConn) shouldRetryRequest(req *Request, err error) bool`
  （由 `:664` 调用）；`:781` 注释明确"从未写出任何内容 ⇒ 可安全重试"；`:785 if !req.isReplayable()`
  则**不重试**。
- `request.go:1543 isReplayable()`：仅当 `Body==nil||NoBody||GetBody!=nil` 且
  方法 ∈ {GET,HEAD,OPTIONS,TRACE} 或带 `Idempotency-Key`/`X-Idempotency-Key` 时为 true。
- 结论边界：外部 SDK 请求为 POST + 非 nil Body + `GetBody=nil` + 无幂等头 ⇒ **已写出的请求不会被重发**；
  **未派发**（从未写出）的请求允许重新建连，二者语义不同，不得泛化为"一律永不重发"。

### 8.4 外部网关 idle 事实与客户端取值

- `997abbd` `service.go:276`：`http.Server{ReadHeaderTimeout 5s, ReadTimeout 15s, WriteTimeout 35s,
  **IdleTimeout 30s**, MaxHeaderBytes 32KiB}` → 外部 keep-alive 空闲上限 **30s**。
- 客户端按事实取 **20s**（低于服务端上限，避免复用被服务端先关闭的连接）；
  **不采用**内部 Gateway→Worker 的 5s（`sql_upstream.go:105-107`，属不同连接层）。
- 内部 5s 与外部 20s 的"对齐"仅为只读建议，本轮不改动任何平台/内部配置。

### 8.5 测试结果（worktree 分支 `e01-sdk-connection-reuse`，commit 见报）

- 9 项定向测试全部通过：单连接复用、默认逐请求、已收到请求后断连**仅派发一次**且 `outcome_unknown`、
  显式 `Connection: close` 后重连、静默关闭空闲连接后重连、响应截断不重放、并发不混读、显式清理幂等。
- 全量 `go test ./sdk/go/` 通过；并发相关用例 `-race` 通过（`GOMAXPROCS=2 GOFLAGS=-p=1`，未跑性能）。
- 未测边界：真实复用性能收益（无健康 fixture，A/B 仍阻塞）、写入中途 RST/代理截断等其它失败形态、
  v1.1.4 上任何平台性能数值。
