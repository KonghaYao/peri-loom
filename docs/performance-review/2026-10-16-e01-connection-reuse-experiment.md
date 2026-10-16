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
