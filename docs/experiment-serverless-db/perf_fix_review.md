# `blocking_recv` 改动复核

结论：**PASS（静态审计及现有测试范围内）**。`blocking_recv` 仅在没有会话、没有活动事务，且本轮没有待处理命令时使用。原先发现的空闲会话延迟清理风险已由 `sessions.is_empty()` 条件消除；未发现其他明确回归风险。

| 检查项 | 结果 | 依据 |
|---|---|---|
| 普通请求与关闭 | PASS | 无会话、无事务时 `blocking_recv()` 收到新 `Execute` / `Stop` 就唤醒；`stop_worker` 会发送 `Stop` 并等待回复（`crates/database-host/src/lib.rs:275–299,454–469,591–605`）。通道所有 sender 都释放时返回 `None`，循环退出。 |
| 活动事务、deferred 与事务超时 | PASS | 有 `transaction_owner` 时仍走 `try_recv` + 10 ms 睡眠，循环顶部仍执行 `expire_sessions` 和 deferred 清理（`lib.rs:432–466,475–502,772–790`）；owner 清除后下一轮优先处理 deferred（第 449–455 行）。 |
| 空闲 session 过期 | PASS | 只要 `sessions` 非空，仍走 `try_recv` + 10 ms 睡眠，循环顶部主动执行 `expire_sessions`（`lib.rs:432–433,458–466,772–790`）。会话清空后才进入阻塞接收。 |
| deferred 请求 | PASS | 无事务时先 `pop_front`；只有本轮无 deferred 命令才会进入 `blocking_recv`。有事务时持续轮询并清理超时的 deferred 请求（`lib.rs:434–466,475–502`）。 |

`database-host` 的 11 个测试和 `db-server simple_http` 1 个测试已由主任务运行并通过；本复核仅审代码，未发压。静态审计不能替代新旧版本的延迟对比。
