# F04 Worker 刷新锁等待边界验证（2026-10-15）

Worker RPC 在连接进入服务线程时记录单调时钟；SQL 请求的 session 入场、同库刷新锁等待及派发前检查共用当时的 `max_query_duration` 截止时间。刷新锁仍按 `DatabaseId` 隔离。到期时不开始后续 SQL 派发；同库锁等待返回类型化 `AdmissionError::Deadline`。打开和关闭 session 使用同一入场截止时间，但已经开始的 native open/close 与 SQL 引擎执行尚不能由这一层强制中断。

故障注入单测持有 A 库刷新锁 500 ms，验证 B 库可在自己的 250 ms 截止时间前取得锁，A 库另一个请求在 30 ms 截止时间返回。该测试只覆盖锁协调；没有注入真实 WAL 恢复或 native close 延迟，不能当作 F04 的 p99 验收。

验证命令与结果：

- `cargo test --lib worker_rpc`：26/26 通过。
- `cargo test --test writer_worker_rpc --test statement_worker`：可执行用例通过，2 个环境依赖用例仍按原设置忽略。
- `cargo test --tests -- --test-threads 1`：完整 Rust 集成套件退出 0。
- 首次默认并行 `cargo test --tests` 在 `p3_verify::process::sigkill_after_compacted_ack_excludes_dirty_tail_and_preserves_old_pitr` 遇到 CLI worker 的 5 秒 writer admission 超时；该单项独立重跑及后续串行全套均通过。该失败位于独立 CLI worker 路径，当前不能证明由本次 Worker RPC 修改导致，也不能忽略并行负载下的时间敏感风险。

F04 还需真实 1/5/20 秒慢打开与慢关闭注入，B/C 健康库持续负载，以及锁等待 p99、最大无进展间隔和完整 native 执行 deadline 数据。
