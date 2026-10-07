# Native session 有界忙等待

## 合同与范围

1. 每个新建 `RemoteSession` 的 native connection 初始化一次
   `set_busy_timeout(Duration::from_secs(1))`。不在每条语句的 `configure` 设置，
   避免反复改变 prepare generation；不修改 core fork 或旧默认 connection 的 busy 策略。
2. 同一 `Statement` 遇到 `StepResult::Sleep` 时按请求的 duration 等待，
   每次请求 sleep 不超过 10ms，并在等待前后检查原 query budget 与 local lease。
   这不是操作系统调度延迟的硬实时保证；短于 1s 的 SQL budget 仍优先失败。
3. `IO` / `Yield` 仍调用原 `_io().step()`，调用前后检查 budget 与 local lease。
   `Row`、`Done`、`Busy`、`Interrupt` 保持原终态语义，native error 保留在 source chain。
4. 等待只持有该 session 的 connection mutex；不增加全数据库串行锁，
   不重新 prepare、不重放 batch、不 retry SQL。持锁事务可以通过自身 session 提交，
   独立 reader 可以继续读一致快照。
5. Local check 不是新的完整 root 健康证明。既有 statement ingress、ownership、
   epoch、physical I/O、WAL、root CAS、checksum 与 AEAD fencing 都未修改。
   超时或失去权限只停止当前调用，不能撤回之前已获得 COMMIT ACK 的 batch 前缀。

## 离线证据

1. 新 reader/holder/writer 测试在修复前无法观察到等待，记录为 RED：
   `/tmp/peri-native-busy-progress-red.log`。修复后同一测试 GREEN：
   `/tmp/peri-native-busy-progress-green.log`。
2. `tests/busy_wait.rs` 六项：holder COMMIT 与 reader 进展、1s typed Busy 耗尽、
   40ms 写 query budget、入口过期无写、等待中 mock clock 到期或 revoke 无 physical I/O、
   已提交前缀后 constraint 的显式 ROLLBACK 只清理后续事务。
3. `tests/concurrent_sessions.rs` 两项同时覆盖 Memory / Local 后端。
   已明确持锁的竞争调用各只执行一次并返回 typed Busy；释放后使用不同业务 ID。
   四个新事务各调用一次，本轮全部 ACK，ledger / balance / integrity oracle 一致。
   这不是任意负载或任意持锁时间下保证全部成功的承诺。
4. 聚焦命令：
   `cargo test --test busy_wait --test concurrent_sessions --test p6_budget -- --test-threads=1 --nocapture`。
   本轮 6 + 2 + 7 = 15 PASS，日志 `/tmp/peri-native-busy-focused.log`。
   Scoped strict clippy 通过，日志 `/tmp/peri-native-busy-clippy.log`。
5. Lease expiry / revoke 测试使用明确的 local-check mock，并不声称替代真实 Authority
   RPC 或完整集群故障门禁。完整 safety / SDK / source gate 由统一验证者随后执行。

## 限制与待验证

1. 原 Round3 HTTP `sql_error` 没有足够原始 error message，离线 Busy 重现仅与该现象
   相容，不能证明原失败的根因。这里也没有新的性能收益结论。
2. 无真实 fixture 请求、load、signal 或 release build；保留旧 3c / b3 binary。
   新产物仅在最终门禁与显式部署授权后写入 `/tmp/peri-stress-native-busy-bin`。
3. 1s busy timeout 不是无限等待，也不是事务恢复机制。调用者必须区分已提交前缀、
   未提交事务及未知远端结果，不以自动 SQL 重放掩盖错误。
