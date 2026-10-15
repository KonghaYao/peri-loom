# 第 6 轮：Worker v2 终结错误响应断言（2026-10-14）

在 P4 测试合同修订之后，再次执行 Rust v1 workspace 全套 `cargo test --locked -- --test-threads=2`，私有原始输出 `trial-ground/private/rust-full-after-p4-contract-2026-10-14.log`。平台包 Go 全套先前 PASS、Rust 的 P2/P4 分项先前 PASS，**但本轮全套依然 FAIL**：`statement_worker` 模块 17/18 PASS，`statement_v2_owner_state_and_cleanup_ack` 在 `prototypes/v1/tests/statement_worker.rs:746` 断言失败。该断言期望一次 SQL 执行完成的错误响应不包含 `session_state`；实际响应存在此字段。还有哪些测试未执行，须以原始日志为准。

不能仅凭字段存在就推断事务已提交、执行未知或安全隔离失效。待核对 v2 协议对已知执行完成错误是否应该带入当前会话状态、是否存在与近期 Worker 会话刷新变更的因果联系，并做定向可复现验证。**首次失败日志不得覆盖或改期望凑绿色。**

没有实际压力运行；业务 ACK/s、p95/p99、Unknown、资源、对象存储和恢复指标全部 **NOT MEASURED**。尚无可签发的 P0–P6 联合 PASS，因此 S01/S04 及容量场景继续 SKIP/NOT RUN。

## 根因与修订后局部验证（后续）

实现对照显示 v2 对 `execution_completed=true` 的已知终结错误响应包含 `session_state`，用于表达未终止的事务归属；v1 保持不包含。旧用例混用了 v1 预期。修订 `prototypes/v1/tests/statement_worker.rs` 后不仅检查 v2 错误响应具有同一 session、owner=true、binding 未换且序列递增，还以同一会话请求 v1 对照验证无 `session_state`，然后提交事务并验证 owner=false。执行协作者报告修订后本模块 18/18 与错误合同 4/4 PASS；完整 Rust workspace 结果另行留存，局部绿色**不替代**联合门禁。未因本次失败改动生产协议逻辑。

后续 Rust workspace 全套原始输出：`trial-ground/private/rust-full-after-v2-contract-2026-10-14.log`，运行 `CARGO_TARGET_DIR=/private/tmp/peri-p6-isolated-target CARGO_BUILD_JOBS=2 cargo test --manifest-path prototypes/v1/Cargo.toml --locked -- --test-threads=2`，shell 退出码 **0**（该退出码未在日志中独立归档）。日志有 **47 条 `test result: ok` 汇总**，逐条相加为 **525 次 passed 计数、0 failed、12 ignored**；包含外层重复汇总，**不得**把 47/525 称为彼此独立的套件/测试。其中 `statement_worker` 18/18 PASS。12 ignored 中真 S3/故障相关 **9 项**未执行，不属于本轮 PASS；也没有 release/S3 全栈压力数据。
