# 第 5 轮：Rust 全套继续暴露 P4 租约截止时间争议（2026-10-14）

P2 测试修订后，重新运行 Rust v1 workspace 的 `cargo test --locked -- --test-threads=2`；完整原始输出 `trial-ground/private/rust-full-after-p2-contract-2026-10-14.log`，本轮退出码 **101**。先前 Rust lib 与 P2 项目均通过，但 `p4_verify` 中 `leases::nonce_binding_exact_deadline_and_receipt_payload_conflict_are_fail_closed` **FAIL**（P4 组 13/14 PASS）。`prototypes/v1/tests/p4_verify/leases.rs:58` 预期时钟推进到 7000 后过期的 `grant(fresh)` 拒绝，实际未拒绝。不能据此立即认定双主、数据错误或时间戳实现缺陷，须复核 `Clock`、挑战/nonce 签发及读时刻约定。

该单项用相同隔离构建目录、显式 `--exact --test-threads=1` 再测仍 **FAIL**（0/1 PASS）；原始日志 `trial-ground/private/p4-lease-recheck-2026-10-14.log`。保留首次失败，不降低截止时间断言，停止后续实际发压。此前 P2 用例修订只纠正“未发现对象无需 pin”的测试预置，不构成 P4 问题的解决。

仍无数据库性能实测：ACK/s、延迟 p95/p99、对象存储请求与资源/恢复均 **NOT MEASURED**；P0–P6 联合门禁未通过，S01/S04/S06/S08 仍为 SKIP/NOT RUN。P4 租约问题定位/修复后应按相同源码/隔离构建重新运行定向及整套测试，并独立复核结果。

## 根因与对照测试（后续，不覆盖失败）

调查发现 challenge nonce 的有效期为 30 秒；原用例从时钟 2000 跳至 7000 只过去 5 秒，却错误地期待新 nonce 过期。修订 `prototypes/v1/tests/p4_verify/leases.rs` 在 7000 时验证仍可授权，另起精确边界用例：到期前 1 ms 可授权；到期瞬间拒绝且 `ProbeStore` 写入计数不增加；签发新 nonce 可授权。执行协作者报告定向 4/4、P4 全组 15/15 PASS；此处仍待完整工作区结果和独立原始证据复核。**未修改生产租约逻辑**，也未由旧断言失败证明真实旧 epoch 写入成功。

后续全套原始输出 `trial-ground/private/rust-full-after-p4-contract-2026-10-14.log` 另行记录修订后的 P4 模块 **15/15 PASS**，但整套在 `statement_worker` 模块 17/18 处失败；详见 `06_experiment_report.md`。P4 模块通过不能覆盖整套其他失败或准入真实压力测试。
