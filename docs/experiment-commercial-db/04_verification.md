# 第 4 轮 Rust 正确性报告：独立复核（2026-10-14）

核验对象：`docs/experiment-commercial-db/04_experiment_report.md`；只读核对 `trial-ground/private/rust-full-after-publish-2026-10-14.log`、`trial-ground/private/p2-reader-gc-recheck-2026-10-14.log` 和相关测试/GC 源码。未运行服务、SQL、测试或负载，未读取凭据。**PASS 仅指陈述有对应日志，不是正确性门禁通过。**

| 报告主张 | 结论 | 证据和边界 |
| --- | --- | --- |
| Rust 全套尝试失败、联合正确性门禁不能通过 | **PASS（失败事实） / PARTIAL（完整覆盖）** | 全套日志中 `src/lib.rs` 127 passed、0 failed；随后多个集成二进制完成，但 `tests/p2_verify.rs` 输出 `13 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out` 和 `error: test failed, to rerun pass --test p2_verify`。Cargo 在失败处停止，日志未给出此后所有集成测试/文档测试的完成结果；“全套”仅能表示发起了扩大范围的尝试，**不能**表示整个 workspace 所有测试均已跑完。以前的 Go 平台 PASS 不覆盖 Rust 失败。 |
| 全套退出码为 101 | **PARTIAL** | 日志明确是测试失败，符合 Cargo 测试失败的非零退出语义，但文件**没有单独的 shell exit-status/101 记录**；报告中的精确值 `101` 不能仅凭提供的原始日志独立核算。需保存命令包装器的退出码事件。 |
| 第一次失败在 `last_reader_handle_protects_lazy_segment_and_corruption_is_not_hidden` 的 `p2_verify.rs:498` | **PASS** | 全套日志指向 `tests/p2_verify.rs:498:5`，断言 `left: 1`、`right: 0`；源码是 `assert_eq!(collector.collect()?.marked, 0)`。这里 **实际 GC marked=1、预期=0**，不是“期望 1、实际 0”，也不是 SQL 查询失败。`src/gc.rs:70-105` 定义 `marked` 为本轮选出的候选 blob/tombstone 数；不是已删除对象数、丢失的 ACK 数或事务数。 |
| 单项重跑仍失败，0/1 PASS 且 13 filtered | **PASS（结果） / PARTIAL（命令细节）** | 单项日志复现**同一**函数和 `tests/p2_verify.rs:498:5` 的 `left: 1`、`right: 0`；汇总 `0 passed; 1 failed; … 13 filtered out`，该测试二进制的耗时 0.01s。日志没有保存完整命令行/环境、`--exact`、`--test-threads=1` 或单项 shell 退出码；13 filtered 与精确筛选**相容但不证明具体参数**。重复失败反驳“只能由全套两线程相互干扰才触发”的单一解释，仍不足以证明与并发完全无关或确诊根因。 |
| reader pin/GC 语义失效，已造成数据丢失 | **NEEDS_MORE（机制） / FAIL（数据丢失结论无证据）** | `prototypes/v1/tests/p2_verify.rs:493-498` 构造 orphan/blob，克隆 branch reader handle、释放一个 handle、在 `retained` 尚存时检查 `collect().marked == 0`。失败发生在首次断言：**后续**的 retained 读回、模拟内容破坏检测、恢复后重收集、释放 retained 后的回收及物理删除核查（第 499–512 行）本次均**未执行到**。尤其不能声称“已证实懒加载读失败/腐败被掩盖/真实用户数据已丢失”，也不能据此保证不会出现误删风险；需按 blob 引用、pin 和 tombstone 生命周期调查。 |
| 性能与场景准入 | **PASS（报告保守结论）** | 两份文件是 Rust 测试输出，没有业务负载、逐请求 ledger 或资源采样。**NOT MEASURED：** ACK/s、offered/actual/admitted、p50/p95/p99、Unknown/错误分母、CPU/RSS/磁盘、对象存储请求及恢复时间。`21.95s`（lib 测试）、`0.90s`（全套 p2 二进制）和 `0.01s`（单项 p2 二进制）是测试耗时，**不可解释为 SQL 性能、GC 清理延迟或吞吐**。S01/S04/S06/S08 的独立场景没有由这些测试完成，P0–P6 联合 PASS 不成立。 |

**总体：PARTIAL。** 已独立证实 lib 127/127 PASS、`p2_verify` 13/14 与单项 0/1 的相同断言失败及其位置/实际值；精确退出码 101、命令参数和整个 workspace 其余测试执行覆盖缺独立原始记录。保持这次失败，不调整断言凑 PASS；明确候选 blob 为何在 reader 存活时被标记，再以保留首败证据的单项与完整门禁重测。本文没有复制私有日志全文或私有凭据。
