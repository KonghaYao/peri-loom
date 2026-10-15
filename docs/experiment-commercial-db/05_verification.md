# 第 5 轮 Rust 门禁报告：独立复核（2026-10-14）

核验对象：`docs/experiment-commercial-db/05_experiment_report.md`。只读核对两份指定私有 Rust 日志、`prototypes/v1/tests/p4_verify/leases.rs` 的日志时期提交版与当前未提交差异，以及相关 nonce/时钟代码；未运行服务、SQL、测试或负载，未读取私有凭据。**PASS 仅指具体日志主张成立，不等于联合门禁 PASS。**

| 主张 | 判定 | 证据与限定 |
| --- | --- | --- |
| P2 修订后的 Rust 全套尝试在 P4 停止 | **PASS（已执行部分） / PARTIAL（全套覆盖）** | `trial-ground/private/rust-full-after-p2-contract-2026-10-14.log` 中 lib `127 passed; 0 failed`，`tests/p2_verify.rs` **15 passed; 0 failed**（上轮为 13/14，且新增了一个 P2 测试），而 `tests/p4_verify.rs` **13 passed; 1 failed**，随后 `error: test failed, to rerun pass --test p4_verify`。Cargo 在此处停止；缺后续测试二进制/文档测试完整结果，不能说整个 workspace 所有测试均运行完毕。此前 P2/Go 通过不能覆盖 P4 失败。 |
| 首败在 `leases::nonce_binding_exact_deadline_and_receipt_payload_conflict_are_fail_closed` 第 58 行 | **PASS（日志对应旧源码）** | 全套日志指向 `tests/p4_verify/leases.rs:58:5`，原始断言文本为 `assertion failed: authority.grant(fresh).is_err()`。在 `git show HEAD:prototypes/v1/tests/p4_verify/leases.rs` 的**提交版**第 55–60 行，`fresh` 在时钟 2000 创建、`clock.set(7000)` 后期待 `grant` 拒绝；这说明**旧测试的拒绝期望失败**，不是日志证明租约过期被生产系统接受、也不是实际双主写入。失败在其后的 store put_count 与 epoch 核查之前。 |
| 当前源码仍是日志所指的第 58 行拒绝断言 | **FAIL（版本不一致）** | 核验时 `leases.rs` 存在未提交修改，文件修改时间约 **22:42:12 CST**，晚于全套日志最后修改 **22:38:20** 及单项日志 **22:39:20**。当前第 57–58 行改为 `grant(fresh)` 成功且 epoch 为 11 的断言，并新增精确截止时间测试；因此**当前第 58 行不能用来解释旧日志的失败**。无冻结源码/测试二进制清单，不能据日志证明当前修订后的测试已通过或版本/二进制完全匹配。 |
| 时钟 7000 时 nonce 确已过期，必须拒绝 | **FAIL（按可见合同） / NEEDS_MORE（运行时全貌）** | `prototypes/v1/tests/p4_verify/support.rs:15-29` 的可控时钟以毫秒设置；`prototypes/v1/src/authority/ownership.rs:234-264` 的 challenge 以当时 now 加 **30,000 ms** 作为 nonce 截止，`prototypes/v1/src/authority/lease_commands.rs:309` 要求 `now < nonce.expires`。若 fresh 在时钟 2000 签发且未被其他规则撤销，其 nonce 截止应是 **32000**，7000 **早于**截止；旧断言把 lease TTL（1000 ms）与 nonce TTL 混为一谈的解释与当前代码相符。日志没有输出签发时间、nonce 到期时刻或内部判断轨迹，故不能从单一失败确诊所有租约/时钟边界正确，亦不能断言生产时间戳实现有缺陷。新增加的截止测试尚无本轮原始通过证据。 |
| 单项重跑失败（0/1 PASS） | **PASS（失败及数量） / PARTIAL（命令与线程）** | `trial-ground/private/p4-lease-recheck-2026-10-14.log` 为同名测试、同一旧断言文本和 `leases.rs:58:5`，汇总 `0 passed; 1 failed; 13 filtered out`。这支持旧失败可定向复现，不支持它只能在全套两线程下发生；日志未包含 `--exact --test-threads=1` 的完整命令，13 filtered 本身不独立证明执行线程数。 |
| Rust 测试退出码精确为 101；命令和构建版本相同 | **PARTIAL / NEEDS_MORE** | 两份日志尾部均是 `error: test failed`，**可以证明测试失败，不能单独复算精确 shell 退出码 101**；文件没有退出码事件、完整 `cargo test --locked …` 命令、工作目录/环境、源码与二进制摘要。两日志显示同一个 `p4_verify` 构建产物路径，与使用相同隔离目录**相容**，但路径相同不证明字节不变；要验证同版须保留前后哈希、冻结源清单、命令及退出状态。 |
| 性能或生产故障 | **NOT MEASURED / NOT PROVEN** | 日志是 Rust 单元/集成测试，不含独占 S3 夹具的业务负载、双主/旧 epoch ACK 或独立 oracle；不得据旧测试期望失败宣称双主、数据损坏或实际事故。ACK/s、offered/actual、p50/p95/p99、Unknown/错误分母、CPU/RSS、磁盘与对象存储请求、恢复时间均 **NOT MEASURED**。S01/S04/S06/S08 未由此证明，P0–P6 联合 PASS 不成立，商用性能及 SpaceX 审阅均无证据。 |

**总体：PARTIAL。** 日志充分支持“修订后 P2 15/15、旧版 P4 13/14、旧断言单项复现失败”；不支持精确退出码的独立验证、当前改动版本 PASS 或生产故障定性。保留旧失败记录，用冻结版本和显式命令/退出状态重新测试真正的 nonce 截止边界及全套门禁；不把旧阈值改动本身当成修复已验证。本文不复制私有日志全文或凭据。

## 后续日志更新

`trial-ground/private/rust-full-after-p4-contract-2026-10-14.log` 现提供修订后 `tests/p4_verify.rs` **15 passed; 0 failed; 0 ignored** 的包级原始证据（**PASS，仅该模块**）；同轮 Rust lib 127/127、P2 15/15。该日志随后在 `tests/statement_worker.rs` **17 passed; 1 failed** 处终止，故不能将当前修订后的 P4 PASS 改写为旧轮 P4 从未失败，或称工作区全套、P0–P6 联合 PASS。报告提及的定向 **4/4** 没有在本轮全套日志中提供对应逐项独立记录；完整命令/退出码/构建摘要亦未由该日志独立保存。新失败的范围见 `docs/experiment-commercial-db/06_verification.md`；真实性能仍 **NOT MEASURED**。
