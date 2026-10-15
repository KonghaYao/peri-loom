# 第 6 轮 Rust Worker 断言报告独立复核（2026-10-14）

对象：`docs/experiment-commercial-db/06_experiment_report.md` 与 `trial-ground/private/rust-full-after-p4-contract-2026-10-14.log`、`prototypes/v1/tests/statement_worker.rs`。仅静态读取，未执行负载、服务或 SQL，也未复制私有日志或凭据。

| 主张 | 判定 | 核验范围 |
| --- | --- | --- |
| 修订后的 P4 通过而 Rust 全套仍失败 | **PASS（日志中的已执行部分）** | 本轮日志中 Rust lib **127/127**、P2 `p2_verify` **15/15**、P4 `p4_verify` **15/15** PASS；`statement_worker` **17 passed; 1 failed; 0 ignored; 0 filtered out**，以 `error: test failed, to rerun pass --test statement_worker` 结束。不能用局部 PASS 覆盖整轮 FAIL；Cargo 在此处停止，之后各测试套件的覆盖/结论不能推定。 |
| 具体失败位置及事实 | **PASS（断言事实） / NEEDS_MORE（完整响应语义）** | 唯一显式 FAIL 为 `statement_v2_owner_state_and_cleanup_ack`，日志记录 `tests/statement_worker.rs:746:5: assertion failed: rejected.get("session_state").is_none()`。源码第 743–746 行先在事务内对不存在的表执行 INSERT，接着检查错误 `ok=false`、`execution_completed=true`，随后期望 `session_state` 不存在。前两项断言已越过，最后断言失败支持**该字段存在**；日志未打印字段内容/owner 值或完整响应。测试在第 746 行中止，后续 COMMIT、owner=false、读回及关闭的断言（第 747–760 行）**未执行**。 |
| v2 合同错误、Worker 会话刷新回归或生产事务已提交 | **NEEDS_MORE** | 字段存在不证明事务提交、Unknown、安全隔离失效或真实数据差错；需要规范中已知执行完成错误的 session state 合同、原始脱敏响应、定向复现和版本/因果对照。当前日志和该断言无法区分“测试期望过旧”与“生产响应错误”，更不能单凭近期会话刷新 diff 确立因果。不得改断言掩盖首次失败。 |
| 完整命令、准确退出码与运行产物身份 | **PARTIAL** | 报告给出 `cargo test --locked -- --test-threads=2`，但该原始日志只显示构建/各测试套件输出和失败总结，不独立记录 shell 命令全文、工作目录、实际线程参数、退出码或运行前后源码/二进制哈希。日志足以证实非成功结果，但不能证明冻结版本、S3/release 运行或数值化 shell 退出码。 |
| 性能、场景及商业准入 | **NOT MEASURED / FAIL（若主张通过）** | 本轮为 Rust 正确性测试，不是 SQL 压力试验。ACK/s、请求 p50/p95/p99、Unknown/错误分母、CPU/RSS、对象存储请求、恢复耗时全部 **NOT MEASURED**；套件耗时 **10.32s** 不是请求延迟。S01/S04/S06/S08 未由本轮证实，P0–P6 联合 PASS 尚不存在，不得推断商用容量或 SpaceX 认可。 |

**历史轮次总体：PARTIAL。** 旧报告第 746 行失败与当时日志一致；新增全套通过记录不改写旧失败。新增核验如下，依然不能用 Rust 测试结果代替独立场景与商用性能门禁。

## 追加核验：修订后 Rust 全套

- **PASS（日志汇总数字，需精确定义）：** `trial-ground/private/rust-full-after-v2-contract-2026-10-14.log` 的 `test result:` 行逐行求和得到 **47 条汇总、525 passed、0 failed、12 ignored、13 filtered**；`statement_worker` 单独显示 **18/18 PASS**。但 47 **不是 47 个独立顶层套件**：日志有 **45** 条顶层 `Running unittests/tests/`、**1** 条 `Doc-tests`，以及 `remote_database` 测试内部子进程的一条 `1 passed; 13 filtered out` 汇总，之后外层 `remote_database` 又以 14 passed 汇总。因此“47 suites/525 passed”严格说是 **47 条 test-result 汇总、累计 525 次通过记录**；按顶层结果合计约 **46 套、524 passed**（内部同一测试被计入外层），不能称 525 个彼此独立的测试。原报告的求和数字正确，但“47 个套件”若解读为顶层套件则超出证据。
- **NEEDS_MORE（命令/退出码/身份）：** 日志末尾为 doc-tests `ok; 0 passed; 0 failed`，无失败结果，支持本次捕获的测试输出为绿色；**没有独立记录 shell 退出码 `0`**、完整 `CARGO_TARGET_DIR=… CARGO_BUILD_JOBS=2 cargo test --manifest-path … --locked -- --test-threads=2` 命令、起止进程标识、冻结源码/构建产物摘要。日志中的隔离 debug 构建路径不证明正在运行的二进制字节或 release 身份。报告写出的命令与退出码只能作为操作者陈述，不能由这份输出独立核实。
- **PARTIAL（ignored 覆盖）：** 12 ignored 可逐项按日志分类：`minio` **5**、`p4_completion` 真 S3 **4**（合计 **9 个 S3 网络/故障相关项**）；另有历史 SQLite 差分 **1**、opt-in 容量 trace **2**。不能把 12 个都称为 S3 测试，也不能把 ignored 真 S3 项计入 PASS；即使独占 S3 容器存在，也没有它们实际运行的证据。`--locked`、顶层 Rust 绿色及先前 Go 平台包绿色均不自动构成 P0–P6 联合/S3/release 正确性证明。
- **NOT MEASURED：** ACK/s、p50/p95/p99、Unknown、CPU/RSS、对象存储放大、恢复和容量曲线；这些 Rust 测试耗时不是数据库真实性能。原有 `statement_worker` **17/18 FAIL** 是较早版本的独立历史结果，本次 **18/18 PASS** 只证明当前日志对应测试输出通过，不证实发布环境、源代码与产物的冻结身份或修复对所有条件成立。
