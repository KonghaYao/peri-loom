# 第 4 轮：Rust 全套正确性门禁发现 GC/reader 失败（2026-10-14）

## 方法和原始证据

在上一轮定向修复通过后，使用 `CARGO_TARGET_DIR=/private/tmp/peri-p6-isolated-target CARGO_BUILD_JOBS=2 cargo test --manifest-path prototypes/v1/Cargo.toml --locked -- --test-threads=2` 扩大到整个 Rust v1 workspace。原始私有输出 `trial-ground/private/rust-full-after-publish-2026-10-14.log`；退出码 **101**，完整联合正确性门禁 **FAIL**，先前已通过的测试不能覆盖这一失败。

`prototypes/v1/tests/p2_verify.rs` 中 `last_reader_handle_protects_lazy_segment_and_corruption_is_not_hidden` 首次报错：`collector.collect()?.marked` 期望 **0**，实际 **1**（`p2_verify.rs:498`），该测试模块统计 13/14 PASS、1 FAIL。未据此断言有已确认数据丢失，须核对 reader pin/GC 的具体语义与存储对象。首次失败输出已留存。

随后使用相同隔离构建目录只运行此测试，显式 `--exact --test-threads=1`，仍为 **FAIL**（0/1 PASS，13 filtered）；原始私有输出 `trial-ground/private/p2-reader-gc-recheck-2026-10-14.log`。这排除了“只在两线程执行时才触发”的单一解释，但不能证明生产环境相同行为或其根因；也不能擅自修改断言为 `1` 取得绿色。

## 性能/准入判定

本轮没有真实 SQL 流量，ACK/s、p95/p99、CPU/RSS/磁盘、Unknown、恢复及对象存储请求仍为 **NOT MEASURED**；S01/S04/S06/S08 仍未执行，不能签发 P0–P6 联合 PASS。GC/reader 安全语义未查清前停止进一步真实发压，保留首次失败现场及原有测试阈值。后续应定位并修复根因，单线程/全套各复跑一次，独立核验后再做准入判断。

## 根因复核与定向修正（后续，不覆盖首次失败）

源码检查显示 BranchStore 的 pin 在访问/列举到对象时才包含该对象；原测试只创建 reader handle，未 `get`/`list` 这个 orphan，就要求它受到 pin 保护，故 `marked=1` 并不单独证明 pin 失效。修订 `prototypes/v1/tests/p2_verify.rs`：先 `list` 以发现并建立 pin，再保留原始 `marked==0` 判定和读回/损坏判定；新增未发现 orphan 应可回收的对照测试，防止把任何 reader 都当全局保留锁。实现协作者报告修订后 P2 全组 15/15 PASS；完整 Rust workspace 复测与独立报告核验尚在进行。未修改 GC 的生产实现，也未据此宣称性能提升。
