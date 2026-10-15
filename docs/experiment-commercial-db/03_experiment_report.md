# 第 3 轮：已 ACK 镜像发布后的可见性回归（2026-10-14）

## 问题与改动

先前独立平台测试观测：Authority `publish-image` 已确认 HEAD 更新，但当前 Worker 上旧/新 SQL 会话查询新表均得到 400 `sql_error`，schema API 返回 `[]`；原始失败证据为私有 `trial-ground/private/platform-go-isolated-recheck-2026-10-14.log`。这只证明该用例的可见性失败，不证明持久化数据丢失。

根因调查发现：已加载 Worker 使用发布前 Authority 会话的旧 HEAD 视图。修复在 `prototypes/v1/src/storage.rs`、`runtime.rs`、`ownership/runtime.rs` 和 `worker_rpc/{sessions,session_entry}.rs` 中增加对**初始空映像**外部 HEAD 更新的识别；在没有活跃事务/忙碌会话时卸载旧实例并重绑会话，避免活动事务被静默改写。初始映像之外的通用同步/并发边界**未**由本修复证明。

## 修复后定向验证

| 检查 | 结果与证据 |
| --- | --- |
| 定向 Rust 存储回归和 lib 单测 | 实现协作者报告 Rust lib 127/127 PASS；此处无独立原始日志，需独立复核后方可计入联合门禁。 |
| `TestPublishImageInvalidatesAlreadyLoadedWorker` | **PASS**（4.72 秒）；私有逐项 JSON 输出 `trial-ground/private/publish-image-regression-2026-10-14.jsonl`。 |
| `TestPublishedImageVisibleAfterFreshWorkerMigration` | **PASS**（4.85 秒）；同一逐项 JSON 输出。 |
| `TestP6VerifyScopedTokensAndNativeReadonly` | **PASS**（8.72 秒）；私有逐项 JSON 输出 `trial-ground/private/p6-scoped-token-recheck-2026-10-14.jsonl`。该时间来自本次保存的单项记录，不覆盖第 2 轮无原始记录的旧时间陈述。 |
| Rust 二进制身份 | 定向发布测试前后 SHA256 均为 `58a427e31ff9c2082f77fd2b925bc65eafa307d90570da1c002c7beb484f2e17`。这是隔离 debug 二进制，不是冻结的 release/S3 发布产物。 |

后续 `GOMAXPROCS=2 PERI_RUST_BINARY=/private/tmp/peri-p6-isolated-target/debug/peri-loom-v1 go test -count=1 -p 1 -timeout 10m ./...`（工作目录 `prototypes/v1/platform`）平台包 **PASS**（148.884 秒），`cmd/peri` 和 Go SDK 子包亦 PASS，`cmd/peri-platform` 无测试文件；原始输出 `trial-ground/private/platform-go-full-after-publish-2026-10-14.log`。运行前后隔离 Rust 二进制的 SHA256 相同；尚缺完整源码冻结、联合各阶段/S3 故障和 release 产物证据，因此仍**不能**宣称 P0–P6 联合 PASS、生产安全或商用容量达标。

## 性能状态

ACK/s、p50/p95/p99、Unknown、资源峰值、恢复时长、对象存储放大等实际数据库指标：**NOT MEASURED**。独占 S3 桶已就绪但数据库夹具/联合门禁仍未就绪，尚未运行 `peri-stress`。不得将上述单测时间当成 SQL 请求延迟，或将 RustFS 独立容器的空闲 RSS 当成数据库性能数据。

本修复对仍为空的初始映像增加了发布状态检查；其可能增加 Authority/对象存储请求并影响 p99，这是**待验证的性能风险**，不是实测回归。正式基准应在冻结版本上单列空库/已加载库场景，记录按请求归属的 HEAD GET 次数、耗时与 ACK/s，不能由 Rust 单测推断开销。
