# 第 3 轮实验报告独立复核（2026-10-14）

对象：`docs/experiment-commercial-db/03_experiment_report.md`。静态交叉核对定向 Go JSONL、前次失败日志、当前 Rust/Go 测试与生产代码差异及私有预检；未运行服务、SQL、测试或压力负载。下文 **PASS 仅表示所述局部证据相符**，不是 P0–P6 或商用性能 PASS。

## 结果与证据等级

| 报告主张 | 结论 | 核验结果与限制 |
| --- | --- | --- |
| 原始失败：发布 ACK 后旧/新会话 400，schema 空 | **PASS（测试观测）** | `trial-ground/private/platform-go-isolated-recheck-2026-10-14.log` 记录 `TestPublishImageInvalidatesAlreadyLoadedWorker` FAIL（4.34s），旧/新查询均为 400 `sql_error`、schema 为 200 且结果 `[]`。`prototypes/v1/platform/publish_image_visibility_test.go:26-45` 先载入空库，再通过 CLI 发布并要求旧/新会话和 schema 可见。测试验证 CLI 返回成功/manifest ID；无独立对象存储持久化读回，所以不能从旧失败推断已 ACK 数据丢失。 |
| 修复针对已加载 Worker 的**初始空映像**外部 HEAD 更新 | **PASS（静态 diff 范围） / PARTIAL（根因实证）** | `prototypes/v1/src/storage.rs` 增加空初始映像的 manifest 与新读 HEAD 对比及内存对象存储单测；`runtime.rs` / `ownership/runtime.rs` 暴露判别与卸载路径，`worker_rpc/sessions.rs` 在开会话和开始调用时刷新，`session_entry.rs` 拒绝忙碌/事务中会话并保留序号。可说明实现意图和范围；缺修复前后 HEAD/缓存状态的原始追踪，故“旧 Authority 会话 HEAD 视图”是与现象及 diff 一致的**根因推断**，非独立证实的唯一原因。busy/事务竞争、通用非初始映像替换、崩溃中途重绑等不由两个定向测试覆盖。 |
| 修复后两个发布用例 PASS（4.72s、4.85s） | **PASS（定向测试）** | `trial-ground/private/publish-image-regression-2026-10-14.jsonl` 是有效 Go 逐项事件：`TestPublishImageInvalidatesAlreadyLoadedWorker` `pass`/4.72s，`TestPublishedImageVisibleAfterFreshWorkerMigration` `pass`/4.85s；包 `pass`/10.467s，无 fail 事件。这证明在所测本地路径下旧失败用例恢复通过，**不能**证明所有 Worker、并发发布或 S3 持久化行为均通过。 |
| P6 scoped-token/只读用例 PASS（8.72s） | **PASS（定向测试）** | `trial-ground/private/p6-scoped-token-recheck-2026-10-14.jsonl` 逐项事件记录 `TestP6VerifyScopedTokensAndNativeReadonly` `pass`/8.72s，包 `pass`/10.356s；并未记录 P0–P6 全部用例。该时间是此轮真实逐项记录，不能追证第 2 轮未保存的单项时间。 |
| Rust lib 127/127 PASS | **NEEDS_MORE** | 原报告注明仅来自实现协作者转述；这两份 Go JSONL 和旧平台失败日志没有 Rust lib 的原始输出、源/构建摘要与命令退出码。不可计入已独立通过的联合门禁。 |
| 发布定向测试前后 Rust SHA256 都是 `58a427e31ff9c2082f77fd2b925bc65eafa307d90570da1c002c7beb484f2e17` | **PARTIAL（当前摘要一致） / NEEDS_MORE（前后身份）** | 只读重算当前 `/private/tmp/peri-p6-isolated-target/debug/peri-loom-v1` 的 SHA256 与该值一致；文件创建/修改时间 22:19:47 CST，早于两组 JSONL。**前、后 SHA256 只是报告中的命令输出陈述**：两份 JSONL 均无哈希事件，未独立保存命令/输出、构建清单或逐轮运行二进制字节快照，不能独立证明运行前后相等、该二进制就是测试所加载的字节或与当前未提交源码一致。不是冻结 release/S3 产物。 |
| 独占 S3 桶已就绪，但无联合 PASS/无负载 | **PARTIAL（准入快照）** | `trial-ground/private/challenger-preflight-2026-10-14.md` 后续记录独占基础设施及 `infra-ready.json`；这不是数据库服务运行或联合测试通过证据。当前仍缺 Main 冻结确认；定向 PASS 与新增的 Go 包级 PASS 均不补足 P0–P6 联合门禁。报告明确未使用 `peri-stress`，两份 JSONL 均为 Go 测试事件而非压测轨迹。 |

## 时序与未测边界

- 首次失败日志最后修改约 **22:02:23 CST**；当前隔离二进制文件时间 **22:19:47 CST**；P6 JSONL 记录 22:21:59–22:22:09（单项 `run` 22:22:01、`pass` 22:22:09）；发布 JSONL 记录 22:23:11–22:23:22（两个单项顺次执行）；报告文件创建/修改 **22:24:21 CST**。这些记录支持“旧失败 → 当前产物 → 两次定向复核 → 报告”的顺序，不能替代可审计的编译命令、执行环境或前后 SHA256 原始事件。
- 上次核验时 `trial-ground/private/platform-go-full-after-publish-2026-10-14.log` 为 **0 字节**；该陈述仅记录当时快照，现已更新，见下节新增核验。**当前 P0–P6 联合 PASS 仍未证明**；不得将定向或 Go 包级 PASS 升格为生产验收。
- **NOT MEASURED：** ACK/s、offered/actual/admitted、SQL 请求 p50/p95/p99/max、错误/Unknown 分母、客户端丢弃、CPU/RSS、磁盘/网络、对象存储放大、恢复时间、租户公平性与容量拐点。4.72s、4.85s、8.72s 是整条 Go 测试的耗时，**不是 SQL 响应时间**；独立 RustFS 的空闲 RSS 不是数据库资源样本。S01/S04 端到端与 S06/S08 容量未由本轮实测，不能作 SpaceX 审阅、商业可用性或真实性能主张。

**总体：PARTIAL。** 定向测试回归、修复范围和新增 Go 平台包通过有日志/当前证据，局部修复效果在所测夹具下成立；根因唯一性、运行二进制前后摘要、Rust lib 结果与 P0–P6 全量正确性仍待独立原始证据。本文不转录私有日志全文、密钥或凭据。

## 追加核验：平台包复测

- **PASS（包级结果）：** `trial-ground/private/platform-go-full-after-publish-2026-10-14.log` 现为 169 字节，记录 `ok peri-loom/platform 148.884s`、`ok peri-loom/platform/cmd/peri 0.535s`、`ok peri-loom/platform/sdk/go 0.546s`；`cmd/peri-platform` 显示无测试文件。两个子包此轮没有显示 `(cached)`。所以新增报告的 **148.884s 平台包 PASS** 与日志相符；这是 Go 测试包耗时，**不是** SQL 延迟、吞吐或数据恢复指标。
- **NEEDS_MORE（命令/产物溯源）：** 此文件只有四行 Go 包结果，**没有**完整命令、工作目录、`GOMAXPROCS=2`、`PERI_RUST_BINARY`、`-count=1 -p 1 -timeout 10m`、执行起止时间、退出码的独立记录，也没有运行前后 SHA256 或冻结源码/二进制清单。报告提供了命令文本及“前后 SHA256 相同”的陈述，但该日志无法独立验证它们。文件创建约 22:23:45、末次修改 22:26:17 CST，和 148.884s 相容，不能替代进程级时间证明。原报告文件于 22:24:21 创建、22:27:23 修改，追加包级结果发生在日志写完之后，时序未见矛盾。
- **边界：** `go test ./...` 在该目录的平台包 PASS 是比定向用例更强的 **Go 包范围**回归证据，不等于冻结源码/产物的 P0–P6 **联合门禁**，不包含独占 S3 数据库夹具的正式端到端 oracle、各阶段故障演练或 release 产物验证。独占 S3 基础设施存在不等于本轮 Go 测试使用了它；Rust lib 127/127 仍无独立原始记录。ACK/s、p50/p95/p99、Unknown/资源/恢复等数据库性能指标继续 **NOT MEASURED**，不能向 SpaceX 或其他客户宣称商用验收。
