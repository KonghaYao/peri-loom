> 后续实测（2026-10-17）：[三轮本地SQL优化结论](../experiment-local-performance/conclusion.md)。固定基线1198、同release负载最终两对2.371×/2.421×，主要收益来自冷重开；不代表热读写普遍改善。首轮复制/编码组合未达标，不能沿用本报告候选成本作为收益排序。

# 读写性能排查与优化建议

日期：2026-10-17。代码基线：`e8beae40291bf61a84acd1aa0a8813ebef8d1234`。

> **对抗复核订正**：原报告基于 e8beae4，以下源码位置和“当前”均指该历史基线。复核实验冻结在 c8990c4；后续已观察到 792f546 的 root decode memo，以及 94534c5 / df93605 / 1198b18 的缓冲、解析和主库预读改动。不能将下列历史问题清单视为最新工作区的未完成列表。实验与反例见 [对抗复核结论](../experiment-read-write-adversarial/conclusion.md)。

范围：原基线 V1 的 Gateway → Worker → Authority → ObjectStore，以及 WAL、段缓存、checkpoint、操作日志与审计。此次做源码排查并复核仓库已有测量；没有启动新压测、修改服务配置或业务代码。下面标明哪些数字来自历史测量，哪些只是源码推导，不能将它们当作当前 HEAD 的性能基线。

## 1. 候选成本与待验证方向

原基线热读存在持久化等待；特定 Authority 增量写入路径复制当前 WAL 镜像；慢 I/O 持锁可能传递等待。这些是机制判断，尚不足以给当前端到端性能排序。checkpoint 另有上层维护独占门，native 页缓存命中也可能绕开 File。拒绝率需与准入限制分开解释。

| 项目编号（不表示收益排序） | 研究方向 | 主要影响 | 改动成本 | 证据强度 |
| --- | --- | --- | --- | --- |
| 1 | operation 按实际长度分配；RPC 单解析 | 分配量、解析 CPU | 小 | 后续已合入；实验确认成本，小批日志时延无稳定收益 |
| 2 | 评估 RemoteIO 同步与发布的锁范围 | 需要 File I/O 时的排队 | 需并发协议证明 | 主库 pread 已改；sync 及维护独占门仍须分别分析 |
| 3 | checkpoint 批次上传、批次登记，最终 fenced publish | checkpoint 耗时、前台停顿、S3 请求数 | 中至大 | 当前源码确认，实际收益未测 |
| 4 | WAL 验证采用按页/块共享的候选镜像；WAL 索引与回执分段 | 长历史写入、提交内存峰值 | 大 | 当前源码及历史复制字节测量 |
| 5 | 分页 ownership 命令回执，隔离租约视图与历史体量 | 长时间运行后的读写成本 | 中至大 | 当前源码及历史规模测量 |
| 6 | 为终态 operation 与 audit 设计共享持久化机制 | 热读固定延迟 | 大，涉及恢复与审计契约 | 历史分段测量确认成本，方案待设计 |
| 配套 | 分开校准准入、维护和恢复预算 | 拒绝率、冷启动、持续写入稳定性 | 中 | 当前配置及已有观测 |

撤回“先做 1、2、再做 3”的收益排序。当前应先采实际请求分段数据、批次/响应体量和维护等待，再选择目标。项目 4、5 的增长成本需要按活跃 WAL、回执数量和维护周期分别度量。项目 6 涉及恢复与审计契约，不能以局部计时为依据直接减少持久化屏障。

## 2. 现有优化已经做到哪里

原基线已经具备以下机制，不能把它们再次列成“尚未实现”的建议：

- 引擎证明只读的语句免写 dispatch intent；只缓存“会提交”的判定，只读判定每次重新取证。
- operation 使用增量追加日志、有界提交队列和已有等待者合批，fsync 已移出共享 state 锁。
- audit 合并已有等待者，每个调用等待自己的持久化结果。
- Authority 大体量对象注册表分页、提交变更集校验、近期恢复点窗口有界。
- WAL checksum 前缀增量验证，WAL 追加的对象与 head 合并发布；部分 S3 group body 支持有界并行。
- 缓存文件读取及段 GET 已移出各自的内层锁；同一不可变段的并发 miss 可合并。
- 冷回放已有预取窗口和 Worker 共享 fetch 许可；维护调度已有实现，默认关闭。

早期 `implementation-status.md` 的部分“未完成”描述已落后于当前源码。另有报告记录的是独立工作树实现，不能仅凭“已实现”字样算入 main。

## 3. 热读：两道持久化屏障仍在关键路径

当前 `platform/gateway_http.go:122` 先结算 operation，再在 `:147` 同步执行 `Audit.Record`。只读 SQL 虽已省掉 intent，仍在 `gateway_operations.go:136` 写持久终态；`operation_commit.go:203` 与 `audit.go:436` 分别等待文件 Sync。并发调用可以合批，孤立请求仍需顺序等待两者。

[历史最终验收](./2026-10-16-read-path-barrier-acceptance.md) 中，保留 B、回退 A 的版本有如下结果：

| 服务端阶段 | 平均耗时 |
| --- | --- |
| 只读分类 describe | 0.730 ms |
| Worker | 4.835 ms |
| operation 终态 | 4.112 ms |
| audit | 4.292 ms |
| 总计 | 14.357 ms |

该历史轮次终态加审计占约 **59%**，不能用于断言当前主因或排序。同轮查询吞吐为 60.17 ops/s，早期基线为 49.81 ops/s；这是共享主机、旧构建上的历史测量，不能视为当前版本容量。

建议：先记录当前构建的两类队列等待、批次大小、Sync 次数、witness/snapshot 周期尖峰。既有合批要测实际覆盖率；同 namespace 的 operation fence 会限制同库合批，不能假定多开连接就会摊薄全部屏障。

更深的方案是研究一个持久追加事务同时承载终态和审计，再恢复各自视图及完整性证明。它需要完整恢复协议与故障验证。历史“并行写 audit”方案已经因 Outcome 语义变化和异常处理边界回退，不能直接恢复；仅删除只读终态记录也会改变操作查询与恢复契约。

另外，当前只读 SQL 每次多一次 describe；Hrana 某些路径还有重复分类。可优先复用同一次请求已有的引擎 prepare 结果。跨请求缓存只读判定需要证明 session、schema 和执行对象的绑定，不能只按 SQL 文本缓存。

## 4. 两项局部成本：后续已合入对应改动

### 4.1 小日志每批预分配 1 MiB

版本订正：94534c5 已改为按实际长度分配。[实验 01](../experiment-read-write-adversarial/01_experiment_report.md) 的冻结版本对照确认小批分配下降，但单条含真实 Sync 的中位耗时为旧 4.43 ms、对照 4.92 ms；满载两臂均分配约 1 MiB。只保留减少分配的结论，不宣称稳定请求时延收益。

`platform/operation_commit.go:195`：

```go
run := make([]byte, 0, operationJournalBatchBytes)
```

`operationJournalBatchBytes` 为 1 MiB。即使批次只有一条小记录，也按最大批次容量申请缓冲。读请求通常有一条终态追加；写请求还需要 intent。此处是按批次付费，实际分配速率取决于合批率，不能直接乘请求数当成实测。

建议：先求本批 `line` 长度之和，再按实际长度分配。先不引入大缓冲池，避免把瞬时分配变为常驻占用。验证批次边界、写入顺序和故障语义，并比较 `alloc_space`、B/op、GC 时间；收益尚未测量。

### 4.2 同一 Authority 响应被完整解析两次

版本订正：df93605 已合入单解析。实验 01 使用独立严格解析 harness，约百字节/1 KiB 响应只省约 0.41/0.51 µs，1 MiB 省约 101 µs；它不证明新提交实现的契约或 HTTP 端到端收益。

`src/authority_rpc/client.rs:283` 先把响应解析为 `serde_json::Value`，只为确认 `result` 字段存在；`:291` 再从相同字节解析 `RpcResponse`。对于带大块 base64 的响应，两次解析会重复处理并分配大字符串。之后 provider 还要将 result 解成目标类型。

建议：通过一次严格反序列化同时完成字段存在性、协议版本、request ID 和结果一致性检查；必要时用 `RawValue` 延迟解析 result。保留“字段缺失”与显式 null 的区别，不要因优化接受畸形成功回复。先比较大 Get 的解析 CPU 和峰值分配，再评估是否值得更改传输格式。

## 5. 锁：原基线的外层覆盖与并行边界

版本订正：1198b18 已将主库 pread 移出 Shared 锁；下述主库读路径是历史机制。native pager 已加载页可能不进入 File；checkpoint/compact 还取得 operations 写锁，前台执行取其读锁。因此，主库放锁不能单独证明维护期间 SQL 能并行。新并发边界的完整正确性不由此次机制复核保证。

`src/io/files.rs:83` 的 `RemoteFile::pread` 在判断 WAL/主库之前就取得 `self.shared.lock()`。主库分支随后调用 `storage.read_at`，该 guard 在调用返回后才释放。缓存磁盘读取、冷段 GET 即使不再持有 storage/cache 内层锁，仍处于这个同库 Shared 锁的覆盖范围。

`src/io/files.rs:123` 的 sync 同样持有 Shared 锁，跨越 `sync_wal` 的远端提交，或者主库 publish 的远端发布。由此可推导：一个慢冷读或提交可能阻塞同库其他需要 Shared 锁的读；不同数据库有各自实例，不能描述为全 Worker 串行。实际等待占比尚未测量。

建议：WAL 分支只在读取镜像期间持锁；主库分支利用现有 storage revision 校验，在一致性证明成立后缩短外层锁。提交与 checkpoint 应进一步拆成“锁内冻结候选 → 锁外 I/O → 核验代际并安装结果”。这需要确认锁原本保护的镜像/主库转换关系，不能仅机械增加 `drop(state)`。

验证要从真实 RemoteFile/native SQL 入口注入慢段 GET，检查另一个会话的不同段读取和热 WAL 读取是否能推进；同时覆盖 checkpoint、truncate、回滚和失败发布。直接测试 `RemoteStorage.read_at` 无法验证外层锁已经消除。

## 6. 写入：冻结版本的镜像复制与全量索引

当前性订正：实验期间另有未提交的 Authority/recovery 改动尝试消费旧状态原地延长，并改变 copied_bytes 含义；本节与实验数据只属于冻结版本，不保证最新工作区仍有相同 clone，也不验证该候选修复。

`src/recovery.rs:476` 的 `validate_index_append` 已增量验证新增记录，但仍然：

1. 解码完整 WalMetadata，并验证旧 entries/receipts/digests 前缀；
2. 在 `:511` 执行 `previous.image.clone()`；WalImage 内部为 `Vec<u8>`，这是完整字节复制；
3. 在候选上应用新记录，保证失败不污染已接受状态。

`src/wal.rs:476` 每次 append 仍读取整个索引、追加 entries 和两种回执；发布时编码整个 index。truncate 减少 entries，但当前代码没有相应移除 receipts/digests。它们与 Authority 的近期分页回执属于不同结构，不能混为一项已完成优化。

在冻结版本的 Authority 增量候选验证分支中，即使新增 payload 很小，也包含 O(当前活跃镜像字节) 的复制，以及随索引规模增长的解析、遍历和编码；这不等于每条 SQL 或每次 sync 都复制。冷验证、失配、CAS 重试会走全量重建，既有 receipt 命中可跳过发布。成功 truncate+compact 能限制镜像与 entries，但不会删去这份 index 的 receipts/digests；不能把活跃镜像等同于终身累计历史。持续追加且未有效整理时才可能出现累计平方工作。

[历史配对测量](./2026-10-16-wal-incremental-scan-measurement.md) 也明确记录：checksum 扫描量下降，但 copied_bytes 与 durable I/O 没有因此减少，不能把扫描微基准加速比当成提交加速比。

建议：

- 候选镜像按页/块共享，只复制改写范围，或采用可丢弃的增量覆盖层；仅换成 `Arc<Vec<u8>>` 后整体 make_mut 仍会全量复制。
- 索引和幂等回执采用不可变分段/分页，head 保存当前代与指针；旧请求的内容绑定和查询能力继续存在。
- compact 中 `wal.rs:643` 仍通过 `read` 收集 `Vec<WalRecord>` 全量记录，应改为流式构建 baseline，单独约束维护内存。

关键验收指标为 `copied_bytes/commit`、index 读写字节/commit、提交 p99 与失败候选隔离；按 16/64/256 MiB WAL 与固定新增字节测规模曲线。

## 7. checkpoint：逐段上传、整段写放大、长时间持锁

`src/storage.rs:323` 的 publish 持 storage state 锁，逐一对 dirty segment 调用 `put_immutable`，再写 manifest 和 HEAD。`write_at:267` 处理部分段更新时还会复制整段；同段反复小写可能反复复制。

段大小为 128 KiB（`manifest.rs:5`）。若每段仅改变一个 4 KiB 页，单次该段上传的 payload 放大为 **32 倍**；例如 1,024 个互异新脏段对应 4 MiB 页面变化、128 MiB 段 payload。这是特定稀疏模式的源码推算，不是所有负载的实测放大率。

反例订正：put_immutable 先 GET 校验，已有内容会跳过 PUT；同内容跨段可以复用，dirty 按段合并。native checkpoint 已按页取最新帧并合并连续页 writev，Authority 每次事务内部已有 put_group。[对抗实验](../experiment-read-write-adversarial/02_experiment_report.md) 中三种稀疏臂虽为 8/1/0 次新段 PUT，但 GET 返回 + PUT body 均为 1 MiB；不能将去重省上传解释为双向总段 body 下降。因此脏段数不等于新 PUT 数，也不能由上述上界推断常规 SQL 的实际网络量。上层 operations 维护独占门也需单独纳入停顿分析。

项目已经提供 UploadSegments/RegisterSegments 批次能力，但常规 checkpoint 的上述循环没有使用它。S3 `put_group` 内部的并行 body 上传，也不等于这些独立 segment put 自动变成一个 checkpoint 批次。

建议：复用既有批次能力建设 checkpoint 专用发布链；按字节限制快照、预约和上传批次，有界并行上传，批次注册，最终核验 epoch/旧 HEAD 后发布。保留 pins、上传不确定性和未引用对象的正确处理。存储锁覆盖范围和 Shared 外层锁需要一起解决。

先优化协议往返和重复复制，再评估段尺寸或增量段格式；后者影响缓存、恢复与持久格式，改动明显更大。

## 8. 长期运行：ownership 命令历史仍拖累 root

版本订正：792f546 已在实验冻结版本前合入按字节摘要复用解码的 memo。同一 head 命中会复用 Arc，不能再声称每次租约检查完整 parse；root GET、完整字节哈希、更新/失效时的解析和 commands 规模成本仍存在。以下规模计时是 memo 之前的历史数据。

`ownership/types.rs:56` 的 `OwnershipCatalog.commands` 仍为内嵌 BTreeMap；`authority/lease_commands.rs:272` 持续插入命令回执。`authority/ownership_catalog.rs:54` 每次 prepare 还逐条检查旧 commands；`authority/ownership_view.rs:17` 从当前 root 构造租约视图；后续 memo 命中可复用解码，仍要读取当前 root。

这部分没有因 objects/blobs 分页或 branch history 窗口有界而消失。它会让续租历史进入 SQL 所需的租约检查和数据提交成本。

[已有剖分](./2026-10-16-read-path-barrier-acceptance.md) 记录，在 release、本地后端的特定测量里，root 从约 1.4 KiB 增长至 95 KiB、945 KiB 时，租约校验 p50 为 22/196/1,782 µs。它证明增长趋势，也修正了早期归因：**95 KiB 档 Authority 校验只有约 0.2 ms，不能把 Worker 的全部 4–5 ms 归给 root 解码**。报告中的独立工作树 decode memo 当时不在 e8beae4 内，但已通过 792f546 合入后续版本，不能继续作为未实施建议。

建议：分页归档 commands，保留请求 ID 与 payload digest 的幂等检查；租约判定只处理有界当前视图。已有按内容身份复用解码结果的实现，仍需每次验证当前 root 身份、完整性及 live lease/撤权状态。缓存只能降低解码成本，无法消除不断增长的 root 读取成本。

## 9. 配额、维护与压测口径

默认项目配额为 MaxConcurrent=4、MaxSessions=16、RequestsPerSecond=100（`platform/management_tenancy.go:23`）。[九臂观测](./2026-10-17-nine-arm-stress-observation.md) 已将查询 known_failure 对应到 concurrency_limit，将 open 失败对应到 session_limit；不能把请求 100 会话、只接受 16 会话的结果当成 100 并发性能。

建议先用客户端在途上限与连接池匹配配额，分别测同库/跨库、单项目/多项目，再在隔离测试配置中逐级校准服务端配额。记录 offered、实际派发、准入、成功、拒绝、timeout、Unknown 和总墙钟；开环工具达不到目标派发率时，应先检查客户端调度，不能由其低吞吐推断服务端容量。

`audit.Record` 的 pending slice 当前没有显式排队上限（`:336`）；拒绝流量同样需要同步审计。应将拒绝洪峰对审计盘和健康请求 p99 的影响单独验证，之后设置入口背压与审计预算；audit batch 大小有上限不等于总 pending 有上限。

维护方面，自动调度默认关闭；不能将已有代码等同于已校准默认策略。应依据实际 WAL 增长速度、恢复耗时和前台延迟，校准 checkpoint/compact 的高低水位、周期与维护字节预算。冷回放预算也只覆盖部分瞬时形态，不等于进程 RSS；增大预取窗口前需测 Worker/Authority 的峰值内存。

## 10. 建议的最小验收矩阵

| 场景 | 控制变量 | 主要观测 |
| --- | --- | --- |
| 热读固定成本 | 同一数据与 SQL，C1/C4，缓存热 | 分段 p50/p99、两日志 Sync/批次、分配量 |
| 冷读与混合读写 | 同库不同段；慢 GET 与慢提交注入 | Shared/storage/cache 锁等待、其他会话推进 |
| 长历史小提交 | WAL 16/64/256 MiB，固定新增字节 | memcpy、index 字节、root 字节、commit p99 |
| checkpoint | 32/256/1,024 个稀疏脏段及连续写对照 | payload 放大、RPC/PUT/CAS、前台 p99 |
| root 历史增长 | 固定数据、不同 commands 数量 | 校验/prepare 时间、元数据字节、分配量 |
| 准入及持续运行 | 合法配额内与超配额分开，维护开关分开 | 完成工作量、拒绝原因、WAL/内存增长、恢复时间 |

性能前后对照应冻结 commit、二进制、release/debug、后端、资源、配额、缓存状态及历史规模。当前 Dockerfile 已使用 release；旧 debug 本地结果不能直接代表生产镜像。每次只验证正在消除的具体成本，复核已确认提交、撤权 fencing、Unknown 和快照恢复，避免以功能测试通过代替性能收益证据。
