# T0 统一基线：WAL 工作量、元数据、锁与执行的可选指标（Rust 侧）

日期：2026-10-17。状态：**观测已实现并自测通过；未做任何性能验证，不宣称收益**。
范围：`prototypes/v1/src` 的 Rust 数据面（Authority / Worker / storage / cache / session）。
本文只列本次新增的观测点、开关、量级与**未测量边界**；分阶段耗时口径仍以
`2026-10-16-read-path-phase-attribution.md` 为准。

## 1. 结论

- 新增一个默认关闭的进程级计数器模块 `src/work_metrics.rs`（开关 `PERI_WORK_METRICS=1`），
  它复用**既有工作点**取数，不新增遍历、不新增分配、不引入 label；
- 三组指标（WAL 工作量 / 元数据 / 锁与执行）与既有 `metrics` RPC 的 `MetricsSnapshot`
  同一个导出通道：开启时多一个 `work_metrics` 字段，关闭时该字段**整个省略**，诊断响应与
  改动前逐字节一致；
- 数据库 ID、请求 ID 等高基数标识不进计数器，仍只出现在 `capacity_trace` 的采样 trace 里；
- 本文所有数字都来自测试入口的一次运行（第 6 节），**不代表任何加速、容量或 p95 结论**。

## 2. 开关与导出

| 项 | 值 |
|---|---|
| 开关 | 环境变量 `PERI_WORK_METRICS=1`；进程内首次读取后固定（`work_metrics.rs:36`） |
| 导出 | 既有 `metrics` RPC：`Metrics::snapshot()` 填 `work_metrics` 字段（`metrics.rs:117`），`None` 时 `#[serde(skip_serializing_if = "Option::is_none")]` 省略（`metrics.rs:73`） |
| 聚合范围 | 进程级累加（`AtomicU64`，`Relaxed`）。同时保留既有的线程级 `recovery::RecoveryObservations`（始终开启，供测试做精确断言） |
| 类型 | 纯整数计数器；无 label、无 key、无时间序列维度 |

关闭时 `snapshot()` 返回 `None`，导出端会看到"未测量"而不是"0"，避免把未开启读成没有工作。

## 3. 指标清单

行号对应本提交的工作树。所有"字节"都是 payload 字节，不含传输/信封开销。

### 3.1 A 组：WAL 工作量

| 指标（JSON 路径） | 含义 | 单位 | 采集点 |
|---|---|---|---|
| `wal_workload.append_calls` | 带 pending batch 的 `sync_wal` 次数 | 次 | `io.rs:333`（判定在 `io.rs:311`） |
| `wal_workload.native_bytes` | 每次 append 面对的 native WAL 镜像大小累加 | B | 同上，取 `state.image.len()` |
| `wal_workload.delta_bytes` | 每次 append 的 pending 写字节 | B | 同上（既有 `bytes` 折叠值，未重复遍历） |
| `wal_workload.position_frames` | 被复用的 commit position 覆盖的 frame 数 | frame | 同上 |
| `wal_workload.append_position.{reused,unscanned,unchanged,not_covered}` | append 的 commit position 解析结果；只有 `reused` 走 Authority 增量校验，其余让 Authority 重新推导整份索引 | 次 | `io.rs:311`（判定）→ `io.rs:333`（计数） |
| `wal_workload.scan_calls` / `scanned_bytes` / `scanned_frames` | WAL 校验链实际走过的扫描次数 / 字节 / frame | 次 / B / frame | 上报在 `recovery.rs:131`（整幅 `committed_position`）与 `recovery.rs:231`（增量 `WalFrameScan::position`）；frame 与字节在**推进处**就地累加（整幅 `recovery.rs:168`，增量 `recovery.rs:310`），不为计分再扫一遍 |
| `wal_workload.copied_bytes` | 校验候选 append 时克隆的 WAL 镜像字节 | B | `recovery.rs:108`，唯一调用点 `recovery.rs:512` |
| `wal_workload.scan_ns` | 扫描内部墙钟 | ns | `recovery.rs:72` |
| `wal_workload.scan_resets.{shrink,short_image,header_changed,write_in_prefix,truncate_in_prefix,forced_whole_image}` | 已验证前缀被放弃的原因 | 次 | `recovery.rs:244/247/253/341/347/424`（`reset_for`，`recovery.rs:219`） |
| `wal_workload.scan_stops.{salt_mismatch,checksum_mismatch,incomplete_tail}` | 扫描无法继续的原因（salt/page 0、校验和不匹配、尾部不足一帧） | 次 | `recovery.rs:312/322/270` |

对应研究文档 6.1 的"native WAL bytes、delta bytes、scanned bytes、copied bytes、frame 数、
校验回退原因"。`forced_whole_image` 是测试对照臂（`#[cfg(test)]`），生产构建里恒为 0。

### 3.2 B 组：元数据

| 指标（JSON 路径） | 含义 | 单位 | 采集点 |
|---|---|---|---|
| `metadata.root_encoded_bytes` | 编码后的 authority head 文档大小 | B | `authority.rs:400`（`encode_with_digest` 之后） |
| `metadata.root_publish_attempts` | 走到 head CAS 的发布尝试 | 次 | 同上 |
| `metadata.root_publish_retries` | head CAS 失败后重试 | 次 | `authority.rs:485` |
| `metadata.pin_refs` | 发布文档里的 reader pin 条目总数 | 条 | `authority.rs:400` |
| `metadata.root_receipts` | 发布文档里的幂等回执数 | 条 | 同上 |
| `metadata.wal_index_commits` / `wal_index_bytes` | 发布的 `wal/index` 文档数与编码字节 | 次 / B | `wal.rs:229` |
| `metadata.wal_index_receipts` | 发布索引携带的幂等回执数 | 条 | 同上 |
| `metadata.wal_index_retries` | `wal/index` 条件写失败后重试（append 与 fence 两条路径） | 次 | `wal.rs:348`、`wal.rs:247` |
| `metadata.index_page_reads` / `index_page_writes` / `index_page_write_bytes` | 分页索引页读取次数、写入次数与写入字节 | 次 / B | `authority/paged_index.rs:380`、`:452` |

### 3.3 C 组：锁与执行

| 指标（JSON 路径） | 含义 | 单位 | 采集点 |
|---|---|---|---|
| `locks.{state,cache,storage}.waits` / `wait_ns` | 取锁等待次数与累计等待时间 | 次 / ns | state：`io.rs:269`（唯一入口，另含 `io.rs:131`、`io.rs:146` 两处直取）；cache：`cache.rs:48`；storage：`storage.rs:41`、`storage.rs:49` |
| `locks.{state,cache,storage}.holds` / `hold_ns` | 持有次数与累计持有时间（guard drop 时结算，`work_metrics.rs:338`） | 次 / ns | 同上 |
| `execution.native_execute_calls` / `native_execute_ns` | 原生语句执行次数与引擎内墙钟 | 次 / ns | `session/execution.rs:592`（`ExecuteTimer::begin`），结算 `:606`（成功）/ `:620`（失败） |
| `execution.native_execute_steps` | 成功执行计入执行预算的 VM 步数（沿用既有 `prior_steps + vm_steps`，不自造口径） | step | `session/execution.rs:606` |

三个锁域的界定：它们是被同一数据库上所有会话共享的 `Mutex`（`RemoteIO` state、segment cache、
`RemoteStorage` state）。会话 lane、registry 等按会话/进程有界的锁**不在**本组，避免把低价值
锁噪声混进结论。

## 4. 为什么默认零开销

关闭时（默认）路径**行为与改动前完全一致**，且不读取任何多余的时钟：

- 每个工作点先 `enabled()`（一次静态读，`OnceLock` 语义的 `AtomicU8`），关闭即提前返回
  （`work_metrics.rs:36`）。热路径上只有一次可预测的分支，没有原子加、没有分配、没有锁。
- 锁观测用 `LockWait::begin`：关闭时 `started = None`，**不取 `Instant::now()`**；
  `ObservedLock::drop` 看到 `hold = None` 直接返回（`work_metrics.rs:305`、`:338`）。
  这条由 `work_metrics::tests::an_unarmed_guard_holds_no_timer` 断言。
- 原生执行用 `ExecuteTimer::begin`：关闭时不取时钟，`finish` 直接返回（`work_metrics.rs:372`）。
- 扫描的帧计数、回退原因计数走的是**既有线程级观测**（`recovery.rs` 的 `OBSERVATIONS`），
  在原本就要写计数器的位置多写一个字段，不为指标再做一次遍历或分配。
- 唯一被改写形状的是 `io.rs::sync_wal` 里的 position 过滤链：原先是两个 `filter`，
  现在是等价的 `match` + 同一个 `filter`（`io.rs:311`、`io.rs:322`），判定结果与原先逐字节相同
  （`Ok(false)` 与 `Err(_)` 都视为不可用，与原 `unwrap_or(false)` 一致）。
- 关闭时 `MetricsSnapshot` 不新增字段，诊断响应与改动前逐字节一致。

**无高基数 label**：计数器没有数据库/请求维度。库身份、请求 ID 仍只经 `capacity_trace`
的采样 trace 记录（`capacity_trace.rs` 已按此分层），本次不把任何身份信息引进计数器。

## 5. 未被采集的东西（明确不做）

| 项 | 理由 |
|---|---|
| 数据库 ID / 请求 ID 维度的计数器 | 高基数标签会让计数无限增长；身份归采样 trace |
| 持久化组的 put/get/CAS 数与字节（研究文档"持久化"组） | 属另一组指标；现有测试已用 `RequestProbe` 统计，生产侧计数需要先定义"每成功事务"的归属，本轮不做 |
| journal/audit Sync 次数、batch size、排队等待 | 在 Go 平台（Gateway）侧，Rust 数据面看不到；本工作包只做 Rust 侧 |
| 维护组（applied 与 durable 差距、replay bytes、reader age、checkpoint duration、GC backlog） | 研究文档单列一组，且需要先确定采样与导出语义 |
| 客户端组、资源与费用组（RSS、pending bytes、每事务对象请求） | 需要外部测量工具或另一层埋点，不在 Rust 服务端计数范围内 |
| 每帧 checksum 计算的 CPU 时间 | 需要 `clock_gettime` 或额外计时点；帧级计时会显著放大观测本身的开销，得不偿失 |
| 原生执行的**线程 CPU** 时间 | `session/execution_clock.rs` 已有（执行预算用），但它是预算内部状态；导出它要另建一条通道，本轮先用墙钟（含引擎内部阻塞 I/O），并在此标注两者口径不同 |
| `scan_stops` 里"因已停止而未再尝试"的提前返回 | 它是复用已判定答案，不是新的失败；本轮不计数以免与真失败混淆 |
| 观测关闭时的对照臂开销 | 未测量。默认零开销是**结构性**论证（无时钟读取、无原子、无分配），没有做指令级/微基准验证 |

## 6. 验证入口与实测输出

单元测试（默认运行，非 ignored）：

- `recovery::tests::scan_fallback_reasons_are_counted`：用真实 fixture 逐个触发
  write-in-prefix、header 变化、truncate、尾部不完整 frame、salt 不匹配、校验和不匹配、
  shrink/short-image，断言线程级计数**精确**值，并断言同一份工作已到达进程级 sink。
- `work_metrics::tests::*`：锁等待/持有、原生执行计时、各 sink 与 JSON 形状。

测量入口（既有 ignored 基准，多打印一行 T0 计数器）：

```
PERI_WORK_METRICS=1 PERI_COMMIT_BENCH_ROWS=200 PERI_COMMIT_BENCH_COMMITS=4 \
PERI_COMMIT_BENCH_BATCH=50 \
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --lib -- \
  --ignored --nocapture commit_incremental_scan_paired_curve
```

本次运行的原始输出（截取两臂同一轮，单机）：

```
COMMIT_BENCH tier_rows=200 arm=incremental round=1 commits=4 ... work_appends=4 work_native_bytes=288528 work_delta_bytes=16480 work_scanned_frames=8 work_position_reused=4 work_position_fallback=0 work_reset_write=0 work_reset_header=0 work_reset_truncate=0 work_reset_forced=0
COMMIT_BENCH tier_rows=200 arm=whole-image round=1 commits=4 ... work_appends=4 work_native_bytes=288528 work_delta_bytes=16480 work_scanned_frames=74 work_position_reused=4 work_position_fallback=0 work_reset_write=0 work_reset_header=4 work_reset_truncate=0 work_reset_forced=4
```

可读出的（仅观测事实，不含收益判断）：

1. 计数器确实跟着真实提交路径动：4 次提交产生 4 次带 position 解析的 append，复制/扫描字节与
   线程级观测一致；
2. `work_native_bytes` 在同一轮内随镜像增长（288528 → 354448 → 420368），说明"native WAL 字节"
   确实是一个随历史增长的量，可以被记录；
3. 对照臂（强制整幅重扫）的 `work_scanned_frames` 为 74/90/106 且 `work_reset_forced=4`，
   增量臂恒为 8 且 `work_reset_forced=0`：**回退原因计数能把两种臂区分开**；
4. 该轮的两臂墙钟差异在噪声范围内，**不能**据此得出任何加速结论（样本小、共享主机）。

## 7. 未验证项

- 未做关闭/开启两种构建的指令级或微基准对照；"默认零开销"是结构论证，不是实测数字。
- 未在加密、S3 后端、并发多会话、长时运行下观察计数器；本轮的计数路径只在单进程测试夹具里跑过。
- 未验证极端 WAL 规模（研究文档 E2 的 16/64/256 MiB 轴）。
- 未跑远端 CI、未跑容量/压力口径；`PERI_WORK_METRICS` 对吞吐与延迟的影响未测。
- 未改协议、持久格式、fencing、Unknown 与审计语义；本工作包不涉及这些路径的验证。
