# W4 维护调度：水位、冷却期与预算（Rust 数据面）

日期：2026-10-17。状态：**调度、留痕与预算已实现并自测通过；未做任何长时运行或性能测量，不宣称 p99 或冷启动改善**。
范围：`prototypes/v1/src` 的 Rust 数据面。Go 平台、Gateway、Authority GC 的周期未改。
研究文档：`peri-loom-tuning-research-2026-10-16.md` 的 W4 节。

本文只写本次改了哪些触发条件、常量表与各自的校准方法、以及**未测量/不承诺**的边界。
指标口径沿用 `2026-10-17-t0-work-metrics.md`（同一开关 `PERI_WORK_METRICS=1`，关闭时零开销）。

---

## 0. 一句话结论

- 维护单位（Full checkpoint、truncate-checkpoint + WAL reset）**语义未改**，仍然只有原来的两个入口；
- 新增的是**什么时候允许它们跑、同时最多跑几个、被读者挡住时留下什么证据**；
- 调度器**默认关闭**（`PERI_MAINTENANCE_SCHEDULE` 未设即关闭）。默认配置下维护频率与改动前**完全一致**——即"不自动跑"，只在显式 admin RPC / CLI 时跑；
- 所有水位都是**未校准的实验输入**（研究文档的 16/64/256 MiB），开启前必须按 §4 重测。

---

## 1. 侦察结论（改动前的事实）

### 1.1 现有触发点

`RemoteDatabase::checkpoint()`（`database.rs`，`CheckpointMode::Full`）与
`RemoteDatabase::compact_wal()`（`database.rs`，`CheckpointMode::Truncate` + `require_complete_backfill`
+ `io.compact_wal()`）的全部调用点：

| 调用点 | 性质 |
|---|---|
| `worker_rpc/dispatch.rs:360`（`WorkerCommand::Checkpoint`） | 显式 admin RPC |
| `worker_rpc/dispatch.rs:372`（`WorkerCommand::Compact`） | 显式 admin RPC |
| `main.rs:75/96`、`capability_cli.rs:111`、`runtime_cli.rs:84/89` | CLI |
| `runtime/handle.rs:122/129`、`ownership/handle.rs:127/132` | 上面几条的转发 |

**结论：改动前 Rust 数据面没有任何周期性 / 自动维护触发点**，两个单位都只由显式管理命令驱动。
commit / query / session open（`session/lane.rs`、`session/execution.rs`）里不出现 `checkpoint`
或 `compact_wal`。所以 W4 要防的"每次 COMMIT 都同步做重活"在改动前并不存在，**本次新增的自动调度
必须保持这个性质**，这就是 §3 的证据。

### 1.2 现有指标机制

- `src/work_metrics.rs`：进程级计数器，开关 `PERI_WORK_METRICS=1`，关闭时每个工作点先读一次静态开关
  即返回，不取时钟、不分配。导出走既有 `metrics` RPC 的 `work_metrics` 字段（关闭时整个字段省略）。
  已有 A 组（WAL 工作量）、B 组（元数据）、C 组（锁与执行）。T0 交付说明里明确把"维护组"列为未做。
- `src/resources.rs` `ResourceUsage`：已有 `wal_bytes`、`pending_wal_bytes`、`dirty_segment_bytes`，
  通过 `RemoteDatabase::resources()` 导出。
- `src/metrics.rs`：`Operation::Checkpoint` 的 RPC 侧耗时（只覆盖 admin RPC 那条路径）。
- `src/recovery.rs` `RecoveryObservations`：扫描/回放的帧数与字节（线程级，供测试精确断言）。

### 1.3 长读阻塞在改动前的形态

`preflight_compaction`（`database/connections.rs`）在 dispatch 之前检查其它 lane 的 native
`holds_read_lock()`，命中就返回 `AdmissionError::Conflict`；这条路径**没有任何留痕**。
另一条是在 dispatch 之后：truncate checkpoint 回填不完整时 `require_complete_backfill` 返回
`UnsettledSqlOutcome`，同样无留痕。两条都在 `io.compact_wal()`（真正的 WAL 索引 rebase）之前返回，
所以旧引用本来就保住了，但"为什么没回收"事后不可查。

### 1.4 排程分离的现状

- checkpoint 与 WAL 索引 compact 在同一个函数族里，但**没有周期**，谈不上对齐；
- Authority GC（`src/gc.rs`）由 `authority/ownership_maintenance.rs` 的 RPC 与 `capability_cli.rs`
  驱动，**Rust 侧没有固定周期**；
- Gateway journal snapshot 在 Go 侧，不在本次范围。

所以"避免周期对齐"目前只需要在两个新 lane 之间成立（§3.4）；如果以后给 Authority GC 加 Rust
侧周期，需要把它的相位与这两个 lane 错开。

---

## 2. 改了什么

### 2.1 新模块 `src/maintenance.rs` + `src/maintenance/`

| 文件 | 职责 |
|---|---|
| `maintenance.rs` | 模块文档、`MaintenanceTarget` 契约（4 个方法：身份、账本、信号、跑一个单位） |
| `maintenance/signals.rs` | `MaintenanceSignals`（每次探测的输入）与 `ReaderAge`（最老读者年龄的峰值） |
| `maintenance/policy.rs` | 水位、冷却期、每 lane 周期与相位、`decide()`（纯函数） |
| `maintenance/budget.rs` | 每库并发上限 + 全局并发/请求数/字节预算 + `MaintenanceLease` |
| `maintenance/ledger.rs` | 每库记账：armed、pick、执行、完成/失败、拒绝、按原因的阻塞计数 |
| `maintenance/scheduler.rs` | `observe()`（只看不跑）与 `drain()`（在自有线程上跑，受预算约束） |
| `maintenance/tests.rs` | 14 个单元测试 |

`MaintenanceTarget` 把调度规则与 `RemoteDatabase` 解耦：调度规则不需要 store、native engine 或线程，
方向是 `maintenance` ← `database`（实现在 `database.rs`）。

### 2.2 改动的既有文件

| 文件 | 改动 |
|---|---|
| `io.rs` | 新增 `RemoteIO::maintenance_signals()`：一次 state 锁 + 一次 storage 锁，**不发对象存储请求** |
| `wal.rs` | `index()` 读出 `wal/index` 后记一笔 `observe_wal_index_read`（本来就要读，不为指标多读一次） |
| `database.rs` | 新增 `maintenance: MaintenanceLedger`；`checkpoint()`/`compact_wal()` 记执行次数与耗时；`partial backfill` 记 `partial_backfill`；`impl MaintenanceTarget` |
| `database/connections.rs` | `preflight_compaction` 命中长读时记录 `long_reader` 与读者年龄（错误类型不变，仍是 `Conflict`）；新增 `oldest_reader_age()` |
| `session/lane.rs` | `SessionLane` 增加 `opened: Instant` 与 `holds_read_lock()`（用于读者年龄） |
| `runtime.rs` | `WorkerRuntime` 持有 `Arc<MaintenanceScheduler>`；新增 `maintenance_tick()`（只 observe + 启线程） |
| `ownership/runtime.rs` | 一行转发 `maintenance_tick()` |
| `worker_rpc/server.rs` | watchdog 循环里调用 `maintenance_tick()`，失败打日志但**不拖垮 worker** |
| `runtime/handle.rs` | `diagnostics` JSON 增加 `maintenance` 字段（每库账本，无需任何开关） |
| `work_metrics.rs` | 新增 D 组（维护）：每 lane 的 calls/ns/backfill_bytes/failures、按原因的 `blocked`、按预算的 `refused`、`wal_index_reads/bytes/entries`、`MaintenanceTimer` |

**没有改**：持久格式、fencing、Unknown 语义、已确认写入语义、`compact_wal` 的"无未确认 WAL"
前置条件、`require_complete_backfill`、Go 侧、任何依赖（`Cargo.toml` 未动）。

### 2.3 语义边界（明确不做）

- **不做渐进式 checkpoint**。checkpoint lane 仍然调用 `CheckpointMode::Full`，与改动前逐字一致。
  研究文档要求的"引擎支持且语义验证完成后再评估"没有满足，所以不换模式。
- **不做 suspend/迁移前预整理**。这是 W4 的一条建议，但需要先定义"在预算内预整理"的边界与
  超时语义；本次只在账本里把"未发生"（无 pick）与"发生但被拒"（`refused`/`blocked`）分开，
  为将来的实现留出席位——**当前没有任何 suspend 路径调用维护**。

---

## 3. 机制

### 3.1 触发链

```
worker watchdog 线程（worker_rpc/server.rs，100 ms 循环）
  └─ OwnershipRuntime::maintenance_tick()
       └─ WorkerRuntime::maintenance_tick()
            ├─ scheduler.start()           # 只启一次，默认关闭时不启
            └─ 对每个已加载库 scheduler.observe(&db, now)
                 ├─ target.signals()       # 一次 state+storage 锁，无 I/O
                 ├─ 更新 armed（穿越高/低水位）
                 ├─ policy.decide(lane…)   # 纯函数
                 └─ record_start + 入队     # 只入队，不执行

maintenance 驱动线程（scheduler.start() 起，50 ms 一轮）
  └─ drain(1, now)
       ├─ budget.acquire(db, bytes, now)  # 被拒 → 记录 budget 阻塞 + refused，整单位跳过
       └─ target.run_maintenance(lane)
            └─ RemoteDatabase::checkpoint() / compact_wal()   # 与 admin 路径同一个入口
```

### 3.2 判定顺序（`MaintenancePolicy::decide`）

1. 调度器关闭 → `Skip(Disabled)`（默认）
2. 该 lane 是 `IndexCompact` 且 `unconfirmed_wal` → `Skip(UnconfirmedWal)`
   （这是 `compact_wal_once` 自己的前置条件，提前拦掉，不在 dispatch 之后才发现）
3. WAL 字节低于低水位，或 lane 未 armed → `Skip(BelowLow)`
4. 该 lane 的槽位未到 → `Skip(NotDue)`
5. 在冷却/退避窗口内 → `Skip(Cooling)`
6. 否则 `Due`

### 3.3 长读阻塞如何留痕、为什么不会误回收

- **记录点**：`preflight_compaction` 发现另一 lane 持有 native 读锁时，先算出该库最老读者年龄
  （`oldest_reader_age`，同时更新峰值），再记 `long_reader` 阻塞，**然后**才返回原来的
  `AdmissionError::Conflict`。错误类型与原因未变，调用方的分支逻辑不变。
- **后置情形**：dispatch 之后 truncate checkpoint 回填不完整时，在 `require_complete_backfill`
  返回之前记 `partial_backfill`。
- **旧引用**：两条路径都在 `io.compact_wal()`（WAL 索引 rebase）之前返回，所以 `wal/index` 的
  `replay_baseline_lsn`、`last_lsn`、条目数都不变；读者继续读它自己的快照。这一点由测试直接断言。
- **退避**：阻塞会把**整库**（不只被阻塞的 lane）推迟 `blocked_backoff`；理由是同一个读者还在，
  让另一 lane 再去回填等于对着同一个读者再花一次代价。
- **预算耗尽**：`drain` 拿到 `BudgetDenied` 时记 `budget` 阻塞 + `refused`，**整个单位跳过**，
  没有部分执行、没有任何回收动作。这是"限制新工作"而不是"绕过预算"。

### 3.4 排程分离

两个 lane 各有自己的**周期**和**相位**，槽位由 `next_slot(lane, origin, last_due)` 计算：

| lane | 周期 | 相位（相对该库打开时刻） |
|---|---|---|
| `Checkpoint` | 60 s | 0 s |
| `IndexCompact` | 300 s | 150 s |

`MaintenancePolicy::configured()` 在构造时 `ensure!` 两个 lane 的周期与相位都不同，把"避免对齐"
变成构造期性质而不是注释。另外 `drain` 每次只跑 1 个单位、全局并发默认 1，所以即使两个 lane 在
同一瞬间都 `Due`，也只会串行执行，不会叠加成共同尖峰。

**未处理的分离**：Authority GC 与 Gateway journal snapshot 仍由各自的所有者驱动，本次没有给它们
加周期，也没有把相位与这两个 lane 对齐/错开。如果以后在 Rust 侧给 GC 加周期，需要重新评估。

### 3.5 默认行为

| 场景 | 行为 |
|---|---|
| `PERI_MAINTENANCE_SCHEDULE` 未设（默认） | `maintenance_tick()` 对每个库只读一次静态开关即返回；不启线程、不探测、不入队。维护频率与改动前完全一致 |
| 设 `PERI_MAINTENANCE_SCHEDULE=1` | 启用调度；水位与预算取本表默认值（未校准） |

保守取值的原因：回放速度尚未测量，任何水位都没有依据；把未校准的自动维护默认打开，等于在生产上
做没有依据的实验。**要开启，请先按 §4 校准。**

---

## 4. 常量表与各自的校准方法

> **所有常量都是实验输入，不是建议直接部署的阈值。** 16/64/256 MiB 来自研究文档，只是实验输入。

### 4.1 水位、冷却期

| 常量 | 默认值 | 配置项 | 含义 | 校准方法（怎么测出该值） |
|---|---|---|---|---|
| checkpoint 低水位 | 64 MiB | `PERI_MAINTENANCE_CHECKPOINT_LOW_BYTES` | 低于它解除 armed | 见下方"回放速度法"；低水位取高水位的 1/4 左右，使"跑完一次"能回到解除状态 |
| checkpoint 高水位 | 256 MiB | `PERI_MAINTENANCE_CHECKPOINT_HIGH_BYTES` | 越过它 armed | `high = 允许的恢复预算秒数 × 实测回放速度 − 下载/校验成本 − 安全余量` |
| checkpoint 周期 | 60 s（代码常量） | — | 该 lane 的槽间距 | 测维护耗时 p99 与 WAL 增长速度：`period ≫ p99(单元耗时)`，且 `period × 增长速度 < high − low` |
| checkpoint 相位 | 0 s（代码常量） | — | 相对该库打开时刻的槽位偏移 | 与另一个 lane 错开；GC/journal 若加周期也需一起错开 |
| compact 低水位 | 16 MiB | `PERI_MAINTENANCE_COMPACT_LOW_BYTES` | 同上 | 同上，但 compact 更重，阈值应更高、冷却应更长 |
| compact 高水位 | 64 MiB | `PERI_MAINTENANCE_COMPACT_HIGH_BYTES` | 同上 | 同上 |
| compact 周期 | 300 s（代码常量） | — | 该 lane 的槽间距 | 一次 compact = 完整回填 + WAL 索引 rebase，按实测耗时取比 checkpoint 大 3–5 倍的周期 |
| compact 相位 | 150 s（代码常量） | — | 同上 | 取周期的一半，与 checkpoint 的槽位不重合 |
| 冷却期 | 30 s | `PERI_MAINTENANCE_COOLDOWN_MS` | 跑过或刚被 pick 后不再 pick | 测"维护后 WAL 重新长到高水位所需时间"；冷却期必须**小于**它以让维护不成为稳态的一部分 |
| 阻塞退避 | 120 s | `PERI_MAINTENANCE_BLOCKED_BACKOFF_MS` | 被长读/预算挡住后的重试间隔 | 测真实长读事务的持续时间分布（p50/p95）；退避取接近 p95 的值，避免对着同一个读者反复花代价 |

### 4.2 预算

| 常量 | 默认值 | 配置项 | 含义 | 校准方法 |
|---|---|---|---|---|
| 每库并发 | 1 | `PERI_MAINTENANCE_PER_DATABASE_CONCURRENCY` | 同一库同时在跑的单位数 | 打开 `locks.state.wait_ns` / `locks.storage.wait_ns`，看第 2 个单位是否让请求侧等待上升；不上升才考虑 >1 |
| 全局并发 | 1 | `PERI_MAINTENANCE_GLOBAL_CONCURRENCY` | 进程内同时在跑的单位数；**这也是 CPU 预算的载体**（同一时刻只有一份回填/发布在工作） | 用 `execution.native_execute_ns` 与请求 p99 做开/关对比；CPU 时间不做计量（见 §4.3） |
| 每窗口请求数 | 4 | `PERI_MAINTENANCE_REQUESTS_PER_WINDOW` | 60 s 内允许启动的单位数 | 用 D 组的 `maintenance.units.*.calls` 观测开启后单位时间的实际发生量，预算取实测值的若干倍 |
| 每窗口字节 | 1 GiB | `PERI_MAINTENANCE_BYTES_PER_WINDOW` | 60 s 内允许的回填字节 | 用 D 组的 `maintenance.units.*.backfill_bytes` 观测实际回填字节/分钟，再按可用出口带宽折算 |
| 预算窗口 | 60 s（代码常量） | — | 上述两个速率的窗口 | 与业务流量周期对齐后取值；窗口内不返还额度（速率，不是水位） |
| 每次 drain 单元数 | 1（代码常量） | — | 单次 drain 最多跑几个 | 保持 1，使全局并发上限是唯一的并发来源 |
| 驱动线程周期 | 50 ms（代码常量） | — | 队列轮询间隔 | 由水位与冷却期决定，与维护频率无关；不需要调 |

### 4.3 回放速度法（水位怎么算出来）

1. 打开 `PERI_WORK_METRICS=1`；
2. 用 admin/CLI 构造已知大小的 WAL（例如 16 / 64 / 256 MiB 三个档，作为**实验输入**）；
3. 测 **冷启动回放**：从 `wal_bytes` 与实际 open 耗时得到"回放 bytes/ms"；`recovery` 侧的
   `RecoveryObservations`（`scanned_bytes`/`scanned_frames`）可交叉核对；
4. 测 **单元耗时**：D 组 `maintenance.units.checkpoint.ns` 与 `backfill_bytes` 得到回填速率；
5. 设定"允许的恢复预算秒数"（产品决定，不是测量结果），然后
   `high = 预算秒数 × 回放速率 − 下载/校验开销 − 安全余量`；
6. 把 `high` 与 `low` 写进环境变量，跑一轮长时观察，确认维护没有变成稳态的一部分。

**没有第 3 步的测量结果之前，这三个数字没有依据。**

### 4.4 关于 CPU 预算

没有做 CPU 时间计量：需要在数据路径上取时钟，而 T0 交付说明已经因为同样的理由放弃了帧级计时。
CPU 由**全局并发 = 1** 表达：同一时刻最多一个回填/发布单元在跑，因此本进程的维护 CPU 上界是常数。
这是一个**建模选择**，不是测量结论。

---

## 5. "不在请求路径"的证据

### 5.1 代码位置

| 位置 | 事实 |
|---|---|
| `worker_rpc/server.rs` watchdog 循环（`sweep_revocations` 之后） | 唯一的生产触发点。它在 watchdog 线程上，不在任何请求线程上 |
| `runtime.rs::maintenance_tick` | 只做 `start()`（启线程）与 `observe()`；`observe` 只读信号 + 入队 |
| `maintenance/scheduler.rs::observe` | 不调用 `drain`、不启动线程；返回后队列里可能有单位，但没有任何东西执行它 |
| `maintenance/scheduler.rs::start` | 由 `maintenance_tick` 调用一次；线程持 `Weak`，runtime 释放后自然退出 |
| `maintenance/scheduler.rs::drain` | 只在驱动线程里调用；也是唯一调用 `budget.acquire` 与 `run_maintenance` 的地方 |
| `session/lane.rs`、`session/execution.rs`（commit / query 路径） | 不含 `checkpoint`、`compact_wal`、`drain`、`observe` 的任何引用 |

### 5.2 调度器与执行者的分离是可测的

- `observe` 需要 `&self`（不是 `&Arc<Self>`），因此它**不可能**自己启动驱动线程；
- 驱动线程只在 `start()` 里创建，而 `start()` 只在 `maintenance_tick` 里被调用；
- 因此"测试里只 observe、不 start"就等价于"排程了但没人执行"，这正是
  `maintenance::tests::maintenance_is_not_on_the_request_path` 断言的状态。

### 5.3 测试层面的证据

`maintenance::tests::maintenance_is_not_on_the_request_path`：

1. 建真实库、建表；
2. 断言 `attempts == 0 && executions == 0`（建表/插入没有 pick、没有执行任何维护）；
3. 显式 `observe` 一次 → 断言 `attempts >= 1` 且 `executions == 0`（探测只入队）；
4. 在"队列里已有待办"的状态下继续 16 次 insert + 一次 query；
5. 断言 `executions` 仍为 `0`——**这是"commit 返回与维护耗时解耦"的直接断言**；
6. 只有 `drain` 之后 `executions >= 1`，且数据仍是 16 行。

把 `checkpoint()` 塞进 `RemoteDatabase::execute` 会让第 2/5 步失败（见 §6 变异验证 M2b）。

---

## 6. 新测试与变异验证

### 6.1 新增测试

| 测试 | 位置 | 断言 |
|---|---|---|
| `cooldown_does_not_repeat_maintenance` | `maintenance/tests.rs` | 首次两种 lane 各跑一次（`drain(1)` 时串行）；此后 WAL 不再变小但连续 20 次探测**一单位都不跑**、`attempts` 也不再增长；越过冷却期后才再次可跑 |
| `watermarks_are_hysteresis_not_a_threshold` | 同上 | 穿越高水位 armed、回落到低水位解除、**带内不动** armed 状态；未 armed 时即使槽位早就过了也不跑 |
| `maintenance_is_not_on_the_request_path` | 同上 | 见 §5.3 |
| `a_long_reader_block_is_recorded_and_backs_the_database_off` | 同上 | `long_reader` 计数 +1；`attempts`/`completed` 为 0；阻塞把**另一个 lane**也一起推迟 |
| `an_unconfirmed_wal_skips_the_compaction_lane_before_any_watermark` | 同上 | `unconfirmed_wal` 时 compact lane 直接 `Skip(UnconfirmedWal)`，checkpoint lane 不受影响 |
| `a_spent_budget_skips_the_unit_instead_of_running_it` | 同上 | 预算耗尽时 `completed=1 / refused=1`，**目标只被调用 1 次**（被拒的不执行），`blocks["budget"]=1`、`failed=0` |
| `the_per_database_cap_holds_while_another_database_is_admitted` | 同上 | 同库第二个单位被 `PerDatabase` 拒；释放后可再用；另一库不受影响 |
| `the_global_cap_admits_one_unit_at_a_time` | 同上 | 全局第 2 个单位被 `GlobalConcurrency` 拒；释放后 in-flight 归零 |
| `the_request_budget_admits_a_rate_and_the_byte_budget_a_ceiling` | 同上 | 请求数是速率（窗口内不返还）；字节是上行天花板；超出分别返回 `Requests` / `Bytes` |
| `a_disabled_scheduler_never_picks_a_lane` | 同上 | 关闭时 `attempts == 0`、目标零调用、`decide()` 返回 `Skip(Disabled)` |
| `a_failing_unit_is_counted_as_a_failure_not_as_a_completion` | 同上 | 失败计入 `failed`，不计入 `completed` |
| `lanes_are_de_aligned` | 同上 | 两个 lane 的首槽、周期、相位都不同 |
| `the_enums_and_the_counters_agree_on_positions` | 同上 | 枚举判别式与计数器下标一致（防止重排后标签错位） |
| `the_reader_age_is_an_upper_bound_that_survives_the_reader` | 同上 | 峰值只升不降，读者离开后仍可查 |
| `a_long_reader_blocks_compaction_records_why_and_keeps_the_old_wal` | `database/connections.rs` | 真实库 + 长读：错误仍是 `Conflict`；`blocks["long_reader"]=1`；`executions=1`；`oldest_reader_peak_ms >= 20`；**`wal/index` 的 `replay_baseline_lsn`/`last_lsn`/条目数逐字段不变**；`wal_lsn` 不变；读者仍在自己快照上拿到 0 行；读者离开后 compact 成功且数据完好 |

### 6.2 变异验证（真的会失败）

方法：逐条把被测性质改坏，跑对应测试，确认失败后还原。脚本见 `/tmp/mutate.py`（临时文件，未入库）。

| 变异 | 改坏了什么 | 对应测试 | 结果 |
|---|---|---|---|
| M1 | `record_start` 不再设置冷却窗口 | `cooldown_does_not_repeat_maintenance` | 失败 ✔ |
| M2a | `observe` 末尾直接 `drain` | `maintenance_is_not_on_the_request_path` | 失败 ✔（`executions` 0→1） |
| M2b | `RemoteDatabase::execute` 尾部加 `checkpoint()` | 同上 | 失败 ✔（报错落在"请求路径不得跑维护"断言） |
| M3a | 不再记录 `long_reader` | `a_long_reader_blocks_compaction_records_why_and_keeps_the_old_wal` | 失败 ✔ |
| M3b | 去掉 `preflight_compaction()` | 同上 | 失败 ✔（错误从 `Conflict` 变成非 `Conflict`，即长读会在 dispatch 后才发现） |
| M4 | 忽略请求数预算 | `a_spent_budget_skips_the_unit_instead_of_running_it` | 失败 ✔ |
| M4b | 忽略字节预算 | `the_request_budget_admits_a_rate_and_the_byte_budget_a_ceiling` | 失败 ✔ |
| M5a | 忽略每库并发上限 | `the_per_database_cap_holds_while_another_database_is_admitted` | 失败 ✔ |
| M5b | 忽略全局并发上限 | `the_global_cap_admits_one_unit_at_a_time` | 失败 ✔ |
| M6 | 忽略"无未确认 WAL"前置条件 | `an_unconfirmed_wal_skips_the_compaction_lane_before_any_watermark` | 失败 ✔ |
| M7 | 把滞回带退化成单一阈值 | `watermarks_are_hysteresis_not_a_threshold` | 失败 ✔ |

11/11 变异全部使目标测试失败，且 11 个变异都能编译（变异未通过编译的情况不会被计为证据）。

**M3b 的额外信息**：去掉 preflight 后长读仍在 `require_complete_backfill` 处被拦下（WAL 索引没有被
rebase），但错误类型变成 `UnsettledSqlOutcome`。也就是说 preflight 的价值是把"长读阻塞"从
"已经 dispatch 且结果不定"提前成"dispatch 前干净拒绝"，这正是本次要保留的性质。

---

## 7. 未验证 / 不承诺

**性能与长时运行全部未测量。本文不宣称 p99、吞吐、冷启动、容量或公平性的任何改善。**

| 未做 | 说明 |
|---|---|
| 长时运行 | 没有跑"backlog 是否稳定"的观察；`maintenance.units.*` 与 `blocks.*` 计数器已就位，但没有任何长时数据 |
| 维护期间 p99 | 未测。开/关对比实验没有做 |
| 冷启动时间 | 未测，也没有证明会下降 |
| 回放速度 / 回填速度 | **未测**。因此 §4.1 的所有水位都没有依据 |
| 16/64/256 MiB | 直接沿用研究文档的实验输入，**不是校准结果** |
| 驱动线程开销 | 开启后常驻 50 ms 轮询；未测其 CPU 占用 |
| `blocked_backoff` 取值 | 未测长读事务时长分布 |
| 排程分离的收益 | 只证明了两个 lane 的槽位不同且串行执行，**未证明**共同尖峰因此减少 |
| Authority GC / Gateway journal 的对齐 | 未做（它们仍由各自所有者驱动，Rust 侧无固定周期） |
| suspend/迁移前预整理 | **未实现** |
| 渐进式 checkpoint | **明确不做**（引擎支持与语义验证都没有完成，不能假设与 Full 等价） |
| 指标 D 组的真实性 | 只在测试里验证过计数；没有在真实负载下核对过口径 |
| `oldest_reader_ms` 精度 | 是"会话开启年龄"的上界，不是读事务年龄；与真实读时长的偏差未测 |
| 默认值的生产适配性 | 默认关闭，所以未在生产形态下跑过开启状态 |

### 未验证项对应的后续动作

1. 按 §4.3 测回放速度与回填速度，再定水位；
2. 在开启状态下跑长时观察，记录 `wal_bytes`、`blocks.*`、`units.*.ns` 的时间序列；
3. 只有在 1、2 有数据之后，才评估是否需要给 Authority GC 加 Rust 侧周期并对齐相位。
