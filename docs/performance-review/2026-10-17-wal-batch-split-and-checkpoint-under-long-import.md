# WAL 批次切分与长事务下的维护：700k 行导入的失败、修复与剩余边界

日期：2026-10-17。状态：**机制观测与修复完成；性能、容量、公平性与 SLO 均未测量**。
范围：`prototypes/v1` 本地全栈（Authority + 两台加密 Worker + Controller + Gateway + Key Broker），
单机 `127.0.0.1`，TLS 开启，本地对象后端。
数据面：Rust debug 二进制 `0e18e2a`（sha256 `1bd7f77c15ed31e05c1381ec11897ee42d6aa1f35abbc499e8f7df104db1b06c`）；
平台二进制 `peri-platform-042476a`（sha256 `ce76151d458a70f464dac88baf32b7fcc00d12668d3cf9415c44be318661d254`）。
对照运行还用了 `e6ba3e9` 的 Rust 构建。
启动时 HEAD 为 `2609828`，但该提交只包含测试与注释，**不在**这两个二进制内；
`0e18e2a` 构建自 detached worktree，未包含 `2609828`。

**本文不宣称 PASS、容量、公平性或 SLO，也不给出任何加速倍数。**
三次运行都以 `outcome=unclean` 收尾；`performance` 与 `capacity` 恒为 `NOT MEASURED`，
`correctness` 为 `INCOMPLETE`。文中的秒数都是单次观测，实验期间 `vm.loadavg` 为
4.00 / 3.26 / 3.14（1 / 5 / 15 分钟），不能当作基线或回归阈值。

---

## 0. 一句话结论

1. 在 `e6ba3e9` 上，700000 行导入在 `commit-62` 因单个 WAL append 超过
   `MAX_APPEND_PAYLOAD_BYTES = MAX_RPC_BYTES / 2 = 16 MiB` 硬失败，错误链为
   `wal_failed` → `UnsettledSqlOutcome`，导入中止且无法在进程内恢复。
2. `0e18e2a` 在 Worker 侧按字节与操作数切分超限批次后，同一导入 **700000 行全部提交并回读**
   （`attempted=insert_ack=commit_ack=readback=700000`）。
3. 维护开启时，导入长事务会长期占用 writer admission，checkpoint pick 连续返回
   `writer admission conflict`；导入结束、事务释放后 checkpoint 可以成功推进
   （第三次运行 `calls 3→5`，`failures` 仍为 3，两个成功单元合计 `ns=133.812854 s`）。
4. 但维护 checkpoint 用的是 **Full** 模式，不截断 WAL：704,140,992 B 的 WAL 仍在。
   本轮又显式关闭了 index_compact（`Truncate`），因此数据库空闲卸载后的第一次打开仍需支付
   整段加载；这次加载超过了 `2 × PERI_SQL_MAX_DURATION_MS = 10 s` 的请求截止点，
   平台收到 `writer admission deadline` 并判为 ambiguous，无法 clean drain。第三次运行同样
   `outcome=unclean`。
5. 这三次运行**没有**证明数据损坏，也**没有**证明 Unknown 操作未提交；它们只说明：
   批次切分修复有效，维护能在长事务释放后推进，而“Full checkpoint + 关闭 compact + 冷打开”
   这一组合在当前数据规模上仍以 unclean 结束。

---

## 1. 实验输入与三次运行

数据集：700000 行，payload 64 / 128 / 256 B，`--batch-size 250 --commit-rows 10000`，
单条 SQL 上限 61257 B；`run_platform_import.py` 不自动重放失败或 Unknown 请求。
维护开关：`PERI_MAINTENANCE_SCHEDULE=1`、`PERI_WORK_METRICS=1`，
checkpoint 水位 64 / 256 MiB（默认），index_compact 水位 **1 GiB / 2 GiB（本轮显式不武装）**；
cooldown 30 s、blocked backoff 120 s、周期 checkpoint 60 s / compact 300 s 均为默认常量。

| 运行 | Rust 二进制 | 运行目录 / 端口 | 证据目录 | 导入结果 | 关栈 |
|---|---|---|---|---|---|
| 1 | `e6ba3e9` | `/tmp/peri-maint-256-e6ba3e9` / 19744 | `maint-256-20261017T1216Z` | 中止于 `commit-62` | unclean |
| 2 | `0e18e2a` | `/tmp/peri-maint-256-0e18e2a` / 19844 | `maint-256-20261017T0439Z` | 700000 行成功 | unclean |
| 3 | `0e18e2a` | `/tmp/peri-maint-256c-0e18e2a` / 19845 | `maint-256-20261017T0447Z` | 700000 行成功 | unclean |

证据根目录：`~/.peri-e01/stress-20261017/`。三次运行都用
`python3 prototypes/v1/platform/dev.py --run-dir <RD> --tls --no-build` 启动，
导入后没有自动重试、没有回滚、没有手工改写数据库状态。

---

## 2. `0e18e2a`：WAL 超限批次的切分

### 2.1 根因

第一次运行的平台日志给出了直接错误文本：

```
SQL outcome unknown: request_id=e01-imp-commit-62 action=execute stage=tool_rpc
rpc=ambiguous=true,rejected=false,execution_completed=false
observation=current SQL operation outcome is unsettled: ... Internal error: remote IO: WAL payload limit exceeded
```

Authority 侧的 `WalAppendRequest::validate` 对单个 append 有两条硬上限
（`prototypes/v1/src/wal.rs`）：

- `MAX_APPEND_OPERATIONS = 16_384`（操作数）；
- `MAX_APPEND_PAYLOAD_BYTES = MAX_RPC_BYTES / 2 = 16 MiB`（所有 `Write` payload 之和）。

导入的每个 `COMMIT` 把自上一个提交点以来的 WAL 帧累积成一个批次；在该数据规模上，
`commit-62` 的批次超过了 16 MiB。Authority 拒绝后，Worker 的 `io` 把失败记为粘性
`wal_failed`，后续 SQL 以 `UnsettledSqlOutcome` 结束，而不是自动重试或静默丢弃。
导入客户端随后按 Unknown 处理并终止，审计追加
`sequence 2661, action=execute, outcome=outcome_unknown, request_id=e01-imp-commit-62`。

### 2.2 切分语义

`0e18e2a` 在 Worker 的 `RemoteWal::append_commit` 中先调用 `split_batch`，再逐条
`append_one`：

- 按“所有 `Write` payload 之和 ≤ 16 MiB”和“操作数 ≤ 16 384”切分；
- 单个大 `Write` 按绝对偏移切开，重放按偏移应用，因此边界等价；
- **只有最后一条**携带 `position`（提交标记），前面的记录只是未提交前缀；
- 单条记录沿用调用方的 `append_id`；切分后每条使用 `{append_id}-{index}`，
  便于 Authority 回执按确定性身份核对；
- 空批次仍发布一条空记录，保持既有回执语义；
- 不新增任何自动重试：失败仍然向上抛，Unknown 仍然按 Unknown 处理。

### 2.3 验证

`mod split_tests` 五个用例全部通过：

- `oversized_write_is_split_and_only_the_last_record_commits`
- `recovery_accepts_a_split_record_chain_ending_in_a_commit`
- `operation_limit_is_split_without_reordering`
- `authority_limits_reject_an_unsplit_record`
- `interrupted_split_keeps_the_prefix_and_no_commit`

提交时全量 `cargo test --lib` 为 309 passed，`cargo clippy --all-targets --no-deps -- -D warnings`
退出码 0。端到端证据是第二次与第三次运行：**同一份 700000 行输入从 `commit-62` 失败变为
全部提交**。这只证明“该硬失败被移除”，不证明吞吐或延迟。

---

## 3. 第一次运行：`commit-62` 中止（`e6ba3e9`）

导入客户端与 Worker 的计数：

| 字段 | 值 |
|---|---|
| planned / attempted / insert_ack | 700000 / 630000 / 630000 |
| commit_ack | 620000 |
| commit 点 | 62（最后一个 `rows=620000, seconds=2.8823`） |
| stop_reason | `failure:outcome_unknown` |
| transaction_final_state | `stopped_without_rollback_or_resume` |
| session_poisoned / session_close | `true` / `session_unusable` |

维护与 WAL（2 s 采样，104 条无错采样）：

| 指标 | 值 |
|---|---|
| WAL 峰值 | 542,109,632 B |
| `pending_wal_bytes` 峰值 | 15,169,840 B |
| checkpoint 单元 | `calls=2, failures=2, ns=33_375, backfill_bytes=729,685,024` |
| `blocked.*` | 全 0 |
| Worker 日志 | 两条 `maintenance outcome=blocked lane=checkpoint elapsed_ms=0 reason=writer admission conflict` |

两次 checkpoint 都是在导入长事务持有 writer owner 时被 pick 的，因此立即
`Conflict`，计入 `failures` 而不是 `blocked.*`。这与既有口径一致：
admission 拒绝表示“该单元没有进入执行”，不是预算或读者阻塞。

关栈：`launcher-stop.json` 最终为 `outcome=unclean`、`unresolved_operations=not_verified`，
platform `exit_code=1`，worker-a / worker-b / authority 全部 `signal=9`。审计最后一条是
`outcome_unknown`；**本文不据此宣称该事务未提交**，也不宣称数据页一致——
只有后续带证明条件的恢复路径才能回答。

---

## 4. 第二次运行：导入成功但查询 Unknown（`0e18e2a`）

| 字段 | 值 |
|---|---|
| planned / attempted / insert_ack / commit_ack / readback | 700000 / 700000 / 700000 / 700000 / 700000 |
| stop_reason | `planned_rows_reached` |
| transaction_final_state / session_close | `committed` / `closed` |
| import_seconds / commit_seconds / ddl_seconds | 109.2097 / 177.0808 / 0.1339 |
| durable_rows_per_second | 2445.069（口径：`readback==planned` 且 `commit_ack==planned` 且 `insert_ack==planned`；分母为 INSERT+COMMIT 阶段秒） |
| commit 点 | 71（最大 `rows=680000, seconds=5.2243`） |
| checkpoint 单元 | `calls=3, failures=3, ns=30_916, backfill_bytes=1,303,098,416` |
| WAL / pending | 704,140,992 B / 0 |

`hold_and_query_256.py` 的记录：

```
{"event":"open","service_ms":null,...}
{"event":"alive_error","index":1,"message":"outcome_unknown",...}
{"event":"final_count_error","message":"session_unusable",...}
{"event":"close_error","message":"session_unusable",...}
```

打开成功，但第一次 `SELECT 1` 在约 30 s 后以 `outcome_unknown` 结束，随后
`count(*)` 与 `close` 都被拒绝为 `session_unusable`。**这是 Unknown，不是“未提交”的证明**；
客户端没有重放该查询。

平台日志：

```
platform stopped: platform shutdown incomplete: gateway drain exceeded the shutdown budget
platform shutdown incomplete: operation checkpoint not attempted
platform shutdown incomplete: audit tail not attempted
```

关栈同样 `unclean`、`not_verified`、worker/authority `signal=9`。

注意：证据里的 `hold_exit=0` 只是客户端脚本捕获异常后正常退出，**不是查询成功**；
`hold.ndjson` 的四条记录才是实际结果。

---

## 5. 第三次运行：checkpoint 成功推进，冷打开失败（`0e18e2a`）

### 5.1 导入与维护时间线

导入结果与第二次一致：

| 字段 | 值 |
|---|---|
| planned / attempted / insert_ack / commit_ack / readback | 700000 / 700000 / 700000 / 700000 / 700000 |
| import_seconds / commit_seconds / ddl_seconds | 108.7984 / 177.312 / 0.131 |
| durable_rows_per_second | 2446.608（同上计数一致性口径） |
| commit 点 | 71（最大 `rows=690000, seconds=5.2815`） |

维护时间线（时间来自 sampler；`/control` 在重回填期间会超时）：

| 时刻（Z） | 观测 |
|---|---|
| 04:52:54 | 导入结束，700000 行回读通过 |
| 04:53:20 / 04:53:52 / 04:54:24 / 04:54:56 | sampler 连续 4 次 `TimeoutError: timed out`（每次 30 s） |
| 04:55:28 | `checkpoint calls=3 failures=3 ns=34_000 backfill=1,303,098,416`；数据库 `loaded=1`，`wal_bytes=704,140,992`，`sessions=0` |
| 04:55:33 | `checkpoint calls=5 failures=3 ns=133,812,853,667 backfill=2,711,380,400`；`native_instances=0`，数据库已卸载 |
| 04:56:32 | hold 打开会话 |
| 04:56:33–04:56:58 | sampler 看到 `lifecycle=loading, active_sessions=1` |
| 04:56:59 | 平台记录 `outcome_unknown`（见 §5.2） |

读法：

1. `backfill_bytes` 是 **pick 时的 `signals.wal_bytes` 计费量**，不是实际写出的字节。
   04:55:28 的 `1,303,098,416` 是三次失败单元各自 pick 时 WAL 水位之和；
   04:55:33 的 `2,711,380,400` 与它相差 `1,408,281,984`，正好等于
   `2 × 704,140,992`——两个成功单元各按当时的完整 WAL 水位计费一次。
   这同时说明 Full checkpoint 没有改变 `wal_bytes`，不能把计费量当作两倍远端写入量。
2. `calls` 在单元完成时递增（`MaintenanceTimer::finish`），因此 `3→5` 表示两个 checkpoint
   单元在 04:55:28–04:55:33 之间完成，`failures` 仍为 3；总单元时间 133.812854 s。
   由于全局并发为 1，两个单元只能串行。把总时间拆成“约 130 s 的长单元 + 约 4 s 的短单元”
   是根据 `calls/ns` 与采样窗口的**推断**，不是逐单元采样；
   长单元的执行窗口与 04:53:20–04:54:56 的四次 `/control` 超时相容。
3. 单元成功发生在数据库仍加载、且 writer owner 已释放的窗口；完成后数据库因空闲
   （`PERI_WORKER_DB_IDLE_UNLOAD_MS=60000`）被卸载。这是本会话首次观测到 704 MB WAL 上的
   checkpoint **成功推进**；前两次运行只有 `conflict`。
4. `blocked.*` 全 0：成功单元没有被记为 partial backfill、long reader 或 unconfirmed WAL。
   本轮没有构造长读，不能据此说这些路径不会发生。

`/control` 的 30 s 超时不等于 Worker 停止服务。`resources()` / `maintenance_signals()`
在 `e6ba3e9` 后先取存储锁；发布仍会持存储锁跨远程写，而 `GetMetrics` 需要读取
`resources()`，因此采样可能在重回填窗口被阻塞。本文只把四次超时当作采样窗口的观测，
不把它归因为单一锁或数据面故障。

### 5.2 hold 打开失败：截止点在加载之后才被检查

`hold.out` 的调用栈停在 `platform.open_session`，平台返回 `outcome_unknown`；
平台日志：

```
SQL outcome unknown: request_id=maint256-hold-open action=open stage=open_session
database=local_default/demo/maint256c ... worker=
rpc=ambiguous=true,rejected=false,execution_completed=false observation=writer admission deadline
platform stopped: gateway drain: gateway has unresolved upstream outcomes; terminal proof required
```

Worker 侧的实际路径（`prototypes/v1/src/worker_rpc/sessions.rs` 与
`prototypes/v1/src/runtime.rs`）：

1. `Sessions::open` 先 `refresh_initial_image`，再检查 `Instant::now() < deadline`；
2. `runtime.open_session` 进入 `load_instance`，**加载期间 `RequestDeadline` 被挂起**，
   只用 `load_budget`（默认 `max(10 s, 60 s) = 60 s`）约束；
3. 加载耗时约 26 s（sampler 的 `lifecycle=loading` 窗口）；
4. 加载完成后 `make_handle` → `database.session()` → `RequestDeadline::new` 取
   “请求级截止点（`accepted + 2 × PERI_SQL_MAX_DURATION_MS = 10 s`）”与自身预算的较小者；
   此时 10 s 早已用尽，`operation_until` 的 `deadline.check()` 立即返回
   `AdmissionError::Deadline`，显示文本即 `writer admission deadline`。

因此这条错误**不是** writer 队列等待超时；它是加载完成后的请求截止点检查。
`ExecutionEvidence` 为 `execution_completed=false`，但平台仍把这次 open 记为 ambiguous
（`rejected=false, execution_completed=false`）；gateway 无法把它当作可终止的拒绝，
要求 terminal proof，于是拒绝 clean drain。

Worker 日志在关栈阶段还有一条：

```
level=error event=ownership_detach outcome=failed database=local_default/demo error=writer admission quarantine
```

这是关栈阶段 admission 已处于隔离状态的留痕；它与失败打开同属一个收尾序列，
但现有日志不足以区分因果，本文不作归因。

### 5.3 为什么 WAL 仍有 704 MB

维护 checkpoint 使用 `CheckpointMode::Full`，回填后**不截断日志**；这一点与
`2026-10-17-maintenance-scheduled-under-load.md` 的观测一致。本轮把 index_compact 的
低水位显式设为 1 GiB、高水位 2 GiB，因此 `Truncate` 从未被武装：704,140,992 B 的 WAL
一直保留。随后数据库空闲卸载，下一次打开必须重新加载这段历史；这正是 §5.2 的 26 s 窗口。

### 5.4 关栈

`launcher-stop.json` 最终为：

```
outcome=unclean
unresolved_operations=not_verified
stages: platform exit_code=1; worker-a/worker-b/authority signal=9
reason: platform exited without a clean proof; worker-a/b/authority skipped graceful shutdown after prior failure
```

端口 19845 在关栈后无监听，`pgrep` 无残留进程；`run.log` 里的 `STOP_OUTCOME=None`
是脚本在 shutdown 仍为 `in_progress` 时读取 `launcher-stop.json` 的时序现象，
不是另一个结果。以最终文件为准。

---

## 6. 解释与边界

### 6.1 `writer admission conflict` 是长事务下的预期结果

`WriterAdmission::maintenance()` 只在
`owner.is_none() && queue.is_empty() && !maintenance` 时成功，否则立即返回 `Conflict`，
不排队、不等待。导入事务持续持有 writer owner，所以每次 checkpoint pick 都
`elapsed_ms=0` 失败；它被计入 `units.checkpoint.failures`，而不是 `blocked.*`。
这是设计选择（fail fast），不是 io 回归，也不是 `4bb7fd9` 的问题。

### 6.2 维护可以推进，但只在长事务释放之后

第三次运行从 `calls=3` 到 `calls=5` 证明失败不是永久性的：导入结束、owner 释放后，
checkpoint 能完成；WAL 水位仍是 704,140,992 B（Full 不截断），因此“成功”不等于
WAL 被回收；回收需要另一条 `Truncate` 路径（index_compact）。

### 6.3 冷打开失败是加载预算与请求截止点的结构性关系

`2026-10-17-open-load-stall.md` 已在 100k 行、约 16 s 冷加载上记录了同一类失败；
本轮把它扩展到 700000 行、704 MB WAL、约 26 s 冷加载。关键事实不是某个绝对秒数，而是：

- 加载不可中断，期间请求截止点被挂起；
- 加载结束后，后续的 `database.session()` 会立刻用已经耗尽的请求截止点做检查；
- 因此“加载成功”仍然可以表现为 `writer admission deadline`，并被平台当作 ambiguous；
- 平台按 fail-closed 规则拒绝 clean shutdown，运行以 unclean 收尾。

这条路径是当前“大 WAL + Full checkpoint + 不 compact”组合的直接后果。本文不把它
归因于 WAL 批次切分或 `e6ba3e9` 的锁序改动。

### 6.4 尚未修复的相邻上限（算术推断，未实测）

1. **`truncate` 的 baseline 镜像**：`RemoteWal::truncate` 把完整镜像作为**一条**
   `EncodedWalRecord`，经 `self.chunk(...)` 上传，**绕过** `WalAppendRequest::validate`
   的 16 MiB 检查；而 `chunk` 仍受 `MAX_RPC_BYTES = 32 MiB` 与 JSON/base64 放大约束。
   704 MB 的 WAL 镜像不可能通过这条路径。若真的触发，`io.compact_wal` 会失败并置
   `wal_failed`，随后进入与第一次运行相同的粘性失败。具体可用阈值没有实测，
   不得把“约 18 MiB”当作已验证数字。
2. **`wal/index` 的单对象增长**：索引随记录数增长，最终同样逼近 `MAX_RPC_BYTES`；
   超过后打开、追加与 compact 都会硬失败。本轮没有把 WAL 增长到那一步，未实测。
3. **长事务下的维护排队**：当前 `maintenance()` 立即 `Conflict`，因此长事务期间 checkpoint
   完全无法推进。是否改成可取消的排队属于调度策略变更，需要单独设计或验收，
   本轮没有改。

在修掉 `truncate` baseline 之前，不应在这类数据规模上把 index_compact 重新武装；
在修掉冷打开与请求截止点的关系之前，也不应把“checkpoint 成功”读成“数据库可立即重开”。

---

## 7. 结论与不宣称

已证实的：

- `e6ba3e9` 的 `commit-62` 失败由单次 WAL append 超过 16 MiB 触发，错误文本与审计记录一致；
- `0e18e2a` 的切分使同一份 700000 行导入全部提交并回读；
- 长事务期间 checkpoint 的 `conflict` 是预期行为，事务释放后 checkpoint 能成功推进；
- Full checkpoint 不截断 WAL，本轮 704,140,992 B 的 WAL 在两次成功 checkpoint 后仍在；
- 冷打开在 10 s 请求截止点内未完成时，平台得到 ambiguous 的 `writer admission deadline`，
  并以 unclean 关栈。

不宣称的：

- 任何吞吐、延迟、容量、公平性或 SLO 结论；表中的秒数都是单次观测；
- Unknown 操作“未提交”或“已提交”；没有 durable 状态查询或恢复证明；
- 数据页一致性、无数据丢失或可安全重启；三次关栈均为 `unresolved_operations=not_verified`；
- `truncate` baseline 与 `wal/index` 上限的具体阈值；它们只是算术推断，未实测；
- 完整 Hrana 或官方 SDK 兼容；本轮只覆盖本地全栈的公开 SQL 路径。

`correctness` 为 `INCOMPLETE`：导入自回读 700000 行只证明该次导入的计数与行链自洽，
不是独立 oracle 的逐行回读；第二次运行的 `count(*)` 与第三次运行的最终计数都没有取得
可信回执。

---

## 8. 证据索引

| 运行 | 关键文件 |
|---|---|
| 1 | `maint-256-20261017T1216Z/import/import.json`、`metrics-after-import.json`、`samples.ndjson`、`run.log`；`/tmp/peri-maint-256-e6ba3e9/logs/{platform,worker-b}.log`、`audit/events.json`、`launcher-stop.json` |
| 2 | `maint-256-20261017T0439Z/import/import.json`、`metrics-after-import.json`、`hold.ndjson`、`run.log`；`/tmp/peri-maint-256-0e18e2a/logs/platform.log`、`launcher-stop.json` |
| 3 | `maint-256-20261017T0447Z/import/import.json`、`metrics-wait-{1,2,3}.json`、`metrics-final.json`、`samples.ndjson`、`hold.out`、`run.log`；`/tmp/peri-maint-256c-0e18e2a/logs/{platform,worker-b}.log`、`launcher-stop.json` |

启动、导入与采样命令只引用本地脚本；凭据从运行目录的私有文件读取，
未写入 argv、日志或本文。`connection.json` 与 `e01-cred` 不在本文中回显。
