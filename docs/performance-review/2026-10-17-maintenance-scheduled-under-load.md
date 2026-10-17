# W4 维护调度长时观测：真实入口、低水位与一次 compaction

日期：2026-10-17。状态：**观测完成；负载侧因客户端限速缺陷被低估，已在本提交修复；性能、容量与 SLO 仍未测量**。
范围：`prototypes/v1` 本地全栈（Authority + 两台加密 Worker + Controller + Gateway + Key Broker），
数据集 100k 行，单机 `127.0.0.1`，TLS 开启，本地对象后端。
数据面：HEAD `3be6c8b` 的 debug 构建；自 `042476a` 起的提交只改测试与 `dev.py`，数据路径等价。
平台二进制沿用 `peri-platform-042476a`。运行目录 `/tmp/peri-maint-head-3be6c8b`，endpoint
`https://127.0.0.1:19444`。本机同时运行另一套旧栈，主机负载约 3–7，未做资源隔离。

**本文不宣称 PASS、容量、公平性或 SLO，也不给出任何加速倍数。**
`queries.json` 内 `performance` 与 `capacity` 恒为 `NOT MEASURED`，`correctness` 为 `INCOMPLETE`。

---

## 0. 一句话结论

在一段约 10 分钟、维护调度显式开启的运行里，两个保护条件都没有被违反：
**checkpoint 与 compact 都只经既有入口执行，WAL 在 compact 前没有被提前截断，compact 后 100000 行
仍可按公开 SQL 读回。** 8 次 checkpoint 中只有 2 次真正发布，其余 6 次是 8–11 ms 的空转；
1 次 compact 用 4.35 s 把 29.66 MiB 的 WAL 归零。全程 `failed`、`blocked.*`、`refused.*` 均为 0。
这不是开销结论：没有长读、没有 partial backfill、没有第二台 worker 参与，且负载侧被客户端缺陷压低。

---

## 1. 开关与实验输入

| 变量 | 值 |
|---|---|
| `PERI_WORK_METRICS` | `1` |
| `PERI_MAINTENANCE_SCHEDULE` | `1` |
| `PERI_MAINTENANCE_CHECKPOINT_LOW_BYTES` | 4 MiB |
| `PERI_MAINTENANCE_CHECKPOINT_HIGH_BYTES` | 8 MiB |
| `PERI_MAINTENANCE_COMPACT_LOW_BYTES` | 1 MiB |
| `PERI_MAINTENANCE_COMPACT_HIGH_BYTES` | 2 MiB |
| `PERI_MAINTENANCE_COOLDOWN_MS` | 5000 |
| `PERI_MAINTENANCE_BLOCKED_BACKOFF_MS` | 5000 |
| `PERI_MAINTENANCE_REQUESTS_PER_WINDOW` | 60 |
| `PERI_MAINTENANCE_BYTES_PER_WINDOW` | 1 GiB |

周期与相位是代码常量，不能由环境变量修改：checkpoint 60 s / phase 0，compact 300 s / phase 150 s。
因此**第一次 compact 槽位在数据库打开后 450 s**；本次观测窗口刚好跨过它。
这些水位是实验输入，不是校准结果，也不是建议部署值。

## 2. 方法

- 导入：`run_platform_import.py`，`--batch-size 250 --commit-rows 10000`。
  结束行 `planned=100000 attempted=100000 insert_ack=100000 commit_ack=100000 readback=100000
  stop=planned_rows_reached`；插入阶段 14.98 s，10 个 commit 点 0.406–0.790 s。
- 观测：一个 2 s 周期的采样器向两台 worker 的 `/control` 发 `GetMetrics`，记录
  `metrics.work_metrics.maintenance`、`resources.databases[].resources` 与会话数；共 734 条记录，
  无采样错误。这是**操作者遥测**，不是黑盒基准，也不用于推断用户侧延迟。
- 负载：导入后立即启动一个 520 s、并发 8、目标速率 20 ops/s 的只读臂，使用与九臂序列相同的
  manifest/数据集/客户端。事务边界与配额未改。
- 完整性：导入自身回读 100000 行；臂内 oracle 抽查 395 次、`mismatched=0`；compact 结束后再用
  公开 SQL 读 `count(*)` 与一个分组计数。

## 3. 维护时间线（worker-a）

所有时间来自 sampler；同一批次内两个 lane 的完成时刻受 2 s 采样粒度限制。

| 时刻 | 单元 | 计划回填字节 | 单元耗时 | WAL 字节 |
|---|---|---|---:|---:|---:|
| 11:00:04 | checkpoint #1 | 29 655 792 | 8.818 s | 29 655 792 |
| 11:02:28 | checkpoint #2 | 29 655 792 | 13.410 s | 29 655 792 |
| 11:03:14 | checkpoint #3 | 29 655 792 | 0.0091 s | 29 655 792 |
| 11:04:15 | checkpoint #4 | 29 655 792 | 0.0082 s | 29 655 792 |
| 11:05:13 | checkpoint #5 | 29 655 792 | 0.0109 s | 29 655 792 |
| 11:06:13 | checkpoint #6 | 29 655 792 | 0.0096 s | 29 655 792 |
| 11:07:13 | checkpoint #7 | 29 655 792 | 0.0111 s | 29 655 792 |
| 11:08:14 | checkpoint #8 | 29 655 792 | 0.0086 s | 29 655 792 |
| 11:08:48 | index_compact #1 | 29 655 792 | 4.353 s | 0 |

读法：

1. **`backfill_bytes` 是计费量，不是实际写出的字节。** 每次被 pick 的单位都按当时
   `signals.wal_bytes` 计入 `units.*.backfill_bytes`，包括 8–11 ms 的空转 checkpoint。
   因此“8 次共 237 MB”只能说明水位与计费口径，不能当作远端写入量或吞吐。
2. 真正昂贵的只有两次：首次发布（8.8 s）与数据库空闲卸载后重载的再次发布（13.4 s）。
   之后 WAL 镜像未变，Full checkpoint 很快返回；WAL 字节保持不变，说明 Full checkpoint
   **不会**截断 WAL，这与设计一致。
3. compact 在数据库打开约 450 s 后按槽触发，把 WAL 从 29.66 MiB 归零；耗时 4.35 s。
   它发生时并发臂仍有会话在跑，`failed=0`，随后公开 SQL 读到 100000 行。
4. 全程 `blocked`（budget/contended/long_reader/partial_backfill/unconfirmed_wal）与
   `refused`（bytes/global_concurrency/per_database/requests）都是 0。本次没有构造长读或
   partial backfill，不能据此说这些路径不会发生；它们由 `maintenance::integration_tests`
   的专门用例覆盖。

worker-b 全程 `loaded_databases=0`、`sessions=0`：本次分支的 8 个会话都落在 writer 所在
worker-a。这不是调度或容量结论，只说明本窗口内维护只在一台 worker 上发生。

## 4. 负载侧：一次被客户端缺陷压低的观测

臂的公开结果（`arms-maint/m5-long-c8-r20-520s/queries.json`）：

| 指标 | 值 |
|---|---|
| 计划 / 派发 | 4000 / 410 |
| success | 395 |
| known_failure | 7（全部 `quota_exceeded`） |
| timeout / unknown | 0 / 0 |
| not_dispatched（事件） | 8（`time_budget_reached`） |
| peak_in_flight | 5 |
| 成功操作端到端 | p50 16 ms，p95 40 ms，max 13.4 s |
| oracle | checked 395，mismatched 0 |

派发数远低于“20 ops/s × 520 s”。原因在客户端而不是服务端：
`run_platform_queries.py` 原先把每个操作的计划时刻写成
`time.monotonic_ns() + index / rate * 1e9`，即**从当前时刻再等 index/rate 秒**。
随着 index 增长，等待时间按平方累积：第 200 个操作的计划时刻落到 +135 s（应为 +10 s），
第 405 个落到 +533 s，最终只有 410 个操作进入派发。成功请求的服务时间仍是毫秒级，
说明服务端并未出现每请求十秒级的退化；**表中的低操作数不能被读成服务端吞吐**。

该缺陷已在本提交修复：所有操作的槽位改为从同一个固定 epoch 计算，`rate` 恢复为聚合到达率。
修复后必须重跑速率整形臂，才能对维护开启时的 goodput、拒绝原因或过载恢复作任何陈述。
同一缺陷也影响 `2026-10-17-nine-arm-stress-042476a.md` 中 h5–h7、h9 四个速率整形臂；
那四臂的观测计数仍然真实，但不能作为开环到达率或过载结论使用。该文已追加更正说明。

另外，worker-a 日志出现 3 条 `native_sql_rejected`，`cause_class=admission_deadline`；
客户端没有记录对应的 timeout/unknown，7 条 `known_failure` 全是 `quota_exceeded`。
两者不能按 1:1 对齐，本文只把它们列为同一窗口内的观测，不归因。

网关计数（`/v1/metrics`，全栈累计）：导入后 429 successful；臂结束后 843 successful、
7 rejected（全部 `concurrency_limit`）。该计数包含 open/close 与探针流量，不能与臂的 SQL
计数直接相减。

### 4.1 修复后的限速回归（同一栈）

同一栈上另跑一个 8 会话、rate 20、120 s 的臂（`m6-fixed-rate-c8-r20-120s`）：

| 指标 | 值 |
|---|---|
| 派发 / success / failure | 2408 / 2400 / 0 |
| timeout / unknown | 0 / 0 |
| 每 10 s 派发数 | 12 个区间各 200 |
| 派发滞后 | p50 9.09 ms，p95 10.0 ms |
| 整体成功速率（臂自报） | 19.941 ops/s |
| oracle | checked 2400，mismatched 0 |

派发与固定速率逐区间一致，说明修复后的 `rate` 是聚合到达率。该臂期间 WAL 为 0，
低于 checkpoint 低水位，维护 lane 未武装，`maintenance.units.*` 计数没有变化；因此它只用于
验证客户端限速，不是第二次维护观测，也不构成延迟或吞吐结论。


## 5. 完整性检查

- 导入回读：100000/100000，事务终态 `committed`，无失败项。
- compact 后公开 SQL：`SELECT count(*) FROM e01_records` → 100000；
  `WHERE group_id = 0` → 24727。
- 臂内 oracle：395 次逐行比对，`mismatched=0`。
- `correctness` 仍为 `INCOMPLETE`：`scale_coverage` 只达到按行数校验，未完成全部计划操作的
  oracle 回读；本文不把它提升为已验证。

## 6. 不能从本次运行推出的结论

- 不能推出维护开启后的请求延迟、goodput 或 CPU 开销；负载被客户端缺陷压低，且主机与另一套
  栈共享。
- 不能推出 16/64/256 MiB 档位的行为；本次只用了 4/8 MiB 与 1/2 MiB 两组实验输入。
- 不能推出长读、partial backfill 或预算拒绝路径的行为；本次这些计数都是 0，相关路径只有测试证据。
- 不能推出多 worker 维护并发或放置行为；worker-b 未加载该库。
- 不能推出容量、公平性或 SLO；`performance` 与 `capacity` 保持 `NOT MEASURED`。
- 不能把 8.8 s / 13.4 s / 4.35 s 当作稳定的单元成本；这是共享主机上的单次观测。
  它们只说明该窗口内一次完整发布与一次 WAL reset 的量级，可供后续校准实验作起点。

## 7. 后续

1. 用修复后的客户端继续覆盖 rate 100 与一次短过载（rate 20 的限速回归已完成，见 §4.1），
   分别记录成功、拒绝原因、timeout/unknown，确认派发率与计划一致。
2. 构造长读（持有 `BEGIN` + 查询）覆盖 `long_reader` 与阻塞退避；构造 partial backfill
   需要 preflight 看不到的读者，当前没有确定性的公开入口，仍需专门设计。
3. 按交付说明 §4.3 测冷启动回放速度，再决定 16/64/256 MiB 档位是否合理。
