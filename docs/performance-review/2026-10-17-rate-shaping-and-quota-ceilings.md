# 修复后客户端的速率整形与平台准入配额：rate 50 / rate 100 / 短时过载

日期：2026-10-17。状态：**探索测量完成；性能、容量与 SLO 仍未测量，correctness 为 INCOMPLETE**。
范围：`prototypes/v1` 本地全栈（Authority + 两台加密 Worker + Controller + Gateway + Key Broker），
数据集 100k 行，单机 TLS，本地对象后端。数据面二进制为 HEAD `3be6c8b` 源码构建的 debug 版本，
平台二进制 `peri-platform-042476a`。运行目录 `/tmp/peri-maint-rate100-e12a628`，endpoint
`https://127.0.0.1:19544`。同一主机上另有其他实验栈在跑，未做资源隔离。

**本文不宣称 PASS、容量、公平性或 SLO，也不给出加速倍数。** 所有 `queries.json` 中
`performance` 与 `capacity` 恒为 `NOT MEASURED`，`correctness` 为 `INCOMPLETE`。

---

## 0. 一句话结论

修复后的客户端把派发时刻改为“固定起点 + index / rate”后，rate 50 与 rate 100 的实测派发率
分别为 49.8/s 与 100.0/s，与计划一致；在该项目配额 `requests_per_second = 100` 下，rate 50
只出现 1 次 `quota_exceeded`，rate 100 出现 1122 次，成功吞吐 71.9/s。32 会话的短时过载
只准入 16 个会话（`max_sessions = 16`），4000 次尝试在 6.33 s 内完成，其中 456 次成功、
3544 次被配额拒绝；随后不重启栈的 rate 50 臂在 20 s 内得到 996/1000 成功（4 次配额拒绝）。
所有成功操作逐条 oracle 比对，`mismatched = 0`。同一栈的维护调度照常工作：导入后约 70 s
完成一次 9.594 s 的 checkpoint 发布，重载后第一次 compact 用 4.409 s 把 WAL 归零，
失败与阻塞计数为 0。

这不是服务端 SQL 能力上限：拒绝来自平台准入层，且本实验没有把 `max_concurrent = 4` 与
`requests_per_second = 100` 解耦。

---

## 1. 环境与实验输入

| 项 | 值 |
|---|---|
| 数据面 | debug 二进制 `565a449ff901d554b84eb9705160cfabe24e1ec06a87852fb473e890c0ff2da2`（`3be6c8b` 源码构建） |
| 平台 | `peri-platform-042476a` |
| 维护开关 | `PERI_MAINTENANCE_SCHEDULE=1`，`PERI_WORK_METRICS=1` |
| checkpoint 水位 | low 4 MiB / high 8 MiB |
| compact 水位 | low 1 MiB / high 2 MiB |
| 冷却 / 阻塞退避 | 5000 ms / 5000 ms |
| 维护预算 | 60 requests / 1 GiB per window |
| 数据集 | 100k 行；导入 14.6243 s，insert/commit/readback 均为 100000，`failures = []` |
| 客户端 | 本提交修复后的 `run_platform_queries.py`（固定 epoch 的聚合到达率） |

项目配额（平台 state，本实验未修改）：`max_sessions = 16`、`max_concurrent = 4`、
`requests_per_second = 100`、`max_sql_bytes = 65536`、`max_result_bytes = 4194304`。

数据库 `local_default/demo/maint2` 是 `main` 的 branch，创建时被放置在 worker-b（epoch 2）；
维护与导入负载都落在 worker-b。worker-a 在本实验中没有加载该 branch。

---

## 2. 速率整形复验

四个臂都使用同一 4000 操作的离线 manifest、8 或 32 个真实独立会话（`per-request` 模式）、
关闭自动重试。`rate = 0` 表示不整形、尽力派发。

| 臂 | 并发 | rate | attempted | success | known_failure | not_dispatched | success ops/s | 派发滞后 p50 / p95 |
|---|---|---|---|---|---|---|---|---|
| rate 50 | 8 | 50 | 2007 | 1999 | 1 | 7 | 49.812 | 8.95 / 9.97 ms |
| rate 100 | 8 | 100 | 4000 | 2878 | 1122 | 0 | 71.927 | 8.91 / 9.98 ms |
| 短时过载 | 32 | 0 | 4000 | 456 | 3544 | 0 | 72.000 | 3174 / 5960 ms |
| 过载后恢复 | 8 | 50 | 1000 | 996 | 4 | 8 | 49.427 | 9.1 / 10.0 ms |

- rate 50 的查询阶段墙钟 40.13 s，派发 2000 次（`not_dispatched = 7` 是 40 s 预算截断，
  不是拒绝）；rate 100 的查询阶段墙钟 40.01 s，派发 4000 次。两者与计划的偏差都在
  一个操作以内，说明修复后的 `rate` 是聚合到达率；作为对照，修复前的同一客户端在
  rate 20 下实际只派发约 0.8 ops/s。
- 所有 `known_failure` 的错误类都是 `quota_exceeded`（平台 429）。没有 timeout，没有
  Unknown。客户端按技能约束不重试、不重放，失败计数即准入拒绝计数。
- 成功操作的 oracle 逐条比对：rate 50 为 1999/1999，rate 100 为 2878/2878，
  短时过载为 456/456，过载后恢复为 996/996，`mismatched = 0`。

### 2.1 配额上限与恢复

- rate 50 的运行时长略超一个 100 请求/秒的窗口，但只收到 1 次 `quota_exceeded`；
  rate 100 的到达率与配额相等，1122 次被拒（28.1%）。这说明可观测的拒绝边界就在
  `requests_per_second = 100` 附近，而不是在 50 附近。
- 短时过载臂在 6.33 s 内派发了全部 4000 次尝试，3544 次被拒；`sessions_opened = 16/32`，
  另外 16 个 open 也返回 429。会话数正好等于 `max_sessions = 16`。在一次更早的同类臂里，
  一个未关闭的探测用 `main` 会话占用了同一名额，结果停在 15/32；该臂只用于确认
  `max_sessions` 的计数方式，本文的表格使用关闭探测会话后的干净重跑。
- 过载臂结束后未重启栈、未改动配置，立即跑 rate 50：1000 次尝试中 996 次成功、
  4 次配额拒绝、8 次因 20 s 预算未派发。这只能说明该窗口后的准入基本恢复，不能推广为
  “任意过载都能自愈”。

### 2.2 延迟观测

| 臂 | index_list p50 / p95 / max | point p50 / p95 / max |
|---|---|---|
| rate 50 | 38.35 / 59.33 / 109.25 ms | 21.28 / 32.19 / 96.55 ms |
| rate 100 | 14.99 / 32.01 / 10029.45 ms | 19.19 / 35.05 / 153.75 ms |
| 短时过载 | 2476.28 / 3132.99 / 3211.48 ms | 823.44 / 1599.77 / 1689.87 ms |
| 过载后恢复 | 无样本（预算只覆盖前 1000 个操作） | 22.99 / 31.91 / 157.25 ms |

rate 100 臂中 `index_list` 有一条 10.03 s 的端到端样本。它与该 branch 导入后约 70 s
的一次 checkpoint（9.594 s，见 §3）在同一时间窗口内；这里只记录重合，不把它归因为
checkpoint 阻塞。短时过载臂的端到端延迟主要由派发排队构成：`dispatch_lag` 的 p50 已达
3.17 s，与端到端同一量级。所有延迟都是单机共享主机上的一次观测，不构成 SLO 证据。

---

## 3. 同一栈上的维护观测

worker-b 的 `maintenance.units` 时间线（首次加载段 2 s 采样；compact 观察段 5 s 采样，
输出见证据目录）：

| 事件 | `checkpoint.backfill_bytes` | `calls` | `ns`（累计） | 说明 |
|---|---|---|---|---|
| 导入结束、maint2 初次加载 | 0 | 0 | 0 | WAL 尚未越过水位 |
| 约 70 s 后 | 29 655 792 | 1 | 9 594 345 917 | 首次完整发布，9.594 s |
| 下一周期 | 59 311 168 | 2 | 9 600 299 917 | 第二次仅 5.95 ms，空转 |
| 后续周期 | 不变 | 3、4 | 不变 | 继续空转 |

- `blocked.*` 与 `refused.*` 全程为 0；`failures = 0`。
- `backfill_bytes` 是 lane 被 pick 时按 `signals.wal_bytes` 计费的量，不是实际写出的字节数；
  Full checkpoint 不截断 WAL，所以发布后 `resources.databases[].wal_bytes` 仍是 29 655 792。
- worker 在导入与查询结束后按 idle 策略卸载了该数据库（`loaded_databases = 0`）。compact 的
  slot 按本次加载的 `opened` 计算（`period 300 s + phase 150 s`），所以第一次加载没有等到
  compact。为了覆盖 compact，实验在数据库卸载后重新打开一个保持会话，让数据库保持加载；
  compact 的首个 slot 为重新加载后的 450 s。重载后 checkpoint 计数从 4 继续累加，说明
  ledger 计数跨实例保留，而相位以新的 `opened` 重新计算。

### 3.1 首次 index compact

重新加载后 `checkpoint` 又跑了 7 次：第 5 次（重载后约 60 s）是完整发布，增量 12.438 s
（比首次 9.594 s 长，含冷段加载）；第 6–11 次是 8.1–10.5 ms 的空转。随后 compact 在
重载后约 450 s 触发：

| 指标 | 值 |
|---|---|
| `index_compact.calls` | 1 |
| `ns` | 4 409 268 625（4.409 s） |
| `backfill_bytes` | 29 655 792 |
| `failures` | 0 |
| `blocked.*` / `refused.*` | 全 0 |
| compact 后 `wal_bytes` | 0 |

- compact 内部先做 truncate checkpoint；本次没有读者持锁，所以 backfill 完整、WAL 被重置。
- compact 后公开 SQL：`SELECT count(*) FROM e01_records` → 100000；
  `WHERE group_id = 0` → 24727。与导入回读一致。
- 4.409 s 与先前一次独立运行的 4.353 s 同量级，但仍是共享主机上的单次观测，不是稳定成本。

---

## 4. 16/64 MiB 档的一次 checkpoint 观测

这一节是另一个栈（端口 `19644`，branch `maint3`，worker-b epoch 2）上的独立观测：
只把 checkpoint 的 low/high 改为 16/64 MiB，compact 也改为 16/64 MiB，其余开关与 §1 相同。
它不是档位对比实验：主机负载、数据集大小与 §2/§3 都不同。

| 指标 | 值 |
|---|---|
| 导入 | 220000 行，33.0714 s，4290.339 durable rows/s，readback 220000，`failures = []` |
| 导入后 WAL | 95 097 872 B（约 90.7 MiB），高于 64 MiB 高水位 |
| 第一次 checkpoint pick | 失败，`ns = 15 416`（15.4 µs），`backfill_bytes = 88 398 752` |
| worker 日志 | `maintenance outcome=blocked lane=checkpoint elapsed_ms=0 reason=writer admission conflict` |
| 第二次 checkpoint pick | 成功，`ns = 23 717 541 666`（23.718 s），`backfill_bytes = 95 097 872` |
| `failures` | 1（第一次），之后不再增长 |
| 发布后 WAL | 95 097 872 B（Full checkpoint 不截断） |

- 第一次 pick 与导入末尾的写入重叠，被 writer admission 拒绝。这是保护写入的保守拒绝，
  不是数据错误；它计入 `units.checkpoint.failures`，但**不**计入 `maintenance.blocked.*`
  （后者全 0），运行时日志把它描述为 `blocked`。两个计数口径不能混用。
- 第二次 pick 在约一个周期后成功，回填 90.7 MiB 用 23.718 s（约 4.0 MiB/s 的描述量级）。
  与 §3 的 29.66 MiB / 9.594 s 相比，本文不据此拟合吞吐或宣称线性。
- compact 在同一档位下的首次 slot 在打开后 450 s；结果见 §4.1。

### 4.1 16/64 MiB 档的首次 compact

数据库在导入后按 idle 策略卸载；重新加载后 `resources.databases[].wal_bytes` 读到 0，
说明 WAL 已在加载前完全发布。compact 的低水位是 16 MiB，因此即使 450 s 的 slot 已过，
lane 也不会武装：采样到 `resident_age = 451 s` 时 `index_compact.calls` 仍为 0，
`blocked.*` 也全为 0（低于低水位是 Skip，不是 block）。

为触发 compact，实验通过公开 SQL 向新表 `e01_compact_probe` 追加 40000 行（15.254 s），
让 WAL 越过低水位。写入期间 slot 已过，compact 随即被 pick：

| 指标 | 值 |
|---|---|
| `index_compact.calls` | 1 |
| `ns` | 13 075 256 250（13.075 s） |
| `backfill_bytes` | 95 097 872 |
| `failures` | 0 |
| `blocked.*` / `refused.*` | 全 0 |
| compact 完成后采样到的 `wal_bytes` | 5 121 192 B |

- 5 121 192 B 是采样时探测脚本收尾批次仍在写入的残量；本文不把它归为 compact 未完成，
  也不宣称这次 compact 后 WAL 归零。
- compact 后公开 SQL：`SELECT count(*) FROM e01_records` → 220000；
  `SELECT count(*) FROM e01_compact_probe` → 40000。写入的数据没有丢失。
- 13.075 s 与 §3.1 的 4.409 s 来自不同数据集与 WAL 大小（95 MB 对 29.66 MB），
  不构成档位对比，也不用于拟合吞吐。

## 5. 不能从本次运行推出的结论

- 不推出服务端 SQL 吞吐上限：429 来自平台准入，`max_concurrent = 4` 与
  `requests_per_second = 100` 的影响没有分离；数据集只有 100k 行，且是单 branch、单 worker。
- 不推出 16/64/256 MiB 档位的行为：§4 只有一次 16/64 MiB 观测，数据集 220k 行，
  没有与 4/8 MiB 在同一负载下对照，也没有 256 MiB 档。
- 不把 §4 的 23.718 s / 90.7 MiB 当作吞吐或线性证据；第一次 pick 的 writer admission
  拒绝也只有一次观测，不能代表写并发下的维护成功率。
- 不推出长期稳定性、多 worker 维护并发、S3 后端或 release 构建的行为。
- 不推出延迟分位数作为 SLO：共享主机、debug 构建、无资源隔离，且延迟统计只覆盖成功操作。
- 不推出公平性或容量：`throughput.per_session_efficiency` 只是把整体成功速率除以会话数的
  描述量，不是公平性指标。
- 不把短时过载后的恢复当作机制证明：恢复观测只有一次，且没有定位配额窗口的边界值。

## 6. 证据位置

- `~/.peri-e01/stress-20261017/rate100-20261017T1130Z/`：
  `import/import.json`、`arms-maint2/{rate50-c8-r50-40s,rate100-c8-r100-60s,overload-c32-r0-20s-clean,recovery-rate50-c8-r50-20s}/`、
  `samples.ndjson`、`compact-watch/`、`poll-*.json`、`metrics-after-import.json`、
  `metrics-after-arms.json`、`launcher.out`。早前的 `overload-c32-r0-20s` 臂保留了
  探测会话未关闭的状态，只作为 `max_sessions` 计数方式的旁证，不用于本文表格。
- `~/.peri-e01/stress-20261017/maint16-64-20261017T1145Z/`：
  `import/import.json`、`metrics-import.json`、`metrics-t2.json`、`metrics-t3.json`、
  `metrics-compact.json`、`watch/`、`watch2/`、`launcher.out`。运行目录
  `/tmp/peri-maint-16-64-e12a628`（含 `launch.json`、`launcher-stop.json`、
  `launcher-terminal.json`、`owned-proof/*.stop.json`）。
- 运行目录 `/tmp/peri-maint-rate100-e12a628`（含 `launch.json`、`launcher-stop.json`、
  `launcher-terminal.json`、`owned-proof/*.stop.json`）；凭据不在本文与证据 JSON 的公开字段中。
