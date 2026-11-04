# R66 第二个上限探针：进一步放宽 Go GC 目标（smoke，零代码）

日期：2026-11-04
状态：**本文件在任何 Gate 0/正式腿之前写完；第一条腿开始后不再修改判据。**
轮次性质：零生产代码改动的上限探针，只改平台进程的环境变量；**不 merge、不 push、不提交**。
基线：`main` @ `39e8e6cf9b41fbb32190c0b414a77edbe17e6535`

## 目的

估计「把平台进程的 GC 目标从 `GOGC=400` 进一步提高到 `GOGC=1000`」对 X1 c8/c16 尾延迟
（p95/p99）的**上限**。另设 `GOGC=off` 绝对上限档（非判决档），用来判断即使完全消除 GC，
X1 尾部还剩多少停顿。这是判别 R64 候选腿 c16 台阶式停顿是否由 GC assist 引起的探针，
不是收益承诺，全部数字为 smoke，不可作为对外性能结论。

## 变的只有环境变量

| 侧 | 平台进程环境 | 说明 |
|---|---|---|
| A | `GOGC=400` | 本机生产默认的等价配置：`collector.go` 在显式 `GOGC` 下不介入；macOS 无 cgroup 文件，生产默认即 `SetGCPercent(400)` 且无内存上限 |
| B | `GOGC=1000` | 进一步放宽 |
| C | `GOGC=off` | 绝对上限，非判决档；关闭 GC，RSS 风险见下 |

- 两侧其他一切相同：同一 platform 二进制、同一 worker 二进制、同一配置、同一 fixture、同一配额。
- `GOMEMLIMIT` 不设（三档一致）。`GODEBUG` 只在 Gate 0 设 `gctrace=1`，正式腿不设。
- 不开启 `PERI_DURABILITY_OBSERVER` / `PERI_SQL_OPEN_OBSERVER*`；`/v1/metrics` 只在计时窗口外取快照。

## 二进制

| 角色 | 文件 | sha256 | 元数据 |
|---|---|---|---|
| platform（三档共用） | `/tmp/peri-perf/bin/peri-platform-r66-main` | `44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104` | `vcs.revision=39e8e6c…`、`vcs.modified=false`、go1.27.3 darwin/arm64 |
| worker（三档共用） | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | Rust v1.2.5 |

## 环境与已记录的偏差

- **`dev.py` 不转发 `GOGC`**：仓库版 `safe_environment()` 只放行 `PATH/HOME/TMPDIR/LANG/TZ`，
  `experimental_environment()` 只放行固定的 `PERI_*` 名单。直接 `GOGC=… python3 dev.py` 时该变量
  到不了平台子进程。因此使用 `/tmp/peri-r66-gc/dev.py`（仓库文件的本地副本，sha256 记入报告），
  唯一改动是在 `run()` / `run_recovery()` 构造 `platform_environment` 后，把 `GOGC`、`GODEBUG`、
  `GOMEMLIMIT` 从调用者环境显式转发给**平台子进程**（authority/worker 不转发）。
  `PERI_PROJECT_ROOT=/Users/kk/code/peri-loom`；仓库文件零改动。
- 每腿：全新 run 目录 `/tmp/peri-perf/run-r66gc-<leg>`，`dev.py --no-build`，local backend，
  `PERI_HRANA_EXPERIMENTAL=1`，无 TLS，默认配额（不放宽：`max_databases=8`、`max_sessions=16`、
  `max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4 MiB`）。
- fixture：`docs/performance-review/2026-11-04-r65-r64-regression/seed_fixture.py`，
  校验 `threads=201`、`messages=8625`、`content_chars=20177069`。
- 端口：Gate 0 用 20511（A）、20512（B）、20501（C）；正式腿 20501–20510，逐腿一个，腿间干净停止。

## Gate 0（先于正式 A/B，不参与判决）

- 三档各跑一次独立栈，`GODEBUG=gctrace=1`，跑 X1（每级 50 请求）；结束后从
  `run/logs/platform.log` 抽取 `gc ` 行。
- **必须确认**：平台进程环境里能看到对应 `GOGC`（`ps eww` 抓取）；A/B 的 gctrace 有 GC 行且
  B 的堆目标（`N MB goal` 的中位/最大值）大于 A；C 的 `gc ` 行数为 0（或用 0 条记录证明未发生 GC）。
- 若无法证明 `GOGC` 到达平台进程，停止并报告，不进入正式腿。

## 场景

- X1.concurrency，`json-pipeline`：每请求 `session open` → `[execute("SELECT 1"), close]`，
  `scripts/perf-reliability/issue28/run.py --scenario X1 --rounds 1 --warmup 0 --samples 1
  --concurrency-requests 50 --allow-misses`。
- 级别 c1/c4/c8/c16，每级 50 请求；判决用 c1/c8/c16，c4 仅报告。
- 正式编排：**2 轮 × A/B/B/A**（A←→B 交替），每轮在 A/B/B/A 之后追加一条 C 腿（绝对上限，非判决）。
  共 10 条正式腿；每腿独立栈。

## 判据（冻结）

- **主指标单元**：c8 p95、c8 p99、c16 p95、c16 p99。每轮比值 = 同轮 B 两腿中位 ÷ A 两腿中位。
- **同向**：四个单元在 round1 与 round2 的比值都 < 1.0。
- **值得考虑生产改动**：同向，且至少一个单元在两轮都 ≤ 0.90；且 c1 p50 护栏通过。
- **c1 护栏**：每轮 B/A 的 p50 中位比值 ≤ 1.10，合并（A 四腿中位 / B 四腿中位）也 ≤ 1.10。
- **判停**：c16 p95 或 p99 在两轮都 > 1.00，或任一单元两轮方向相反（一 <1.0、一 >1.0）
  ⇒ 「GC 尾延迟假设否证 / 不可确认」，停止。
- 少于两轮有效数据 ⇒ 不可确认，不得写成 PASS。
- **C 档**：只报告同轮 C/A 的 c8/c16 p95/p99 与 RSS 峰值，不做判决；用于说明「零 GC 后的剩余尾部」。

## 腿有效性

- 每腿：计时样本 `errors=0`、`status_429=0`、fixture `complete=true`、成功请求 `rows=1`、
  `launcher-stop.json` 为 `outcome=clean` 且 `unresolved_operations=false`、每级计时请求 50 条。
- 无效腿重跑一次；仍无效则记录并排除。某轮某侧有效腿不足 2 条 ⇒ 该轮不可确认。
- 平台进程 RSS 每 100ms 采样一次，记录峰值。C 档若 RSS 峰值超过 **3 GiB** 或进程 OOM/退出，
  立即停止该档并如实记录（不强杀掩盖），A/B 判决继续。

## 证据

- `/tmp/peri-r66-gc/{protocol.md,dev.py,run_leg.sh,run_all.sh,aggregate.py}`
- `/tmp/peri-r66-gc/gate0/<arm>/{gctrace.txt,summary.json,platform-env.txt,rss.txt,launcher-stop.json}`
- `/tmp/peri-r66-gc/legs/<level>/<leg>/{summary.json,raw.jsonl,metrics-before.json,metrics-after.json,launcher-stop.json,rss.txt,platform-env.txt}`
- `/tmp/peri-r66-gc/logs/<leg>.log`、`logs/legs.txt`（驱动日志，含每腿 sha256/进程核对）
- `/tmp/peri-r66-gc/aggregate.txt`（报告数字来源）
