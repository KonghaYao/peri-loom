# R66 冻结协议：legacy 全队列 settle 的上限探针（smoke）

日期：2026-11-03
状态：**本文件在第一条 A/B 腿之前写完；测量开始后不得修改判据。**
轮次性质：上限探针（不安全、绝不 merge、绝不作为发布候选）。
探针分支：`probe/r66-legacy-settle-ceiling` @ `066ba9e`
基线：`main` @ `39e8e6cf9b41fbb32190c0b414a77edbe17e6535`

## 目的

估计「让 legacy 操作日志的 `awaitOperationCommit` 不再于状态锁内等待整个未确认队列
（`settleOperations`）」对 X1 尾延迟的**上限**。这是判断「值不值得实现安全版本」的探针，
不是收益承诺：探针跳过确认与压缩，可能留下未确认编辑，语义不安全。

## 变更

`prototypes/v1/platform/operation_store.go`：`awaitOperationCommit` 的 legacy 分支
（`store.operations.completion == nil`，即 `audit_file_v1`）不再调用 `settleOperations()`；
`waitObserved` 与 `commit.err` 返回保持不变；unified completion 分支不变。
`beginOperationCompactionLocked` 要求未确认队列为空，因此该调用不再推进 legacy 压缩。

## 二进制

| 角色 | 文件 | sha256 | 构建 |
|---|---|---|---|
| A 基线 | `/tmp/peri-perf/bin/peri-platform-r66-main` | `44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104` | `main@39e8e6c`，`go build ./cmd/peri-platform`，go1.27.3 darwin/arm64，`vcs.modified=false` |
| B 探针 | `/tmp/peri-perf/bin/peri-platform-r66-probe` | `ec4c724e4a3dd5c1d568507bbe1164cb6287b1202615602315302951ab36c321` | `066ba9e`，同上，`vcs.modified=false` |
| worker（两侧相同） | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | 既有 v1.2.5 |
| 交叉构建（按任务书） | `/tmp/peri-perf/bin/peri-platform-r66-probe-linux-arm64` | `745821f353fd30a97acaf3235967ba84093add6f876b8b1ef883f2ab6a03cdce` | `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`，未参与测量 |

**环境偏差（测量前记录）**：任务书指定的 `GOOS=linux GOARCH=arm64` 二进制无法在本机
`dev.py` 栈上执行（宿主 macOS/arm64，worker 也是 Mach-O）；测量使用同源同日构建的
darwin/arm64 二进制，linux/arm64 产物一并构建留档但不运行。除 GOOS 外两侧构建方式一致。

## 环境

- 每条腿：`dev.py --run-dir /tmp/peri-perf/run-r66-<leg> --listen 127.0.0.1:<port> --no-build`，
  全新 run 目录（local backend，无残留），`PERI_HRANA_EXPERIMENTAL=1`，无 TLS。
- 端口 20401–20408，逐腿一个；换平台二进制前干净停止（SIGTERM + join）并核对
  `sha256sum`（宿主文件与进程启动记录一致）。
- 默认配额，不放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4 MiB`。
- fixture：`docs/performance-review/2026-11-04-r65-r64-regression/seed_fixture.py`，
  校验 `threads=201`、`messages=8625`、`chars=20177069`。
- **不开启任何观测器**（`PERI_DURABILITY_OBSERVER`、`PERI_SQL_OPEN_OBSERVER*` 均不设）。
  `/v1/metrics` 只在每腿计时窗口之外各取一次。

## 场景

- X1.concurrency，`json-pipeline`：每请求 `session open` → `[execute("SELECT 1"), close]`，
  由 `scripts/perf-reliability/issue28/run.py --scenario X1 --rounds 1 --warmup 0 --samples 1
  --concurrency-requests 50` 驱动。
- 级别：c1 / c8 / c16 用于判决；c4 是 harness 固定级别，仅报告。
- 每级 50 请求；2 轮 × A/B/B/A（A=基线、B=探针），共 8 腿；每腿独立栈。

## 判据（冻结）

- **上限** = 同一轮内 B 相对 A 的 p95、p99 比值（c8、c16 各两项，共四个单元）。
- **值得实现安全版本**：两轮同向，且 **c16 p95 或 c16 p99 中至少一项 ≤ 0.90**；
  同时不得出现相反方向 >1.10 的单元（否则记为不可确认）。
- **c1 护栏**：每轮 B 的 p50 中位 ≤ A 的 1.10×，合并也 ≤1.10×。
- 不满足 ⇒ 停止该方向，不实现安全版本。

## 腿有效性

- 每腿：计时样本 `errors=0`、`status_429=0`、fixture `complete=true`、
  `launcher-stop.json` 为 `outcome=clean` 且 `unresolved_operations=false`；
  成功请求 `rows=1`。
- 无效腿重跑一次；仍无效则记录并排除。少于两轮有效数据 ⇒ 「不可确认」。

## 证据

- `/tmp/peri-r66-probe/legs/<level>/<leg>/`：`summary.json`、`raw.jsonl`、
  `metrics-before.json`、`metrics-after.json`、`launcher-stop.json`。
- `/tmp/peri-r66-probe/legs.txt`：完整驱动日志。
- `/tmp/peri-r66-probe/aggregate.txt`：逐腿关键指标与两轮比值。
- 所有结论标注 **smoke / 上限探针，不可作为对外性能结论**。

## 停止条件

若探针导致挂起、无法干净停止、或 `unresolved_operations` 无法收敛，如实记录为
「探针不可测」并停止；不用强杀掩盖，不为跑通修改判据。
