# R66 上限探针报告：legacy 全队列 settle（smoke，不可作为对外性能结论）

日期：2026-11-03
性质：不安全上限探针；**不 merge、不 push、不是发布候选**。

## 探针

- 分支/提交：`probe/r66-legacy-settle-ceiling` @ `066ba9efeae6bd5802ba8f2e007608ab290ac895`
- 变更：`prototypes/v1/platform/operation_store.go` +5/−1。`awaitOperationCommit` 的 legacy
  （`completion == nil`，本栈为 `audit_file_v1`）分支不再调用 `settleOperations()`；
  `waitObserved` 与 `commit.err` 返回不变。压缩也因此不再由该路径推进（`compactions_delta=0`）。
- 基线：`main` @ `39e8e6cf9b41fbb32190c0b414a77edbe17e6535`。

## 二进制

| 角色 | 文件 | sha256 | go version -m |
|---|---|---|---|
| A 基线 | `/tmp/peri-perf/bin/peri-platform-r66-main` | `44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104` | `vcs.revision=39e8e6c…`、`vcs.modified=false`、go1.27.3 darwin/arm64 |
| B 探针 | `/tmp/peri-perf/bin/peri-platform-r66-probe` | `ec4c724e4a3dd5c1d568507bbe1164cb6287b1202615602315302951ab36c321` | `vcs.revision=066ba9e…`、`vcs.modified=false`、go1.27.3 darwin/arm64 |
| worker（两侧相同） | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | v1.2.5 |
| 任务书要求的交叉构建 | `/tmp/peri-perf/bin/peri-platform-r66-main-linux-arm64` / `…-probe-linux-arm64` | `f9f740907a9fbddd3e190f8f0bdbef96e5a49bb963bd5eec691e24203d69fc1b` / `745821f353fd30a97acaf3235967ba84093add6f876b8b1ef883f2ab6a03cdce` | `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`；未运行 |

环境偏差：宿主是 macOS/arm64，worker 为 Mach-O，任务书指定的 linux/arm64 平台二进制无法在
`dev.py` 栈上执行；测量用同源 darwin/arm64 构建，linux 产物留档但不参与测量。

## 环境

- 每腿独立栈、全新 run 目录、同一 fixture：`threads=201 messages=8625 chars=20177069`
  （`fixture complete=true`）。
- 端口 20401–20408，2 轮 × A/B/B/A；X1 json-pipeline，每级 50 请求；c1/c8/c16 判决（c4 仅报告）。
- 默认配额、无 TLS、**无任何观察器**；每腿运行时核对平台进程路径与 sha256。
- 全部 8 腿：`errors=0`、`429=0`、`rejected=0`、`stop=clean`、`unresolved_operations=false`；
  1,600 个计时请求，`session_seats=0`、`active_permits=0`。无无效腿，无重跑。

## 逐腿结果（ms）

| leg | 侧 | c1 p50/p95/p99 | c8 p50/p95/p99 | c16 p50/p95/p99 |
|---|---|---|---|---:|
| r1a1 | A | 0.50/0.94/9.33 | 1.32/8.67/9.01 | 1.99/3.91/5.14 |
| r1b1 | B | 0.47/0.53/0.89 | 1.12/1.89/2.07 | 2.21/15.59/16.37 |
| r1b2 | B | 0.50/0.60/6.06 | 1.12/1.90/2.03 | 1.97/3.32/4.57 |
| r1a2 | A | 0.54/12.72/16.52 | 1.43/14.95/15.20 | 1.98/4.54/4.72 |
| r2a1 | A | 0.55/7.22/16.63 | 1.08/1.92/1.97 | 2.17/3.02/4.48 |
| r2b1 | B | 0.49/0.63/1.03 | 1.25/8.06/8.24 | 2.04/2.84/3.44 |
| r2b2 | B | 0.49/0.65/10.13 | 1.09/1.82/2.07 | 2.42/17.35/18.30 |
| r2a2 | A | 0.54/7.91/16.88 | 1.16/1.76/1.86 | 1.85/3.04/3.91 |

## 每轮 B/A 比值（同侧两腿中位）

| 轮 | 单元 | 基线 | 探针 | 比值 |
|---|---|---:|---:|---:|
| 1 | c8 p95 | 11.81 | 1.89 | 0.160 |
| 1 | c8 p99 | 12.10 | 2.05 | 0.169 |
| 1 | c16 p95 | 4.23 | 9.45 | **2.237** |
| 1 | c16 p99 | 4.93 | 10.47 | **2.123** |
| 2 | c8 p95 | 1.84 | 4.94 | **2.685** |
| 2 | c8 p99 | 1.92 | 5.16 | **2.692** |
| 2 | c16 p95 | 3.03 | 10.09 | **3.333** |
| 2 | c16 p99 | 4.19 | 10.87 | **2.591** |
| c1 合并 | p50 | 0.54 | 0.49 | 0.900 |

## 机制核对（不是收益，只证明改动生效）

| 计数器（每 204 请求） | A 四腿 | B 四腿 | 方向 |
|---|---|---|---|
| `state_lock_wait_ns_total` / 请求 | 2.07 / 2.95 / 3.08 / 3.03（中位 3.01ms） | 2.42 / 1.58 / 1.66 / 1.81（中位 1.74ms） | **−42%** |
| `operation_journal_syncs_total` / 请求 | 0.142（中位） | 0.123（中位） | −13% |
| `operation_journal_appends_total` | 404 | 404 | 不变 |
| `operation_journal_compactions_total` | 0 | 0 | 不变 |
| `deferred_durability.pending_max` | 696–830 | 749–800 | 同量级 |
| 耐久来源 | `audit_file_v1` | `audit_file_v1` | 不变 |

结论：改动确实生效（状态锁等待 −42%，c1 尾部 p95 两轮 6.83→0.57 / 7.57→0.64ms），
但等待没有消失，只是不再发生在 deferred worker 的 `awaitOperationCommit` 内。

## 冻结判据与判决

- 「c8/c16 四单元两轮同向 <1.0」：**不满足**（c8 两轮方向相反）。
- 「每轮 c16 p95 或 p99 ≤0.90」：**两轮都不满足**（2.12–3.33×）。
- 「无反向 >1.10 单元」：**不满足**（round1 c16、round2 c8/c16 均反向）。
- c1 中位护栏：通过（0.934 / 0.890 / 合并 0.900）。

**判决：上限不足，停止该方向，不实现安全版本。**

## 噪声与限制

- c8/c16 的 p95/p99 在 n=50 时被 5–8 个连续样本的台阶式停顿主导（同一腿内 top5 相差 <2ms，
  如 r2b2 c16 16.57–18.30ms），4 条探针腿中有 3 条含此类台阶、4 条基线腿中有 1 条。
  冻结判据不排除停顿窗口，故判决按原规则执行；但 c8/c16 尾部的单轮方向不能解读为
  「探针系统性变慢」，只能说明**没有出现 ≥10% 的改善**。
- 该探针只跳过 `awaitOperationCommit` 的 legacy 分支，读路径的 `settleOperations()` 仍在；
  因此它测量的是「移除 deferred worker 侧全队列 settle」的上限，而不是「完全消除 settle」的
  理论上限。若要测后者需要同时跳过读路径（破坏准入语义），不在本次范围。
- 全部数字为 smoke / 上限探针，**不可作为对外性能结论**，也不是生产候选。

## 证据

- `/tmp/peri-r66-probe/protocol.md`（测量前冻结）、`run_leg.sh`、`run_all.sh`、`aggregate.py`
- `/tmp/peri-r66-probe/legs/<level>/<leg>/`：summary.json、raw.jsonl、metrics-before/after.json、launcher-stop.json
- `/tmp/peri-r66-probe/legs.txt`（驱动日志，含每腿运行进程与 sha256 核对）
- `/tmp/peri-r66-probe/aggregate.txt`（本文件数字来源）
- 预检：`smoke-main.out` / `smoke-probe.out`（正式腿前各一条，均 clean/unresolved=false）；
  `smoke0.out` 是驱动器进程核对 bug 的失败预检（非协议腿，栈已干净停止，raw 目录为空后移除）。
