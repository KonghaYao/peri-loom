# R69 Gate 0：W1 intent 屏障重排的安全上限测量

日期：2026-11-05
基线：`main` @ `6060eb0d99d21dd29333149c885608ff47dad85d`
轮次性质：**单场景 smoke 测量（Gate 0）**，不是发布级 A/B，也不是容量/SLA 证明。
授权原文（逐字）：**「授权语意变更，你有最高权限」**。本轮只测量，不实现生产改动、不合并探针。

## 结论

**STOP：判定比 `< 10%`，不实现 R69 的首选方案。**

| 口径 | F | A | F − A | 同窗口 W1.insert p50 | 判定比 |
|---|---:|---:|---:|---:|---:|
| 设计口径（最近一条延迟记录 → intent 提交） | 0.9612ms（6 窗口中位） | 0.0645ms（4 窗口 p50 中位） | 0.8967ms | 15.634ms（6 窗口中位） | **5.7%** |
| 替代口径（上一轮 close 记录 → intent 提交） | 0.9612ms | 1.34ms（4 窗口 p50 中位） | **−0.38ms** | 15.634ms | **< 0（无可回收等待）** |
| 理论上限（假设 A = 0） | 0.9612ms | 0 | 0.9612ms | 15.634ms | **6.1%** |

- 逐窗口的绝对上限 `F / W1` 也只有 **5.75%–7.13%**；即使 A 为零，也达不到 10%。
- 本窗口的 10% 门槛是 **1.34–1.77ms**（W1 13.4–17.7ms），而一次 journal 批量 fsync 实测只有
  **0.92–1.12ms**：**F 本身已经低于门槛**，任何保留 intent durable-before-dispatch 的实现都不可能过线。
- 判定比按冻结判据 `< 10%` → **停止**：不写生产代码、不跑 A/B、不合入探针分支。

## 方法

### 定义

- **F**：一次 operation journal 批量 fsync 的耗时
  `Δ(operation_journal_sync_ns_total) / Δ(operation_journal_syncs_total)`；`sync_ns_total` 由
  `operation_commit.go` 在 `file.Sync()` 前后计时，`syncs_total` 每批 +1。
- **A**：延迟记录提交到下一次 committing intent 提交的间隔，按数据库命名空间取中位数。
  - **设计口径（主口径）**：最近一条 deferred 记录（W1 中是同一 HTTP 请求内的隐式 open）提交 →
    intent 提交。对应设计文档 Gate 0 第 4 步「各记一个单调时钟」。
  - **替代口径（敏感性）**：上一轮 close 记录（延迟记录 run 的起点）→ intent 提交。对应设计
    时间轴里的 `T0 → intent`。两个口径都报告，因为设计正文与操作步骤的措辞不完全一致。
- **判定比** = `(F − A) / 同窗口 W1.insert p50`；判据 ≥ 10% 才实现（W1 是 20–40ms 级场景，适用
  10%，不是 20%）。
- **安全收益** = `F − A`。按 R69 设计，保留 intent 自身 fsync，只能回收「上一批屏障的剩余时间」。

### 环境

- 宿主 macOS 26.5.1 / arm64，OrbStack Docker；`--network host`、`--init`、`--stop-timeout 60`，
  `--security-opt no-new-privileges --cap-drop ALL`；无 TLS。
- 运行镜像两侧相同：`ghcr.io/konghayao/peri-loom:v1.2.7`
  （manifest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`）。
- 平台：base 从 `6060eb0` 构建；探针从 `probe/r69-intent-gap`（`7881c38`）构建。
  worker 两侧复用 v1.2.7 发布二进制（`b17a0708…`）；main 与 v1.2.7 的 Rust 差异只有执行前
  拒绝的证据包装（`runtime.rs` / `worker_rpc/dispatch.rs`），不改变 W1 成功路径，且两侧完全同
  worker，不构成 F/A 比较偏差。详见 `binaries.txt`。
- 默认配额，未放宽：`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_sql_bytes=65536`、`max_result_bytes=4194304`。
- fixture 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625`
  （见 `seed-verify.txt`）。
- 每个栈一个全新卷、独立端口；测量后停止并删除容器，卷保留（未触碰任何既有容器/卷）。
  - base：`peri-r69-gate0-base` / `peri-r69-gate0-base-data` / 20711
  - probe1：`peri-r69-gate0-probe` / `peri-r69-gate0-probe-data` / 20712
  - probe2：`peri-r69-gate0-probe2` / `peri-r69-gate0-probe2-data` / 20713

### 运行

- 场景：`W1.insert`，协议 `json-pipeline`，c1，无观测器。
- harness 参数：`--rounds 3 --warmup 1 --samples 5`（每窗口 15 个 timed 样本）。
- 每个窗口前后各取一次 `/v1/metrics`，只用嵌套键差值；证据中不保存整份 metrics。
- 每轮 W1 迭代 = 一次 `create` + 一次 `insert`（timed）+ 一次 `drop`，三个 HTTP 请求；
  timed 请求只测 `insert` 那一次。

### 探针（分支 `probe/r69-intent-gap`，commit `7881c38`，不合入）

只读计时，不改耐久时点/记录顺序/journal 格式：

- `settlePendingOperationUsing` 的 deferred submit 返回处记录时间；
- `beginCorrelatedOperation` 的 submit 返回处记录时间，并在 `wait()` 前后记录 intent 自身屏障等待；
- 提交瞬间读 `journal.writing` 与队列长度（committer 不在 fsync 期间持 journal 互斥锁）；
- 经 `/v1/metrics` 的 `r69_gap_probe` 暴露 p50/p90/p99、直方图与原始样本。

## 实测数字

### 六个测量窗口

| 窗口 | journal syncs | syncs/迭代 | appends | F | W1.insert p50 | 轮内中位 |
|---|---:|---:|---:|---:|---:|---|
| base-run1 | 182 | 10.11 | 218 | 0.9155ms | 14.491ms | 21.83 / 16.77 / 13.26 |
| base-run2 | 181 | 10.06 | 218 | 0.9640ms | 16.777ms | 18.10 / 13.30 / 17.57 |
| probe1-run1 | 179 | 9.94 | 218 | 0.9584ms | 13.444ms | 22.29 / 12.85 / 12.75 |
| probe1-run2 | 175 | 9.72 | 218 | 0.9240ms | 14.016ms | 17.47 / 12.14 / 10.78 |
| probe2-run1 | 179 | 9.94 | 218 | 1.0699ms | 17.658ms | 17.66 / 16.90 / 17.77 |
| probe2-run2 | 177 | 9.83 | 218 | 1.1155ms | 16.872ms | 18.60 / 16.05 / 16.54 |

- F 六窗口中位 **0.9612ms**（范围 0.9155–1.1155ms）；W1.insert p50 六窗口中位 **15.634ms**
  （范围 13.444–17.658ms）。
- 每窗口 218 条 journal append、约 10 次 fsync；`appends/sync ≈ 1.20`。
  **每个完整迭代（create+insert+drop）约 10.1 次 fsync / 12 条记录**。
  timed 请求只是三个请求之一；设计文档里的「基线约 4 次/迭代」指单请求的 open+intent+terminal+close
  四条记录。
- 所有 6 窗口 × 15 个 timed 样本均为 HTTP 200 / `ok`，`warnings=[]`，无 429、无
  `outcome_unknown`、无 `session_moved`；结束 `session_seats=0`、`active_permits=0`。

### A 与 intent 自身等待（W1 窗口切片，n=54/窗口）

| 窗口 | A_run p50 / p90 | A_last p50 / p90 | intent_wait p50 / p90 |
|---|---:|---:|---:|
| probe1-run1 | 1.377 / 1.728ms | 0.068 / 0.172ms | —（探针版本未含） |
| probe1-run2 | 1.324 / 1.607ms | 0.058 / 0.112ms | — |
| probe2-run1 | 1.334 / 1.599ms | 0.062 / 0.148ms | 1.421 / 2.704ms |
| probe2-run2 | 1.343 / 1.844ms | 0.067 / 0.147ms | 1.546 / 2.647ms |

- **A_last（设计口径）≈ 0.065ms**：隐式 open 记录提交后约 60–70µs，intent 就提交了；两者几乎同时。
- **A_run（上一轮 close 起点）≈ 1.34ms**：中位数比 F 还大。逐样本 `P(A_run < F)` 只有 1/54–5/54，
  `E[max(0, F−A_run)] ≈ 0.001–0.004ms`，即上一轮 close 屏障几乎从不在 intent 提交时仍在进行；
  **不存在可回收的「上一批剩余时间」**。
- **intent 自身屏障等待 p50 = 1.42–1.55ms**，其中约 50% 的 intent 提交时 journal 正在 fsync
  （probe2：27/54、26/54）。这与 A_run 中位数 > F 一致：真正相撞的是**同一请求的 open 批**，不是
  上一轮 close 批。按 `intent_wait − F` 的 p50 差值估算，一次完美合并最多省掉这次单记录 fsync
  约 **0.35–0.43ms**，比 `F − A` 的名义上限更小。
- 每样本的延迟记录数 p50 = 2（close + open），max = 4（少数边界样本）。

### 判定

- 按设计口径：`(0.9612 − 0.0645) / 15.634 = 5.7%`；逐窗口 5.7%–6.6%。
- 按替代口径：`F − A_run < 0` → 可回收时间为 0。
- 绝对上限（A=0）：逐窗口 `F / W1` = 5.75%–7.13%。
- **三种算法都 < 10% → STOP**。不实现、不跑 A/B、不合入探针。`main` 只增加本证据目录。

## 与 R60/R61 窗口的差异（重要）

R61 的 W1.insert 基线是 40.56/40.94ms，按 `2F−A` 反推 F≈4.5–5.5ms；本窗口 W1 只有
13.4–17.7ms，F 只有 0.92–1.12ms。宿主/卷窗口不同，绝对数字不可混用；本报告只做**同窗口**判定。
差异本身也说明：即使同一协议，fsync 成本占 W1 的比例会随宿主窗口显著变化，R69 的收益上限
不是常数。按本轮同窗口证据，上限低于 10%。

## 偏离与限制

1. **第一次基线运行误用了 `--scenario W1`**（匹配 `W1.create/insert/delete/drop` 四个场景），
   其 W1.insert p50 为 16.09ms、混合窗口 F≈1.0023ms。该窗口不用于判定，只作为运行记录保留在
   `/tmp/peri-r69-gate0/base/`；判定窗口全部是 `--scenario W1.insert`。
2. probe1 的探针版本（`2e58c0e`）只测 A，不含 intent 自身等待；probe2 用最终版（`7881c38`）
   补测了 intent 等待与提交瞬间的 journal 状态。两者 A 的口径一致。
3. 每个窗口的第一个 W1 样本有冷启动离群（A_run 3.1–28.0s，4 个窗口各一次）；中位数不受影响，
   本报告不引用均值。
4. 未跑 durability observer、未跑崩溃恢复 E2E、未做 A/B：Gate 0 判停，按设计不进入实现阶段。
5. 单场景 smoke、c1、无 TLS、单宿主；不能外推为发布级、容量或 SLA 结论。
6. 探针只读计时会给每条 deferred/intent 记录加一次互斥锁；probe 栈的 W1 p50（13.4–17.7ms）
   与 base（14.5–16.8ms）重叠，未观察到可辨别的探针放大，但探针不是生产路径。

## 证据清单

| 文件 | 内容 |
|---|---|
| `metrics-before.txt` / `metrics-after.txt` | 基线主窗口 `/v1/metrics` 的嵌套键摘要（不整份倾倒） |
| `metrics-delta.txt` | 基线主窗口逐项差值 + F |
| `metrics-windows.txt` | 六个窗口的 F、syncs/迭代、W1 p50、A/判定比汇总 |
| `decision.txt` | 判定输入与结论（STOP） |
| `probe-metrics.txt` | 四个探针窗口的 `r69_gap_probe` 分布（A_run/A_last/intent_wait/直方图） |
| `summary.json` / `raw.jsonl` | 基线主窗口 harness 产出 |
| `windows/` | 六个判定窗口各自的 `summary.json` / `raw.jsonl`，以及首次误用 `--scenario W1` 的 `base-mixed-all-w1/`（不用于判定） |
| `seed-verify.txt` | 三个栈的 fixture 校验输出 |
| `binaries.txt` | 二进制 sha256、构建来源、镜像 digest、容器/卷/端口 |
| `probe-branch.txt` | 探针分支与 commit sha（不合入） |
| `run_gate0.sh` | 可复跑脚本（fresh volume、seed、W1.insert、metrics 差值、判定打印） |

复跑：`WORKER=... PROBE_PLATFORM=... ./run_gate0.sh base probe`。探针平台必须从
`probe/r69-intent-gap`（`7881c38`）构建；base 平台在未提供 `BASE_PLATFORM` 时从当前 checkout
交叉编译。脚本的内嵌分析块已用本目录已记录的 JSON 验证，输出与 `decision.txt` 一致；脚本没有在
证据窗口之后重新跑栈，以免重建卷改变已登记的数字。
