# R64 冻结协议：在新判据下重新评估 R62 候选

日期：2026-11-04
状态：**本文件在第一次 A/B 腿之前提交并推送；测量开始后不得修改。**
轮次性质：测量轮（无生产代码改动）。候选代码仍来自 `perf/r62-deferred-bound` @ `530fcf7`。

## 目的

R62 的候选（延后耐久账户待办上限 256→4096）主指标通过、冻结护栏 c1 p95 失败，因此不合入。
R63 的归因轮证明：c1 尾部由宿主单次 fsync 停顿（7–12ms）支配，两侧共有、形态相同；且该改动在
c1 的 50 请求配置下（`pending_max` 200–243 < 基线上限 256、`inline_total=0`）**不可能生效**。

本轮用一份**新冻结**的判据重新评估同一候选，以决定它能否合入 `main`。
R62 与 R63 的任何性能数字都不作为本轮结果；本轮所有数字必须由本轮运行现算。

## 判据修订的理由（A/B 之前写明）

R62 的 c1 护栏是"每腿 50 个样本的 p95"，即**第 3 高值**：单次宿主停顿就能把它从 0.5ms 推到
5–9ms。R63 在同宿主上测得两侧都有 >5ms 的 c1 样本（观察器腿八条腿每一条两侧都有），
c1-only 腿两侧的 max 同为 7.7–16.7ms 量级，而同一腿的 journal 等待 max 为 8.1–12.3ms、
audit fsync max 为 7.2–9.8ms。该统计量度量的是宿主，不是候选。

机制上，候选在 c1 上只可能让回复更早：它把原本在回复前支付的屏障交给回复之后的账户；
账户饱和时会回退到内联支付，那就是基线的行为，没有新增等待路径。

因此本轮把 c1 的**尾部**（p95 / p99 / max / >2ms 计数）列为**报告项**，c1 的**中位数**保留为护栏。
这个修改发生在任何 R64 测量之前，修改理由来自 R63 的实测与机制核对。

## 候选与基线

| 角色 | 分支 / 提交 | 二进制 | sha256 |
|---|---|---|---|
| 基线 A | `main` @ `1399f15`（R62 基线内容） | `/tmp/peri-perf/bin/peri-platform-r62-main` | `fd959df7a0680f80f7b15e9b86c851b9ca24d555c673e69b2659f0d1f15159a1` |
| 候选 B | `perf/r62-deferred-bound` @ `530fcf7` | `/tmp/peri-perf/bin/peri-platform-r62-cand` | `98487e5ea6d9e2590a41619ef3848ed8f78072b7aab5909254873985745867a1` |
| worker（两侧相同） | v1.2.5 | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` |

两侧同一 Go 工具链（go1.27.3）、同模块路径。运行前用 `shasum -a 256` 复核上表。

## 场景与环境

- 场景：`X1.concurrency`，`json-pipeline`，`SELECT 1`，`want_rows`，`expected_rows=1`；
  每级 **50** 请求（R62 已就 100/级下的 500 rps 令牌桶混杂作过说明；配额不变）。
- 每腿：独立栈 + 全新 run dir + 重新导入同一 fixture（`threads=201 messages=8625 chars=20177069`）
  + 校验；跑前跑后各取一次 `/v1/metrics` 快照。
- **不开启任何观察器**（`PERI_SQL_OPEN_OBSERVER*` 全部不设）。
- 配额：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_sql_bytes=65536`、`max_result_bytes=4 MiB`；不放宽。
- 腿结构：2 轮 × A/B/B/A（A=基线、B=候选），共 8 条腿；端口 20101–20108。
- 命令：`bash run_leg.sh <leg> <port> <platform-binary>`。

## 主指标（硬门）

X1 的 **c8 与 c16** 的 **p95、p99**（`summary.json` 的 `concurrency[].p95_ms/p99_ms`）：

- 每轮：候选两条 B 腿的合并中位 ≤ 基线两条 A 腿合并中位的 **90%**，四个单元（c8 p95、
  c8 p99、c16 p95、c16 p99）**全部**满足；
- **两轮同向**（每轮四单元都达标）；
- 四条腿合并后四个单元也须 ≤ 90%。

## 护栏

- **c1 p50**：每轮候选两条腿的中位 ≤ 基线两条腿中位的 **110%**，合并也 ≤ 110%。
- **c4 p50、c4 p95**：合并 ≤ 110%。
- **c1 尾部为报告项，不设门槛**（p95 / p99 / max / >2ms 样本数），理由见上；报告中必须给出
  两侧的逐腿数值与总尖峰数。
- 运行性（任一不满足即判本轮无效）：
  - 全部腿 `errors=0`、`status_429=0`、`rejection_reasons={}`；
  - 所有成功请求 `rows=1`；
  - 响应字节分布两侧一致（逐样本集合相同，允许 `query_duration_ms` 位数差异）；
  - fixture 校验通过；
  - 候选腿 `deferred_durability.inline_total=0`；
  - 每条腿 `launcher-stop.json` 为 `outcome=clean`、`unresolved_operations=false`。
- **无效腿规则**：任一腿出现 `errors>0` 或任何拒绝，该轮四条腿全部重跑；保留无效腿目录并记录原因。

## Gate 0（A/B 之前）

- 候选腿 `/v1/metrics` 的 `deferred_durability.maximum` = **4096**，`semantics` 字段存在；
- 至少一条候选腿 `deferred_durability.pending_max` > **256**，证明上限在测量窗口内被触及
  （基线在该点会走内联回退）；
- 基线腿没有 `deferred_durability` 指标（`main` @ `1399f15` 尚未包含该字段）。

Gate 0 不通过则本轮作废，先修环境再重测。

## 判决

- 主指标与全部护栏通过 → 合入 `perf/r62-deferred-bound` 到 `main`（`git merge --no-ff`），
  并按记录规范写 `CLAUDE.md`。
- 任一门不通过 → 不合入，分支保留作证据，写清失败项与本轮限速器状态。
- 判据不因结果调整；若判据本身有问题，只能用于下一轮。

## 测量脚本

- `run_leg.sh`：单腿驱动（栈 → seed → X1 → 指标快照 → clean stop），与本文件同目录。
- `run_all.sh`：按 A/B/B/A 顺序串行执行 8 条腿。
- 汇总脚本与结果随记录一起提交。
