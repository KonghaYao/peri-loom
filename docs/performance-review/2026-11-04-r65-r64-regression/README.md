# R65：R64 合入后的回归门（W1 / F1 / H1，测量轮）

日期：2026-11-04
状态：A/B 完成；判据在第一条腿之前冻结并写入 `protocol.md`，测量开始后未修改。
轮次性质：测量轮，无生产代码改动。

结论：**24/24 腿有效；按冻结判据，W1、F1、H1 均判「无回归 PASS」**。
其中 W1 是**边界 PASS**：round2 比值 1.153、合并比值 1.101 都超过 1.10，
只因 round1 比值 0.972 未超阈值才未判回归；不能表述为「候选中位数稳健无回归」。
F1 与 H1 的合并比值分别为 0.987、1.022，远离阈值。

## 目的

R64 的冻结协议只覆盖 X1；W1、F1、H1 在合入判决时没有复测，合入记录已把这一点写成缺口。
本轮补做回归门，确认延后耐久账户待办上限 256→4096 没有让任何主场景的端到端中位数退化 >10%。
R64 只提高待办上限，不改变耐久时点、审计链顺序、隔离与 fail-closed 规则；
写语句的 intent 与 terminal 仍同步 durable（R37 的用户授权边界）。

## 协议与二进制

| 角色 | 来源 | 二进制 | sha256 |
|---|---|---|---|
| A 基线 | `main` @ `1399f15` | `/tmp/peri-perf/bin/peri-platform-r62-main` | `fd959df7a0680f80f7b15e9b86c851b9ca24d555c673e69b2659f0d1f15159a1` |
| B 候选 | `main` @ `1a6b9dd`（重新构建） | `/tmp/peri-perf/bin/peri-platform-r65-main` | `9214cd0c95ea636bc3d7898714d0eb884d2769957ece44b9063480f453aa3a6a` |
| worker（两侧相同） | v1.2.5 | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` |

- B 与 R64 接受过的候选二进制 `98487e5e…` 相比，两侧平台源码 `git diff` 为空，
  二进制仅构建 ID 区不同；被验收的代码与合入后的代码一致。
- 每场景 2 轮 × A/B/B/A = 8 条腿，共 24 条腿；端口 20301–20308。
  每条腿独立栈、全新 run dir、重新导入同一 fixture：`threads=201`、`messages=8625`、
  `content_chars=20177069`；worker 两侧相同，只切换平台二进制。
- 主协议：W1 = `json-pipeline`、F1 = `json-pipeline`、H1 = `json-cursor`；
  另测 W1 `protobuf-pipeline`、F1 的四个协议、H1 `protobuf-cursor`。
  每腿 1 warmup + 5 timed（H1 为 3 timed）。
- 默认配额、无 TLS、**无观测器**；`/v1/metrics` 只在每条腿的计时窗口之外各取一次快照。
- 冻结判据：同一场景两轮比值都 >1.10 且合并（4 腿中位）比值 >1.10 才判回归；
  否则记「无回归」。少于两轮有效数据判「不可确认」。

## 24 腿有效性与运行性不变量

- 24/24 腿有效：fixture `complete=true`（threads=201 / messages=8625 / chars=20177069），
  `warnings` 与 `fatal` 为空。
- 352/352 请求（含 warmup）`outcome=ok`、HTTP 200；无 429、无 `expected_limit`、
  无 `audit_unavailable`。
- 样本数 5/5/3（W1/F1/H1）；W1 `rows=0`（写入）、F1 `rows=1`、H1 `rows=8625`。
- 24/24 `launcher-stop.json` 为 `outcome=clean`、`unresolved_operations=false`；
  所有 stage `ack=true`、`exit_code=0`。
- 窗口后 `dropped_total=0`、`head_checkpoint_failures_total=0`、
  `operation_journal_compaction_failures_total=0`；`queue_count=0`、`in_flight_count=0`、
  `session_seats=0`、`active_permits=0`、`rejected_requests_total=0`。
- `aggregate.py` 退出码 0（无回归 PASS）；连续三次运行输出 sha256 一致；
  独立 verifier 从 `raw.jsonl` 重算 64 个协议行，0 mismatch。

## 主指标

单位 ms；比值为候选 / 基线，等于 aggregate 的每轮两腿中位与合并四腿中位。

| 场景 | 轮 | 基线中位 | 候选中位 | 比值 |
|---|---|---:|---:|---:|
| W1.insert | round1 | 33.657 | 32.708 | 0.972 |
| W1.insert | round2 | 36.178 | 41.696 | **1.153** |
| W1.insert | 合并 | 33.705 | 37.125 | **1.101** |
| F1.select-1 | round1 | 0.532 | 0.525 | 0.987 |
| F1.select-1 | round2 | 0.540 | 0.533 | 0.987 |
| F1.select-1 | 合并 | 0.532 | 0.525 | 0.987 |
| H1.content-20mb | round1 | 55.835 | 53.046 | 0.950 |
| H1.content-20mb | round2 | 50.437 | 54.357 | 1.078 |
| H1.content-20mb | 合并 | 53.171 | 54.357 | 1.022 |

判决：按冻结判据，W1、F1、H1 均为**无回归 PASS**。W1 的 PASS 属于边界情形，见下节。

### p95（报告项，不判）

| 场景 | 轮 | 基线 p95 | 候选 p95 | 比值 |
|---|---|---:|---:|---:|
| W1.insert | round1 | 43.538 | 43.979 | 1.010 |
| W1.insert | round2 | 45.370 | 48.076 | 1.060 |
| W1.insert | 合并 | 43.538 | 45.705 | 1.050 |
| F1.select-1 | round1 | 9.211 | 0.586 | 0.064 |
| F1.select-1 | round2 | 6.763 | 0.644 | 0.095 |
| F1.select-1 | 合并 | 6.774 | 0.599 | 0.088 |
| H1.content-20mb | round1 | 59.855 | 58.277 | 0.974 |
| H1.content-20mb | round2 | 54.094 | 55.175 | 1.020 |
| H1.content-20mb | 合并 | 55.741 | 56.286 | 1.010 |

- F1 的 p95 出现 A/B 两侧的 8–18ms 单样本离群：A `r1a1` 17.819、`r1a2` 8.513、
  `r2a1` 13.578、`r2a2` 12.946（同腿另有 8.659 单样本）；B `r1b1` 15.480。
  这些离群直接抬高腿 p95，而中位数保持稳定。
  **aggregate 的 F1 p95 比值 0.064 / 0.095 不能解读为真实 p95 改善**。

## W1 边界与归因

W1 round2 的基线/候选腿中位分别为 36.178 / 41.696ms，比值 1.153；合并四腿中位
33.705 / 37.125ms，比值 1.101。两项都越过 1.10；冻结判据要求两轮**都** >1.10 才判回归，
而 round1 为 0.972，因此判决为 PASS。**这是边界 PASS，不是稳健无回归。**

归因指向 worker 自报 SQL 时间在 round2 的阶跃，而不是平台侧开销：

- W1 `query_ms` 从 round1 的约 9–11ms 阶跃到候选 `r2b1` 16.96ms、`r2b2` 20.41ms；
  基线最后一条腿 `r2a2` 同样升到 16.14ms，说明该阶跃同时出现在 A/B 两侧。
- 用 `duration_ms − query_ms` 表示平台 + 网络开销，按腿内该差值的腿中位再取中位：
  round1 **0.977**、round2 **1.031**、合并 **0.977**。
  平台侧没有回归信号；W1 的边界比值由 worker SQL 时间抬升主导。
- 因此不能把 W1 描述为「候选中位数稳健无回归」；若后续要复测 W1，
  应同时记录 `query_ms` 与宿主状态，避免把 worker 侧阶跃记到平台候选上。

## 次协议、字节与 query_ms（报告项，不判）

| 场景 | 协议 | round1 | round2 | 合并 |
|---|---|---:|---:|---:|
| W1.insert | protobuf-pipeline | 1.004 | 1.110 | 1.135 |
| F1.select-1 | protobuf-pipeline | 1.025 | 0.953 | 0.981 |
| F1.select-1 | json-cursor | 1.033 | 0.962 | 0.995 |
| F1.select-1 | protobuf-cursor | 0.977 | 1.024 | 1.028 |
| H1.content-20mb | protobuf-cursor | 1.089 | 0.970 | 1.027 |

- W1 `protobuf-pipeline` 的 round2 与合并比值同样 >1.10，且 round2 `query_ms`
  也上移到 16.07–17.49ms，形态与主协议一致；该项按协议只报告，不参与判决。
- 响应字节：W1 json 252（`r2b2` 261，仅 `query_duration_ms` 位数差异）、protobuf 31；
  F1 json-pipeline 312、protobuf-pipeline 40、json-cursor 252、protobuf-cursor 57；
  H1 json-cursor **20,608,525**、protobuf-cursor **20,272,006**，逐腿一致。
- `query_ms`：F1 为 0.006–0.012ms；W1 见上节；H1 的 cursor 协议无该指标。

## 机制快照与上限 caveat

12 条 B 腿都带 `deferred_durability`：`maximum=4096`、`inline_total=0`。

| 场景 | B 腿数 | `maximum` | `pending_max` | `inline_total` |
|---|---:|---:|---:|---:|
| W1.insert | 4 | 4096 | 4 | 0 |
| F1.select-1 | 4 | 4096 | 91 | 0 |
| H1.content-20mb | 4 | 4096 | 18–21 | 0 |

- 12 条 A 腿没有 `deferred_durability` 指标（`main` @ `1399f15` 尚未包含该改动）。
- **`pending_max` 最高只有 91，远低于 4096**：本轮没有逼近或验证新上限的饱和行为，
  `inline_total=0` 只说明内联回退未触发，不证明 4096 在高待办水位下无风险。
- 二进制身份：`go version -m` 显示 A `vcs.revision=1399f15`、B `1a6b9dd`，
  两侧均 `vcs.modified=false`。两侧 `metadata.platform_label` 都是 `a46afa2`，
  不能用于区分 A/B；身份判定依赖 sha256 与机制指标。

## 数据质量与限制

1. `run_all.sh` 只在全部腿开始前复核一次二进制 sha256，而冻结协议措辞是「每次运行前」；
   对结果无实质影响，但属执行与措辞偏差。
2. `aggregate.py` 在某一侧缺一条腿时会静默少用一条腿仍计算比值；本轮 24 腿齐全，
   不影响结论，后续工具应增加严格性。
3. 审计链连续性没有直接的链头/连续性 artifact；间接证据是 24/24 clean stop 与
   `head_checkpoint_failures_total=0`。
4. 未做观测器（证据中无 observer 痕迹）；`concurrency_requests=50` 只被 X1 使用，
   本场景为串行，不适用。

## 结论

- R64 合入候选（延后耐久账户待办上限 256→4096）在本轮冻结判据下，
  W1、F1、H1 均无 >10% 回归，判「无回归 PASS」。
- W1 是边界 PASS：round2 1.153、合并 1.101 越过 1.10，仅因 round1 0.972 未超阈值；
  平台侧 `duration_ms − query_ms` 比值为 0.977 / 1.031 / 0.977，回归信号在 worker SQL 时间。
- 本轮无生产代码改动。

## 后续 CI 修复声明

R65 测于 `a46afa2`（即 `main` @ `1a6b9dd` 的生产代码）。之后为修复 CI 回归，
R43 的 adapter 级 fused close 被撤销（保留 fused open）；这会改变 F1 的绝对延迟。
R65 的判决只针对 R64 的 deferred durability 256→4096 变更在 `a46afa2` 上的表现，
不代表修复后的 F1 绝对值，也不应把本轮的 F1 数字用于修复后的对比。

## 证据清单

- `protocol.md`：测量前冻结的判据与场景/样本定义。
- `run_leg.sh` / `run_all.sh`：单腿驱动与 24 腿调度。
- `aggregate.py`：冻结判据判决；`seed_fixture.py` / `snap_metrics.py`：fixture 与指标快照。
- `legs/<scenario>/<leg>/`：`summary.json`、`summary.md`、`raw.jsonl`、`metrics-before.json`、
  `metrics-after.json`、`launcher-stop.json`；`legs.txt`：完整运行日志。
