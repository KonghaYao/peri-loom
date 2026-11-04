# F1.select-1 / json-pipeline 发布 A/B 归因测量协议

日期：2026-11-05
状态：**在第一条腿之前冻结；判据测量开始后不得修改。**
上位协议：`docs/performance-review/2026-11-04-v1.2.7-release/perf-protocol.md`
（v1.2.6 vs v1.2.7 发布 A/B 的冻结协议）。本文件只覆盖该协议中的
`F1.select-1 / json-pipeline` 主指标复测，用于回答「发布报告里的 FAIL 是否可复现」。

## 问题

2026-11-04 发布报告把 `F1.select-1 / json-pipeline` 判为 FAIL：

| 轮 | 基线中位 | 候选中位 | 比值 |
|---|---:|---:|---:|
| round1 | 3.159ms | 3.880ms | 1.228 |
| round2 | 1.706ms | 2.629ms | 1.541 |
| 合并 | 2.433ms | 2.891ms | 1.188 |

但基线两轮相差 85%、候选两轮相差 47%。本窗口先验证该差异在更高采样下是否可复现；
只有判为「可复现」才进入提交级归因。

## 对比对象

| 角色 | tag | 平台 sha256 | worker sha256 |
|---|---|---|---|
| A 基线 | `v1.2.6`（`292507f`） | `2a4a41d84011021e7c7b4d556f737a9582c5d7a48dec6a6c3887f6fbbae8294d` | `61f742362c04d095b6f23754e22bf6aa2d807ac7154feaf4af211406282faeff` |
| B 候选 | `v1.2.7`（`640ae7e`） | `95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912` | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |

- 复用 `/tmp/peri-release-ab/bin/` 下发布 A/B 的两侧二进制；每腿在容器内重新
  `sha256sum` 并与上表比对，不匹配即该腿无效。
- 运行镜像：`ghcr.io/konghayao/peri-loom:v1.2.7`
  （`sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`），
  两侧同一镜像，只覆盖挂载平台与 worker 二进制。
- 未重新构建二进制。若后续需要中间点 P1，则在临时 worktree 的临时分支构建，
  不合入、不推送。

## 环境与腿结构

- Docker linux/arm64（OrbStack），`--network host --init --stop-timeout 60`，
  `--security-opt no-new-privileges --cap-drop ALL`；无 TLS。
- 2 轮 × A/B/B/A = 8 条腿：`r1a1 r1b1 r1b2 r1a2 r2a1 r2b1 r2b2 r2a2`，
  端口 20401–20408；每腿一个全新 named volume（前缀 `peri-f1attr-`），
  挂载到 `/data/peri`，用完保留不删。
- `PERI_HRANA_EXPERIMENTAL=1`；无观测器（`PERI_DURABILITY_OBSERVER` /
  `PERI_SQL_OPEN_OBSERVER` 未设置）；未设置 `PERI_COMPLETION_JOURNAL`
  （默认 legacy v1）。
- 默认配额：`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_sql_bytes=65536`、`max_result_bytes=4194304`；不改配额、不跳过审计/持久化。
- fixture：`threads=201`、`messages=8625`、`content_chars=20177069`；
  每腿通过 `seed_fixture.py` 重新导入并校验 `complete=true`。
- 场景：只跑 `F1.select-1` 的 `json-pipeline` 协议（SQL `SELECT 1`，返回 1 行）；
  pipeline 请求形态为 `[execute, close]`（harness 默认 `close_stream=True`）。

## 样本数与偏离记录（测量前冻结）

**核对结果**：上位冻结协议 `perf-protocol.md` 的表格与 `aggregate.py` 的
`SCENARIOS["F1.select-1"]["samples"]` 实际都是 **5 个 timed 样本/腿（warmup 1）**，
不是 1；正式发布窗口的 `legs/F1.select-1/*/raw.jsonl` 也证实每腿每协议
5 个 timed 样本。因此任务说明中的「协议里原来是 1」与仓库证据不符。

**本轮取值**：为了真正提高采样、降低腿级中位估计的方差，本窗口取
**20 个 timed 样本/腿（warmup 1）**。这是相对上位冻结协议的**唯一偏离**，
理由是该场景腿级方差过大（发布窗口单样本跨度 0.8–8ms，腿中位 1.7–3.9ms）；
除样本数外，环境、配额、腿结构、A/B 交替顺序、二进制、镜像与判据均不变。
`aggregate.py` 仅把 F1 的 `samples` 常量从 5 改为 20、协议列表收窄为
`json-pipeline`，判决公式与阈值未改。

## 预注册判据（不得事后修改）

主指标：每条腿 `summary.json` 中 `json-pipeline` 的 `median_ms`（20 个 timed 样本的中位）。

- 每轮比值 = 候选两腿中位的中位 / 基线两腿中位的中位。
- 合并比值 = 候选 4 腿中位的中位 / 基线 4 腿中位的中位。
- **可复现（候选更慢）** 当且仅当：
  1. 2 轮方向一致（两轮比值都 > 1.0，且本任务关注回归，故要求两轮都 > 1.10）；
  2. 合并比值 > 1.10；
  3. 每轮两侧样本分布不重叠，**或** 两轮各自的中位差 > 腿内 3 倍标准差。
- **不可复现/噪声主导**：合并比值落在 0.95–1.10，或两轮方向相反。
- 其余情况（例如合并 >1.10、方向一致，但某轮分布分离不足，或只有一轮 >1.10）
  记 **部分复现**，不进入第二步。
- 补充统计（报告项，不改判据）：每轮样本级 permutation test（双侧，20,000 次，
  固定种子 20261105）与 median diff 的 bootstrap 95% CI（10,000 次）。
- 同时按上位冻结规则输出 FAIL/PASS/IMPROVED（两轮都 >1.10 且合并 >1.10 判 FAIL）。

## 机制证据（不构成额外 A/B）

在可复现时（或无论判决如何都作为报告项）检查：

- F1 的 pipeline 是否包含 `close`：读 harness 场景定义与 `raw.jsonl` 的
  `request_bytes` / 代码路径。
- `/v1/metrics` 的 `actions_total`（`statement` / `close` / `open` 的**调用次数**）
  在 A/B 两侧的每腿增量是否不同。
- `state_operation_cost.operation_journal_appends_total` /
  `operation_journal_syncs_total` 每请求数是否不同。
- A/B 二进制的提交差异：`git log v1.2.6..v1.2.7` 的实际代码提交（若与任务说明的
  「只有三类」不符，如实记录）。

## 归因（仅在可复现时）

按任务要求构建/使用三点：

- P0 = v1.2.6；
- P1 = 仅应用 R64（`deferredDurabilityMaximum` 256→4096）的提交；
- P2 = v1.2.7。

每点 1 轮 A/B/B/A（4 腿），标注为**归因**而非验收；判据仍用主指标中位比值，
不据此宣称发布判决。若三点不可分辨，则如实记录「差异不由指定改动解释」。

## 证据目录

`legs/F1.select-1/<leg>/`：`summary.json`、`summary.md`、`raw.jsonl`、
`metrics-before.json`、`metrics-after.json`、`launcher-stop.json`、`stop-exit.txt`、
`binaries.txt`、`image-id.txt`、`image-digest.txt`、`harness.log`、`seed.log`、
`container.log`。

`legs.txt`、`run_all.sh`、`run_leg.sh`、`aggregate.py`、`aggregate.txt`、
`invariants.py`、`invariants.txt`、`mechanism/`、`README.md`。

`connection.json` 含私有 API Key，**不进入仓库**，只保存在 `/tmp/peri-f1-attr/conn/`
并保持 chmod 600。
