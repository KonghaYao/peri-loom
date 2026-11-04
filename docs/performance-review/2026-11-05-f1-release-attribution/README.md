# F1.select-1 / json-pipeline 发布 A/B 可复现性与归因

日期：2026-11-05
协议：`perf-protocol.md`（在第一条腿之前冻结）
上位协议：`docs/performance-review/2026-11-04-v1.2.7-release/perf-protocol.md`
性质：测量与归因，**不是**发布验收；本文件不修改 v1.2.7 已发布的判决。

## 结论

**判决：不可复现 / 噪声主导。**

- 本窗口 2 轮 × A/B/B/A 的比值为 **round1 0.800、round2 1.276**，两轮方向相反；
  合并比值 **0.917**（候选反而略快），不满足「可复现」判据。
- 按上位冻结规则（两轮都 >1.10 且合并 >1.10 才 FAIL），本窗口为 **PASS**，
  不是 FAIL。
- 样本级统计：合并 80 样本/侧，中位比值 0.939，Mann-Whitney 双侧 **p=0.997**，
  permutation p=0.787；每轮 permutation p=0.386 / 0.484；bootstrap 95% CI
  均跨 0。分布完全重叠，中位差 0.4–0.75ms，远小于腿内 3 倍标准差（≈6.5ms）。
- 按预注册规则，**未进入第二步（P0/P1/P2 提交归因）**。

一个补充事实：**发布报告原窗口自己的样本级数据也无法把该差异与噪声区分开**。
用原窗口 8 条腿的 40 个 timed 样本（20/侧）做 Mann-Whitney，双侧 **p=0.989**，
permutation p=0.599。发布报告的 1.188 来自「腿中位再取中位」（每轮 2 条腿、
合并 4 条腿）的估计量，在原窗口腿内 CV 45–91%、本窗口 52–74% 的分布上，
这个估计量的方差远大于它要检测的 0.24–0.5ms 效应。

## 测量方法

- 结构：2 轮 × A/B/B/A = 8 条腿（`r1a1 r1b1 r1b2 r1a2 r2a1 r2b1 r2b2 r2a2`），
  端口 20401–20408；每腿一个全新 named volume，挂载 `/data/peri`。
- 场景：只跑 `F1.select-1 / json-pipeline`（SQL `SELECT 1`，返回 1 行）。
- 样本：**20 个 timed 样本/腿 + 1 warmup**（偏离说明见下）。
- 两侧：A = v1.2.6 平台+worker 二进制（sha256 `2a4a41d8…` / `61f74236…`），
  B = v1.2.7 平台+worker 二进制（`95716e4d…` / `b17a0708…`）；两侧同一运行镜像
  `ghcr.io/konghayao/peri-loom:v1.2.7`（manifest `sha256:8a8ba43b…`），
  只把平台/worker 二进制挂载覆盖。
- 环境：OrbStack linux/arm64，`--network host --init --stop-timeout 60`，
  `--security-opt no-new-privileges --cap-drop ALL`；无 TLS；
  `PERI_HRANA_EXPERIMENTAL=1`；**无观测器**；默认配额
  （`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_result_bytes=4194304`）；fixture `threads=201 / messages=8625 /
  content_chars=20177069`，每腿重新导入并校验 `complete=true`。
- 有效性：8/8 腿二进制 sha256 匹配、8/8 镜像 digest 匹配、8/8 fixture 完整、
  8/8 `launcher-stop` clean 且 `unresolved_operations=false`、8/8 `stop-exit=0`；
  160 个 timed 样本全部 `outcome=ok`、HTTP 200、rows=1、`response_bytes=312`。
  `invariants.py` 退出码 0。

## 测量表

逐腿（`summary.json` 的 `median_ms`，每腿 20 个 timed 样本）：

| leg | 侧 | 样本 | min (ms) | median (ms) | max (ms) | sd (ms) | CV |
|---|---|---:|---:|---:|---:|---:|---:|
| r1a1 | A | 20 | 1.116 | 2.201 | 8.632 | 2.387 | 70.8% |
| r1b1 | B | 20 | 0.732 | 2.386 | 6.526 | 1.719 | 66.7% |
| r1b2 | B | 20 | 0.736 | 1.826 | 5.860 | 1.591 | 74.2% |
| r1a2 | A | 20 | 0.898 | 3.061 | 7.272 | 1.903 | 59.6% |
| r2a1 | A | 20 | 0.871 | 1.455 | 6.705 | 1.897 | 73.6% |
| r2b1 | B | 20 | 1.054 | 2.441 | 7.754 | 2.275 | 69.1% |
| r2b2 | B | 20 | 1.037 | 3.644 | 7.242 | 2.059 | 56.7% |
| r2a2 | A | 20 | 1.169 | 3.314 | 6.803 | 1.792 | 52.4% |

每轮两侧分布（样本级，40/侧放大到 min/p25/median/p75/max）：

| 轮 | 侧 | n | min | p25 | median | p75 | max | sd（腿内合并） |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| round1 | A | 40 | 0.898 | 1.274 | 2.460 | 4.922 | 8.632 | 2.158 |
| round1 | B | 40 | 0.732 | 0.848 | 2.052 | 3.357 | 6.526 | 1.656 |
| round2 | A | 40 | 0.871 | 1.342 | 2.586 | 4.224 | 6.803 | 1.845 |
| round2 | B | 40 | 1.037 | 1.293 | 3.337 | 4.813 | 7.754 | 2.170 |

主指标（与上位协议同规则，候选/基线）：

| 指标 | 轮 | 基线中位 (ms) | 候选中位 (ms) | 比值 |
|---|---|---|---:|---:|---:|
| json-pipeline | round1 | 2.631 | 2.106 | **0.800** |
| json-pipeline | round2 | 2.384 | 3.042 | **1.276** |
| json-pipeline | 合并 | 2.631 | 2.413 | **0.917** |

- 逐腿中位来自 20 样本中位；每轮/合并按上位协议取「腿中位的中位」。
- 冻结判决：**PASS**（两轮未同时 >1.10，合并也未 >1.10）。
- p95/p99：round1 A/B = 6.552/5.420ms、8.632/6.526ms；round2 = 5.997/6.976ms、
  6.803/7.754ms。尾部方向也不一致。

## 可复现性判据（预注册）

| 判据 | 要求 | 实测 | 结果 |
|---|---|---|---|
| 两轮方向一致 | 都 >1.10（回归） | 0.800 / 1.276 | **否（方向相反）** |
| 合并比值 | >1.10 | 0.917 | **否** |
| 每轮分布分离 | 不重叠 或 中位差 > 腿内 3×SD | 全部重叠；差 0.407/0.751ms vs 3×SD 6.475/6.509ms | **否** |

补充统计（报告项）：

| 检验 | round1 | round2 | 合并 |
|---|---:|---:|---:|
| 样本级中位差 B−A | −0.407ms | +0.751ms | −0.152ms |
| permutation p（双侧） | 0.386 | 0.484 | 0.787 |
| bootstrap 95% CI（中位差） | [−2.480, 0.653] | [−1.203, 2.349] | [−1.395, 1.070] |
| 10% trimmed mean 比值 | — | — | 0.912 |
| Mann-Whitney 双侧 p | — | — | 0.997 |
| 样本 >5ms 比例 | — | — | A 21.2% / B 16.2% |

**为什么 5→20 样本没有救回可复现性**：噪声主要不在「腿内抽样」而在腿间/宿主状态。
20 样本时腿内 SD 仍为 1.6–2.4ms、腿中位仍在 1.455–3.644ms 间跳动。
按本窗口 σ≈2.0ms 估算，要在 80% power 下检测 0.5ms 差异，约需 **248 样本/侧**；
本窗口只有 80/侧，发布窗口只有 20/侧，且判决实际只用每轮 2 条腿的中位。
因此该场景的 FAIL/PASS 在现有协议下接近掷硬币。

## 机制证据

### F1 的 pipeline 确实含 `close`

harness 的 `_request_once` 默认 `close_stream=True`，`_pipeline_requests` 在
execute 后追加 `{"type": "close"}`，`hrana.py::pipeline_json` 原样编码。F1 的实际
请求体为：

```json
{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"close"}]}
```

编码后 **102 bytes**，与 8 条腿 `raw.jsonl` 的 `request_bytes=102` 一致。
即每个 F1 样本都是一次 `[execute, close]` pipeline。

### `/v1/metrics` 的 action 调用次数：A/B 完全相同

每腿从 `metrics-before` 到 `metrics-after` 的增量（21 个 pipeline 请求 + fixture discovery）：

| leg | 侧 | open | statement | close | describe | cursor_* |
|---|---|---:|---:|---:|---:|---:|
| r1a1 | A | 22 | 21 | 22 | 0 | 0 |
| r1b1 | B | 22 | 21 | 22 | 0 | 0 |
| r1b2 | B | 22 | 21 | 22 | 0 | 0 |
| r1a2 | A | 22 | 21 | 22 | 0 | 0 |
| r2a1 | A | 22 | 21 | 22 | 0 | 0 |
| r2b1 | B | 22 | 21 | 22 | 0 | 0 |
| r2b2 | B | 22 | 21 | 22 | 0 | 0 |
| r2a2 | A | 22 | 21 | 22 | 0 | 0 |

A/B 两侧的 `open=22`、`statement=21`、`close=22` 完全一致。**调用次数差为零**，
没有证据表明 v1.2.7 在 F1 路径上多做了 close/open RPC。

发布报告原窗口（每腿 4 个协议、5 样本）也一样：A/B 都是
`open=25`、`statement=12`、`close=25`，`journal_appends=50`。

### journal 每请求数：A/B 相同

| 指标 | A（4 腿中位） | B（4 腿中位） | 每 pipeline 请求（÷21） |
|---|---:|---:|---:|
| `operation_journal_appends_total` | 44 | 44 | 2.095 / 2.095 |
| `operation_journal_syncs_total` | 29 | 28.5 | 1.381 / 1.357 |
| `audit.fsync_count` | 26 | 26 | 1.238 / 1.238 |
| `operation_journal_coalesced_total` | 15 | 15.5 | 0.714 / 0.738 |
| `operation_journal_batch_max` | 4.5 | 4.5 | — |
| `admitted_requests_total` | 69 | 69 | — |

worker 侧 `native.workers.*.metrics.query.total` 两侧也都是 **25**（21 个 harness
execute + 4 个 fixture 查询），说明 SQL 执行量相同。

### R64（deferred durability 256→4096）在本场景不可能生效

v1.2.7 的 `/v1/metrics.deferred_durability` 在 4 条 B 腿上：

| leg | pending_max | inline_total | maximum | enqueued_total |
|---|---:|---:|---:|---:|
| r1b1 | 17 | 0 | 4096 | 118 |
| r1b2 | 35 | 0 | 4096 | 118 |
| r2b1 | 18 | 0 | 4096 | 118 |
| r2b2 | 22 | 0 | 4096 | 118 |

`pending_max` 最高 35，远低于旧上限 256，`inline_total=0`。即使把上限保持 256，
本场景也不会触发 inline 路径；R64 对 F1 的延迟不可能有影响。

### 代码差异的事实核对

任务说明称「v1.2.6 → v1.2.7 的代码差异只有三类（R64、撤销 fused close、文档）」。
`git log v1.2.6..v1.2.7 -- prototypes/v1/{platform,src,tests}` 实际包含 17 个非 merge
代码提交（详见 `mechanism/code-diff.txt`），包括：

- **R39**（`0b879c9`，11-03 21:35）：worker 镜像刷新闸门由互斥改为共享/独占租约，
  直接影响同库 `open`/`begin`/`close` 并发路径；
- **R42**（`d41ca9d`，11-03 23:47）：Hrana 隐式 open 融合首条只读语句；
- **R43**（`6d5c9da`，11-04 00:06）：`[execute, close]` 融合会话关闭；
- **R46–R50**：fused open 答案解析、JSON/protobuf execute 单遍直转码；
- **R51/R52、R56/R57**：cursor 页转码/预取/借用序列化；
- **R64**（`861eb8a`，11-04 16:31）：deferred durability 上限 256→4096；
- **17854a8 / 05534aa / 39e8e6c**：撤销 R43 的适配器层 fused close。

关键事实：**v1.2.6 根本没有 `hrana_fused_open.go`，也没有 fused close**。
R43 是在 v1.2.6 之后引入、又在 v1.2.7 发布前撤销的。因此
「撤销 fused close」不是 v1.2.6→v1.2.7 的行为差异，而是 v1.2.7 开发窗口内部
的引入+撤销；用 v1.2.6 vs v1.2.7 的 A/B 无法把 F1 差异归因到它。
若未来 F1 差异真的复现，R39（每库 open/close 闸门）是比 R64 更值得先测的候选。

## 协议偏离

1. **样本数**：上位冻结协议实际是 **5 个 timed 样本/腿**（不是任务说明里的 1；
   `perf-protocol.md` 表格、`aggregate.py` 的 `samples=5` 与发布窗口
   `raw.jsonl` 每腿 5 个 timed 样本均可核对）。本窗口取 **20 个 timed 样本/腿**
   （≥5），理由是该场景腿级方差过大；这是相对上位冻结协议的**唯一**参数偏离。
   判决公式、阈值、腿结构、A/B 顺序、环境、配额、镜像、二进制均未改。
2. **协议范围**：只跑 `F1.select-1 / json-pipeline`，因此 `run_leg.sh` 的协议列表
   收窄为 `json-pipeline`；`aggregate.py` 的 `SCENARIOS` 也只保留该场景。
   这是任务指定的范围收窄，不是测量口径变化。
3. **脚本来源**：`run_leg.sh` / `run_all.sh` / `aggregate.py` / `invariants.py`
   均复制自 `2026-11-04-v1.2.7-release/`，只改上述范围与样本数常量；
   `seed_fixture.py` / `snap_metrics.py` 原样复制。
4. **工具链**：未重新构建。直接复用 `/tmp/peri-release-ab/bin/` 的发布 A/B 二进制，
   逐腿容器内 sha256 与报告 `binaries.txt` 逐字节一致（A 平台 `2a4a41d8…`、
   worker `61f74236…`；B 平台 `95716e4d…`、worker `b17a0708…`）。
5. **卷名**：`peri-f1attr-f1attr-f1-select-1-<leg>`（脚本前缀 `peri-f1attr-` +
   `WINDOW=f1attr`，出现重复段，但满足唯一前缀要求）；8 个卷全部保留未删。
   容器名同卷名，运行结束后已删除容器、保留卷。
6. **隔离**：连接文件只写入 `/tmp/peri-f1-attr/conn/`（chmod 600，未打印）；
   未修改 `/tmp/peri-r9-fix`、`/tmp/peri-release-ab` 的既有证据；
   未触碰 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*` 容器；
   未提交、未推送、未合入任何代码。

## 未确认项

- **未做 P0/P1/P2 归因**：按预注册规则，两轮方向相反即结案，不做第二步。
  因此没有「R64 vs fused close」的三点中位表。
- 单宿主、单窗口；绝对值只对本窗口的镜像/二进制/宿主负责，不能外推到其他窗口。
- 20 样本/腿仍不足以在 0.5ms 量级上给出 80% power（估算需 ~248/侧）；
  F1 的双峰慢尾（~1ms 与 ~5–7ms 两簇）的来源未定位（无观测器，不看 fsync 相位）。
- 「撤销 fused close」的静态结论（v1.2.6 从未包含它）是代码事实；
  但本窗口没有构建中间二进制去实测 P1，故不对「R64 / fused close 对 F1 的
  因果效应」给正负结论。
- 发布报告原窗口的 FAIL 仍按其冻结规则成立；本文件只证明它在更高采样下
  不可复现，不改写已发布报告。

## 建议

1. **不要把 F1.select-1 的现有 FAIL 当作真实回归**。本窗口 4× 采样下合并比值
   0.917，样本级 p≈0.997；原窗口自己的样本级 p≈0.989。
2. **修改 F1 这类亚 5ms 场景的验收口径**：不要用「每轮 2 条腿中位的中位」；
   改用样本级合并统计（如 Mann-Whitney / bootstrap）并给出最小样本量。
   按 σ≈2ms、目标效应 0.5ms 估算，至少 ~250 样本/侧；若只看 1.10 比值
   （0.24ms），需要更多。
3. 更省样本的做法是**请求级交错**：在同一栈内交替发出 A/B 请求，或每条腿内
   交替重启两侧栈，以抵消腿间宿主漂移；仅靠 A/B/B/A 的 4 条腿无法消除。
4. 若未来 F1 差异复现，优先测 **R39**（worker 每库 open/close 闸门改为共享/独占
   租约），其次 R42 fused open；R64 已被 `pending_max≤35 << 256` 排除，
   fused-close 撤销不是 v1.2.6→v1.2.7 的行为差异。
5. 在新的宿主窗口或 CI 上重复本 8 腿窗口，再决定是否修改发布报告中的 F1 行。

## 证据清单

```
perf-protocol.md                 本窗口冻结协议（含样本数偏离与判据）
run_all.sh / run_leg.sh          复制并收窄到 F1/json-pipeline 的运行脚本
aggregate.py / aggregate.txt     逐腿、逐轮、合并、可复现性判据
invariants.py / invariants.txt   二进制 sha、镜像 digest、fixture、stop proof、
                                 audit/journal 计数
legs.txt                         8 腿完整运行日志（含时间、load、sha 首行）
legs/F1.select-1/<leg>/          summary.json、raw.jsonl、metrics-before/after、
                                 launcher-stop.json、binaries.txt、image-*、
                                 harness.txt、seed.txt、container.txt 等 13 个文件/腿
mechanism/f1-request-shape.txt   F1 pipeline 含 close 的请求体与 102 字节核对
mechanism/code-diff.txt          v1.2.6..v1.2.7 实际代码提交与关键 ancestry
mechanism/leg-metadata.txt       每腿 rounds/warmup/samples/配额/镜像
mechanism/reproducibility_stats.txt  样本级统计、per-leg CV、成对位置比较
mechanism/release-window-comparison.txt  原发布窗口的腿样本与 action/journal 增量
mechanism/release-window-sample-stats.txt 原发布窗口的样本级检验
mechanism/worker_metrics.txt     worker 侧 query/execute 计数
```

**文件改名说明**：仓库 `.gitignore` 含 `*.log`，会让日志证据被静默忽略。提交时把
`run_leg.sh` 产出的 `harness.log` / `seed.log` / `container.log` 原样改名为 `.txt`，
内容未改；`perf-protocol.md` 是测量前冻结的文件，其中列出的仍是原始 `.log` 文件名。
重新运行 `run_all.sh` 会再次生成 `.log` 文件，需同样改名后才会进入版本控制。

复现命令：

```bash
cd docs/performance-review/2026-11-05-f1-release-attribution
bash run_all.sh
python3 aggregate.py > aggregate.txt
python3 invariants.py > invariants.txt
```
