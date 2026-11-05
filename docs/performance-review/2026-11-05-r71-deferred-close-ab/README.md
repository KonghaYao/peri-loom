# R71 冻结 A/B：只读无状态会话的 worker close 异步派发

日期：2026-11-05
分支：`perf/r71-deferred-session-close`
候选：`ac5e0c7`（父 `02a05a7`，根 `9e30afa`）；平台二进制 SHA256
`3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014`，与 Gate 2 记录一致。
性质：**冻结判据的完整 A/B + 回归门**。只构建、启动隔离栈、测量、留证；不改生产代码，不推送远端。
判据在第一条腿之前冻结于 `criteria.txt`（SHA256
`37d2cec465d1175c8d542f34104023dbd471803424ea27a3e9ae60f4e264f19f`，冻结时间
`2026-11-05T04:01:52Z`）。

## 结论

**本轮 FAIL：P0 功效检查未通过（位置 A/A = 1.0680，超出 1.00 ± 0.05），按冻结规则候选结果不得解释、不得合入。**
分支与证据保留。P1 的原始测量值很大（合并 B/A = 0.75835，−24.2%），但它不能作为本轮判决依据。

| 检查 | 结果 | 数字 |
|---|---|---|
| P0 功效 | **FAIL** | a2/a1 = **1.06800**，CI 1.02864–1.11644；逐轮 1.06425 / 1.10234；轮次漂移 1.02831 |
| P1 主指标 | **FAIL（不得解释：P0 功效不足）** | 合并 B/A **0.75835**（CI 0.71521–0.78866），逐轮 0.76663 / 0.74216，配对差值中位 **−0.6218ms**（CI −0.7602–−0.5035） |
| P2 回归门 | **PASS**（冻结的 F1/L1/c8 指标；c16 产物另记） | F1 p50 0.56427、p95 0.65951；L1 p50 0.67492、p95 0.80951；X1 c8 吞吐 1.12685；无 >10% 可复现退化 |
| P3 不变量 | **PARTIAL** | P1/F1/L1 全绿；X1 c8 窗口全绿；标准 X1 场景的 c16 层在两侧都触发默认配额 429（协议节奏产物，非冻结 c8 指标） |
| 总判决 | **FAIL** | P0 功效不足 → P1 不得解释；候选不合入 |

## 冻结判据（逐字摘要，全文见 `criteria.txt`）

- **P0.1**：本轮 A/A（`a1` vs `a2`，同侧合并）比值必须落在 1.00 ± 0.05；超出即判功效不足，
  候选结果不得解释。`a1` = `r1-a1 + r2-a1`，`a2` = `r1-a2 + r2-a2`，比值 = 合并中位 `a2` / 合并中位 `a1`。
- **P0.2**：阳性对照引用 Gate 1b 已登记结果：100µs 忙等探针合并 B/A = 1.13258
  （CI 1.08304–1.20891，逐轮 1.13602 / 1.12804）。本轮未重跑，未与候选腿混算。
- **P1**：样本 = 连续 100 个 `[execute SELECT 1 want_rows, close]` pipeline 请求的总墙钟 ÷ 100；
  每腿 1 warmup + 30 计时块，块间隔 0.75s，首块前 1.2s；2 轮 × A/B/B/A = 8 腿。
  PASS 条件：两轮同向且合并中位比值 ≤ 0.92，且逐腿配对 bootstrap 95% CI 排除 0。
  0.92–1.00 判 FAIL；不得事后放宽。
- **P2**：标准 issue28 harness；F1/L1 json-pipeline 每腿 125 个计时请求（2 轮 × A/B/B/A，
  每侧 500 请求）；X1 c8 每腿 100 请求，2 轮 × A/B/B/A。可复现退化 = 合并 B/A > 1.10
  且两轮同向（X1 吞吐为 < 0.90 且两轮同向）。
- **P3**：计数器方向、seats/permits/worker sessions/pending_close 归零、审计链连续且同工作量条数一致、
  wire 形状一致、无 429/503/`outcome_unknown`/`session_moved`/`audit_unavailable`、干净停止。
- **判决**：P1 PASS 且 P2 无 >10% 可复现退化且 P3 全绿 → 通过；否则 FAIL。

## 环境

| 项 | 值 |
|---|---|
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（OrbStack，服务端 29.4.0） |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，id/digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| worker（两侧相同） | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 候选 platform | 从 `ac5e0c7` 的独立 worktree 交叉编译：`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`；SHA256 `3059c47a…`（与 Gate 2 一致） |
| A 腿 | 同一候选二进制，`PERI_DEFERRED_SESSION_CLOSE` 未设置 |
| B 腿 | 同一候选二进制，`PERI_DEFERRED_SESSION_CLOSE=1` |
| launcher | 分支 `dev.py`，SHA256 `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129`，两侧挂载相同 |
| Go | `go1.27.3 darwin/arm64` |
| 耐久模式 | `audit_file_v1`（所有腿一致） |
| 配额 | 默认，未放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |
| fixture | 每腿重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 未开启 |
| 并发 | 任一时刻只有一个隔离栈运行 |

- 容器：`peri-r71-ab-p1-<leg>`、`peri-r71-ab-p2-<scenario>-<leg>`、`peri-r71-ab-wire-<a|b>`；
  全部干净停止（`stop-exit=0`）后删除。
- 卷：37 个 `peri-r71-ab-*` 卷全部保留（`volumes.txt`）。
- 端口：P1 20980–20987；P2 F1 21000–21007、L1 21008–21015、X1 21016–21023；wire 20990–20991；
  烟雾腿 20978/20979/20999。
- run-dir：`/tmp/peri-r71-ab/{p1,p2,wire}/…`；连接文件只在 `/tmp`，`chmod 600`，未入库、未打印。
- 未触碰 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker、未 prune、未删除任何卷。

## P1：块平均 A/B（8 腿）

命令：

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-ab
./run_p1.sh          # r1-a1 r1-b1 r1-b2 r1-a2 r2-a1 r2-b1 r2-b2 r2-a2
python3 analyze_p1.py
```

| 腿 | 侧 | 轮 | 块数 | p50（块中位 ms） | 均值 ms | p95 ms | 标准差 | 计时请求 | 失败 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| r1-a1 | A（flag off） | r1 | 30 | 2.4816 | 2.4224 | 2.8436 | 0.2792 | 3,000 | 0 |
| r1-b1 | B（flag on） | r1 | 30 | 1.9853 | 1.9201 | 2.3772 | 0.3260 | 3,000 | 0 |
| r1-b2 | B | r1 | 30 | 1.8649 | 1.8559 | 2.3648 | 0.3392 | 3,000 | 0 |
| r1-a2 | A | r1 | 30 | 2.6411 | 2.6313 | 3.2439 | 0.3275 | 3,000 | 0 |
| r2-a1 | A | r2 | 30 | 2.4810 | 2.5236 | 3.0841 | 0.3571 | 3,000 | 0 |
| r2-b1 | B | r2 | 30 | 1.9877 | 1.9348 | 2.2939 | 0.2891 | 3,000 | 0 |
| r2-b2 | B | r2 | 30 | 1.8500 | 1.8783 | 2.3907 | 0.3288 | 3,000 | 0 |
| r2-a2 | A | r2 | 30 | 2.7350 | 2.6671 | 3.2092 | 0.3998 | 3,000 | 0 |

- 每腿另有 1 个 warmup 块（100 请求，丢弃），故 B 腿窗口 `enqueued_total=3100`。
- 合并：A 120 块中位 **2.5600ms**、均值 2.5611、p95 3.1892；B 120 块中位 **1.9414ms**、
  均值 1.8973、p95 2.3889。
- 合并 B/A = **0.75835**，bootstrap 95% CI **0.71521–0.78866**（10,000 次，seed 20261105）。
- 逐轮：r1 **0.76663**、r2 **0.74216**（同向，均远低于 0.92）。
- 配对（每轮 `a1↔b1`、`a2↔b2` 按块序号，共 120 对）：B−A 中位 **−0.621842ms**，
  均值 −0.663844ms，95% CI **−0.760196–−0.503498ms**，等于配对 A 中位的 **−24.291%**；
  CI 上界 < 0。
- **若只按 P1 冻结阈值，上列数字会判 PASS（≤0.92、两轮同向、CI 排除 0）；但 P0 功效检查失败，
  按冻结规则 P1 不得解释。**

### P0：位置 A/A 与轮次漂移

| 口径 | 中位 ms | 比值 |
|---|---:|---:|
| `a1`（r1-a1 + r2-a1，60 块） | 2.4816 | — |
| `a2`（r1-a2 + r2-a2，60 块） | 2.6504 | **a2/a1 = 1.06800** |
| r1 的 A 腿合并（60 块） | 2.5389 | — |
| r2 的 A 腿合并（60 块） | 2.6108 | r2/r1 = 1.02831 |

- `a2/a1` 的 bootstrap 95% CI **1.02864–1.11644**；逐轮 `a2/a1` = **1.06425**（r1）、
  **1.10234**（r2）。**全部超出 1.00 ± 0.05。**
- 轮次漂移只有 +2.83%（在 ±5% 内），超限来自**位置**：每轮第 4 腿（A2）比第 1 腿（A1）慢
  6.4%–10.2%。这正是 Gate 1b 记录过的位置漂移；本轮它超过了预登记的功效带。
- 冻结规则原文：“超出则判功效不足，候选结果不得解释。”没有重跑或放宽条款。

### close / open / statement 动作桶（窗口增量，均值）

| 腿 | open µs | statement µs | close µs |
|---|---:|---:|---:|
| r1-a1 | 659.459 | 138.209 | **632.138** |
| r1-b1 | 632.184 | 116.248 | **94.590** |
| r1-b2 | 639.284 | 105.146 | **93.959** |
| r1-a2 | 710.920 | 157.264 | **727.860** |
| r2-a1 | 680.292 | 144.710 | **671.167** |
| r2-b1 | 637.197 | 112.282 | **94.810** |
| r2-b2 | 634.805 | 108.786 | **92.427** |
| r2-a2 | 737.749 | 143.674 | **713.375** |

- A 腿 close 均值 632–728µs；B 腿 92–95µs，约 **−85%**，与 Gate 2 的 −83.75% 同向。
- 工作没有消失，只是换了归属：B 腿的 open/statement 均值略低或相当，块级端到端仍明显下降。
- 该动作桶是支持证据，不参与 P1 判决。

## P2：回归门

命令：

```bash
./run_p2.sh f1 && ./run_p2.sh l1 && ./run_p2.sh x1
python3 analyze_p2.py
```

### F1.select-1 json-pipeline（每腿 125 计时请求；每侧 500）

| 腿 | 侧 | 轮 | p50 ms | p95 ms | 失败 |
|---|---|---|---:|---:|---:|
| r1-a1 | A | r1 | 1.9293 | 5.9623 | 0 |
| r1-b1 | B | r1 | 1.2173 | 3.5770 | 0 |
| r1-b2 | B | r1 | 0.9583 | 3.6687 | 0 |
| r1-a2 | A | r1 | 1.4696 | 5.1039 | 0 |
| r2-a1 | A | r2 | 2.3412 | 5.6513 | 0 |
| r2-b1 | B | r2 | 0.9114 | 3.2283 | 0 |
| r2-b2 | B | r2 | 1.0527 | 3.5162 | 0 |
| r2-a2 | A | r2 | 1.6346 | 4.3075 | 0 |

- 合并 p50：A 1.7820 / B 1.0055，B/A **0.56427**；逐轮 0.64009 / 0.49401。
- 合并 p95：A 5.3776 / B 3.5466，B/A **0.65951**；逐轮 0.65476 / 0.67724。
- 无可复现退化（改善方向）。

### L1.list-20 json-pipeline（每腿 125 计时请求；每侧 500）

| 腿 | 侧 | 轮 | p50 ms | p95 ms | 失败 |
|---|---|---|---:|---:|---:|
| r1-a1 | A | r1 | 2.1726 | 4.6358 | 0 |
| r1-b1 | B | r1 | 1.4650 | 4.5728 | 0 |
| r1-b2 | B | r1 | 1.3268 | 3.6672 | 0 |
| r1-a2 | A | r1 | 2.1495 | 4.2523 | 0 |
| r2-a1 | A | r2 | 1.9147 | 4.6496 | 0 |
| r2-b1 | B | r2 | 1.4162 | 3.2231 | 0 |
| r2-b2 | B | r2 | 1.2412 | 3.5278 | 0 |
| r2-a2 | A | r2 | 1.8860 | 3.7358 | 0 |

- 合并 p50：A 2.0321 / B 1.3715，B/A **0.67492**；逐轮 0.64594 / 0.69919。
- 合并 p95：A 4.4440 / B 3.5975，B/A **0.80951**；逐轮 0.92708 / 0.80508。
- 无可复现退化（改善方向）。

### X1 c8 吞吐（标准 X1 场景的 c8 层，每腿 100 请求）

| 腿 | 侧 | 轮 | c8 吞吐 qps | c8 p50 ms | c8 p95 ms | c8 失败/429 |
|---|---|---|---:|---:|---:|---:|
| r1-a1 | A | r1 | 1724.14 | 3.9885 | 8.1435 | 0 / 0 |
| r1-b1 | B | r1 | 1851.85 | 3.8001 | 9.2595 | 0 / 0 |
| r1-b2 | B | r1 | 1538.46 | 4.2014 | 13.5128 | 0 / 0 |
| r1-a2 | A | r1 | 1818.18 | 3.6615 | 9.3750 | 0 / 0 |
| r2-a1 | A | r2 | 2127.66 | 3.0717 | 8.3417 | 0 / 0 |
| r2-b1 | B | r2 | 2222.22 | 2.2675 | 9.0583 | 0 / 0 |
| r2-b2 | B | r2 | 2173.91 | 2.4176 | 6.6553 | 0 / 0 |
| r2-a2 | A | r2 | 1754.39 | 4.0872 | 9.1379 | 0 / 0 |

- 合并吞吐：A 1786.28 / B 2012.88 qps，B/A **1.12685**；逐轮 **0.95709 / 1.13243**。
  两轮不同向，且合并方向为正；无退化。
- c8 层全部 400 请求（8 腿 × 100）零失败、零 429、`rejected_delta=0`、`session_seats=0`、
  `active_permits=0`。

### X1 标准场景的 c16 配额产物（非冻结指标）

- 标准 X1 场景依次跑 c1/4/8/16，各 100 请求；4 层合计约 800 个准入 token，而默认桶
  burst=500、500/s，窗口只有约 0.3–0.4s。**c16 层在 A 与 B 两侧都出现 429**：
  A 腿 29–37 个、B 腿 46–54 个（B 更快，单位时间消耗更多 token）。
- 因此 8 条 X1 腿的 harness `driver_exit=1`（c16 verdict FAIL），整腿 `errors_total` 为
  `quota_exceeded`、`rejection_reasons_total` 为 `rate_limit`。c8 层记录本身全部干净。
- 这是**协议节奏产物**，不是候选回归；但也意味着 X1 腿不能为 P3 的“无 429”提供整腿级证明。
  冻结的 X1 指标只有 c8；本报告不把 c16 数字用于任何判决。

### wire 字节

- F1 计时响应字节分布：A `{309:3, 310:10, 311:26, 312:343, 323:6, 324:112}`；
  B `{308:1, 309:6, 310:14, 311:23, 312:345, 324:111}`（每侧 500）。
- L1：A `{15624:2, 15625:8, 15626:32, 15627:318, 15638:140}`；
  B `{15622:1, 15624:2, 15625:3, 15626:31, 15627:329, 15638:134}`。
- **逐字节分布不完全相同**：差异来自响应内 `query_duration_ms` 的浮点小数位数
  （R4 已记录同一现象）。F1 参考值 312、L1 参考 ~15.6KB（众数 15627）在两侧一致，
  没有候选特有的字段或体积膨胀。
- 结构探针（`run_wire_probe.sh` + `wire_probe.py`，A/B 各发 3 次同一 pipeline）：
  把 duration 与不透明 ID 归一化后，两侧、各次响应的规范化 SHA256 全部为
  `c8cda4a455789dbf1811d3a6b0d7a1f2d653b2225da50724ff44eba7c9f00b80`。
  原始字节分别为 A `[323, 312, 324]`、B `[312, 312, 324]`，即差异只在 duration 位数。

## P3：不变量

逐腿证据在 `legs/<campaign>/<scenario>/<leg>/`；汇总在 `invariants.json`、`audit-summary.txt`、
`audit-verify.json`（每腿）。

| 不变量 | 结果 | 证据 |
|---|---|---|
| A 腿无 `deferred_session_close` 键 | 8/8 P1 + 4/4 F1 + 4/4 L1 + 4/4 X1 A 腿均为 absent | `metrics-after.json`、`invariants.json` |
| B 腿 `enqueued_total > 0` | P1 **3100/腿**；F1/L1 **126/腿**；X1 **341–351/腿** | `invariants.json` |
| B 腿 failed/retried/unresolved = 0，pending = 0 | 全部 B 腿 0；`pending_max` P1=2、F1/L1=1–2、X1=4（恰为每库上限） | `deferred_delta` / `deferred_now` |
| `session_seats=0`、`active_permits=0` | 32/32 测量腿在 settle 后为 0 | `metrics-after.json` |
| worker sessions = 0 | `native.workers.worker-a/worker-b.sessions` 全部 0 | `metrics-after.json` |
| `cleanup_ledger.pending_close.count=0`、worker `lifecycle.pending_close.count=0` | 32/32 腿为 0 | `metrics-after.json` |
| 审计链连续 | **32/32 腿** sequence 逐条 +1、`previous` 链一致、干净停止时 head 与尾记录一致 | `audit-summary.txt`、`audit-verify.json` |
| 同工作量审计条数一致 | P1 **9752 = 9752**；F1 **836 = 836**；L1 **836 = 836**；X1 不相等（A 1587–1603，B 1553–1569） | `invariants.json` |
| wire 形状 | 归一化结构 SHA256 两侧一致；原始字节差仅 duration 位数 | `legs/wire/*/wire-bodies.json` |
| 无 429/503/`outcome_unknown`/`session_moved`/`audit_unavailable` | P1/F1/L1 全部 0；X1 c8 窗口 0；X1 c16 层为 `rate_limit`（见上） | `errors`/`rejections` 与 harness `summary.json` |
| 干净停止 | 32/32 腿 `stop-exit=0`，`launcher-stop` outcome=`clean`、`unresolved_operations=false` | `launcher-stop.json`、`stop-exit.txt` |

- X1 的审计条数不相等，是因为 c16 层被 429 拒绝的请求**没有产生审计记录**；同一工作量下
  P1/F1/L1 的条数逐腿完全相等。
- X1 的 B 腿有 4–6 次 `inline_total`（每库上限 4 触顶），符合设计；`failed/retried/unresolved=0`。

## 判决

1. **P0 FAIL**：`a2/a1 = 1.06800`，CI 1.02864–1.11644，超出 1.00 ± 0.05。
   冻结规则要求“判功效不足，候选结果不得解释”。
2. **P1 NOT INTERPRETABLE**：原始测量为合并 B/A 0.75835、两轮同向、配对 CI 排除 0；
   但 P0 失败使该结果不能用于判决。
3. **P2 PASS**（冻结指标）：F1/L1 p50/p95 与 X1 c8 吞吐均无 >10% 可复现退化。
4. **P3 PARTIAL**：P1/F1/L1 全绿；X1 c8 窗口全绿，c16 层为标准场景的默认配额产物。
5. **总判决 FAIL，候选不得合入**。分支 `perf/r71-deferred-session-close` 与全部证据保留。

## 未验证项与剩余风险

- **未测**：崩溃/重启、进程崩溃或掉电、写路径（W1）、`max_concurrent`/`max_sessions` 变配、
  c>8 的候选行为、TLS、Hrana HTTP v3/WS、跨命名空间并发、长期泄漏。
- **P0 失败的后果**：本宿主本轮的位置漂移 6.4%–10.2%，使协议无法认证即使观测到的效应很大。
  下一轮若仍要测此候选，需要重新预登记一个对位置漂移不敏感的设计（例如块级交错或在同一容器内
  交替 A/B），但**不能用本轮数据事后改判据**。
- **X1 c16 429**：标准场景在默认配额下的节奏产物，两侧都有；c8 是本轮唯一冻结的 X1 指标。
  若未来要用 c16 做回归门，必须显式加层间补桶等待或降低每层请求数，并在预登记中写明。
- **wire 字节**：`query_duration_ms` 使原始字节不可能逐字节相等；本轮用归一化结构探针补证。
  该探针只覆盖 F1 形状的 JSON pipeline，不覆盖 protobuf/cursor。
- 单机、无 TLS、OrbStack 环境；不是容量、SLA 或完整协议矩阵证明。

## 运行命令与输出摘要

```bash
# 1) 构建候选（独立 worktree，避免工作树污染）
git worktree add --detach /tmp/r71-ab-src ac5e0c7
cd /tmp/r71-ab-src/prototypes/v1/platform
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -o /tmp/r71-ab/bin/peri-platform-ab ./cmd/peri-platform
shasum -a 256 /tmp/r71-ab/bin/peri-platform-ab
# 3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014

# 2) 冻结判据后先做链路烟雾（不进 8 腿统计）
BASE_PORT=20979 BLOCKS=2 BLOCK_SIZE=10 ./run_p1.sh r0-a1
BASE_PORT=20978 BLOCKS=2 BLOCK_SIZE=10 ./run_p1.sh r0-b1   # enqueued_total=30
BASE_PORT=20999 SAMPLES=2 ./run_p2.sh f1 r0-a1

# 3) P1 八腿
./run_p1.sh > /tmp/r71-ab/p1-run.txt 2>&1
python3 analyze_p1.py

# 4) P2 三个场景各八腿
./run_p2.sh f1 && ./run_p2.sh l1 && ./run_p2.sh x1
python3 analyze_p2.py

# 5) P3 与汇总
python3 collect_invariants.py
python3 make_summary.py
```

关键输出：

```
P0.1 a2/a1 = 1.06800 (CI [1.02864, 1.11644])
P1 merged B/A = 0.75835 (CI [0.71521, 0.78866]), rounds {'r1': 0.76663, 'r2': 0.74216}
paired B-A median -0.621842 ms (CI [-0.760196, -0.503498], -24.291% of paired A median, n=120)
VERDICT: FAIL P0.1 positional A/A ratio outside 1.00 +/- 0.05: power insufficient

f1 median_ms  A 1.7820 B 1.0055  B/A 0.56427  rounds 0.64009 / 0.49401
f1 p95_ms     A 5.3776 B 3.5466  B/A 0.65951  rounds 0.65476 / 0.67724
l1 median_ms  A 2.0321 B 1.3715  B/A 0.67492  rounds 0.64594 / 0.69919
l1 p95_ms     A 4.4440 B 3.5975  B/A 0.80951  rounds 0.92708 / 0.80508
x1 throughput A 1786.28 B 2012.88 qps B/A 1.12685 rounds 0.95709 / 1.13243
P2 VERDICT: PASS

audit chains: OK 32/32; P1 audit records 9752 = 9752 (A/B)
invariants: 27/35 legs pass (the 8 X1 legs fail only the whole-leg c16 rate-limit checks)
overall: FAIL (P0 power insufficient)
```

## 证据清单

| 文件/目录 | 内容 |
|---|---|
| `criteria.txt` / `criteria-freeze.txt` | 冻结判据与 SHA256/时间 |
| `run_p1.sh` / `run_p2.sh` / `run_wire_probe.sh` | 本轮实际使用的启动脚本 |
| `block_driver.py` / `snap_metrics.py` / `wait_settled.py` | 块平均驱动、指标快照、settle 等待 |
| `verify_audit.py` / `collect_invariants.py` | 审计链校验、P3 汇总 |
| `analyze_p1.py` / `analyze_p2.py` / `make_summary.py` | 冻结判据分析与根汇总 |
| `analysis-p1.json` / `analysis-p2.json` | 逐腿与合并结果 |
| `invariants.json` / `audit-summary.txt` | P3 逐腿检查与审计链 |
| `summary.json` | 环境、P0/P1/P2/P3、总判决 |
| `legs/p1/…`、`legs/p2/…`、`legs/wire/…` | 每腿 metrics、raw、summary、driver、binaries、stop proof、audit-verify |
| `legs/**/audit/` | 原始审计段（工作树与命名卷保留；因体积不入库） |

- 本轮未修改生产代码；提交为独立的 `docs(perf)：…` commit，未 amend、未推送。
