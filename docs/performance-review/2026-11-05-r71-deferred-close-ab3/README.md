# R71c 测量轮：短版交错 P1 + 标准 X1 + 持续 c8

日期：2026-11-05
分支：`perf/r71-deferred-session-close`
候选：`ac5e0c7`（父 `02a05a7`，根 `9e30afa`）
性质：**新的预登记测量轮（R71c）**。只构建、启动隔离栈、测量、留证；不改生产代码，不推送远端。
判据在第一腿之前冻结于 `criteria.txt`（SHA256
`d35d46870d484066da63013296fe9d592f3e15eabc8624c1a8c83bc4737c3e97`；冻结记录见
`criteria-freeze.txt`）。

本轮目标：判定 R71b 的 c8 退化（冻结的 c8-only 冷突发驱动 B/A=0.84384）是真实稳态退化，
还是冷突发驱动产物。**持续 c8 是本轮的关键判据；标准 X1 未退化不能掩盖持续 c8 的退化。**

## 判据与口径

全文见 `criteria.txt`。要点：

- **P0.1 零对照**：短版交错 1 轮 × 30 triples；每 triple 的 `A2/A1`，30 个比值中位在
  1.00 ± 0.05 且 bootstrap 95% CI 覆盖 1.0。判据原文的 P0 条目写「60 triples」，但设计段与 P1
  条目明确是「1 轮 × 30 triples」。零对照必须与 P1 同栈、同时段、同一批 triples，因此在第一腿前
  记录了口径调和：P0 在 30 个 triples 上评估，60 个 A 侧块（30 A1 + 30 A2）作为上下文报告；
  数值带与 CI 规则未改。
- **P0.2 阳性对照**：引用 R71b 的 100µs busy-wait 双栈交错结果（中位 1.09317 ≥ 1.08，PASS），
  本轮不重跑、不混算。
- **P1**：同一 30 triples；`effect_i = B_i / mean(A1_i, A2_i)`；合并中位 ≤0.92 且 bootstrap
  95% CI 上界 <1.0；0.92–1.00 判 FAIL。
- **P2.1**：F1/L1 json-pipeline，各 500 requests/side（2 轮 × A/B/B/A，8 腿），p50/p95 不得
  >10% 可复现退化（合并 B/A >1.10 且两轮都 >1.0）。
- **P2.2**：标准 Issue 28 X1（levels 1/4/8/16，100 requests/level；c1/c4 热身后 c8），c8 吞吐
  2 轮 A/B/B/A，不得 >10% 可复现退化（合并 B/A <0.90 且两轮都 <1.0）；c16 的默认配额 429 只作诊断。
- **P2.3 持续 c8**：8 并发客户端，每个客户端 500 个 pipeline 请求（隐式 open → execute(SELECT 1)
  → close；100 warmup + 400 timed），200 请求/块、块间 1.2s 配额回填；吞吐、p50、p95 不得 >10%
  可复现退化。块设计是默认配额下唯一不产生 429 的等价稳态设计，已在第一腿前冻结。
- **P3**：A 腿无 `deferred_session_close` 键；B 腿 `enqueued_total>0`、failed/retried/unresolved=0、
  pending=0；seats/permits/worker sessions/pending_close 全 0；审计链连续且条数可解释；无 503/
  `outcome_unknown`/`session_moved`/`audit_unavailable`（429 仅允许标准 X1 c16 诊断层）；干净停止
  `unresolved_operations=false`。

## 环境

| 项 | 值 |
|---|---|
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（10 vCPU，VM 内存 12.6 GB） |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| worker（两侧相同） | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 候选 platform | 从 `ac5e0c7` 的独立 worktree 重建：`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`；SHA256 `3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014`（与 Gate 2/R71b 一致） |
| launcher | 分支 `dev.py`，SHA256 `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129`，两侧挂载相同 |
| Go | `go1.27.3 darwin/arm64` |
| 耐久模式 | `audit_file_v1`（各腿 metrics 一致） |
| 配额 | 默认，未放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |
| fixture | 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 未开启 |
| 端口 | P1 21200–21201；F1 21210–21217；L1 21220–21227；标准 X1 21230–21237；持续 c8 21240–21247 |
| 卷 | 新卷 `peri-r71-ab3-*`；R71b 的 39 个 `peri-r71-ab2-*` 卷未触碰 |
| run-dir | `/tmp/peri-r71-ab3/{p1,p2}/…`；连接文件只在 `/tmp`，`chmod 600`，未入库、未打印 |

未触碰生产容器 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker、未 prune、
未删除任何卷/容器/失败证据。

## P0：功效对照

### P0.1 零对照（A1 vs A2，同栈同时段，30 triples）

| 口径 | 中位比值 | bootstrap 95% CI | 判定 |
|---|---:|---|---|
| 合并 30 triples | **1.02137** | 0.93005–1.21135 | **PASS**（在 1.00±0.05 内，CI 覆盖 1.0） |

A1 块中位 4.2981 ms、A2 块中位 4.4685 ms（60 个 A 侧块作为上下文）。
CI 比 R71b 的 60-triple 口径更宽（30 个样本），但判据只要求中位在带内且 CI 覆盖 1.0。

### P0.2 阳性对照

引用 R71b：探针二进制 `6517583e…`、B 侧 `PERI_R71_CLOSE_DELAY_US=100`、1 轮 × 30 triples，
`B / mean(A1,A2)` 中位 **1.09317 ≥ 1.08 → PASS**。本轮未重跑，未与候选 triples 混算。

## P1：短版交错主指标（30 triples）

命令：

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-ab3
./run_interleaved.sh p1
python3 analyze_interleaved.py --campaign p1
```

运行结果：`valid_triples=30/30`、`void_triples=0`、`failed_blocks=0`、`blocks=90`、
`requests_total=1800`（A 1,200、B 600）；两栈 `driver_exit=0`、`settle_exit=0`、`stop_exit=0`。

| 块 | n | 中位 ms | 均值 ms | p95 ms | min | max |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 30 | 4.2981 | 4.0604 | 5.2860 | 2.0623 | 5.7247 |
| A2 | 30 | 4.4685 | 4.3018 | 5.6741 | 2.6160 | 5.8593 |
| B | 30 | 3.3263 | 3.2258 | 4.4358 | 1.5332 | 4.6798 |

- `B / mean(A1,A2)` 中位 **0.77194**（CI **0.72116–0.83306**，上界 <1.0），≤0.92 → **P1 PASS**。
- 方向：30 个 triples 中 **24 个 B 更快**、6 个不更快；中位差
  `B − mean(A1,A2) = −1.05803 ms`（CI −1.27511–−0.68873）。
- 块顺序分布（seed 20261105）：`A2,A1,B` 7；`B,A2,A1` 7；`A1,B,A2` 6；`A2,B,A1` 4；
  `B,A1,A2` 3；`A1,A2,B` 3。
- 动作桶（P1 窗口增量，均值）：close **1,086.234µs → 161.967µs（−85.1%）**；
  open 1,152.5µs → 1,098.0µs（−4.7%）；statement 291.1µs → 189.4µs（−34.9%）。
  与 R71b 的 close −86.5% 方向一致。

P1 两栈 P3：A 审计 4,052 条、delta 3,600（1,200×3）；B 审计 2,252 条、delta 1,800（600×3），
A:B=2.0 与请求比一致；B `enqueued_total=600`、`inline_total=0`、failed/retried/unresolved=0、
`pending=0`；两栈 seats/permits/worker sessions/pending_close 全 0；干净停止。

## P2：回归门

每个 P2 腿都是新卷、独立容器/端口、同 fixture、默认配额、无观测器。F1/L1 每腿 125 个计时请求
（2 轮 × A/B/B/A，每侧 500 请求）；标准 X1 每腿 levels 1/4/8/16 各 100 请求；持续 c8 每腿
8 客户端 × 500 请求（100 warmup + 400 timed）。

### P2.1 F1.select-1 json-pipeline

命令：`./run_p2.sh f1`，`python3 analyze_p2.py .`

| 腿 | 侧 | p50 ms | p95 ms | 样本 | 状态 |
|---|---|---:|---:|---:|---|
| r1-a1 | A | 2.1925 | 4.4995 | 125 | 200×125 |
| r1-b1 | B | 0.7956 | 2.8700 | 125 | 200×125 |
| r1-b2 | B | 0.9065 | 4.0125 | 125 | 200×125 |
| r1-a2 | A | 1.1382 | 6.9416 | 125 | 200×125 |
| r2-a1 | A | 1.4841 | 5.5756 | 125 | 200×125 |
| r2-b1 | B | 0.8467 | 4.7986 | 125 | 200×125 |
| r2-b2 | B | 0.6491 | 3.2498 | 125 | 200×125 |
| r2-a2 | A | 1.7869 | 5.4885 | 125 | 200×125 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 1.6355 | 0.8212 | **0.50208** | 0.51103 | 0.45729 | 无退化 |
| p95 ms | 5.5320 | 3.6311 | **0.65638** | 0.60156 | 0.72743 | 无退化 |

F1 的 B 侧显著更快，两轮同向；无 >10% 可复现退化 → **P2.1 F1 PASS**。每腿审计 delta 384
（126 请求 × 3 + 6 discovery），A/B 一致；B 每腿 `enqueued_total=126`、inline=0、pending=0。

### P2.1 L1.list-20 json-pipeline

命令：`./run_p2.sh l1`

| 腿 | 侧 | p50 ms | p95 ms | 响应字节 | 状态 |
|---|---|---:|---:|---:|---|
| r1-a1 | A | 2.3692 | 5.5104 | 15,627 | 200×125 |
| r1-b1 | B | 1.4715 | 4.0702 | 15,627 | 200×125 |
| r1-b2 | B | 1.4066 | 4.0480 | 15,627 | 200×125 |
| r1-a2 | A | 2.6235 | 5.7185 | 15,627 | 200×125 |
| r2-a1 | A | 2.0740 | 4.8286 | 15,627 | 200×125 |
| r2-b1 | B | 1.6666 | 4.0488 | 15,627 | 200×125 |
| r2-b2 | B | 1.1482 | 3.4721 | 15,627 | 200×125 |
| r2-a2 | A | 2.5185 | 4.8963 | 15,627 | 200×125 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 2.4439 | 1.4390 | **0.58885** | 0.57646 | 0.61291 | 无退化 |
| p95 ms | 5.2034 | 4.0484 | **0.77804** | 0.72297 | 0.77337 | 无退化 |

L1 的响应字节两侧完全一致（15,627），无 >10% 可复现退化 → **P2.1 L1 PASS**。
每腿审计 delta 384、A/B 一致；B 每腿 `enqueued_total=126`、inline=0、pending=0。

### P2.2 标准 Issue 28 X1（c8 判据；c16 429 仅诊断）

命令：`./run_p2.sh x1`。每腿 `run.py --scenario X1 --concurrency-requests 100`，levels 1/4/8/16；
c1/c4 在 c8 之前执行，作为热身。`run.py` 因 c16 的已知默认配额 429 返回 1，这是记录的诊断，
不是 P2/P3 失败；c8 层每腿 429=0。

| 腿 | 侧 | c8 elapsed ms | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c16 429 |
|---|---|---:|---:|---:|---:|---:|---:|
| r1-a1 | A | 96 | 1,041.7 | 6.3082 | 16.0975 | 0 | 26 |
| r1-b1 | B | 41 | 2,439.0 | 3.0019 | 5.8546 | 0 | 45 |
| r1-b2 | B | 57 | 1,754.4 | 3.5118 | 7.8527 | 0 | 50 |
| r1-a2 | A | 36 | 2,777.8 | 2.1811 | 5.2536 | 0 | 17 |
| r2-a1 | A | 110 | 909.1 | 7.5902 | 15.8253 | 0 | 12 |
| r2-b1 | B | 41 | 2,439.0 | 2.1482 | 8.3183 | 0 | 47 |
| r2-b2 | B | 56 | 1,785.7 | 3.6794 | 10.1737 | 0 | 59 |
| r2-a2 | A | 51 | 1,960.8 | 2.6282 | 10.8317 | 0 | 33 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| c8 吞吐 qps | 1,501.2 | 2,112.4 | **1.4071** | 1.09791 | 1.4721 | 无退化 |

标准 X1 的 c8 吞吐 B 更快，未复现 R71b 的 c8-only 冷突发退化（0.84384）；c8 p50/p95 也更快
（B 2.15–3.68ms vs A 2.18–7.59ms），但本判据只取吞吐。c16 每腿 12–59 个 429，全部记录为诊断；
c8 每腿 429=0。→ **P2.2 PASS**。

### P2.3 持续 c8（关键判据）

命令：`./run_p2.sh c8`，驱动 `sustained_c8_driver.py`。每腿 8 个 keep-alive 客户端，
每个 500 个 pipeline 请求（隐式 open → execute(SELECT 1) → close；100 warmup + 400 timed），
200 请求/块、块间 1.2s 配额回填；吞吐只按 16 个计时块的墙钟计算。

| 腿 | 侧 | 吞吐 qps | p50 ms | p95 ms | 计时失败 | 失败详情 | B enqueued/inline/pending | settle | stop | seats/permits/unknown | errors |
|---|---|---:|---:|---:|---:|---|---|---:|---:|---:|---|
| r1-a1 | A | 1,405.9 | 3.2980 | 15.7668 | **1** | 502 @ block14 worker7 111.8ms | — | **timeout** | **1** | 1/1/1 | `outcome_unknown:1` |
| r1-b1 | B | 1,390.7 | 3.4032 | 15.0641 | **1** | 502 @ block15 worker5 77.8ms | 3,940/68/0 | settled | **1** | 0/1/1 | `outcome_unknown:1` |
| r1-b2 | B | 1,296.5 | 3.6096 | 18.9257 | 0 | — | 3,939/69/0 | settled | 0 | 0/0/0 | — |
| r1-a2 | A | 1,385.7 | 3.8881 | 14.8451 | 0 | — | — | settled | 0 | 0/0/0 | — |
| r2-a1 | A | 1,400.1 | 3.8471 | 13.7898 | 0 | — | — | settled | 0 | 0/0/0 | — |
| r2-b1 | B | 1,392.2 | 3.7617 | 15.3692 | 0 | — | 3,949/59/0 | settled | 0 | 0/0/0 | — |
| r2-b2 | B | 1,404.3 | 3.6179 | 14.3811 | 0 | — | 3,946/62/0 | settled | 0 | 0/0/0 | — |
| r2-a2 | A | 1,300.0 | 3.5399 | 16.9210 | **1** | 502 @ block14 worker1 103.8ms | — | settled | **1** | 0/1/1 | `outcome_unknown:1` |

`analyze_p2.py` 的合并口径（包含全部 8 腿，失败腿仍计入吞吐中位）：

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| 吞吐 qps | 1,392.9 | 1,391.4 | **0.99893** | 0.96257 | 1.03567 | 无 >10% 退化 |
| p50 ms | 3.6935 | 3.6138 | 0.97841 | 0.97588 | 0.99900 | 无 >10% 退化 |
| p95 ms | 15.3059 | 15.2166 | 0.99417 | 1.11035 | 0.96872 | 无 >10% 退化 |

只取 5 条干净腿的诊断口径：A（r1-a2 1,385.7、r2-a1 1,400.1）中位 1,392.9；
B（r1-b2 1,296.5、r2-b1 1,392.2、r2-b2 1,404.3）中位 1,392.2；B/A **0.99950**。
干净腿的 r1 配对为 1,296.5/1,385.7=0.93565；r2 只有 r2-a1 一条干净 A 腿，
对 B 中位 1,398.25 的比值为 0.99868。两种口径都没有 >10% 退化。

**但 P2.3 判 FAIL**：8 腿中 3 腿各有 1 个计时请求失败（HTTP 502），按冻结判据
“Any failed request ... makes the leg invalid” 这 3 腿无效；设计要求的 8 条有效腿不完整。
干净腿的 0.99950 只是诊断，不能替代无效腿，也不能据此把 R71b 的 FAIL 改判。

### 502 的机制证据

三处失败都是 pipeline 请求的 `http_502`（响应 284 bytes），平台把 `open` 判为
`outcome_unknown`。A 腿 r1-a1 运行中抓到的平台日志：

```
SQL outcome unknown: request_id=187f73ed... action=open stage=open_session
  database=local_default/demo/main session=089cab52... worker=worker-a
  rpc=none observation=none
Hrana internal gateway failure: path=/v1/sql/open status=502 code=outcome_unknown
```

失败后的度量：

| 腿 | seats | permits | unknown_held | orphan_sessions | worker-a sessions | errors_total |
|---|---:|---:|---:|---:|---:|---|
| r1-a1 A | 1 | 1 | 1 | 1（age 39.1s） | 1 | `outcome_unknown:1` |
| r1-b1 B | 0 | 1 | 1 | 0 | 0 | `outcome_unknown:1` |
| r2-a2 A | 0 | 1 | 1 | 0 | 0 | `outcome_unknown:1` |

- 失败发生在 **A（flag off）和 B（flag on）两侧**，不是 B 侧 close 异步派发引入的；
  B 的 `deferred_session_close` 计数器在失败腿仍然健康（enqueued 3,940、inline 68、
  failed/retried/unresolved=0、pending=0）。
- r1-a1 的 unknown open 留下了 orphan session（worker-a sessions=1、seats=1），
  30s settle 仍无法归零；三条失败腿的 `launcher-stop` 都是 `outcome=unclean`、
  `unresolved_operations=not_verified`、platform stage exit=1，因此 `docker stop` 退出码为 1。
- r1-b1 与 r2-a2 的 seat 已释放，但 `active_permits=1` 与 `unknown_held_permits=1` 保留；
  `wait_settled.py` 只检查 deferred/cleanup/worker sessions，不检查这两个字段，所以它们报告
  `settle: settled`；P3 检查器（`collect_invariants.py`）能发现。这是工具口径的一个已知缺口，
  已记录，不作为通过理由。

**与 R71b 冷突发的对比**：R71b 的 c8-only 冷突发为 B/A **0.84384**（两轮同向退化）。
R71c 持续 c8 的干净腿诊断值为 **0.99950**（全部腿口径 0.99893），没有复现 >10% 退化；
但本轮因 3/8 腿的 502 `outcome_unknown` 与 unclean stop 不能 PASS。**R71b 仍为 FAIL，
R71c 不改判它**：R71c 的结论是“持续 c8 没有复现冷突发的吞吐退化，但持续 c8 暴露了另一个
平台级 `open_session` 未知结果/许可泄漏问题；在该问题修复前，c8 无法得到干净的 A/B 判决”。

## P3：不变量

命令：`python3 collect_invariants.py .`

结果：**31/34 腿通过**。P1 2/2；F1 8/8；L1 8/8；标准 X1 8/8（c16 429 按预登记例外）；
持续 c8 5/8。失败腿与失败检查：

| 腿 | 失败检查 |
|---|---|
| c8/r1-a1（A） | `session_seats=1`、`active_permits=1`、worker-a sessions=1、settle timeout、clean stop proof（unclean/not_verified）、docker stop exit 1、`outcome_unknown:1` |
| c8/r1-b1（B） | `active_permits=1`、clean stop proof（unclean/not_verified）、docker stop exit 1、`outcome_unknown:1` |
| c8/r2-a2（A） | `active_permits=1`、clean stop proof（unclean/not_verified）、docker stop exit 1、`outcome_unknown:1` |

其余不变量：

- **deferred_session_close 方向**：所有 A 腿的 metrics-before/after 都没有该键；所有 B 腿都有。
  P1 B：`enqueued_total=600`、`inline_total=0`、pending=0、failed/retried/unresolved=0。
  F1/L1 B：每腿 `enqueued_total=126`、inline=0、pending=0。
  标准 X1 B：`enqueued_total=337–355`、pending=0、failed/retried/unresolved=0。
  持续 c8 B：干净腿 enqueued 3,939–3,949、inline 59–69、pending=0；失败腿 r1-b1 enqueued 3,940、
  inline 68、pending=0；failed/retried/unresolved 全 0。inline 是 per-database bound=4 的预期行为。
- **审计链连续性**：34/34 腿的 `verify_audit.py` 都通过（seq 连续、previous hash 链接、head 匹配）；
  失败腿的链也连续。审计条数：
  P1 A 3,600 / B 1,800（A:B=2.0，与请求比 1,200:600 一致）；
  F1/L1 每腿 384（126 请求 × 3 + 6 discovery），A/B 一致；
  标准 X1 1,091–1,185，等于 successful actions + c16 rate_limit refusals（逐腿 ±2）；
  持续 c8 12,022–12,024（4,008 请求 × 3；r1-a1 少 2 条，容差内）。
- **A/B 同工作量审计计数**：F1/L1/持续 c8 的分组全部相等（容差 2 条）；P1 按 2:1 请求比解释。
- **错误/拒绝**：除 3 条 c8 失败腿的 `outcome_unknown:1` 外，所有腿的 `errors_total` 与
  `rejection_reasons_total` 为空；标准 X1 仅 c16 有 `rate_limit`/`quota_exceeded`（逐腿 12–59），
  c1/c4/c8 每层 429=0；持续 c8 干净腿 0 失败、0 429。
- **干净停止**：31 腿 `launcher-stop.outcome=clean`、`unresolved_operations=false`、stop exit 0；
  3 条 c8 失败腿为 unclean/not_verified、stop exit 1，已保留证据。
- **seats/permits/worker sessions/pending_close**：31 腿全 0；3 条失败腿按上表保留 1 个
  seat/unknown permit/orphan session。

证据位置：`invariants.json`、每个腿的 `audit-verify.json`、`metrics-after.json`、
`settle.json`、`launcher-stop.json`、`audit-count-*.txt`、`harness/raw.jsonl`。

## 判决

| 判据 | 结果 | 关键数字 |
|---|---|---|
| P0.1 零对照 | **PASS** | 中位 1.02137，CI 0.93005–1.21135（30 triples；60 A 侧块上下文） |
| P0.2 阳性对照 | **PASS（引用）** | R71b 中位 1.09317 ≥1.08；本轮未重跑 |
| P1 主指标 | **PASS** | 中位 0.77194，CI 0.72116–0.83306；24/30 triples B 更快 |
| P2.1 F1 | **PASS** | p50 0.50208、p95 0.65638；两轮同向 |
| P2.1 L1 | **PASS** | p50 0.58885、p95 0.77804；两轮同向 |
| P2.2 标准 X1 c8 | **PASS** | 吞吐 1.4071（r1 1.09791、r2 1.4721）；c8 429=0 |
| P2.3 持续 c8 | **FAIL** | 3/8 腿各有 1 个 502 `outcome_unknown`；干净腿诊断 B/A 0.99950，但无效腿使设计不完整 |
| P3 不变量 | **FAIL** | 31/34 腿；3 条 c8 腿 outcome_unknown/unknown permit/unclean stop |
| **总判决** | **FAIL** | P2.3 与 P3 失败；候选不合入 |

**判决解释（必须与判决一起阅读）**：

1. 本轮 **没有** 复现 R71b 的 c8-only 冷突发吞吐退化：持续 c8 的干净腿 B/A=0.99950，
   全部腿口径 0.99893；标准 X1 c8 也是 B 更快（1.4071）。这与 R71b 的 0.84384 相反。
   但 R71b 的 FAIL 是历史判决，本轮**不改判**；R71c 自身也因 P2.3/P3 失败。
2. 本轮 FAIL 的直接原因是 **平台级 `open_session` 结果未知**：3/8 腿各 1 个 502
   `outcome_unknown`，A、B 两侧都出现；A 腿 r1-a1 还留下 orphan session 与 seats=1，
   A 腿 r2-a2/B 腿 r1-b1 留下 `active_permits=1`/`unknown_held_permits=1`，三条腿都无法
   clean stop。这不是 B 侧 deferred close 的吞吐退化，但它是 P3 的硬失败，也让持续 c8
   无法给出干净的 A/B 结论。
3. **下一步**：先修复 `open_session` 未知结果/许可泄漏（让 worker 在执行前拒绝时携带
   rejected/not-executed 证据，或让 orphan session 可被恢复清理），再用同一预登记设计重跑
   持续 c8；在此之前不应把候选合入，也不应把本轮干净腿的 0.99950 当作 c8 通过。
4. 本轮未修改生产代码、未推送远端、未 amend；候选仍留在分支 `perf/r71-deferred-session-close`。

## 证据、脚本与复现

| 文件 | 说明 |
|---|---|
| `criteria.txt` / `criteria-freeze.txt` | 冻结判据（SHA256 `d35d4687…`）与冻结时间/候选 SHA |
| `run_interleaved.sh` / `interleaved_driver.py` / `analyze_interleaved.py` | P1 双栈 1×30 triples 与 P0.1 分析 |
| `run_p2.sh` / `analyze_p2.py` | F1/L1/标准 X1/持续 c8 的腿驱动与分析 |
| `sustained_c8_driver.py` | 持续 c8 驱动（8 客户端、100 warmup + 400 timed、200 请求/块、1.2s 回填） |
| `collect_invariants.py` / `verify_audit.py` | P3 不变量、审计链连续性、X1 c16 例外 |
| `wait_settled.py` / `snap_metrics.py` / `gate2_common.py` | settle 与 metrics 辅助 |
| `analysis-p1.json` / `analysis-p2.json` / `invariants.json` / `summary.json` | 机器可读结果 |
| `legs/p1/{a,b}/…` | P1 两栈 metrics/audit/settle/launcher-stop |
| `legs/p2/{f1,l1,x1,c8}/<leg>/…` | 32 条 P2 腿的 harness/metrics/audit/stop 证据 |
| `diagnostics/smoke/` | 脚本验证用的 smoke（不计入判据） |
| `volumes.txt` | 全部 `peri-r71-ab3-*` 卷清单；确认无残留 campaign 容器 |

具体卷与 run-dir：

- P1：`peri-r71-ab3-p1-a-data`、`peri-r71-ab3-p1-b-data`；
  run-dir `/tmp/peri-r71-ab3/p1/{a,b}`；端口 21200/21201。
- F1：`peri-r71-ab3-p2-f1-<leg>-data`（8 腿）；run-dir `/tmp/peri-r71-ab3/p2/f1/<leg>`；
  端口 21210–21217。
- L1：`peri-r71-ab3-p2-l1-<leg>-data`（8 腿）；run-dir `/tmp/peri-r71-ab3/p2/l1/<leg>`；
  端口 21220–21227。
- 标准 X1：`peri-r71-ab3-p2-x1-<leg>-data`（8 腿）；run-dir `/tmp/peri-r71-ab3/p2/x1/<leg>`；
  端口 21230–21237。
- 持续 c8：`peri-r71-ab3-p2-c8-<leg>-data`（8 腿）；run-dir `/tmp/peri-r71-ab3/p2/c8/<leg>`；
  端口 21240–21247。3 条失败腿的卷（r1-a1、r1-b1、r2-a2）全部保留。
- smoke：`peri-r71-ab3-smoke-*` 卷保留，容器已删除；不计入任何判据。

真实命令与关键输出：

```bash
# 候选重建与 SHA 核对
cd /tmp/r71-ab-src/prototypes/v1/platform
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -o /tmp/r71-ab3/bin/peri-platform-ab3 ./cmd/peri-platform
shasum -a 256 /tmp/r71-ab3/bin/peri-platform-ab3
# 3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014

# P1
cd docs/performance-review/2026-11-05-r71-deferred-close-ab3
./run_interleaved.sh p1
python3 analyze_interleaved.py --campaign p1
# zero control 1.02137 CI [0.93005,1.21135] pass=True
# effect 0.77194 CI [0.72116,0.83306] pass=True

# P2
./run_p2.sh f1
./run_p2.sh l1
./run_p2.sh x1
./run_p2.sh c8
python3 analyze_p2.py .
python3 collect_invariants.py .
python3 make_summary.py .
# f1 p50 0.50208 p95 0.65638 ok
# l1 p50 0.58885 p95 0.77804 ok
# x1 throughput 1.4071 r1 1.09791 r2 1.4721 ok
# c8 throughput 0.99893 r1 0.96257 r2 1.03567 ok (but 3 invalid legs)
# VERDICT: FAIL
```

环境事实：镜像 digest `sha256:8a8ba43b…`；worker SHA `b17a0708…`；Go `go1.27.3 darwin/arm64`；
候选 platform SHA `3059c47a…`；`durability_source=audit_file_v1`；默认配额未放宽；
fixture 每栈 `threads=201 messages=8625 chars=20177069 focus=8625`；无观测器。
端口：P1 21200–21201；F1 21210–21217；L1 21220–21227；标准 X1 21230–21237；持续 c8 21240–21247。
卷：所有 `peri-r71-ab3-*` 卷保留（含 3 条失败 c8 腿的卷）；容器已按脚本停止并删除；
R71b 的 39 个 `peri-r71-ab2-*` 卷未触碰；未 prune、未重启 Docker。

## 未验证项与剩余风险

1. **持续 c8 没有干净结论**：3/8 腿因 502 `outcome_unknown` 无效；干净腿的 0.99950 只是诊断，
   不能当作 P2.3 PASS，也不能据此推翻 R71b。需要先修复 `open_session` 未知结果/许可泄漏后重跑。
2. **worker 侧根因未插桩**：平台日志只给出 `rpc=none observation=none` 的分类；本轮不改生产代码、
   不开观测器，无法从 worker 内部证明是执行前拒绝无证据、RPC 丢失还是 orphan session 清理缺口。
   r1-a1 的 orphan session（worker-a sessions=1、seats=1、age 39s）是最强的现场证据。
3. **settle 工具口径缺口**：`wait_settled.py` 不检查 `active_permits`/`unknown_held_permits`，
   所以 r1-b1/r2-a2 报告 settled 但 P3 检查器判失败；后续应把这两个字段加入 settle 条件。
4. **未做崩溃/掉电恢复**：本轮没有重复 R37 的 crash-recovery E2E；未知 open 后的重启/隔离行为
   未测。
5. **未跑可选 c8-only 冷突发诊断**：R71b 的 0.84384 作为历史证据引用；本轮没有重跑该驱动，
   因为它不是判据，且已因 P2.3/P3 失败。
6. **标准 X1 的 c16 429**：每腿 12–59 个，已知默认配额产物；c8 每腿 0 个 429。未测 c16/c32/c64
   的候选行为。
7. **宿主噪声**：F1/L1/X1/c8 的绝对数字只在本窗口可比；c8 的单腿吞吐有 ±5–10% 波动，
   但 3 条失败腿的吞吐（1,300–1,406 qps）与干净腿（1,297–1,404 qps）重叠，失败不是“慢腿”造成的。
8. **未推送远端**；本轮只有本地证据与待提交的 `docs(perf)` commit。
