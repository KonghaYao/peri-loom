# R71d 测量轮：在含结算修复的二进制上重跑 R71c 冻结设计

日期：2026-11-05
分支：`perf/r71-deferred-session-close`
候选：合并提交 `af66931`（R71 实现 `02a05a7`、修复 `ac5e0c7`，合入 `main` `b1e8ae9` / PR #35）
性质：**预登记测量轮（R71d）**。只构建、启动隔离栈、测量、留证；不改产品代码、不推送远端。
判据在第一腿之前冻结于 `criteria.txt`（SHA256
`4a04203e1f9e4e9cf2ad598fd1a2829cbf779b00f5615970eaadf6c86fb2fd29`；冻结时间
`2026-11-05T06:32:23Z`，见 `criteria-freeze.txt`）。

本轮目标：R71c 的 P1/P2 已通过，但 3/8 条持续 c8 腿各出现 1 个 502 `outcome_unknown`，
根因是 `main` 上本地结算队列满被误判（PR #35 修复）。本轮在合入该修复的同一候选二进制上，
按 R71c 的冻结设计重跑，判定 R71 能否进入 Gate 3。

## 判据与口径

全文见 `criteria.txt`。要点：

- **P0.1 零对照**：30 triples（与 P1 同一批），`A2/A1` 中位在 1.00±0.05 且 bootstrap
  95% CI 覆盖 1.0。R71c 的 30/60 triples 歧义在本轮第一腿前消解为：**P0 与 P1 都在
  30 triples 上评估**，60 个 A 块仅作上下文。
- **P0.2 阳性对照**：引用 R71b 历史窗口（探针 `6517583e…`，1 轮 × 30 triples，中位
  1.09317 ≥ 1.08），**本轮不重跑、不混算**；选择已在 criteria 中写死。
- **P1**：`B / mean(A1,A2)` 中位 ≤0.90、bootstrap 95% CI 上界 <1.0、且 30 triples 中
  至少 20 个（2/3）B 更快。
- **F1/L1**：合并 p50 与 p95 的 B/A 均 <1.10；L1 响应字节两侧一致。
- **标准 X1**：c8 吞吐 B/A >0.90；c16 的 429 只作诊断。
- **持续 c8**：吞吐 B/A >0.90 且每腿无错误；p50/p95 只作诊断。
- **P3**：每腿 0 个 `outcome_unknown`/502/429（c16 诊断除外）、结算后
  `active_permits=0`、`unknown_held_permits=0`、`session_seats=0`、worker sessions=0、
  审计链连续、`launcher-stop` clean、两侧审计条目数与请求数一致。
- **Gate 0**：每腿 `/v1/metrics` 必须出现 `operation_settle`；若发生队列满，必须落在
  `retry_total`/`retry_success_total` 或 `refused_total{reason}`，不得再出现队列满导致的 502。

## 环境

| 项 | 值 |
|---|---|
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（10 vCPU，VM 内存 12.6 GB） |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| worker（两侧相同） | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 候选 platform | 从合并 `af66931` 树重建：`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`；SHA256 `a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca` |
| launcher | 分支 `dev.py`，SHA256 `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129`，两侧挂载相同 |
| Go | `go1.27.3 darwin/arm64` |
| 耐久模式 | `audit_file_v1`（34/34 腿实测一致） |
| 配额 | 默认，未放宽；每腿从 durable state 实读：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304`（34/34 腿相同） |
| fixture | 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 未开启 |
| 端口 | P1 21300–21301；F1 21310–21317；L1 21320–21327；标准 X1 21330–21337；持续 c8 21340–21347 |
| 卷 | 全新 `peri-r71-ab4-*` 卷；本轮 37 个（2 P1 + 32 P2 + 3 smoke），全部保留 |
| run-dir | `/tmp/peri-r71-ab4/{p1,p2}/…`；连接文件只在 `/tmp`、`chmod 600`，未入库、未打印 |

合并说明：`git merge main` 在 `gateway.go`（`deferredClose` 与 `settle` 字段）和
`metrics.go`（`deferred_session_close` 与 `operation_settle` 键）各有一处冲突，均按
「两侧都保留」解决；`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...`、`gofmt`
干净，未跑完整 `go test ./...`。

未触碰生产容器 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker、未 prune、
未删除任何卷/容器/失败证据。

## P0：功效对照

### P0.1 零对照（A1 vs A2，同栈同时段，30 triples）

| 口径 | 中位比值 | bootstrap 95% CI | 判定 |
|---|---:|---|---|
| 合并 30 triples | **1.016016** | 0.880791–1.163224 | **PASS**（在 1.00±0.05 内，CI 覆盖 1.0） |

A1 块中位 4.5475 ms、A2 块中位 4.4283 ms；60 个 A 侧块（30 A1 + 30 A2）作为上下文。
CI 宽度与 R71c（30 样本）同量级。

### P0.2 阳性对照

引用 R71b：探针二进制 `6517583e751e2dd936c44fcbd80dd77d13b10757503a0917a7f83bad16096916`、
B 侧 `PERI_R71_CLOSE_DELAY_US=100`、1 轮 × 30 triples，`B / mean(A1,A2)` 中位
**1.09317 ≥ 1.08 → PASS**。本轮未重跑，未与候选 triples 混算。

## P1：短版交错主指标（30 triples）

命令：

```bash
cd docs/performance-review/2026-11-05-r71d-deferred-close-ab4
./run_interleaved.sh p1
python3 analyze_interleaved.py --campaign p1
```

运行结果：`valid_triples=30/30`、`void_triples=0`、`failed_blocks=0`、`blocks=90`、
measured requests 1800（A 1,200、B 600），另有 2 个不计入统计的 warmup 请求（每栈 1 个）。
两栈 `driver_exit=0`、`settle_exit=0`、`stop_exit=0`。

| 块 | n | 中位 ms | 均值 ms | p95 ms | min | max |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 30 | 4.5475 | 4.3856 | 5.5455 | 2.2284 | 6.0898 |
| A2 | 30 | 4.4283 | 4.3649 | 5.3762 | 2.3783 | 5.7673 |
| B | 30 | 3.4025 | 3.2790 | 4.3782 | 1.6022 | 4.5996 |

- `B / mean(A1,A2)` 中位 **0.765771**（CI **0.665629–0.870177**，上界 <1.0），≤0.90 → **P1 PASS**。
- 方向：30 个 triples 中 **26 个 B 更快**、4 个不更快（≥20 的门槛满足）。
- 配对差值 `B − mean(A1,A2)` 中位 **−1.035947 ms**（CI −1.596626–−0.594858）。
- 块顺序分布（seed 20261105）：`ABA` 10、`AAB` 10、`BAA` 10。
- 动作桶（P1 窗口增量，均值）：close **1,080.68µs → 175.72µs（−83.7%）**；
  open 1,297.09µs → 1,152.55µs（−11.1%）；statement 292.21µs → 230.57µs（−21.1%）。

逐 triple 原始比值（完整原始数据见 `legs/p1/driver/triples.jsonl` 与 `raw.jsonl`）：

| triple | order | A1 ms | A2 ms | B ms | A2/A1 | B/mean(A1,A2) | B faster |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | ABA | 4.6559 | 5.7673 | 3.3397 | 1.23870 | 0.64083 | yes |
| 2 | AAB | 4.9650 | 5.0064 | 2.9893 | 1.00834 | 0.59958 | yes |
| 3 | BAA | 5.5285 | 3.9146 | 2.8703 | 0.70808 | 0.60793 | yes |
| 4 | ABA | 2.8380 | 4.0655 | 4.3773 | 1.43253 | 1.26815 | no |
| 5 | BAA | 4.3731 | 4.4767 | 2.6428 | 1.02369 | 0.59725 | yes |
| 6 | AAB | 4.1779 | 5.0693 | 3.8782 | 1.21337 | 0.83877 | yes |
| 7 | ABA | 5.5455 | 2.8219 | 3.8827 | 0.50886 | 0.92805 | yes |
| 8 | BAA | 4.2901 | 2.7586 | 3.7756 | 0.64302 | 1.07128 | no |
| 9 | ABA | 3.0262 | 4.1573 | 4.5996 | 1.37380 | 1.28060 | no |
| 10 | BAA | 2.2284 | 5.0723 | 3.5587 | 2.27614 | 0.97490 | yes |
| 11 | BAA | 4.3572 | 4.5476 | 2.2425 | 1.04369 | 0.50367 | yes |
| 12 | AAB | 4.1193 | 4.7963 | 3.7091 | 1.16435 | 0.83205 | yes |
| 13 | BAA | 5.1756 | 4.1780 | 2.1183 | 0.80725 | 0.45295 | yes |
| 14 | AAB | 4.5549 | 5.2932 | 3.3997 | 1.16210 | 0.69043 | yes |
| 15 | BAA | 3.7879 | 4.7553 | 1.8556 | 1.25539 | 0.43440 | yes |
| 16 | ABA | 5.2346 | 3.8759 | 3.4053 | 0.74043 | 0.74756 | yes |
| 17 | BAA | 4.5620 | 5.3762 | 1.6022 | 1.17848 | 0.32243 | yes |
| 18 | AAB | 5.2668 | 4.0235 | 3.5666 | 0.76394 | 0.76782 | yes |
| 19 | ABA | 3.2059 | 5.2027 | 3.2109 | 1.62284 | 0.76372 | yes |
| 20 | ABA | 4.9186 | 4.3798 | 3.6945 | 0.89046 | 0.79465 | yes |
| 21 | AAB | 3.9781 | 3.3660 | 3.8341 | 0.84614 | 1.04414 | no |
| 22 | ABA | 4.7813 | 4.3412 | 4.3782 | 0.90794 | 0.95986 | yes |
| 23 | ABA | 4.3569 | 3.7954 | 2.4072 | 0.87113 | 0.59056 | yes |
| 24 | AAB | 4.6530 | 4.3750 | 4.0698 | 0.94025 | 0.90158 | yes |
| 25 | AAB | 2.6232 | 4.2638 | 2.6515 | 1.62538 | 0.76999 | yes |
| 26 | AAB | 6.0898 | 4.6982 | 3.9860 | 0.77148 | 0.73897 | yes |
| 27 | ABA | 4.5401 | 4.9519 | 1.6097 | 1.09071 | 0.33916 | yes |
| 28 | BAA | 4.9200 | 4.4932 | 3.2896 | 0.91326 | 0.69892 | yes |
| 29 | BAA | 4.6272 | 2.3783 | 3.2969 | 0.51398 | 0.94124 | yes |
| 30 | AAB | 4.1857 | 4.7455 | 4.1278 | 1.13374 | 0.92434 | yes |

## P2：回归门

每个 P2 腿都是新卷、独立容器/端口、同 fixture、默认配额、无观测器。F1/L1 每腿 125 个计时请求
（2 轮 × A/B/B/A，每侧 500 请求）；标准 X1 每腿 levels 1/4/8/16 各 100 请求；持续 c8 每腿
8 客户端 × 500 请求（100 warmup + 400 timed）。**逐轮交替顺序**：第 1 轮 A/B/B/A，
第 2 轮 B/A/A/B。

### P2.1 F1.select-1 json-pipeline

命令：`./run_p2.sh f1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`，`python3 analyze_p2.py .`

| 腿 | 侧 | p50 ms | p95 ms | 样本 | 状态 |
|---|---|---:|---:|---:|---|
| r1-a1 | A | 1.6153 | 3.3630 | 125 | 200×125 |
| r1-b1 | B | 0.6966 | 4.7483 | 125 | 200×125 |
| r1-b2 | B | 0.6623 | 3.1375 | 125 | 200×125 |
| r1-a2 | A | 0.8850 | 5.4580 | 125 | 200×125 |
| r2-b1 | B | 0.6898 | 4.2101 | 125 | 200×125 |
| r2-a1 | A | 1.8951 | 4.8820 | 125 | 200×125 |
| r2-a2 | A | 1.7995 | 6.3186 | 125 | 200×125 |
| r2-b2 | B | 0.8117 | 4.2903 | 125 | 200×125 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 1.7074 | 0.6932 | **0.40600** | 0.54349 | 0.40640 | **PASS**（<1.10） |
| p95 ms | 5.1700 | 4.2502 | **0.82209** | 0.89398 | 0.75892 | **PASS**（<1.10） |

请求字节 102（8 腿一致）、响应字节众数 312（8 腿一致）、rows=1。每腿审计 delta 384
（126 请求 × 3 + 6 discovery），A/B 一致；B 每腿 `enqueued_total=126`、inline=0、pending=0。

### P2.1 L1.list-20 json-pipeline

命令：`./run_p2.sh l1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`

| 腿 | 侧 | p50 ms | p95 ms | 样本 | 状态 |
|---|---|---:|---:|---:|---|
| r1-a1 | A | 2.4062 | 6.0677 | 125 | 200×125 |
| r1-b1 | B | 2.1088 | 5.2320 | 125 | 200×125 |
| r1-b2 | B | 1.4038 | 3.8183 | 125 | 200×125 |
| r1-a2 | A | 2.5922 | 5.4323 | 125 | 200×125 |
| r2-b1 | B | 1.5724 | 4.3677 | 125 | 200×125 |
| r2-a1 | A | 2.5950 | 6.6739 | 125 | 200×125 |
| r2-a2 | A | 2.1843 | 4.7367 | 125 | 200×125 |
| r2-b2 | B | 1.4948 | 6.1041 | 125 | 200×125 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 2.4992 | 1.5336 | **0.61364** | 0.70274 | 0.64177 | **PASS**（<1.10） |
| p95 ms | 5.7500 | 4.7999 | **0.83476** | 0.78698 | 0.91773 | **PASS**（<1.10） |

响应字节：请求字节 386（8 腿一致）、rows=40、rows_read=201（每腿全部计时样本），
响应字节众数 **15,627（8 腿一致）**。原始样本的 15,623–15,638 波动来自
`query_duration_ms` 数字位数，不是行内容差异；每腿审计 delta 384，A/B 一致；
B 每腿 `enqueued_total=126`、inline=0、pending=0。

### P2.2 标准 Issue 28 X1（c8 判据；c16 429 仅诊断）

命令：`./run_p2.sh x1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`。每腿
`run.py --scenario X1 --concurrency-requests 100`，levels 1/4/8/16；c1/c4 在 c8 之前执行。
`run.py` 因 c16 的已知默认配额 429 返回 1，这是记录的诊断，不是 P2/P3 失败；c8 每腿 429=0。

| 腿 | 侧 | c8 elapsed ms | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c16 429 |
|---|---|---:|---:|---:|---:|---:|---:|
| r1-a1 | A | — | 2,381.0 | 2.32 | 9.69 | 0 | 20 |
| r1-b1 | B | — | 2,439.0 | 2.27 | 6.24 | 0 | 46 |
| r1-b2 | B | — | 2,272.7 | 2.77 | 7.07 | 0 | 35 |
| r1-a2 | A | — | 1,886.8 | 2.51 | 9.32 | 0 | 39 |
| r2-b1 | B | — | 1,219.5 | 6.01 | 9.97 | 0 | 46 |
| r2-a1 | A | — | 1,408.5 | 3.77 | 14.23 | 0 | 20 |
| r2-a2 | A | — | 1,851.9 | 2.85 | 7.66 | 0 | 37 |
| r2-b2 | B | — | 1,886.8 | 3.78 | 8.72 | 0 | 48 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| c8 吞吐 qps | 1,869.3 | 2,079.8 | **1.11257** | 1.10404 | 0.95277 | **PASS**（>0.90） |

c8 吞吐合并值高于 0.90；r2 内 B 略低于 A（0.95277），但判据只要求合并 B/A >0.90。
逐腿吞吐波动较大（1,219–2,439 qps），r2-b1 是唯一的低值腿，未触发判据。c16 每腿 20–48 个
429，全部记录为诊断，c8 每腿 0 个。

### P2.3 持续 c8（关键判据）

命令：`./run_p2.sh c8 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`，驱动
`sustained_c8_driver.py`。每腿 8 个 keep-alive 客户端，每个 500 个 pipeline 请求
（隐式 open → execute(SELECT 1) → close；100 warmup + 400 timed），200 请求/块、
块间 1.2s 配额回填；吞吐只按 16 个计时块的墙钟计算。

| 腿 | 侧 | 吞吐 qps | p50 ms | p95 ms | 计时失败 | 校准/warmup 失败 | B enqueued/inline/pending | 状态 |
|---|---|---:|---:|---:|---:|---:|---|---|
| r1-a1 | A | 1,295.5 | 3.6476 | 17.1467 | 0 | 0/0 | — | settled/clean |
| r1-b1 | B | 1,383.0 | 3.2040 | 15.5812 | 0 | 0/0 | 3,868/140/0 | settled/clean |
| r1-b2 | B | 1,227.8 | 3.6166 | 18.3419 | 0 | 0/0 | 3,884/124/0 | settled/clean |
| r1-a2 | A | 1,362.3 | 3.3410 | 15.8761 | 0 | 0/0 | — | settled/clean |
| r2-b1 | B | 1,488.9 | 3.1249 | 14.4049 | 0 | 0/0 | 3,894/114/0 | settled/clean |
| r2-a1 | A | 1,511.8 | 3.1876 | 13.5571 | 0 | 0/0 | — | settled/clean |
| r2-a2 | A | 1,446.5 | 3.2772 | 14.5356 | 0 | 0/0 | — | settled/clean |
| r2-b2 | B | 1,562.6 | 2.8286 | 14.0817 | 0 | 0/0 | 3,908/100/0 | settled/clean |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| 吞吐 qps | 1,404.4 | 1,435.9 | **1.02246** | 0.98235 | 1.03148 | **PASS**（>0.90，8/8 腿无错误） |
| p50 ms（诊断） | 3.3091 | 3.1644 | 0.95629 | 0.97596 | 0.92091 | — |
| p95 ms（诊断） | 15.2058 | 14.9931 | 0.98601 | 1.02726 | 1.01402 | — |

**R71c 的 502 未复现**：8/8 腿 `driver_exit=0`，校准、warmup、计时请求失败均为 0，
`worker_errors` 为空。R71b 的 c8 冷突发退化（0.84384）同样未复现（本设计为持续 c8）。
每腿 B 的 `inline_total` 为 100–140（超出异步边界的 close 在回复前内联关闭），
failed/retried/unresolved/pending 全 0。

## P3：不变量

命令：`python3 collect_invariants.py .`

结果：**34/34 腿通过**（P1 2/2；F1 8/8；L1 8/8；标准 X1 8/8；持续 c8 8/8）。

- **零异常**：34 腿的 `errors_total` 没有 `outcome_unknown`/502/503/`session_moved`/
  `audit_unavailable`；非 X1 腿没有任何错误或拒绝计数；X1 腿只有 c16 的
  `quota_exceeded`/`rate_limit`（诊断，允许）。
- **资源归零**：每腿结算后 `active_permits=0`、`unknown_held_permits=0`、
  `session_seats=0`、worker sessions=0、worker `pending_close=0`、cleanup ledger
  `pending_close=0`。新增的 settle 校验（`wait_settled.py` 现同时要求这三项）与 P3 一致。
- **审计链**：每腿 `chain_ok=true`、head 匹配 tail；条目数分组一致：
  F1/L1 每腿 384（A/B 相同）；持续 c8 每腿 12,024（A/B 相同）；
  P1 A 3,603 / B 1,803，比值 1.9983（请求比 2.0）；X1 的审计增量与成功动作数可解释。
- **干净停止**：34/34 `launcher-stop` 为 `outcome=clean`、`unresolved_operations=false`，
  `docker stop` 退出码 0。
- **方向键**：A 腿 metrics 均无 `deferred_session_close`；B 腿均存在且
  `enqueued_total>0`、failed/retried/unresolved=0、pending=0。

B 腿 `deferred_session_close` 窗口增量：

| 腿组 | enqueued | inline | failed | retried | unresolved | pending | pending_max |
|---|---:|---:|---:|---:|---:|---:|---:|
| P1 b | 601 | 0 | 0 | 0 | 0 | 0 | 2 |
| F1 每腿 | 126 | 0 | 0 | 0 | 0 | 0 | 1 |
| L1 每腿 | 126 | 0 | 0 | 0 | 0 | 0 | 1–2 |
| X1 每腿 | 341–348 | 5–19 | 0 | 0 | 0 | 0 | 4 |
| c8 每腿 | 3,868–3,908 | 100–140 | 0 | 0 | 0 | 0 | 4 |

平台日志：34 腿的 `container.txt` 中 `outcome unknown` 与 ` 502` 命中数均为 **0**，
无 ERROR 级行。

## Gate 0：结算修复机制核验

命令：`python3 check_gate0.py .`

- 34/34 腿的 `/v1/metrics` 都包含 `operation_settle`（`retry_budget_ms=300`，
  `refusal_values` 含 `queue_full`）。
- 本窗口内 `retry_total=0`、`retry_success_total=0`、`refused_total={}`、
  `exhausted_total=0`：没有发生本地结算队列满，因此没有需要归因的队列满事件。
- 全窗口 `outcome_unknown=0`；非 X1 腿无任何错误；X1 的 `quota_exceeded` 只对应 c16
  诊断层的限流（按腿序 r1-a1/r1-b1/r1-b2/r1-a2/r2-b1/r2-a1/r2-a2/r2-b2 =
  35/74/50/52/60/36/52/68）。
- **Gate 0 PASS**：修复以可观测账户形式存在，且 R71c 的队列满→502 路径在本窗口未出现。

## 失败腿与异常腿

- **测量腿：无**。34/34 腿 driver 退出码符合预期、settle=0、stop=0；无
  `outcome_unknown`、审计不可用或启动失败，因此本轮没有换卷重跑的腿。
- **诊断性 smoke（不计入任何判据）**：首次两次 smoke 都在启动后、驱动前因工具读取
  `state.json` 的路径/载荷假设错误而中止（第一次路径应为 `/data/peri/platform/state.json`，
  第二次 payload 已是对象而非字符串）；两次容器已删除、卷保留
  （`peri-r71-ab4-smoke-f1-r1-a1-data`、`…-a1b-data`）。修正后第三次 smoke 通过，
  未使用任何测量卷。
- **工具修正（按任务要求）**：`wait_settled.py` 的 settled 条件加入
  `active_permits`、`unknown_held_permits` 与 `session_seats`，保留原有
  deferred/cleanup/worker 检查；因此「settled」不再掩盖 R71c 中
  `active_permits=1` 的泄漏。
- **已知非判据项**：X1 c16 的 429（每腿 20–48）是默认配额产物，按判据只作诊断；
  标准 X1 的 `run.py` 因此每腿退出码 1，已记录，不构成失败。

## 总判决

| 判据 | 结果 | 关键数字 |
|---|---|---|
| P0.1 零对照 | **PASS** | 1.016016，CI 0.880791–1.163224 |
| P0.2 阳性对照（引用） | **PASS** | 1.09317 ≥ 1.08 |
| P1 主指标 | **PASS** | 0.765771，CI 上界 0.870177，26/30 同向 |
| F1 p50/p95 | **PASS** | 0.40600 / 0.82209 |
| L1 p50/p95 | **PASS** | 0.61364 / 0.83476；响应字节众数 15,627 两侧一致 |
| 标准 X1 c8 吞吐 | **PASS** | 1.11257 |
| 持续 c8 吞吐 | **PASS** | 1.02246，8/8 腿无错误 |
| P3 不变量 | **PASS** | 34/34 腿 |
| Gate 0 结算修复 | **PASS** | 34/34 腿有 `operation_settle`；0 个队列满 502 |

**总判决：PASS。候选可进入 Gate 3。** 本轮不改判 R71 的实现，也不改判 R71b/R71c 的历史
结论；它只说明：在含 PR #35 修复的合并二进制上，同一冻结设计的全部预登记判据首次同时通过。

## 复跑命令与证据位置

```bash
# 候选重建与 SHA 核对
cd prototypes/v1/platform
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -o /tmp/peri-r71-ab4/bin/peri-platform-ab4 ./cmd/peri-platform
shasum -a 256 /tmp/peri-r71-ab4/bin/peri-platform-ab4
# a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca

# P1
cd docs/performance-review/2026-11-05-r71d-deferred-close-ab4
./run_interleaved.sh p1
python3 analyze_interleaved.py --campaign p1

# P2（逐轮交替：r1 A/B/B/A，r2 B/A/A/B）
./run_p2.sh f1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2
./run_p2.sh l1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2
./run_p2.sh x1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2
./run_p2.sh c8 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2
python3 analyze_p2.py .
python3 collect_invariants.py .
python3 check_gate0.py .
python3 make_summary.py .
# f1 p50 0.40600 p95 0.82209 ok
# l1 p50 0.61364 p95 0.83476 ok
# x1 throughput 1.11257 ok
# c8 throughput 1.02246 ok (8/8 legs clean)
# P3 34/34; gate0 34 legs; VERDICT: PASS
```

证据目录：`docs/performance-review/2026-11-05-r71d-deferred-close-ab4/`
（`criteria.txt`、`criteria-freeze.txt`、`environment.txt`、`volumes.txt`、
逐腿 `metrics-before/after.json`、`harness/raw.jsonl`、`harness/summary.json`、
`audit-verify.json`、`audit-count-*.txt`、`launcher-stop.json`、`container.txt`、
`analysis-p1.json`、`analysis-p2.json`、`invariants.json`、`gate0.json`、`summary.json`）。
卷：37 个 `peri-r71-ab4-*` 全部保留；容器已按脚本停止并删除；R71b/R71c 的卷未触碰。

## 未验证项与剩余风险

1. **未做崩溃/掉电恢复**：本轮不重复 R37 的 crash-recovery E2E；R71 的异步 close 在崩溃时
   丢失尾记录的隔离行为仍按既有 fail-closed 规则，未在本轮重测。
2. **未开启观测器**：按 A/B 协议要求关闭 phase/durability 观测器；close 异步派发的内部
   排队/失败路径只有 metrics 计数器证据。
3. **c8 绝对数字只在本窗口可比**：8 条腿的吞吐跨度 1,228–1,563 qps，方向按合并中位判定；
   宿主噪声仍足以让单腿相差 10–20%。
4. **标准 X1 的 r2-b1 是低值腿**（1,219.5 qps，p50 6.01ms），合并值仍 >0.90；未据单腿改判。
5. **未测 c32/c64**：默认配额下维持原协议，不扩并发档。
6. **未推送远端**；本轮只有本地证据与待提交的 `docs(perf)` commit。
