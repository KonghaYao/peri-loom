# R72c：L1 c1 p95 非劣定向复测（8 对 × 125 计时请求）

判决：**PASS**（冻结判据 `criteria-r72c.json`，SHA256 `5d89e33b72c64afd4999d494b69695060ae809d401653f608793733bca27a9a4`）

本campaign只复测 R72 唯一未达标项 C4（L1 c1 p95 B/A），不重新裁决 R72 的任何其他判据。
R72 的冻结判决保持原样（C4 FAIL）；本campaign是独立命名的补充测量，不构成对 R72 判决的改写。

## 1. 判据与冻结

| 项 | 值 |
|---|---|
| 判据文件 | `criteria-r72c.json` |
| SHA256 | `5d89e33b72c64afd4999d494b69695060ae809d401653f608793733bca27a9a4` |
| 冻结时间（UTC） | `2026-11-05T08:32:12Z` |
| 冻结校验 | `criteria-r72c-freeze.txt` 中的 SHA 与文件实测一致（True） |
| 冻结时是否存在 R72c 栈 | 否。第一条腿于 `2026-11-05T08:32:39Z` 启动，晚于冻结时间 |

冻结规则：

- 主判据：8 对「每腿 p95 的比值（B/A）」的中位数 < 1.10。
- 辅助判据：8 对 p50 比值中位数 < 1.10。
- PASS 还要求 16 腿全部有效且不变量正常。
- p95 定义：125 个计时样本升序排列后取 `min(n-1, int(n*95/100))`，即索引 118（与 R72 / Issue 28 harness 相同）。
- 无效腿（429/quota、栈启动失败、fixture 不符、证据不可读等）允许同一对同臂换新卷重跑一次；
  本次 16 腿全部有效，**未发生任何重跑或替换**。
- 未修改配额、未加 pacing、未修改样本量、未修改判据。

## 2. 环境与血缘

| 项 | 值 |
|---|---|
| 候选分支 / commit | `perf/r72-deferred-close-default` / `56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6` |
| 基线 commit | `ee1da7d` |
| 平台二进制 | `/tmp/peri-r72-ab/bin/peri-platform-r72`，SHA256 `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` |
| worker | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| 启动器 | `prototypes/v1/platform/dev.py`，SHA256 `80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2` |
| 运行器 | R72 的 `run_p2.sh`，只读复用，SHA256 `fd02651bdf071c535ad77df1cecfb684f57ef0323694597885dc699f2f53928d` |
| harness | `scripts/perf-reliability/issue28/run.py`，SHA256 `499ad87f0d6264f8c387209fcc832204274058119604cd6fea0febacaec36676` |
| fixture 脚本 | `docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py`，SHA256 `092c2ad78024084cc9364de6b7710276e2fd04a03a552cc61dd1699f86626210` |
| Docker | 29.4.0（OrbStack，Darwin arm64） |
| 网络 | `--network host`，localhost，无 TLS，每腿独立栈 |
| 观测器 | 关闭（镜像 ENV 与每腿 `docker run` 均未设置任何 observer 变量） |
| 配额 | 平台默认，未放松：`requests_per_second=500`、`max_sessions=16`、`max_concurrent=100`、`max_databases=8`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |
| 耐久模式 | `audit_file_v1`（每腿 metrics 读取） |
| 运行窗口 | `2026-11-05T08:32:39Z` – `2026-11-05T08:36:27Z` |
| 宿主负载 | campaign 开始时 load average 2.00 / 1.99 / 2.02（未做额外控制，靠 8 对交替与中位数抵御噪声） |

血缘：

- R72 证据：`docs/performance-review/2026-11-05-r72-deferred-close-default-ab/`，判据 SHA `de7860a1…`，C4 合并 p95 B/A = 1.17471，两轮方向相反（1.26545 / 0.83121）。
- R72b 证据：`docs/performance-review/2026-11-05-r72b-l1-p95-retest/`，判据 SHA `0b2f2769…`，500 样本/腿时 B 臂耗尽默认 quota，ENVIRONMENT_BLOCKED，未产生主统计量。
- R72c 用 R72 完全相同的每腿负载（1 warmup + 125 timed L1.list-20 json-pipeline c1），只把对数从 4 对增加到 8 对。

## 3. 设计

- 8 对 A/B，共 16 腿；A=容器与平台进程环境显式 `PERI_DEFERRED_SESSION_CLOSE=0`，B=完全未设置（默认延迟关闭）。
- 对序模式：A,B,B,A,B,A,A,B（4 对 A-first、4 对 B-first，整体平衡）。
- 实际腿序：`p1-a p1-b p2-b p2-a p3-b p3-a p4-a p4-b p5-b p5-a p6-a p6-b p7-a p7-b p8-b p8-a`。
- 端口 21600–21615；每腿全新卷 `peri-r72c-l1-p2-l1-<leg>-data`；容器用后即删，卷保留作证据。
- 每腿命令（连接文件为私有 /tmp 文件，权限 0600，未在本文或日志中输出）：
  `python3 scripts/perf-reliability/issue28/run.py --connection-file <private> --out <leg>/harness --container <leg container> --scenario L1 --protocol json-pipeline --rounds 1 --warmup 1 --samples 125`
- 125 timed 远低于 500 req/s 默认配额；R72 的 8 条 L1 腿在同样负载下零 429。本次同样零 429。

## 4. 逐腿结果

| leg | pair | arm | port | volume | timed | p50 ms | p95 ms | p99 ms | errors | audit delta | valid |
|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| p1-a | p1 | A explicit-0 | 21600 | peri-r72c-l1-p2-l1-p1-a-data | 125 | 2.6492 | 5.8411 | 7.1135 | 0 | 384 | yes |
| p1-b | p1 | B unset-default | 21601 | peri-r72c-l1-p2-l1-p1-b-data | 125 | 1.1112 | 3.0964 | 4.7257 | 0 | 384 | yes |
| p2-b | p2 | B unset-default | 21602 | peri-r72c-l1-p2-l1-p2-b-data | 125 | 1.0980 | 4.0476 | 6.6762 | 0 | 384 | yes |
| p2-a | p2 | A explicit-0 | 21603 | peri-r72c-l1-p2-l1-p2-a-data | 125 | 1.8658 | 4.0442 | 5.5240 | 0 | 384 | yes |
| p3-b | p3 | B unset-default | 21604 | peri-r72c-l1-p2-l1-p3-b-data | 125 | 1.4832 | 2.9370 | 4.2027 | 0 | 384 | yes |
| p3-a | p3 | A explicit-0 | 21605 | peri-r72c-l1-p2-l1-p3-a-data | 125 | 1.5053 | 5.5765 | 7.7247 | 0 | 384 | yes |
| p4-a | p4 | A explicit-0 | 21606 | peri-r72c-l1-p2-l1-p4-a-data | 125 | 2.0205 | 4.9104 | 7.7259 | 0 | 384 | yes |
| p4-b | p4 | B unset-default | 21607 | peri-r72c-l1-p2-l1-p4-b-data | 125 | 1.3116 | 3.4110 | 4.8647 | 0 | 384 | yes |
| p5-b | p5 | B unset-default | 21608 | peri-r72c-l1-p2-l1-p5-b-data | 125 | 1.2153 | 4.8298 | 6.0024 | 0 | 384 | yes |
| p5-a | p5 | A explicit-0 | 21609 | peri-r72c-l1-p2-l1-p5-a-data | 125 | 2.5336 | 6.5257 | 8.7741 | 0 | 384 | yes |
| p6-a | p6 | A explicit-0 | 21610 | peri-r72c-l1-p2-l1-p6-a-data | 125 | 2.2112 | 5.0012 | 6.5827 | 0 | 384 | yes |
| p6-b | p6 | B unset-default | 21611 | peri-r72c-l1-p2-l1-p6-b-data | 125 | 1.1653 | 2.8333 | 3.7931 | 0 | 384 | yes |
| p7-a | p7 | A explicit-0 | 21612 | peri-r72c-l1-p2-l1-p7-a-data | 125 | 1.9478 | 4.1285 | 7.6472 | 0 | 384 | yes |
| p7-b | p7 | B unset-default | 21613 | peri-r72c-l1-p2-l1-p7-b-data | 125 | 1.2665 | 4.0010 | 5.3378 | 0 | 384 | yes |
| p8-b | p8 | B unset-default | 21614 | peri-r72c-l1-p2-l1-p8-b-data | 125 | 1.4547 | 3.5216 | 4.6654 | 0 | 384 | yes |
| p8-a | p8 | A explicit-0 | 21615 | peri-r72c-l1-p2-l1-p8-a-data | 125 | 2.0084 | 4.1742 | 5.9069 | 0 | 384 | yes |

全部 16 腿：`timed=125`、`warmup=1`、状态全 200、`outcome=ok`、无错误码、无 429、无 quota 拒绝。

## 5. 逐对统计与判决

| pair | order | A p50 | B p50 | p50 B/A | A p95 | B p95 | p95 B/A |
|---|---|---:|---:|---:|---:|---:|---:|
| p1 | A_then_B | 2.6492 | 1.1112 | 0.41945 | 5.8411 | 3.0964 | 0.53011 |
| p2 | B_then_A | 1.8658 | 1.0980 | 0.58849 | 4.0442 | 4.0476 | 1.00084 |
| p3 | B_then_A | 1.5053 | 1.4832 | 0.98532 | 5.5765 | 2.9370 | 0.52667 |
| p4 | A_then_B | 2.0205 | 1.3116 | 0.64915 | 4.9104 | 3.4110 | 0.69465 |
| p5 | B_then_A | 2.5336 | 1.2153 | 0.47967 | 6.5257 | 4.8298 | 0.74012 |
| p6 | A_then_B | 2.2112 | 1.1653 | 0.52700 | 5.0012 | 2.8333 | 0.56652 |
| p7 | A_then_B | 1.9478 | 1.2665 | 0.65022 | 4.1285 | 4.0010 | 0.96912 |
| p8 | B_then_A | 2.0084 | 1.4547 | 0.72431 | 4.1742 | 3.5216 | 0.84366 |

- **主统计量：p95 比值中位数 = 0.71738 < 1.10 → PASS**。
- 辅助统计量：p50 比值中位数 = 0.61882 < 1.10 → PASS。
- 8 对中**每一对**的 p95 比值都 < 1.10；最差一对 p2 为 1.00084（基本持平），不是仅靠中位数过线。
- 逐对 p95 比值 bootstrap 95% CI（seed 20261105，10000 draws）：[0.53010, 0.96910]。
- 逐对 p50 比值 bootstrap 95% CI：[0.47966, 0.72432]。
- 前半（p1–p4）p95 中位 0.61238；后半（p5–p8）0.79189，均远低于 1.10。
- A-first 对 p95 中位 0.63059；B-first 对 0.79189。顺序分组均不逆转方向，未发现「先跑的一腿更慢」造成的伪差。
- 合并全部样本（每侧 1000 timed）：A p50 2.0680 / p95 5.1944；B p50 1.2631 / p95 3.6418；
  p95 比值 0.70110，pooled bootstrap 95% CI [0.61133, 0.76938]；p50 比值 0.61076。

解释：B 臂（默认延迟关闭）把只读会话的 worker close 派发挪到回复之后，客户端观测到的
请求延迟（这是本判据的测量对象）因此下降；p50 与 p95 同向。R72 的 C4 p95 比值 1.17471
在 8 对、每对 125 样本的独立复测中未复现：8 对全部低于 1.10，其中 7 对 B 更快，p2 基本持平
（1.00084）。这不改写 R72 的冻结判决，只说明 C4 的 p95 信号在该负载下不稳定。

## 6. 不变量核对（16/16 腿全部通过）

| 检查 | 结果 |
|---|---|
| 平台 / worker / 镜像 | 每腿 `binaries.txt` 记录宿主与容器内 SHA 均等于冻结值；镜像 digest 等于 `sha256:8a8ba43b…` |
| 臂机制 | A：metrics-before/after 均无 `deferred_session_close` 键，平台进程 env 为 `PERI_DEFERRED_SESSION_CLOSE=0`；B：env 为 unset，metrics-after 有键，`enqueued_total` 增量 126，`failed/retried/unresolved` 增量 0，`pending=0`，`pending_max` 1–2（上限 256，每库上限 4） |
| 请求 | 每腿 125 timed + 1 warmup；状态 200；`outcome=ok`；error_code 空；无 429/quota/rate_limit |
| 响应形状 | 每次 timed 样本 `rows=40`、`rows_read=201`；众数 `(response_bytes, rows, rows_read, rows_written, affected_row_count) = (15627, 40, 201, 0, 0)`；字节差异仅来自 `query_duration_ms` 位数（15626/15638），与 R72 相同 |
| 审计链 | 每腿 `verify_audit.py`：`chain_ok=true`、`first_sequence=1`、`head_sequence=836`、`head_matches_tail=true`、`records=836`、errors 空；审计增量 384 条/腿 |
| 账户 | 每腿 metrics-after：`session_seats=0`、`active_permits=0`、`unknown_held_permits=0`；worker sessions 0；worker pending_close 0；`cleanup_ledger.pending_close.count=0` |
| 结算/关闭 | 每腿 settle exit 0、`verdict=settled`、deferred_pending 0；docker stop exit 0、launcher `outcome=clean`、`unresolved_operations=false` |
| fixture | 每腿 `threads=201 messages=8625 content_chars=20177069 focus=8625 complete=True`；seed 行一致 |
| 错误计数 | 每腿 `errors_total` 与 `rejection_reasons_total` 增量为空；`operation_settle.exhausted_total=0`、`refused_total` 空 |
| 隔离 | 16 条独立卷、16 个独立端口；生产容器 `peri-loom` 与无关容器未触碰；无 r72c 容器残留；卷未删除 |

## 7. 判决

**PASS**。依据冻结判据：

- p95 比值中位 0.71738 < 1.10；
- p50 比值中位 0.61882 < 1.10；
- 16/16 腿有效，零错误、零 429，全部不变量通过；
- 无无效腿、无替换、无重跑。

未闭合项 / 限制：

- 本campaign不是容量或 SLA 证明；只测 L1.list-20 c1 的客户端观测延迟。
- 只覆盖 125 timed/腿；未重复 R72b 的 500 样本/腿（该规模会触发默认 quota）。
- 宿主负载未做严格控制（开始时 load 2.0），靠 8 对交替与中位数抵御；逐对与合并口径均远低于阈值。
- 不覆盖崩溃恢复；不重新裁决 R72 的 C1/C2/C3/C5/C6/C7/C8。
- B 臂的延迟优势部分来自延迟关闭的机制设计（close 不再占用回复路径）；这正是被判据测量的效果。

## 8. 证据文件清单

- `criteria-r72c.json` / `criteria-r72c-freeze.txt`：冻结判据与 SHA
- `environment.txt`：环境、二进制、镜像、运行器哈希
- `run.txt`：16 腿执行日志（16 次 start / 16 次 done，无异常）
- `raw.jsonl`：合并 16 腿的 2016 条样本（16 × (1 warmup + 125 timed)），带 leg/pair/side/arm 标注
- `summary.json`：逐腿、逐对、主/辅助统计、halves、pooled、bootstrap、不变量
- `metrics.json`：逐腿 metrics 增量、账户、deferred 机制键、响应形状
- `analysis.txt`：脚本生成的可读摘要
- `analyze_r72c.py`：分析脚本（R72b 脚本的只读适配版；判据不变）
- `legs/p2/l1/<leg>/`：每腿 `leg.txt`、`binaries.txt`、`seed.txt`、`metrics-before/after.json`、`settle.json`、`audit-count-*.txt`、`driver.txt`、`container.txt`、`exit-code.json`、`settle-exit-code.json`、`stop-exit.txt`、`launcher-stop.json`、`audit/`、`audit-verify.json`、`harness/`（raw/summary）
