# R71b 交错 A/B：只读无状态会话的 worker close 异步派发

日期：2026-11-05
分支：`perf/r71-deferred-session-close`
候选：`ac5e0c7`（父 `02a05a7`，根 `9e30afa`）
性质：**新的测量轮（R71b）**。只构建、启动隔离栈、测量、留证；不改生产代码，不推送远端。
判据在第一腿之前冻结于 `criteria.txt`（最终 SHA256
`57a38ba4df5b8e08abe69c81195eac2a5fc5b82c8468f041c27bf7160bd1234a`；冻结记录见
`criteria-freeze.txt`）。

## 结论

**本轮按冻结判据 FAIL：P2 的 X1 c8 吞吐出现可复现 >10% 退化，候选不得合入。**
P0 零对照、P0 阳性对照、P1 主指标与 P3 不变量均通过；P2 的 F1/L1 没有退化（反而更快），
但 X1 c8 的冻结指标为 B/A = **0.84384**（r1 0.73235、r2 0.79333），满足“合并 <0.90 且两轮同向”
的可复现退化定义。

| 检查 | 结果 | 关键数字 |
|---|---|---|
| P0 零对照（A1 vs A2） | **PASS** | 60 triples 中位比值 **1.02884**，CI 0.94851–1.09255（在 1.00±0.05 内且覆盖 1.0） |
| P0 阳性对照（100µs 忙等） | **PASS** | 30 triples 中位 **1.09317**（≥1.08），CI 0.93139–1.17633 |
| P1 主指标 | **PASS** | 60 triples 中位 **0.81810**（CI 0.74557–0.91785），r1 0.81017、r2 0.82793，均 <1.0 |
| P2 回归门 | **FAIL** | F1 p50/p95 0.55629/0.77936；L1 0.67219/0.73358（无退化）；**X1 c8 吞吐 0.84384（r1 0.73235、r2 0.79333）** |
| P3 不变量 | **PASS** | 26/26 腿全部通过；审计链连续、seats/permits/worker sessions/pending_close 全 0、干净停止 |
| 总判决 | **FAIL** | P2 的可复现退化 → 按冻结规则不得合入 |

**材料性协议分歧（必须与判决一起阅读）**：P2 的 X1 指标按冻结判据使用 Gate 2 的 c8-only 驱动。
作为**非判据诊断**，随后用上一轮同样的标准 Issue 28 X1 场景跑了 8 条新腿，c8 的 B/A =
**1.26021**（r1 0.97934、r2 1.54219），没有复现退化，反而更快。诊断不改变冻结判决，但它说明
P2 的失败是 **runner 相关**的：c8-only 驱动测的是“8 请求热身 + 1.2s 后的 100 请求冷突发”，
标准 X1 的 c8 层前面还有 c1/c4 的 200 请求热身。两者都是真实 c8 负载，但本轮的冻结指标是前者。
是否要把标准 X1 作为下一轮的预登记主指标，需要协调者决定；本轮不能事后改判。

## 冻结判据（摘要，全文见 `criteria.txt`）

- **两栈并发**：A = 同一候选二进制、`PERI_DEFERRED_SESSION_CLOSE` 未设置；B = 同一二进制、
  `PERI_DEFERRED_SESSION_CLOSE=1`。不同端口/run-dir/卷/容器名；同镜像、同 worker、同 launcher、
  同 fixture、默认配额、无观测器。
- **triple**：A1（20 个连续 pipeline 请求）、A2（20 个）、B（20 个）；请求为
  `[execute SELECT 1 want_rows, close]`；块平均 = 20 请求墙钟 ÷ 20；三块顺序用 seed 20261105
  固定随机化；块间 0.75s，轮间 5s；2 轮 × 30 triples = 60 triples。
- **P0.1**：每 triple 的 `A2/A1`，60 个比值中位必须在 1.00±0.05，且 bootstrap 95% CI 覆盖 1.0。
- **P0.2**：Gate 1b 的 100µs 忙等探针（SHA `6517583e…`），同一交错设计跑 1 轮 30 triples；
  B 侧 `PERI_R71_CLOSE_DELAY_US=100`；`B / mean(A1,A2)` 中位必须 ≥1.08。
- **P1**：每 triple `B / mean(A1,A2)`；60 个比值中位 ≤0.92，bootstrap 95% CI 上界 <1.0，
  两轮中位均 <1.0；另报配对绝对差 `B − mean(A1,A2)` 的中位与 CI。
- **P2**：F1/L1 标准 harness 各 500 requests/side、2 轮 A/B/B/A；X1 c8 吞吐 2 轮 A/B/B/A。
  可复现退化 = p50/p95 合并 B/A >1.10 且两轮 >1.0；X1 吞吐合并 B/A <0.90 且两轮 <1.0。
  X1 使用 Gate 2 的 c8-only 驱动（`read_concurrency_driver.py`，100 请求/腿，c8）；该 runner
  选择在第一条腿之前写入判据并二次冻结，原因是标准 X1 场景的 c16 层会触发已知的默认配额 429，
  会把配额节奏产物注入 P3 的“无 429”检查。
- **P3**：A 无 `deferred_session_close` 键；B `enqueued_total>0`、failed/retried/unresolved=0、
  pending=0；seats/permits/worker sessions/pending_close 全 0；审计链连续且条数可解释；
  无 429/503/`outcome_unknown`/`session_moved`/`audit_unavailable`；干净停止
  `unresolved_operations=false`。
- 上一轮的 P1 原始数字（B/A 0.75835）与 P0（A/A 1.06800）只作为“为什么换设计”的历史证据，
  **没有**参与本轮任何统计。

## 环境

| 项 | 值 |
|---|---|
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（10 vCPU，VM 内存 12.6 GB） |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，id/digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| worker（两侧相同） | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 候选 platform | 从 `ac5e0c7` 的独立 worktree 重建：`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`；SHA256 `3059c47a…`（与 Gate 2 一致） |
| launcher | 分支 `dev.py`，SHA256 `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129`，两侧挂载相同 |
| Go | `go1.27.3 darwin/arm64` |
| 耐久模式 | `audit_file_v1`（P1 A/B 一致） |
| 配额 | 默认，未放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |
| fixture | 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 未开启 |
| 端口 | P1 21120–21121；P0 21130–21131；P2 F1 21140–21147、L1 21148–21155、X1 21156–21163；诊断 21174–21181 |
| 卷 | 39 个 `peri-r71-ab2-*` 卷全部保留（`volumes.txt`；含 2 个 smoke、9 个诊断卷） |
| run-dir | `/tmp/peri-r71-ab2/{p1,p0,p2}/…`；连接文件只在 `/tmp`，`chmod 600`，未入库、未打印 |

未触碰生产容器 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker、未 prune、
未删除任何卷。诊断阶段有一个失败的 diag 容器被干净停止并保留（`Exited (0)`），其卷也保留。

## P1：交错块平均 A/B（60 triples）

命令：

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-ab2
./run_interleaved.sh p1          # 双栈 + 60 triples
python3 analyze_interleaved.py --campaign p1
```

运行结果：`valid_triples=60/60`、`void_triples=0`、`failed_blocks=0`、`blocks=180`、
`requests_total=3600`（A 2,400、B 1,200）；两栈 `driver_exit=0`、`settle_exit=0`、`stop_exit=0`。

| 块 | n | 中位 ms | 均值 ms | p95 ms | min | max |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 60 | 4.4971 | 4.2605 | 5.3617 | 2.3094 | 5.6081 |
| A2 | 60 | 4.4476 | 4.3825 | 5.6694 | 2.3519 | 5.8369 |
| B | 60 | 3.7344 | 3.4407 | 4.4139 | 0.9904 | 4.6562 |

块顺序分布（seed 20261105，60 triples）：`A1,B,A2` 17；`A2,A1,B` 11；`A2,B,A1` 10；
`B,A2,A1` 10；`B,A1,A2` 6；`A1,A2,B` 6。

### P0.1 零对照（A1 vs A2，同一栈同时段）

| 口径 | 中位比值 | bootstrap 95% CI |
|---|---:|---|
| 合并 60 triples | **1.02884** | 0.94851–1.09255 |
| r1（30 triples） | 1.02849 | 0.96735–1.08577 |
| r2（30 triples） | 1.04374 | 0.90538–1.18103 |

中位在 1.00±0.05 内，CI 覆盖 1.0，**P0.1 PASS**。上一轮的 A2/A1 为 1.06800（CI
1.02864–1.11644）；本轮的双栈交错把 A 侧的位置漂移压到 2.9%，在冻结带内。

### P1 主指标

| 口径 | 中位比值 | bootstrap 95% CI |
|---|---:|---|
| 合并 60 triples | **0.81810** | 0.74557–0.91785 |
| r1（30 triples） | 0.81017 | 0.72188–0.92514 |
| r2（30 triples） | 0.82793 | 0.74704–0.98458 |

- 合并 0.81810 ≤0.92；CI 上界 0.91785 <1.0；两轮均 <1.0 → **P1 PASS**。
- 配对绝对差 `B − mean(A1,A2)`：中位 **−0.859561 ms**，CI **−1.252371–−0.339134 ms**
  （上界 <0，排除 0）。
- 绝对量级：A 块中位约 4.47 ms/请求，B 块约 3.73 ms/请求；配对差约 0.86 ms/请求。

### 动作桶（P1 窗口增量，均值）

| 侧 | open µs | statement µs | close µs |
|---|---:|---:|---:|
| A（flag off） | 1,240.1 | 305.8 | **1,078.3** |
| B（flag on） | 1,191.7 | 270.0 | **146.1** |

close 均值 **−86.5%**；open −3.9%、statement −11.7%。总效应（约 0.86 ms/请求）主要由 close
解释，open/statement 的差异不足以单独解释 18.2% 的块平均差。

## P0.2 阳性对照（同一交错设计，100µs 忙等）

- 探针二进制：`/tmp/r71-bin/peri-platform-probe`，SHA256
  `6517583e751e2dd936c44fcbd80dd77d13b10757503a0917a7f83bad16096916`。
- 两栈都用探针二进制与探针 worktree 的 `dev.py`；仅 B 栈带 `PERI_R71_CLOSE_DELAY_US=100`。
  容器内验证：A 侧 `<unset>`、B 侧 `100`；平台进程 environ 同样只出现在 B。
- 30 triples、0 void、90 blocks、1,800 请求（A 1,200、B 600）。

| 块 | n | 中位 ms | 均值 ms | p95 ms |
|---|---:|---:|---:|---:|
| A1 | 30 | 3.2605 | 3.1773 | 4.0766 |
| A2 | 30 | 3.3205 | 3.3227 | 4.4816 |
| B | 30 | 3.4256 | 3.2229 | 3.9523 |

- `B / mean(A1,A2)` 中位 **1.09317**（≥1.08，方向正确）→ **P0.2 PASS**。
- CI 0.93139–1.17633 作为支持信息报告；**判据只要求中位 ≥1.08**，未要求 CI 排除 1.0。
- 该对照与本轮候选腿**分开运行**，未混算。它与 Gate 1b 的 1.13258 是**不同设计**（Gate 1b 是
  顺序腿；本轮是双栈交错），所以只作上下文，不作本轮结果。
- 阳性对照自身的位置零对照：A2/A1 中位 1.01020，CI 0.90013–1.14218；噪声明显大于候选 P1 的
  零对照（CI 宽度约 0.24 vs 0.14），这也是本轮只把 +9.3% 检出称为“过线”而非“强检出”的原因。

## P2：回归门

命令：

```bash
./run_p2.sh f1
./run_p2.sh l1
./run_p2.sh x1
python3 analyze_p2.py
```

每个腿都是新卷、独立容器/端口、同 fixture、默认配额、无观测器；F1/L1 每腿 125 个计时请求
（2 轮 × A/B/B/A，每侧 500 请求）；X1 c8 每腿 100 请求（Gate 2 c8-only 驱动）。

### F1.select-1 json-pipeline

| 指标 | A | B | 合并 B/A | r1 | r2 | 退化 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 1.7499 | 0.9734 | **0.55629** | 0.57648 | 0.53082 | 否 |
| p95 ms | 5.7942 | 4.5157 | **0.77936** | 0.77798 | 0.76767 | 否 |
| 失败 | — | — | — | — | — | 0 |

### L1.list-20 json-pipeline

| 指标 | A | B | 合并 B/A | r1 | r2 | 退化 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 2.2736 | 1.5283 | **0.67219** | 0.62242 | 0.67219 | 否 |
| p95 ms | 5.7080 | 4.1873 | **0.73358** | 0.73378 | 0.72201 | 否 |
| 失败 | — | — | — | — | — | 0 |

### X1 c8 吞吐（冻结指标：Gate 2 c8-only 驱动）

| 腿 | 侧 | 轮 | qps | p50 ms | p95 ms | 失败 |
|---|---|---|---:|---:|---:|---:|
| r1-a1 | A | r1 | 1,161.7 | 5.4802 | 12.7624 | 0 |
| r1-b1 | B | r1 | 1,324.6 | 4.0915 | 11.7354 | 0 |
| r1-b2 | B | r1 | 1,012.0 | 5.9099 | 20.5323 | 0 |
| r1-a2 | A | r1 | 2,028.8 | 2.8019 | 8.6958 | 0 |
| r2-a1 | A | r2 | 1,176.0 | 6.4030 | 20.0170 | 0 |
| r2-b1 | B | r2 | 1,001.5 | 5.1995 | 24.9120 | 0 |
| r2-b2 | B | r2 | 891.5 | 7.8644 | 19.8607 | 0 |
| r2-a2 | A | r2 | 1,210.1 | 6.1580 | 17.3832 | 0 |

- 合并中位：A **1,193.055** qps、B **1,006.746** qps，B/A = **0.84384**。
- 逐轮：r1 **0.73235**、r2 **0.79333**，两轮 <1.0。
- 规则“合并 <0.90 且两轮 <1.0”成立 → **可复现退化，P2 FAIL**。

机制：B 的 X1 腿在 `pending_max=4`（每库上限）处饱和，`inline_total` 分别为 1/2/4/5（每 100 请求），
即部分 close 回落到同步 inline 路径；B 腿的 p95 与墙钟都更高。A 侧没有该队列。
这与 P1（`pending_max=2`、`inline_total=0`、close −86.5%）形成清晰对照：**c1 的收益在 c8 冷突发下
被队列饱和与 inline 回退抵消**。

### 非判据诊断：标准 Issue 28 X1 场景（8 腿）

为判断上述退化是否与 runner 形状有关，额外用上一轮的标准 X1 场景（levels 1/4/8/16，各 100 请求）
跑了 2 轮 × A/B/B/A 的 8 条新腿。该诊断**不是**冻结指标，只作证据；细节见
`diagnostics/standard-x1/README.txt` 与 `diagnostics/standard-x1/c8-summary.json`。

| 轮 | A 中位 qps | B 中位 qps | B/A |
|---|---:|---:|---:|
| r1 | 1,862.1 | 1,823.6 | 0.97934 |
| r2 | 1,296.9 | 2,000.0 | 1.54219 |
| 合并 | 1,556.5 | 1,961.5 | **1.26021** |

- 标准 X1 的 c8 层每腿 `429=0`；c16 层每腿 13–53 个 429（已知默认配额节奏产物），因此诊断腿的
  `driver_exit=1`，且**不属于** P3 的 26 腿。
- 该诊断**没有复现** c8-only 驱动的 0.84384 退化；合并方向甚至相反。结论：P2 的失败对 runner
  形状敏感，不能据此断言候选在所有 c8 负载下都退化；但按冻结判据，本轮 P2 仍是 FAIL。

## P3：不变量（P1 两栈 + P2 24 腿 = 26 腿）

命令：

```bash
python3 collect_invariants.py .
```

结果：**26/26 腿通过**（每个腿的 check 全绿）。

| 检查 | P1 A | P1 B | P2 F1/L1（8+8 腿） | P2 X1（8 腿） |
|---|---|---|---|---|
| `deferred_session_close` 键 | 不存在 | 存在 | A 不存在 / B 存在 | A 不存在 / B 存在 |
| `enqueued_total` | — | 1,200 | — / 126 | — / 103–107 |
| `inline_total` | — | 0 | — / 0 | — / 1–5 |
| `failed/retried/unresolved` | — | 0/0/0 | 0/0/0 | 0/0/0 |
| `pending`（drain 后） | — | 0 | 0 | 0 |
| `session_seats` / `active_permits` | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| worker sessions / pending_close | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| cleanup ledger pending_close | 0 | 0 | 0 | 0 |
| 审计链连续 | 7,652 条，seq 1..7652，head 匹配 | 4,052 条，seq 1..4052，head 匹配 | 每腿 836 条，链连续 | 每腿 776 条，链连续 |
| 审计条数可解释 | 7,200 = 2,400×3 | 3,600 = 1,200×3 | A/B 每腿 delta 384 | A/B 每腿 delta 324 |
| 错误/拒绝计数 | 0 | 0 | 0 | 0 |
| 干净停止 | outcome=clean, unresolved=false, stop exit 0 | 同左 | 同左 | 同左 |

- P1 审计条数比 A:B = 7,200:3,600 = **2.0**，与请求数比 2,400:1,200 完全一致；每请求 3 条
  （open/statement/close）。
- B 的 `pending_max=2`（P1）< 每库上限 4；X1 的 B 腿 `pending_max=4` 饱和，与 P2 退化机制一致。
- 证据位置：`invariants.json`、`legs/p1/{a,b}/audit-verify.json`、`legs/p2/*/*/audit-verify.json`、
  每个腿的 `launcher-stop.json`、`settle.json`、`metrics-after.json`、`audit-count-*.txt`。

## 证据、脚本与复现

| 文件 | 说明 |
|---|---|
| `criteria.txt` / `criteria-freeze.txt` | 冻结判据与两次冻结哈希/时间（第二次只明确 X1 c8 runner，发生在任何栈启动前） |
| `interleaved_driver.py` | 双栈 triple 驱动（20 请求/块、随机块序、void 重试） |
| `run_interleaved.sh` | 双栈启动、seed、驱动、settle、干净停止、审计拷贝（p1/p0） |
| `analyze_interleaved.py` | P0.1/P0.2/P1 的 median + 10,000 次 percentile bootstrap（seed 20261105） |
| `run_p2.sh` | F1/L1 标准 harness；X1 走 Gate 2 c8 driver |
| `read_concurrency_driver.py` / `gate2_common.py` | Gate 2 c8-only 驱动及其 helper（复用） |
| `analyze_p2.py` | F1/L1/X1 的 p50/p95/吞吐与可复现退化判定 |
| `collect_invariants.py` / `verify_audit.py` | P3 不变量与审计链连续性 |
| `snap_metrics.py` / `wait_settled.py` | `/v1/metrics` 快照与 drain 等待 |
| `analysis-p1.json` / `analysis-p0.json` / `analysis-p2.json` | 三个分析产物 |
| `summary.json` / `invariants.json` | 机器可读汇总 |
| `diagnostics/standard-x1/` | 非判据标准 X1 8 腿诊断（driver.txt、summary.json、c8-summary.json） |
| `legs/p1/{a,b}/…` | P1 两栈的 metrics、audit、launcher-stop、settle、审计计数 |
| `legs/p0/{a,b}/…` | 阳性对照两栈的同类证据 |
| `legs/p2/{f1,l1,x1}/<leg>/…` | 24 条 P2 腿的 harness、metrics、audit、停止证据 |

真实命令与关键输出：

```bash
# 1) 候选重建（SHA 必须与 Gate 2 一致）
cd /tmp/r71-ab-src/prototypes/v1/platform
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -o /tmp/r71-ab2/bin/peri-platform-ab2 ./cmd/peri-platform
shasum -a 256 /tmp/r71-ab2/bin/peri-platform-ab2
# 3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014

# 2) P1（双栈交错）
cd docs/performance-review/2026-11-05-r71-deferred-close-ab2
./run_interleaved.sh p1
# p1 stack a done driver_exit=0 settle_exit=0 stop_exit=0
# p1 stack b done driver_exit=0 settle_exit=0 stop_exit=0
# p1 interleaved campaign complete
python3 analyze_interleaved.py --campaign p1
# zero control A2/A1: median 1.02884 CI [0.94851, 1.09255] pass=True
# effect B/mean(A1,A2): median 0.81810 CI [0.74557, 0.91785] pass=True
# paired difference: median -0.859561 ms CI [-1.252371, -0.339134]
# VERDICT: PASS

# 3) P0 阳性对照
./run_interleaved.sh p0
python3 analyze_interleaved.py --campaign p0
# effect B/mean(A1,A2): median 1.09317 CI [0.93139, 1.17633] pass=True
# VERDICT: PASS

# 4) P2
./run_p2.sh f1   # 8 legs, all driver_exit=0
./run_p2.sh l1   # 8 legs, all driver_exit=0
./run_p2.sh x1   # 8 legs, all driver_exit=0
python3 analyze_p2.py
# f1 median 0.55629 p95 0.77936 ok
# l1 median 0.67219 p95 0.73358 ok
# x1 throughput 0.84384 r1 0.73235 r2 0.79333 DEGRADED
# VERDICT: FAIL

# 5) P3 + 汇总
python3 collect_invariants.py .
# 26/26 legs passed; audit count groups equal=True; VERDICT=PASS
python3 make_summary.py
```

## 未验证项与剩余风险

1. **P2 的 runner 依赖（最重要）**：冻结的 c8-only 驱动给出 0.84384 退化；标准 X1 的 8 腿诊断给出
   1.26021（无退化）。两者都只测 c8，但热身/前置层不同。本轮按冻结判据判 FAIL；若要判定候选在
   标准 c8 场景是否安全，需要下一轮**预登记**标准 X1 作为主指标，不能在本轮事后改判。
2. **未做崩溃/掉电恢复**：本轮没有重复 R37 的 crash-recovery E2E；deferred close 在崩溃时的
   尾部记录行为未测。
3. **未做跨栈 A/A**：P0.1 是 A 栈内部的 A1/A2 对照，不能直接排除“A 栈本身比 B 栈慢”的栈级偏差；
   动作桶显示 close −86.5%、open −3.9%、statement −11.7%，效应主要由 close 解释，但跨栈
   A/A（两栈都 flag off）未跑。
4. **阳性对照的检出边际**：+9.3% 刚过 8% 门槛，CI 覆盖 1.0；候选效应 18.2% 约为其两倍，故本轮
   对 18% 量级的检出能力足够，但对 8–10% 量级的判定能力有限。
5. **X1 的噪声**：c8-only 驱动的 A 腿有 2,028.8 qps 的离群值；B 腿 891.5–1,324.6。即使把 A 的
   离群值按约 1,200 qps 处理，合并 B/A 仍约 0.85，退化判定不变；但幅度对宿主噪声敏感。
6. **只测了 c8**：未测 c16/c32/c64 的候选行为；标准 X1 的 c16 层只作为诊断，且有已知 429。
7. **未开启观测器**：没有 phase/durability observer 的绝对值；动作桶来自 `/v1/metrics` 的
   `actions_total`/`action_latency_ns_total` 窗口增量。
8. **P3 的“无 429”只覆盖冻结的 26 腿**：标准 X1 诊断的 c16 层每腿 13–53 个 429，是默认配额的节奏
   产物，不属于 P3；这一点已在判据与诊断说明中标注。
9. **未推送远端**；本轮只有本地证据与待提交的 `docs(perf)` commit。

## 判决

按冻结判据：**FAIL**。P0 零对照、P0 阳性对照、P1 与 P3 通过；P2 的 X1 c8 冻结指标出现
可复现 >10% 退化（0.84384，两轮同向），因此候选不得合入。标准 X1 的非判据诊断没有复现该退化，
这是下一轮需要协调者裁决的协议分歧，不改变本轮判决。
