# R72b 独立验证：L1 c1 p95 非劣定向复测（协议阻塞）

日期：2026-11-05
分支：`perf/r72-deferred-close-default`
commit：`56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6`（起点 `main` `ee1da7d`）
性质：**独立验证轮（verification）**。只使用既有构建产物、隔离栈、既有 L1 驱动与只读分析脚本；
不改产品代码、测试、判据、配额或驱动；不提交、不推送、不删卷/证据。

## 结论

**判决：环境阻塞（ENVIRONMENT_BLOCKED）。** 冻结协议与平台默认配额冲突：
`requests_per_second=500`，而每腿 500 次 c1 L1 请求在较快的 B 臂上会在约 0.8 s 内耗尽
令牌桶。第一对腿中 p1-a 完成 500/500、零错误；p1-b 在计时样本 488 起触发
11 次 `rate_limit`/`quota_exceeded`（1 次 HTTP 429 + 10 次 pipeline 配额错误），
驱动退出码 1，按冻结的腿有效性规则无效。

因此：

- **没有** 8 个有效对，主判据（8 对 p95 比值中位）**无法计算**；不宣称 PASS，也不宣称 p95 FAIL。
- p1 对的 p95/p50 比值只作上下文，不能替代冻结统计量。
- 未使用「补一对」：这不是与 A/B 处理无关的瞬时环境异常，而是**更快的一臂更容易触发默认配额**
  的系统性偏差；补对会从同一偏差机制中抽样，不能修复协议。
- 其余 14 腿未执行：按任务要求「发现缺陷停止并报告」，继续执行只会重复同一偏差。

平台侧行为本身正确：返回 429/配额错误、没有数据损坏、没有 seat/permit 泄漏、审计链连续；
缺陷在**测量协议**，不在产品。

## 判据冻结

判据在任何 R72b 栈启动之前写入并冻结：

```text
criteria_file: criteria-r72b.json
criteria_sha256: 0b2f2769344a8798d64130e68d75e3acd710349fe4d1f17e8e53209316a8af00
frozen_at_utc: 2026-11-05T08:23:00Z
candidate_commit: 56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6
baseline_commit: ee1da7d
```

冻结记录：`criteria-r72b-freeze.txt`。判据文件在测量后未修改；分析脚本复核
`criteria_sha256` 与冻结记录一致。R72 原判据 SHA256 为
`de7860a1b1737d4fd20b14fd0f743c1f14f38455892e8b162d44c6b59ce0ba39`（本文件只引用，不改写）。

## 环境核对

| 项 | 值 | 核对 |
|---|---|---|
| 平台二进制 | `/tmp/peri-r72-ab/bin/peri-platform-r72` | SHA256 `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba`，与 R72 一致 |
| worker | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7` | SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7` | digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| launcher | `prototypes/v1/platform/dev.py` | SHA256 `80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2` |
| 项目配额 | 默认，未放宽 | `requests_per_second=500`、`max_sessions=16`、`max_concurrent=100`、`max_result_bytes=4194304` |
| fixture | 每栈重新导入并校验 | `threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 全程关闭 | 无 `PERI_DURABILITY_OBSERVER`，无相位观测器 |
| 耐久模式 | `audit_file_v1` | 逐腿 metrics 实读 |

两臂唯一差异：A 显式 `PERI_DEFERRED_SESSION_CLOSE=0`，B 完全未设置。两腿 `binaries.txt`
均记录了容器环境与 `/proc/<pid>/environ`：p1-a 两处均为 `0`；p1-b 容器 `<unset>`、
平台进程 `unset`。

## 16 腿运行清单

设计序列（8 对，A/B 顺序交替、整体平衡）：
`p1-a p1-b p2-b p2-a p3-b p3-a p4-a p4-b p5-b p5-a p6-a p6-b p7-a p7-b p8-b p8-a`。
端口 21500 起；每腿全新卷 `peri-r72b-l1-*`。**实际只执行 p1 对；其余 14 腿未启动。**

| 对 | 臂 | 腿 | 卷 | 端口 | 样本 | p50 ms | p95 ms | p99 ms | 错误 | 状态 |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| p1 | A | p1-a | `peri-r72b-l1-p2-l1-p1-a-data` | 21500 | 500 | 1.8229 | 3.7360 | 6.5120 | 0 | 有效 |
| p1 | B | p1-b | `peri-r72b-l1-p2-l1-p1-b-data` | 21501 | 500 | 1.2066 | 3.6258 | 5.6725 | 11 | **无效（配额）** |
| p2 | B | p2-b | 未创建 | 21502 | 0 | — | — | — | — | 未运行 |
| p2 | A | p2-a | 未创建 | 21503 | 0 | — | — | — | — | 未运行 |
| p3 | B | p3-b | 未创建 | 21504 | 0 | — | — | — | — | 未运行 |
| p3 | A | p3-a | 未创建 | 21505 | 0 | — | — | — | — | 未运行 |
| p4 | A | p4-a | 未创建 | 21506 | 0 | — | — | — | — | 未运行 |
| p4 | B | p4-b | 未创建 | 21507 | 0 | — | — | — | — | 未运行 |
| p5 | B | p5-b | 未创建 | 21508 | 0 | — | — | — | — | 未运行 |
| p5 | A | p5-a | 未创建 | 21509 | 0 | — | — | — | — | 未运行 |
| p6 | A | p6-a | 未创建 | 21510 | 0 | — | — | — | — | 未运行 |
| p6 | B | p6-b | 未创建 | 21511 | 0 | — | — | — | — | 未运行 |
| p7 | A | p7-a | 未创建 | 21512 | 0 | — | — | — | — | 未运行 |
| p7 | B | p7-b | 未创建 | 21513 | 0 | — | — | — | — | 未运行 |
| p8 | B | p8-b | 未创建 | 21514 | 0 | — | — | — | — | 未运行 |
| p8 | A | p8-a | 未创建 | 21515 | 0 | — | — | — | — | 未运行 |

执行命令（未修改 R72 的 `run_p2.sh`，SHA256 `fd02651bdf071c535ad77df1cecfb684f57ef0323694597885dc699f2f53928d`）：

```bash
cd docs/performance-review/2026-11-05-r72-deferred-close-default-ab
OUT=.../2026-11-05-r72b-l1-p95-retest \
PREFIX=peri-r72b-l1 BASE_PORT=21500 SAMPLES=500 \
./run_p2.sh l1 p1-a p1-b
```

## p1 对数字（上下文，不参与判决）

| 腿 | 侧 | p50 ms | p95 ms | p99 ms | 样本 | 失败 | driver | settle | stop |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| p1-a | A（显式 0） | 1.8229 | 3.7360 | 6.5120 | 500 | 0 | 0 | 0 / settled | 0 / clean |
| p1-b | B（未设置） | 1.2066 | 3.6258 | 5.6725 | 500 | 11 | 1 | 0 / settled | 0 / clean |

| 指标 | A | B | B/A | 有效性 |
|---|---:|---:|---:|---|
| 每腿 p50 | 1.8229 | 1.2066 | 0.66191 | 对无效，仅上下文 |
| 每腿 p95 | 3.7360 | 3.6258 | 0.97049 | 对无效，仅上下文 |

**主判据：不可计算。** 有效对 0/8；8 对 p95 比值中位为 N/A，bootstrap CI 为 N/A。
分半中位、合并 500×8 样本比值均为 N/A（未运行 p2–p8）。
若只看 p1-b 的前 487 个成功样本（**非判据、不能替代 500 样本**）：p50 1.1945、
p95 3.5324，与 p1-a 前 487 样本的 p95 比值 0.9455；这是截断前缀，存在选择偏差，
不构成结论。

## 缺陷：500 样本协议与默认 500 req/s 配额冲突

平台按项目令牌桶限流，`requests_per_second=500`。L1 pipeline 请求 = 隐式 open +
execute + close；close 走 control lane，不消耗速率令牌，open/execute 消耗。以 p1-b
第一次限流为锚点做经验核算（`quota-conflict.txt`）：

```text
p1-b: timed=500 timed_sum_s=0.7981 mean_ms=1.5962 failures=11
first failure: sample=488, cumulative timed 0.7739 s
available tokens = 500 + 500 * 0.7739 = 886.94
effective tokens/request = 886.94 / 488 = 1.8175
完成 500 次不失流需要 timed_sum_s >= 0.8175（平均每请求 >= 1.635 ms）
p1-b 0.7981 s -> 低于阈值；p1-a 1.0153 s -> 高于阈值
```

p1-b 的失败集中在样本 488–500（1 次 HTTP 429 + 10 次 HTTP 200 的 pipeline 配额错误），
与令牌桶耗尽位置一致。p1-b metrics 侧：

```text
errors_total delta          = {"quota_exceeded": 11}
rejection_reasons_total delta = {"rate_limit": 11}
```

关键点：**B 臂更快，因此更容易在 500 次请求内跨过配额阈值**。这不是与 A/B 处理
无关的瞬时异常；它直接与处理效应（延迟更低）相关。R72 的 125 样本 L1 数据也支持
这一点（按均值外推 500 次请求的 timed sum）：

```text
A 腿（4 条）：1.0091 – 1.2947 s
B 腿（4 条）：0.8025 – 1.0184 s
其中最快的 r2-b2 外推 0.8025 s，低于 0.8175 s 阈值
```

即 500 样本设计会**选择性使较快的 B 腿失效**；继续执行 p2–p8 不会得到无偏的 8 对。
因此没有使用「唯一一次补对」，也没有通过改配额/加 pacing/减样本绕过——这些都违反
冻结判据。

## 次生缺陷：harness 把 pipeline 配额错误标成 row_count_mismatch

`scripts/perf-reliability/issue28/runner.py` 的 `_pipeline_measurement` 先根据
pipeline error 设置 `outcome="error"` / `error_code`，随后无条件执行 expected-rows
检查；当失败语句返回 0 行时，后者把 `outcome` 覆盖为 `mismatch`、
`error_code` 覆盖为 `row_count_mismatch`。因此 p1-b 的 `raw.jsonl` 显示
10 个 `row_count_mismatch`，而 metrics 的真实原因是 `quota_exceeded` / `rate_limit`。
这不改变腿无效的结论（任何非 ok 都使腿无效），但会掩盖诊断原因；本轮只记录，不修改。

## 不变量核对

**p1-a（有效）**

| 检查 | 结果 |
|---|---|
| 计时样本 | 500/500 `status=200`、`outcome=ok`、空 `error_code` |
| 响应形状 | 模式 `(15627, 40, 201, 0, 0)`；所有计时样本 rows=40、rows_read=201 |
| 审计链 | 1961 条，`chain_ok=true`，head 与 tail 一致；audit delta 1509 |
| seats/permits | `session_seats=0`、`active_permits=0`、`unknown_held_permits=0` |
| worker | 两 worker `sessions=0`、`pending_close=0`；cleanup ledger `pending_close.count=0` |
| 错误计数 | `errors_total` delta `{}`；`rejection_reasons_total` delta `{}` |
| 机制 | A 腿 metrics 无 `deferred_session_close` 键（符合显式 0） |
| 停止 | driver 0、settle 0/settled、docker stop 0、launcher `clean`、`unresolved_operations=false` |

**p1-b（无效，但结构不变量正常）**

| 检查 | 结果 |
|---|---|
| 计时样本 | 500 个，其中 489 成功、11 失败（488–500） |
| 成功样本响应形状 | 模式 `(15627, 40, 201, 0, 0)`；失败样本 147 bytes / 0 rows |
| 审计链 | 1959 条，`chain_ok=true`，head 与 tail 一致；audit delta 1507 |
| seats/permits | `session_seats=0`、`active_permits=0`、`unknown_held_permits=0` |
| worker | 两 worker `sessions=0`、`pending_close=0`；cleanup ledger `pending_close.count=0` |
| 错误计数 | `errors_total` delta `{quota_exceeded: 11}`；`rejection_reasons_total` delta `{rate_limit: 11}` |
| 机制 | B 腿 `deferred_session_close` 存在：`enqueued_total=500`、`failed_total=0`、`retried_total=0`、`unresolved_total=0`、`pending=0`、`pending_max=2`（≤4） |
| 停止 | settle 0/settled、docker stop 0、launcher `clean`、`unresolved_operations=false`；driver 退出 1（请求失败） |

两腿平台/worker/镜像 SHA 与 R72 一致；fixture 计数一致；无审计断链、无 seat/permit
泄漏、无 502/`outcome_unknown`。**失败仅来自默认速率配额。**

## 判决与未闭合项

**判决：环境阻塞。** 在冻结判据下无法得到 8 个有效对，主判据不可计算；这不是对
「L1 c1 p95 非劣」的 PASS 或 FAIL。p1-b 的 11 个失败是平台按默认配额正确拒绝，
不是产品缺陷；产品侧未观察到数据损坏、审计断链或资源泄漏。

未闭合项：

1. **R72 C4 仍未解决**：本轮没有提供可替代 R72 p95 结论的新统计量；R72 的 FAIL 保持原样。
2. **协议需要重新冻结**：要完成 500 样本/腿且不受默认 500 req/s 影响，必须在新一轮中
   预先选择：对请求加 pacing（改变协议）、提高/取消配额（改变环境）、或减少每腿样本
   （改变统计功效）；三者都不能在本轮冻结后自行更改。
3. **harness 错误码覆盖**未修复（`row_count_mismatch` 掩盖 `quota_exceeded`），
   属测量工具缺陷，需单独任务处理。
4. **p2–p8 未测量**：未运行不是通过；它们在证据中明确标记为 missing。

## 证据清单

```text
criteria-r72b.json              冻结判据（测量前写入）
criteria-r72b-freeze.txt        冻结时间与 SHA256
environment.txt                 构建/镜像/worker/配额/端口/观测器
run_command.txt                 实际执行命令与 R72 run_p2.sh SHA
quota-conflict.txt              配额冲突的定量核算（含 R72 外推）
analyze_r72b.py                 只读聚合/不变量脚本
summary.json                    逐腿、逐对、主/辅判据、判决
metrics.json                    逐腿 metrics 摘要
raw.jsonl                       实际执行腿的 1002 条原始样本（含 metadata）
analysis.txt                    人类可读表
legs/p2/l1/p1-a/                leg.txt、binaries.txt、seed.txt、metrics-*.json、
                                settle.json、audit-count-*.txt、driver.txt、
                                container.txt、launcher-stop.json、audit/、
                                audit-verify.json、harness/{raw.jsonl,summary.json,summary.md}
legs/p2/l1/p1-b/                同上（含 11 个失败样本与 driver 退出 1）
```

连接文件与私有 API Key 不在证据目录；容器已删除，卷
`peri-r72b-l1-p2-l1-p1-a-data` 与 `peri-r72b-l1-p2-l1-p1-b-data` 保留，未 prune/删除。
生产容器 `peri-loom` 未触碰（`Up 2 days`）。未做 git 提交/推送。

## 限制

- 单机 macOS 26.5.1 / arm64、Docker 29.4.0、localhost、无 TLS；不是容量或 SLA 证明。
- 只执行了 p1 对；没有 16 腿的逐对/分半/合并统计，不能对外宣称任何 p95 结论。
- 配额冲突的 token 核算是经验值（1.8175 tokens/request），用于解释失败位置，不是
  平台令牌桶的逐请求重实现；关键证据是 p1-b 的实际失败样本与 metrics 计数。
