# R72 独立验证：默认启用延迟会话关闭

日期：2026-11-05
分支：`perf/r72-deferred-close-default`
commit：`56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6`（起点 `main` `ee1da7d`）
性质：**独立验证轮（verification）**。只构建、启动隔离栈、测量、核对与留证；
不改产品代码/测试，不提交、不推送、不删卷/证据。

## 结论

**总判定：FAIL。** 8 项判据中 C1、C2、C3、C5、C6、C7、C8 通过；
**C4（L1 c1 p95）失败**：合并 p95 B/A = **1.17471**，超过预冻结阈值 `< 1.10`。
C4 的 p50 通过（0.69538），失败只落在尾部。按预冻结判据，本轮不建议合入；
未因结果不理想修改判据，也没有为通过而重跑 L1 腿。

| 判据 | 结果 | 关键数字 |
|---|---|---|
| C1 Gate 0 机制 | PASS | A 无 deferred 键；B enqueued=5、pending_max=1；两臂 operation_settle.retry_total=0 |
| C2 P1 主指标 | PASS | 中位 0.77010，CI [0.69508, 0.83490]，24/30 B 更快 |
| C3 F1 c1 | PASS | p50 0.49494，p95 0.80161 |
| C4 L1 c1 | **FAIL** | p50 0.69538，**p95 1.17471 (> 1.10)** |
| C5 标准 X1 c8 | PASS | 吞吐 B/A 1.05891 |
| C6 持续 c8 | PASS | 吞吐 B/A 1.18520，8/8 腿零错误 |
| C7 不变量 | PASS | P3、响应形状与日志扫描均通过 |
| C8 P3 | PASS | 34/34 腿 |
| C9 崩溃语义 | 引用 | R71 Gate 3 T1–T9 PASS；代码路径一致性见下文 |

**总判定：FAIL。**

## 判据冻结

判据于任何测量腿之前写入 `criteria.json`；冻结记录见 `criteria-freeze.txt`：

```text
frozen_at_utc: 2026-11-05T07:57:03Z
criteria_file: criteria.json
criteria_sha256: de7860a1b1737d4fd20b14fd0f743c1f14f38455892e8b162d44c6b59ce0ba39
candidate_branch: perf/r72-deferred-close-default
candidate_commit: 56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6
baseline_commit: ee1da7d
note: frozen before the first Docker stack of this campaign was started. No measurement leg,
      Gate 0 leg or smoke run existed at freeze time. Nothing in criteria.json may be changed
      after data is seen.
```

`criteria.json` SHA256 以冻结记录为准：

```text
de7860a1b1737d4fd20b14fd0f743c1f14f38455892e8b162d44c6b59ce0ba39  criteria.json
```

## 构建与环境

| 项 | 值 |
|---|---|
| 平台二进制 | `/tmp/peri-r72-ab/bin/peri-platform-r72`，SHA256 `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` |
| 平台构建 | `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath ./cmd/peri-platform`（从 `56dc914`；两次构建 SHA 一致） |
| worker | R71d 同一二进制 `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| launcher | `prototypes/v1/platform/dev.py`，SHA256 `80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2` |
| Go | `go1.27.3 darwin/arm64` |
| 耐久模式 | `audit_file_v1`（每腿 metrics 实读） |
| 配额 | 默认，未放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |
| fixture | 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |
| 观测器 | 全程关闭 |
| 端口 | Gate0/P1 21400–21401；F1 21410–21417；L1 21420–21427；X1 21430–21437；持续 c8 21440–21447 |
| 卷 | 全部新建 `peri-r72-ab-*`，未复用/未删除旧卷 |

两侧使用同一二进制、同一 worker、同一镜像、同一配额；唯一差异是平台进程环境：
A 显式 `PERI_DEFERRED_SESSION_CLOSE=0`，B 完全未设置。每腿 `binaries.txt` 都记录了
容器环境与 `/proc/<pid>/environ` 探测结果：

```text
A: container PERI_DEFERRED_SESSION_CLOSE=0; platform process env: PERI_DEFERRED_SESSION_CLOSE=0
B: container PERI_DEFERRED_SESSION_CLOSE=<unset>; platform process env: unset
```

## C1 Gate 0：机制

| 腿 | 开关 | deferred 键 | enqueued | pending_max | operation_settle | retry_total |
|---|---|---|---:|---:|---|---:|
| gate0/a | explicit-0 | False | 0 | 0 | True | 0 |
| gate0/b | unset-default | True | 5 | 1 | True | 0 |

- **PASS**：B（默认）发布 `deferred_session_close` 且 `enqueued_total=5>0`；
  A（显式 0）前后都没有该键；两臂 `operation_settle.retry_total=0`。
- 34 条测量腿也都发布 `operation_settle`。其中 **1 条持续 c8 B 腿**（`p2/c8/r1-b2`）
  出现 `retry_total=33`、`retry_success_total=8`、`exhausted_total=0`、
  `refused_total={}`、`errors_total={}`：这是本地结算队列满后的有界重试路径，
  不是 502/`outcome_unknown`，不违反 C1（C1 的 `retry_total=0` 针对 Gate 0 短腿）。

## C2 P1：短版交错主指标（30 triples）

命令：

```bash
cd docs/performance-review/2026-11-05-r72-deferred-close-default-ab
./run_interleaved.sh p1
python3 analyze_interleaved.py --campaign p1 --root .
```

运行：`valid_triples=30/30`、`void_triples=0`、`failed_blocks=0`、`blocks=90`、
measured requests 1800（A 1,200、B 600），另有 2 个不计入统计的 warmup 请求。
两栈 `driver_exit=0`、`settle_exit=0`、`stop_exit=0`。

| 块 | n | 中位 ms | 均值 ms | p95 ms | min | max |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 30 | 4.0813 | 3.8172 | 4.9777 | 2.1009 | 4.9950 |
| A2 | 30 | 3.9509 | 3.9100 | 5.1385 | 2.2743 | 5.3578 |
| B | 30 | 3.1245 | 3.0871 | 4.1663 | 1.9240 | 4.2785 |

- `B / mean(A1,A2)` 中位 **0.770102**（bootstrap 95% CI **0.695077–0.834904**，上界 <1.0），
  ≤0.90 → **C2 PASS**。
- 方向：30 个 triples 中 **24 个 B 更快**、6 个不更快（≥20 的门槛满足）。
- 配对差值中位 **-0.933768 ms**（CI -1.178368–-0.643512）。
- 上下文（非判据）：A2/A1 零对照中位 0.978294，CI 0.844517–1.207982。

### 附录：P1 逐 triple 原始比值

| triple | order | A1 ms | A2 ms | B ms | A2/A1 | B/mean(A1,A2) | B faster |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | ABA | 3.1630 | 5.1385 | 2.8619 | 1.62456 | 0.68948 | yes |
| 2 | AAB | 4.6507 | 4.1361 | 3.6903 | 0.88935 | 0.83997 | yes |
| 3 | BAA | 4.0749 | 3.3448 | 3.0786 | 0.82084 | 0.82984 | yes |
| 4 | ABA | 3.3258 | 5.1328 | 3.1786 | 1.54332 | 0.75158 | yes |
| 5 | BAA | 2.9415 | 3.5624 | 2.5962 | 1.21108 | 0.79835 | yes |
| 6 | AAB | 4.5203 | 3.2048 | 3.3600 | 0.70897 | 0.86991 | yes |
| 7 | ABA | 2.7261 | 4.2157 | 2.4320 | 1.54639 | 0.70067 | yes |
| 8 | BAA | 4.3319 | 3.0715 | 4.2785 | 0.70904 | 1.15583 | no |
| 9 | ABA | 2.5644 | 3.9169 | 1.9240 | 1.52745 | 0.59370 | yes |
| 10 | BAA | 4.0877 | 4.2734 | 2.4515 | 1.04543 | 0.58641 | yes |
| 11 | BAA | 4.2292 | 2.7945 | 3.0574 | 0.66077 | 0.87060 | yes |
| 12 | AAB | 3.9823 | 3.4697 | 3.9725 | 0.87126 | 1.06615 | no |
| 13 | BAA | 2.3343 | 4.3337 | 1.9919 | 1.85656 | 0.59744 | yes |
| 14 | AAB | 3.4634 | 3.9849 | 2.6523 | 1.15057 | 0.71219 | yes |
| 15 | BAA | 4.1774 | 3.5108 | 2.5920 | 0.84042 | 0.67430 | yes |
| 16 | ABA | 2.1009 | 4.9108 | 2.8210 | 2.33746 | 0.80465 | yes |
| 17 | BAA | 3.3942 | 3.1881 | 2.5483 | 0.93927 | 0.77428 | yes |
| 18 | AAB | 3.7693 | 4.7300 | 3.2549 | 1.25487 | 0.76592 | yes |
| 19 | ABA | 2.7183 | 3.2752 | 3.7474 | 1.20488 | 1.25048 | no |
| 20 | ABA | 4.6249 | 4.6669 | 3.1705 | 1.00908 | 0.68242 | yes |
| 21 | AAB | 4.3796 | 3.1723 | 4.1663 | 0.72434 | 1.10337 | no |
| 22 | ABA | 4.4453 | 2.6532 | 3.7205 | 0.59686 | 1.04825 | no |
| 23 | ABA | 2.6801 | 2.2743 | 3.2370 | 0.84861 | 1.30671 | no |
| 24 | AAB | 4.7367 | 4.6936 | 3.8424 | 0.99089 | 0.81490 | yes |
| 25 | AAB | 4.9950 | 3.3126 | 2.4005 | 0.66318 | 0.57791 | yes |
| 26 | AAB | 4.8077 | 3.5377 | 2.8655 | 0.73585 | 0.68673 | yes |
| 27 | ABA | 3.2101 | 4.5316 | 3.5929 | 1.41165 | 0.92820 | yes |
| 28 | BAA | 4.9777 | 4.1709 | 3.4696 | 0.83793 | 0.75850 | yes |
| 29 | BAA | 4.1981 | 5.3578 | 2.4014 | 1.27624 | 0.50259 | yes |
| 30 | AAB | 4.9042 | 4.7359 | 3.2582 | 0.96570 | 0.67597 | yes |

## C3 F1 c1

命令：`./run_p2.sh f1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`；
每腿 1 warmup + 125 timed，2 轮 × A/B/B/A，逐轮交替顺序（第 1 轮 A/B/B/A，第 2 轮 B/A/A/B）。

| 腿 | 侧 | p50 ms | p95 ms | 样本 | 失败 |
|---|---|---:|---:|---:|---:|
| r1-a1 | a | 1.7546 | 4.8807 | 125 | 0 |
| r1-b1 | b | 0.6260 | 3.7932 | 125 | 0 |
| r1-b2 | b | 0.9464 | 4.2186 | 125 | 0 |
| r1-a2 | a | 1.8854 | 5.3904 | 125 | 0 |
| r2-a1 | a | 1.8247 | 3.9421 | 125 | 0 |
| r2-b1 | b | 1.0461 | 4.4845 | 125 | 0 |
| r2-b2 | b | 0.8322 | 3.0069 | 125 | 0 |
| r2-a2 | a | 1.7689 | 5.1139 | 125 | 0 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 1.796800 | 0.889300 | **0.49494** | 0.43198 | 0.52268 | PASS |
| p95 ms | 4.997300 | 4.005900 | **0.80161** | 0.78003 | 0.82723 | PASS |

**C3 PASS**：p50 0.49494、p95 0.80161，均 <1.10。

## C4 L1 c1（FAIL）

命令与形状同 C3，场景为 `L1.list-20`。

| 腿 | 侧 | p50 ms | p95 ms | 样本 | 失败 |
|---|---|---:|---:|---:|---:|
| r1-a1 | a | 1.9416 | 3.6387 | 125 | 0 |
| r1-b1 | b | 1.8498 | 4.5808 | 125 | 0 |
| r1-b2 | b | 1.2642 | 4.7138 | 125 | 0 |
| r1-a2 | a | 1.8512 | 3.7062 | 125 | 0 |
| r2-a1 | a | 1.8838 | 4.1181 | 125 | 0 |
| r2-b1 | b | 1.3959 | 4.6105 | 125 | 0 |
| r2-b2 | b | 1.2025 | 3.3295 | 125 | 0 |
| r2-a2 | a | 2.5147 | 5.4342 | 125 | 0 |

| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |
|---|---:|---:|---:|---:|---:|---|
| p50 ms | 1.912700 | 1.330000 | **0.69538** | 0.82103 | 0.59075 | PASS |
| p95 ms | 3.912200 | 4.595700 | **1.17471** | 1.26545 | 0.83121 | FAIL |

**C4 FAIL**：p50 通过（0.69538），但合并 p95 B/A = **1.17471 > 1.10**。
两轮 p95 比值方向相反：r1 = 1.26545（B 更慢）、r2 = 0.83121（B 更快）；
合并值按冻结规则取每侧 4 腿 p95 的中位数，点估计超过阈值。

上下文（非判据）：对 4 条腿做 bootstrap（10,000 次，seed 20261105），
p95 B/A 的 95% 区间约 **0.81–1.27**；对每条腿的 125 个样本重采样，
区间约 **0.85–1.32**。区间很宽，但预冻结判据没有 CI 条款，
因此按点估计判 FAIL；不据此宣称 p95 一定退化，也不据此改写判据。

口径敏感性（非判据）：若把每侧 500 个计时样本直接合并后取 p95，
A=4.6742、B=4.5808，比值 **0.98**（<1.10）；但 R71d 冻结口径是
「每侧 4 条腿 p95 的中位数」，本轮按该口径判 FAIL。
不得在看到数据后切换聚合口径；该对比只用于说明点估计对口径敏感。

8 条 L1 腿全部有效：125/125 timed 请求、0 失败、driver exit 0、
审计 delta 384（A/B 相同）、响应形状一致（mode 15,627 bytes / rows 40）。
因此这不是无效腿或环境异常，不能以“换卷重跑”处理。

## C5 标准 X1 c8

命令：`./run_p2.sh x1 ...`，每腿 `run.py --scenario X1 --concurrency-requests 100`，
levels 1/4/8/16。c16 的默认配额 429 是已知诊断，不作为判据；c8 必须零 429。

| 腿 | 侧 | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c16 429 |
|---|---|---:|---:|---:|---:|---:|
| r1-a1 | a | 1449.3 | 4.68 | 11.16 | 0 | 29 |
| r1-b1 | b | 2439.0 | 2.75 | 5.25 | 0 | 65 |
| r1-b2 | b | 1694.9 | 3.12 | 11.10 | 0 | 38 |
| r1-a2 | a | 2040.8 | 2.74 | 9.52 | 0 | 41 |
| r2-a1 | a | 1388.9 | 3.29 | 16.85 | 0 | 26 |
| r2-b1 | b | 1886.8 | 3.92 | 7.52 | 0 | 45 |
| r2-b2 | b | 1724.1 | 4.10 | 9.18 | 0 | 52 |
| r2-a2 | a | 1960.8 | 2.98 | 8.68 | 0 | 46 |

**C5 PASS**：c8 吞吐合并 B/A = **1.05891**（A 1705.0、B 1805.5 qps），>0.90；c8 每腿 429=0。

## C6 持续 c8

命令：`./run_p2.sh c8 ...`；每腿 8 客户端 × 500 pipeline 请求（100 warmup + 400 timed），
200 请求/块、块间 1.2 s 配额回填。

| 腿 | 侧 | 吞吐 qps | p50 ms | p95 ms | 失败 | B enqueued/inline/pending_max |
|---|---|---:|---:|---:|---:|---|
| r1-a1 | a | 1388.4 | 3.4466 | 17.1082 | 0 | — |
| r1-b1 | b | 1600.5 | 3.2726 | 12.9797 | 0 | 3900/108/4 |
| r1-b2 | b | 1603.5 | 2.8584 | 12.9187 | 0 | 3898/110/4 |
| r1-a2 | a | 1356.8 | 3.7264 | 17.0819 | 0 | — |
| r2-a1 | a | 1342.3 | 3.9126 | 17.2782 | 0 | — |
| r2-b1 | b | 1664.4 | 3.0941 | 11.9734 | 0 | 3902/106/4 |
| r2-b2 | b | 1650.1 | 2.9815 | 13.1554 | 0 | 3875/133/4 |
| r2-a2 | a | 1500.7 | 3.2620 | 13.6118 | 0 | — |

**C6 PASS**：吞吐合并 B/A = **1.18520**，>0.90；8/8 腿校准、warmup、计时失败均为 0。
p50/p95 只作诊断：p50 B/A 0.84701、p95 B/A 0.75748。

## C7 不变量

- **零异常**：36 个容器日志（Gate0 2 + P1 2 + P2 32）中 `outcome unknown`、` 502`、
  `audit_unavailable`、`session_moved` 命中数均为 **0**。34 条测量腿的 `errors_total`
  没有 `outcome_unknown`/502/503；X1 只有 c16 的 `quota_exceeded`/`rate_limit` 诊断。
- **资源归零**：34/34 腿结算后 `session_seats=0`、`active_permits=0`、
  `unknown_held_permits=0`、worker sessions=0、worker `pending_close=0`、
  cleanup ledger `pending_close=0`。
- **审计链**：34/34 腿 `chain_ok=true`、head 匹配 tail。分组条目数一致：
  P1 A 3,603 / B 1,803（比值 1.9983，请求比 2.0）；F1/L1 每腿 384；
  持续 c8 每腿 12,024；X1 的审计增量与成功动作数可解释。
- **响应形状**：F1 8/8 腿 mode `312 bytes / 1 row`；L1 8/8 腿 mode
  `15,627 bytes / 40 rows / 201 rows_read`，与 R71d 的 15,627 一致。
- **deferred 上限**：B 腿 `pending_max` 最大 4（每库上限 4），满足 `≤4`；
  `failed_total=0`、`retried_total=0`、`unresolved_total=0`、`pending=0`。

| 检查 | 结果 |
|---|---|
| P3 34/34 腿 | PASS |
| 响应字节/行数 | PASS |
| 日志扫描 | PASS |

## C8 P3：34/34

`python3 collect_invariants.py .`：**34/34 腿通过**（P1 2/2；F1 8/8；L1 8/8；
标准 X1 8/8；持续 c8 8/8）。逐腿检查项：A 腿 13 项、B 腿 18 项；
34 腿的 driver/settle/stop 退出码均符合预期；`launcher-stop` 为 `outcome=clean`、
`unresolved_operations=false`、`docker stop` 退出码 0。

## C9 崩溃语义（引用 R71 Gate 3）

- **状态：引用，未重跑。** R71 Gate 3 T1–T9 在显式启用的同一机制上 PASS；
  T2（丢弃 close 耐久记录）得到预期的 fail-closed：重启隔离命名空间、
  Hrana `SELECT 1` 返回 HTTP 409 `database_isolated`、`verify_no_unresolved_operations`
  拒绝 clean restart，受支持的 release 路径恢复 200 且不删除 ownership marker。
- **代码路径一致性**：探针分支 `probe/r71-g3-fault-injection` @ `090aad5`，
  探针平台 SHA256 `5c3e8f2a9a290620de39d56ffcb02292381ba0616873efa04f03cfa0b9c71152`
  （已核对）。`git diff probe/r71-g3-fault-injection 56dc914 --
  prototypes/v1/platform/session_close_deferred.go` 只有默认值翻转与注释差异；
  探针分支其余平台差异是 4 个调用点 hook + `probe_r71_g3.go`，仅在
  `PERI_R71_G3_*` 设置时生效。显式 `=1` 且不设探针变量时，探针二进制执行与
  R72 B 臂相同的 deferred close 路径。完整 diff 见 `c9-code-path-check.txt`。

## 失败腿、异常与重跑

- **无效腿：无。** 34/34 测量腿有效；无 502/`outcome_unknown`/审计不可用/启动失败，
  因此没有换卷重跑的腿。
- **唯一 FAIL 是 C4 的 L1 p95 点估计**，不是环境异常或无效腿；没有为通过而重跑。
- **Gate 0 之外的结算重试**：`p2/c8/r1-b2` 的 `operation_settle.retry_total=33`、
  `retry_success_total=8`、`exhausted_total=0`、`refused_total={}`、`errors_total={}`；
  请求全部成功、审计链连续、clean stop。这是有界重试路径，不是 502。

## 验证命令与结果

```bash
cd prototypes/v1/platform
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o /tmp/peri-r72-ab/bin/peri-platform-r72 ./cmd/peri-platform
go vet ./...
PERI_RUST_BINARY=/Users/kk/code/peri-loom/prototypes/v1/target/debug/peri-loom-v1 go test ./... -count=1
gofmt -l <changed files>   # 无输出
```

构建产物 SHA256：`e07e6da8…c621ba`；`go vet` 通过；`gofmt` 无输出。

完整 Go 测试套件（`PERI_RUST_BINARY=prototypes/v1/target/debug/peri-loom-v1 go test ./... -count=1`）：

```text
ok  	peri-loom/platform	185.809s
ok  	peri-loom/platform/cmd/peri	1.710s
ok  	peri-loom/platform/cmd/peri-platform	1.042s
ok  	peri-loom/platform/sdk/go	2.521s
ok  	peri-loom/platform/sqlcontract	2.457s
exit=0
```

针对性延迟关闭测试 `go test . -run 'TestDeferredSessionClose' -count=1` 全部 PASS，
包括 `unset/empty 启用`、`显式 0 关闭`、`显式 1 启用`、`非法值拒绝`。

## 环境与限制

- 单机 macOS 26.5.1 / arm64、Docker 29.4.0；不是容量或 SLA 证明。
- C4 的 p95 点估计失败，但 bootstrap 区间宽、两轮方向相反；报告保留这一事实，
  不据此宣称稳定退化，也不放宽判据。
- C9 引用 R71 Gate 3，未做真实掉电/机器崩溃；T2 是确定性故障注入。
- 观测器全程关闭；未开启 `PERI_DURABILITY_OBSERVER` 或相位观测器。

## 证据清单

```text
criteria.json                 冻结判据
criteria-freeze.txt           冻结时间与 SHA256
environment.txt               构建/镜像/worker/配额/端口
raw.jsonl                     P1 的 90 个块原始记录（triples 在 legs/p1/driver/）
summary.json                 C1–C9 机器可读结论
metrics.json                 36 条腿的合并指标视图
analysis-p1.json             P1 统计
analysis-p2.json             F1/L1/X1/c8 统计
invariants.json              P3 34/34
gate0.json                   C1 + operation_settle 检查
response-bytes.json          F1/L1 响应形状
log-scan.json                36 个容器日志扫描
c9.json / c9-code-path-check.txt  C9 引用与代码路径一致性
go-test.txt                  完整 Go 测试套件输出
go-test-deferred-close.txt   延迟关闭定向测试输出（含开关解析）
go-vet.txt / gofmt.txt       go vet 与 gofmt 输出
l1-p95-bootstrap.txt         C4 p95 的上下文 bootstrap（非判据）
run_gate0.sh / gate0_driver.py / check_gate0_r72.py
run_interleaved.sh / interleaved_driver.py / analyze_interleaved.py
run_p2.sh / run_all_p2.sh / sustained_c8_driver.py / analyze_p2.py
collect_invariants.py / verify_audit.py / wait_settled.py / snap_metrics.py
check_response_bytes.py / check_logs.py / collect_metrics_summary.py / make_summary_r72.py
l1_p95_bootstrap.py / make_readme.py
legs/                         每腿 leg.txt/binaries.txt/metrics-*/audit-verify/harness/
```

连接文件与私有 API Key 不在证据目录；容器/卷保留在 Docker 中，未 prune、未删除。

