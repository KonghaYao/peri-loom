# R73：v1.2.8 发布级 endpoint A/B 独立验证报告

对比对象：A = v1.2.7（发布源码 `640ae7e3ce035f1d2b02d9367dc6aa755a018789`），
B = `main` @ `03d9b87fe687ac2ea22c72548741bf5fc22e01b5`。
性质：独立验证；未修改产品代码/测试，未提交 git，未删除卷/证据/分支，未触碰生产或其他容器。

## 1. 判据路径、SHA256 与冻结时间

| 项 | 值 |
|---|---|
| 冻结判据 | `/tmp/r73-evidence/criteria.json` |
| SHA256 | `77bdd4bc56ecfdceba1f5d5346252c389b59b2aab4e6af1fb4df38b3d7dca2af` |
| 冻结时间（UTC） | `2026-11-05T09:16:59Z` |
| 冻结记录 | `/tmp/r73-evidence/criteria-freeze.txt`（记录时间 `09:17:20Z`） |
| 冻结时是否已有 R73 测量腿 | 否；只有平台/worker 构建产物、SHA 复现和 T1 探针编译 |

**路径偏差说明（重要）**：任务指定的证据目录是
`docs/performance-review/2026-11-05-r73-v1.2.8-release-ab/`。本会话的验证角色写入边界只允许
`.peri/plans/`，因此全部证据保存在 `/tmp/r73-evidence/`，本报告保存在 `.peri/plans/`；
仓库保持 `git status --porcelain` 为空。需要入库时执行：

```bash
mkdir -p docs/performance-review/2026-11-05-r73-v1.2.8-release-ab
cp -R /tmp/r73-evidence/. docs/performance-review/2026-11-05-r73-v1.2.8-release-ab/
```

冻结后未修改判据、配额、样本量或统计口径；有效腿未因结果不理想而重跑或换口径。

## 2. 构建信息

| 角色 | 源码 commit | 平台 SHA256 | worker SHA256 |
|---|---|---|---|
| A（v1.2.7） | `640ae7e3ce035f1d2b02d9367dc6aa755a018789` | `95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912` | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| B（main） | `03d9b87fe687ac2ea22c72548741bf5fc22e01b5` | `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` | `8e62601274ca06a6503ba6eb41c4f20c358b4432c928f456970372e1e21bbd5d` |

- A 平台/worker：v1.2.7 发布证据中的同 SHA 二进制；`go version -m` 显示
  `go1.27.3`、`CGO_ENABLED=0`、`vcs.revision=640ae7e`、`vcs.modified=false`；
  worker 由 `rust:1.98-bookworm`（rustc 1.98.1）构建。
- B 平台：从 `56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6` 加未跟踪标记复现任务预期 SHA
  `e07e6da8…`；`git diff 56dc914..03d9b87` 在 `prototypes/v1/platform`、`prototypes/v1/src`、
  `prototypes/v1/controller`、`scripts` 上为空，因此平台源码与 main 等价（差异仅在嵌入的
  VCS 元数据）。干净 main 重建为 `88aa249f0e2a3cc4dc310b6800d2152a344427bb77bc0beada015c102f746c99`，
  行为等价，作为交叉核对保留。
- B worker：`rust:1.98-bookworm`（rustc 1.98.1）、
  `RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`、`CARGO_HOME=/cargo-home`、
  `cargo build --release --locked --offline --bin peri-loom-v1`，从 main 的只读挂载构建；
  二进制大小 32,371,720 bytes（v1.2.7 为 32,302,296），含 PR #33 的执行前证据包装变更。
- worker 血缘：B worker 二进制包含 PR #33 的 `not_executed` 符号字符串（9 次），A worker 为 0，
  证明 B 确实从 main 的 `dispatch.rs` 构建。
- 运行镜像：`ghcr.io/konghayao/peri-loom:v1.2.7`，
  digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`；
  两侧同一镜像，只挂载覆盖平台/worker 二进制。
- 启动器：两侧同为 `prototypes/v1/platform/dev.py`，
  SHA256 `80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2`。
- T1 探针平台：main + `probe/r71-g3-fault-injection` @ `090aad5` cherry-pick 到 `03d9b87`，
  SHA256 `831d647c052a817e374cae87ea75a906afcbb384adf12b8ab35ca753cc18ac1f`；
  启动器用 R71 `dev-probe.py`，SHA256 `74ef196b764371acba0c9571c64946e5811156d46bc8122edada32dc228f975d`。

**镜像二进制血缘发现（未闭合项）**：v1.2.7 镜像自带的 `peri-platform` 是
`go1.23.12`、`CGO_ENABLED=1` 构建，SHA256 `3286df64…`；镜像自带 worker 为 `9e9f2fd6…`。
它们与发布证据/本轮 A 臂使用的本地构建（Go 1.27.3 / CGO=0，`95716e4d…` / `b17a0708…`）
不同。本轮按任务给定 SHA 与 v1.2.7 发布报告的口径使用本地重建二进制，镜像自带二进制不参与测量。
本 PASS 因此证明“v1.2.7 源码构建 vs main 源码构建”，不是镜像内二进制的直接性能证明；
v1.2.8 发布前应确认镜像构建口径或对镜像自带二进制补 smoke。

## 3. 逐场景结果

环境：Docker 29.4.0（OrbStack，linux/arm64），`--network host`，无 TLS，观测器关闭
（dev.py 将三个开关显式设为 `0`），默认配额（`max_sessions=16`、`max_concurrent=100`、
`requests_per_second=500`、`max_databases=8`、`max_sql_bytes=65536`、
`max_result_bytes=4194304`），每腿全新卷并重新导入 fixture
（`threads=201 messages=8625 chars=20177069 focus=8625`）。

### 3.1 F1.select-1（json-pipeline，c1，8 对 × 125 timed，1000 样本/侧）

| pair | A leg p50 ms | B leg p50 ms | B/A |
|---|---:|---:|---:|
| p1 | 1.0530 | 0.9636 | 0.9151 |
| p2 | 1.9834 | 0.6738 | 0.3397 |
| p3 | 1.6690 | 0.6231 | 0.3733 |
| p4 | 1.7110 | 0.6715 | 0.3925 |
| p5 | 2.0340 | 0.7027 | 0.3455 |
| p6 | 0.9404 | 1.1640 | 1.2378 |
| p7 | 1.4651 | 0.7261 | 0.4956 |
| p8 | 1.6124 | 0.7585 | 0.4704 |

- 逐对 p50 比值中位 **0.43146**（≤0.90），bootstrap 95% CI **[0.34547, 0.91512]**；
  8 对中 7 对 B 更快。p6 是唯一反向对（+23.8%），但其余 6 对为 0.34–0.50，单对不构成可复现回归。
- 合并 1000 样本/侧：A p50 1.5478 / p95 5.7272 ms；B p50 0.7186 / p95 4.0220 ms；
  **p95 比值 0.70225**（≤1.10），CI **[0.63833, 0.81075]**；p50 比值 0.46430（诊断）。
- **PASS**。

### 3.2 L1.list-20（json-pipeline，c1，8 对 × 125 timed，1000 样本/侧）

| pair | A leg p50 ms | B leg p50 ms | B/A |
|---|---:|---:|---:|
| p1 | 2.0000 | 1.3313 | 0.6657 |
| p2 | 2.1386 | 1.5248 | 0.7130 |
| p3 | 3.0513 | 1.6730 | 0.5483 |
| p4 | 1.8511 | 1.1760 | 0.6353 |
| p5 | 2.0679 | 1.4698 | 0.7108 |
| p6 | 2.1395 | 1.1728 | 0.5482 |
| p7 | 1.9783 | 1.4371 | 0.7264 |
| p8 | 2.0194 | 1.3400 | 0.6635 |

- 逐对 p50 比值中位 **0.66461**（≤0.90），CI **[0.59174, 0.71298]**；8/8 对 B 更快。
- 合并 1000 样本/侧：A p50 2.0377 / p95 5.2710 ms；B p50 1.3577 / p95 3.8840 ms；
  **p95 比值 0.73686**（≤1.10），CI **[0.68485, 0.79194]**；p50 比值 0.66632。
- 响应模态 15,627B / 40 rows / rows_read=201 在 16 条腿一致。
  R72 的 C4 不一致（合并 1.17471 vs R72c 0.717）在本轮 8 对中未复现；本轮 8/8 对均 <1.0。
- **PASS**。

### 3.3 X1.concurrency（json-pipeline，2 轮 × A/B/B/A，100 请求/级）

| leg | side | round | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c8 errors | c16 429（诊断） |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| r1-a1 | a | r1 | 2564.1 | 2.31 | 5.50 | 0 | 0 | 22 |
| r1-b1 | b | r1 | 1785.7 | 3.16 | 10.46 | 0 | 0 | 47 |
| r1-b2 | b | r1 | 1538.5 | 4.27 | 10.85 | 0 | 0 | 54 |
| r1-a2 | a | r1 | 1388.9 | 4.97 | 10.91 | 0 | 0 | 26 |
| r2-b1 | b | r2 | 2439.0 | 2.93 | 5.35 | 0 | 0 | 37 |
| r2-a1 | a | r2 | 1886.8 | 3.34 | 10.21 | 0 | 0 | 46 |
| r2-a2 | a | r2 | 1785.7 | 3.60 | 8.17 | 0 | 0 | 24 |
| r2-b2 | b | r2 | 2325.6 | 3.07 | 5.90 | 0 | 0 | 76 |

- c8 合并：A 中位 **1836.25 qps**，B 中位 **2055.65 qps**，**B/A = 1.11948**（≥0.90），bootstrap 95% CI **[0.74686, 1.52091]**（4 腿/侧重采样；
  下界 <0.90，非回归结论不具统计强度，仅按冻结点估计判 PASS）。
  轮内比值 r1 **0.84093**、r2 **1.29737**；100 请求/级的 elapsed 仅 39–72ms，方差大，
  按冻结口径使用合并值，不做单轮结论，也不宣称每一轮都无回归。
- c1/c4/c8 全部 429=0、errors=0；c16 的 429 是默认配额已知诊断项，不用于豁免 c8。
  driver exit=1 由 c16 判定触发，按冻结判据视为有效腿。
- **PASS**。

### 3.4 持续 c8（json-pipeline，8 客户端 × 500/腿，100 warmup + 400 timed）

| leg | side | round | timed | qps | p50 ms | p95 ms | failures |
|---|---|---:|---:|---:|---:|---:|---:|
| r1-a1 | a | r1 | 3200 | 1457.2 | 3.323 | 14.99 | 0 |
| r1-b1 | b | r1 | 3200 | 1523.4 | 2.875 | 14.35 | 0 |
| r1-b2 | b | r1 | 3200 | 1387.8 | 2.857 | 16.86 | 0 |
| r1-a2 | a | r1 | 3200 | 1429.9 | 3.253 | 15.64 | 0 |
| r2-b1 | b | r2 | 3200 | 1583.1 | 2.686 | 13.56 | 0 |
| r2-a1 | a | r2 | 3200 | 1428.8 | 3.259 | 15.16 | 0 |
| r2-a2 | a | r2 | 3200 | 1385.1 | 3.184 | 15.89 | 0 |
| r2-b2 | b | r2 | 3200 | 1557.0 | 2.966 | 14.14 | 0 |

- 合并：A 中位 **1429.34 qps**，B 中位 **1540.19 qps**，**B/A = 1.07755**（≥0.90），bootstrap 95% CI **[0.97093, 1.12406]**；
  轮内 r1 1.00834、r2 1.11590。8 腿各 3,200 timed、0 失败、0 warmup/calibration 失败、
  0 worker error。
- **PASS**。

### 3.5 W1.insert（json-pipeline，c1，8 腿 × 20 timed）

| 有序对 | A leg | B leg | B/A |
|---|---|---|---:|
| pair1 | r1-a1 18.638 | r1-b1 15.928 | 0.8546 |
| pair2 | r1-a2 18.411 | r1-b2 23.882 | 1.2972 |
| pair3 | r2-a1 24.825 | r2-b1 18.201 | 0.7332 |
| pair4 | r2-a2 16.479 | r2-b2 18.727 | 1.1364 |

- p50 逐对（B/A）中位 **0.99552**（≤1.10），bootstrap 95% CI **[0.73315, 1.29718]**。
  每腿 20 timed、0 错误、每样本 `affected_row_count=1`。
- 方向在同一轮内混合（pair1/pair3 B 更快 0.8546/0.7332；pair2/pair4 B 更慢 1.2972/1.1364），
  p50 本身在 15.9–24.8ms 波动；按冻结口径 PASS，
  不宣称写路径有稳定改善。
- 写会话不走延迟 close：B 腿 `deferred_session_close.enqueued_total=0` 是任务判据明确的预期。
- **PASS**。

### 3.6 H1.content-20mb（可选，json-cursor，c1，8 腿 × 3 timed）

| 有序对 | A leg p50 ms | B leg p50 ms | B/A |
|---|---:|---:|---:|
| pair1 | 59.72 | 61.88 | 1.0362 |
| pair2 | 61.05 | 61.02 | 0.9996 |
| pair3 | 64.46 | 60.89 | 0.9446 |
| pair4 | 61.58 | 62.60 | 1.0166 |

- p50 逐对（B/A）中位 **1.00802**（≤1.10），bootstrap 95% CI **[0.94456, 1.03624]**；
  每腿 3 timed、0 错误、8,625 行、
  响应模态 20,608,525 bytes。pair1/pair4 B 略慢（+3.6%/+1.7%），pair2/pair3 B 略快
  （−0.04%/−5.5%），低于阈值；
  每腿仅 3 个样本，只作非劣检查，不作容量或尾延迟结论。
- **PASS**。

## 4. 不变量核对

64 条 campaign 腿（F1 16 + L1 16 + X1 8 + c8 8 + W1 8 + H1 8）全部有效：

| 检查 | 结果 |
|---|---|
| 平台/worker SHA 与冻结值 | 64/64 一致（A `95716e4d`/`b17a0708`；B `e07e6da8`/`8e626012`） |
| 镜像 digest / launcher SHA | 64/64 一致（`sha256:8a8ba43b…` / `80bcab2…`） |
| 观测器 | 64/64 平台进程均为 `PERI_DURABILITY_OBSERVER=0`、`PERI_SQL_OPEN_OBSERVER=0`、`PERI_COMPLETION_JOURNAL=0` |
| `outcome_unknown` / 502 / 503 / `session_moved` / `audit_unavailable` | 0 条（容器日志扫描 + metrics 增量） |
| 审计链连续、head 匹配 tail | 64/64 |
| 结束 `session_seats` / `active_permits` / `unknown_held_permits` | 64/64 全 0 |
| worker sessions / pending close / cleanup ledger | 64/64 全 0 |
| launcher-stop | 64/64 `outcome=clean`、`reason=terminal_proof_written`、`unresolved_operations=false` |
| docker stop exit | 64/64 = 0 |
| 响应形状 | F1 模态 312B/1 row；L1 模态 15,627B/40 rows/rows_read=201；F1/L1 32 腿一致（`RESPONSE-SHAPE: PASS`） |
| A/B 开关 | A 无 `deferred_session_close` 键；B 默认发布且 settle 后 `pending=0`、`failed_total=0`、`unresolved_total=0` |
| 写会话豁免 | W1 B 腿 `enqueued_total=0` 为预期；复用 R72 collector 因此报 4 条 “flag on enqueues closes”，属口径差异，不是缺陷 |

## 5. T8 / T1（发布阻断 smoke，不参与性能判决）

### T8：显式 `PERI_DEFERRED_SESSION_CLOSE=0`

| leg | 平台进程 env | deferred 键 | immediate enqueued | immediate pending | worker sessions | cleanup pending |
|---|---|---|---:|---:|---|---:|
| t8-b1 | `PERI_DEFERRED_SESSION_CLOSE=0` | 无 | — | — | worker-a/b = 0 | 0 |
| t8-b2（对照） | unset（默认） | 有 | 6 | 0 | worker-a/b = 0 | 0 |

显式 0 时无指标键、close 走同步路径（无 deferred 账户）；默认时键存在且 6 个 close 入账。
**PASS**。

### T1：2s close 延迟 + SIGTERM 优雅 drain

- 平台：main + `090aad5` cherry-pick，SHA `831d647c…`；启动器 `dev-probe.py`（`74ef196…`）。
- 注入：`PERI_R71_G3_CLOSE_DELAY_MS=2000`；4 个并发 Hrana pipeline `[SELECT 1, close]`。
- 结果（t1-b4）：burst 4/4 HTTP 200（≈11.8ms）；stop 前 metrics
  `enqueued_total=4, pending=4, pending_max=4, failed_total=0, unresolved_total=0`；
  `docker stop` 用时 **2.147s**，平台 stage `waited_ms=1916.7`；
  launcher-stop `clean / terminal_proof_written / unresolved_operations=false`，stop exit 0；
  审计 464 条连续、head 匹配，close 记录 5 条（1 条 seed + 4 条 burst）；
  0 条 `deferred session close unresolved`。
- 过程记录：t1-b1 因脚本 case 分支错误在 driver 前失败；t1-b2 用 stock `dev.py` 未转发探针 env，
  注入未生效（pending=0、stop 0.324s）；t1-b3/t1-b4 用 `dev-probe.py` 后注入生效。
  失败尝试保留在 `legs/smoke/t1/`，未删除。
- **PASS**。

## 6. 判决

**判决：PASS（冻结判据口径）。** 全部必测场景满足阈值；64 条 campaign 腿全部有效；
T8、T1 release-blocking smoke 均通过。

未闭合项（不改变本判决，但发布前应处理/知悉）：

1. **镜像二进制血缘**：v1.2.7 镜像内平台为 Go 1.23.12 / CGO=1（`3286df64…`），
   worker 为 `9e9f2fd6…`；本轮 A 臂为 Go 1.27.3 / CGO=0 的 `95716e4d…` / `b17a0708…`。
   v1.2.8 若仍用 `golang:1.23-bookworm` + `CGO_ENABLED=1` 构建，则该镜像的实际二进制
   未被本轮性能数据直接覆盖。
2. X1 c8 的 100 请求/级短样本方差大：r1 B/A 0.841、r2 1.297，合并 1.119，
   bootstrap CI [0.747, 1.521]（下界 <0.90）；合并口径 PASS，但不宣称每一轮都无回归，
   也不宣称 c8 非回归具有统计强度。
3. W1 方向在同一轮内混合（2 对 B 更快、2 对 B 更慢），中位 0.9955；按非劣判据 PASS，
   不宣称写路径有稳定改善。
4. H1 2/4 对 B 略慢（中位 1.0080），低于 1.10；每腿仅 3 样本，只作非劣检查。
5. 本报告是受控单机 A/B，不是容量/SLA 证明。

## 7. 证据目录与文件清单

实际证据目录：`/tmp/r73-evidence/`（请求的 `docs/performance-review/...` 因验证角色写入边界未创建；
见第 1 节复制命令）。

| 文件/目录 | 内容 |
|---|---|
| `criteria.json` / `criteria-freeze.txt` | 冻结判据、SHA256、冻结时间 |
| `environment.txt` | 宿主、Docker、镜像、二进制、配额、端口、卷 |
| `build/` | `platforms.txt`（各平台 `go version -m` 与 SHA）、`toolchains.txt`、`worker-build.log` |
| `image-binaries/` | 从 v1.2.7 镜像提取的二进制（`3286df64…` / `9e9f2fd6…`）与血缘核对 |
| `legs/p2/{f1,l1,x1,c8,w1,h1}/<leg>/` | 64 条 campaign 腿的 `leg.txt`、`binaries.txt`、`seed.txt`、`metrics-before/after.json`、`driver.txt`、`harness/`、`settle.json`、`container.txt`、`launcher-stop.json`、`audit/`、`audit-verify.json`、`stop-exit.txt`、`stop-seconds.txt` |
| `legs/smoke/t8/`、`legs/smoke/t1/` | T8/T1 证据，含 `metrics-immediate.json`、`burst.json`、`launcher-stop.json`、`audit/`、失败尝试记录 |
| `summary.json` | 全部逐腿统计、pair 比值、CI、verdicts、T8/T1、`release_verdict` |
| `metrics.json` | 每腿 metrics 关键字段（errors/rejections/seats/permits/deferred/audit/SHA/stop） |
| `raw.jsonl` | 聚合逐请求数据（39,464 行） |
| `analysis/` | `analysis.txt`、`analysis-raw.json`、`invariants.txt`、`response-bytes.txt` |
| `README.md` | 完整证据说明、逐腿表、补充观察、复跑命令 |

私有 `connection.json` 已全部移出证据目录到 `/tmp/r73-private-state/`（0700）；
证据目录中 0 个 connection.json，扫描确认没有证据文件包含 API Key。

## 附录：关键检查命令与输出（验证格式）

### Check: 冻结判据完整性
**Command run:**
  `shasum -a 256 /tmp/r73-evidence/criteria.json`
**Output observed:**
  `77bdd4bc56ecfdceba1f5d5346252c389b59b2aab4e6af1fb4df38b3d7dca2af  /tmp/r73-evidence/criteria.json`
**Result: PASS**

### Check: 构建来源与预期 SHA
**Command run:**
  `go version -m /tmp/r73-evidence/bin/peri-platform-56dc914-dirty | grep -E 'vcs|go1'; git diff --stat 56dc914..03d9b87 -- prototypes/v1/platform prototypes/v1/src prototypes/v1/controller scripts`
**Output observed:**
  `go1.27.3`；`vcs.revision=56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6`；`vcs.modified=true`；
  SHA256 `e07e6da8…`；`git diff` 输出为空（编译源码与 main 等价）。
**Result: PASS**

### Check: F1 主判据
**Command run:**
  `python3 /tmp/r73-evidence/analyze_r73.py`
**Output observed:**
  `F1 pair p50 ratios [0.9151, 0.3397, 0.3733, 0.3925, 0.3455, 1.2378, 0.4956, 0.4704]; median 0.43146; pooled p95 0.70225`
**Result: PASS**

### Check: L1 主判据
**Command run:**
  `python3 /tmp/r73-evidence/analyze_r73.py`
**Output observed:**
  `L1 pair p50 ratios [0.6657, 0.7130, 0.5483, 0.6353, 0.7108, 0.5482, 0.7264, 0.6635]; median 0.66461; pooled p95 0.73686`
**Result: PASS**

### Check: X1 c8 吞吐与错误
**Command run:**
  `python3 /tmp/r73-evidence/analyze_r73.py`
**Output observed:**
  `X1 A_median_qps 1836.25, B_median_qps 2055.65, ratio 1.11948; c8 429=0, c8 errors=0; round ratios 0.84093 / 1.29737`
**Result: PASS**

### Check: 持续 c8 吞吐与零失败
**Command run:**
  `python3 /tmp/r73-evidence/analyze_r73.py`
**Output observed:**
  `c8 A_median_qps 1429.34, B_median_qps 1540.19, ratio 1.07755; rounds 1.00834 / 1.11590; all legs 3200 timed, failures=0`
**Result: PASS**

### Check: W1 / H1 非劣
**Command run:**
  `python3 /tmp/r73-evidence/analyze_r73.py`
**Output observed:**
  `W1 median 0.99552 (pairs 0.8546/1.2972/0.7332/1.1364); H1 median 1.00802 (pairs 1.0362/0.9996/0.9446/1.0166)`
**Result: PASS**

### Check: 64 腿审计链
**Command run:**
  `for d in /tmp/r73-evidence/legs/p2/*/*/; do python3 .../verify_audit.py "$d"; done; python3 -c '...'`
**Output observed:**
  `64` 个 `audit-verify.json`；`bad []`；`chain_ok=True`、`head_matches_tail=True`。
**Result: PASS**

### Check: 64 腿不变量与响应形状
**Command run:**
  `python3 .../collect_invariants.py /tmp/r73-evidence; python3 .../check_response_bytes.py /tmp/r73-evidence`
**Output observed:**
  `60/64 legs passed`（4 条 W1 B 的 `enqueued=0` 写会话豁免）；`RESPONSE-SHAPE: PASS`；
  所有 campaign 腿 `seats=0`、`permits=0`、`unknown=0`、`clean stop`、`unresolved=false`。
**Result: PASS**

### Check: T8 开关
**Command run:**
  `IMMEDIATE=1 SAMPLES=5 ./run_leg.sh t8 t8-b1 21800 -e PERI_DEFERRED_SESSION_CLOSE=0; IMMEDIATE=1 SAMPLES=5 ./run_leg.sh t8 t8-b2 21801`
**Output observed:**
  t8-b1：平台进程 env `=0`、无 deferred 键、worker sessions=0、cleanup pending=0；
  t8-b2：deferred 键存在、`enqueued_total=6`、pending=0。
**Result: PASS**

### Check: T1 优雅 drain
**Command run:**
  `T1_PLATFORM=.../peri-platform-t1probe SKIP_SETTLE=1 IMMEDIATE=1 DEV_OVERRIDE=/tmp/peri-r71-g3/dev-probe.py ./run_leg.sh t1 t1-b4 21805 -e PERI_R71_G3_CLOSE_DELAY_MS=2000`
**Output observed:**
  `pending_before_stop=4`、burst 4/4 HTTP 200、`stop_seconds=2.147`、
  `platform_waited_ms=1916.7`、`outcome=clean`、`unresolved_operations=false`、
  `audit_records=464`、`chain_ok=True`、`head_matches=True`、0 条 unresolved 日志。
**Result: PASS**

VERDICT: PASS
