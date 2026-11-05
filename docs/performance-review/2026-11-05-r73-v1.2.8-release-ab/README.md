# R73：v1.2.8 发布级 endpoint A/B（v1.2.7 vs main@03d9b87）

性质：独立验证。只构建、启动隔离栈、测量、核对与留证；未修改产品代码/测试，未提交 git，
未删除任何卷或证据。

## 结论

**冻结判据口径：PASS。** 全部必测场景满足阈值；64 条 campaign 腿全部有效；
T8、T1 release-blocking smoke 均通过。

**发布血缘注意项（未闭合，不影响本 A/B 判决）：** v1.2.7 **镜像自带**的
`peri-platform` 是 `go1.23.12 / CGO_ENABLED=1` 构建（SHA `3286df64…`），
`peri-loom-v1` 也不是发布证据中的 `b17a0708…`（镜像内为 `9e9f2fd6…`）。
本轮按 v1.2.7 发布报告与任务给定的 SHA，使用**本地重建的 v1.2.7 二进制**
（平台 `95716e4d…`、worker `b17a0708…`）挂载覆盖；镜像自带二进制不参与测量。
因此本 PASS 证明的是“v1.2.7 源码构建 vs main 源码构建”，不是镜像内二进制的直接性能证明。
v1.2.8 发布前应确认镜像构建口径或对镜像自带二进制补一次 smoke。

## 1. 判据冻结

| 项 | 值 |
|---|---|
| 判据文件 | `criteria.json` |
| SHA256 | `77bdd4bc56ecfdceba1f5d5346252c389b59b2aab4e6af1fb4df38b3d7dca2af` |
| 冻结时间（UTC） | `2026-11-05T09:16:59Z` |
| 冻结记录 | `criteria-freeze.txt` |
| 冻结时是否已有 R73 测量腿 | 否（只有构建产物） |

冻结后未修改判据、配额、样本量或统计口径。

## 2. 构建与血缘

| 角色 | 源码 | 平台 SHA256 | worker SHA256 |
|---|---|---|---|
| A（v1.2.7） | `640ae7e3ce035f1d2b02d9367dc6aa755a018789` | `95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912` | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| B（main） | `03d9b87fe687ac2ea22c72548741bf5fc22e01b5` | `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` | `8e62601274ca06a6503ba6eb41c4f20c358b4432c928f456970372e1e21bbd5d` |

- A 平台/worker 为 v1.2.7 发布证据中的同 SHA 二进制；`go version -m` 显示
  `go1.27.3`、`CGO_ENABLED=0`、`vcs.revision=640ae7e`、`vcs.modified=false`。
- B 平台按任务预期复现 `e07e6da8…`：从 `56dc914`（v1.2.7..main 的编译源码 diff 为 0）
  加未跟踪标记构建，`vcs.modified=true`；干净 main 重建为 `88aa249f…`（仅 VCS 元数据不同）。
- B worker 从 main 用 `rust:1.98-bookworm`（rustc 1.98.1）、
  `RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`、`--locked --offline` 构建。
- worker 血缘：B worker 二进制包含 PR #33 的 `not_executed` 符号字符串（9 次），A worker 为 0，
  证明 B 确实从 main 的 `dispatch.rs` 构建。
- 两侧同一镜像 `ghcr.io/konghayao/peri-loom:v1.2.7`（digest
  `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`），
  仅挂载覆盖平台/worker 二进制；launcher 两侧同为
  `prototypes/v1/platform/dev.py`（SHA `80bcab2…`）。

## 3. 设计

- 网络 `--network host`，无 TLS，观测器关闭（dev.py 将三个开关显式设为 `0`），
  默认配额，未放宽；每腿全新卷 `peri-r73-release-*`，端口 21700 起。
- 每腿重新导入 fixture：`threads=201 messages=8625 chars=20177069 focus=8625`。
- F1/L1：8 对 ×（1 warmup + 125 timed），pair 序
  `p1-a p1-b p2-b p2-a p3-b p3-a p4-a p4-b p5-b p5-a p6-a p6-b p7-a p7-b p8-b p8-a`
  （4 对 A-first、4 对 B-first）。
- X1 / 持续 c8 / W1 / H1：2 轮 × A/B/B/A，下一轮 B/A/A/B，共 8 腿/场景。
- 证据按 `legs/p2/<scenario>/<leg>/` 保存；T8/T1 在 `legs/smoke/`。

## 4. 场景结果

### F1.select-1（json-pipeline，c1，125 timed/腿）

| leg | side | round | p50 ms | p95 ms | p99 ms | errors | mode (bytes,rows,rows_read) |
|---|---|---|---:|---:|---:|---:|---:|
| p1-a | a | p1 | 1.0530 | 6.6535 | 7.9758 | 0 | [312, 1, 0] |
| p1-b | b | p1 | 0.9636 | 4.0140 | 5.5373 | 0 | [312, 1, 0] |
| p2-a | a | p2 | 1.9834 | 5.5803 | 8.1430 | 0 | [312, 1, 0] |
| p2-b | b | p2 | 0.6738 | 3.8846 | 5.2032 | 0 | [312, 1, 0] |
| p3-a | a | p3 | 1.6690 | 4.5555 | 5.3287 | 0 | [312, 1, 0] |
| p3-b | b | p3 | 0.6231 | 3.8706 | 4.8721 | 0 | [312, 1, 0] |
| p4-a | a | p4 | 1.7110 | 5.4124 | 7.1484 | 0 | [312, 1, 0] |
| p4-b | b | p4 | 0.6715 | 4.1003 | 6.4999 | 0 | [312, 1, 0] |
| p5-a | a | p5 | 2.0340 | 4.9840 | 6.8768 | 0 | [312, 1, 0] |
| p5-b | b | p5 | 0.7027 | 4.6300 | 4.8448 | 0 | [312, 1, 0] |
| p6-a | a | p6 | 0.9404 | 5.9340 | 7.5205 | 0 | [312, 1, 0] |
| p6-b | b | p6 | 1.1640 | 4.0220 | 5.6974 | 0 | [312, 1, 0] |
| p7-a | a | p7 | 1.4651 | 5.3658 | 7.7097 | 0 | [312, 1, 0] |
| p7-b | b | p7 | 0.7261 | 4.0176 | 5.1638 | 0 | [312, 1, 0] |
| p8-a | a | p8 | 1.6124 | 6.0305 | 8.0350 | 0 | [312, 1, 0] |
| p8-b | b | p8 | 0.7585 | 3.6187 | 5.2797 | 0 | [312, 1, 0] |

**逐对 p50（B/A）**：[0.9151, 0.3397, 0.3733, 0.3925, 0.3455, 1.2378, 0.4956, 0.4704]；
中位 **0.43146**（≤0.90），bootstrap 95% CI
[0.34547, 0.91512]，8/8 对中 7 对 B 更快。
**合并 1000 样本/侧 p95**：A 5.7272 ms、B 4.0220 ms，
比值 **0.70225**（≤1.10），CI [0.63833, 0.81075]。
p50 合并比值 0.46430（诊断）。**PASS**。

### L1.list-20（json-pipeline，c1，125 timed/腿）

| leg | side | round | p50 ms | p95 ms | p99 ms | errors | mode (bytes,rows,rows_read) |
|---|---|---|---:|---:|---:|---:|---:|
| p1-a | a | p1 | 2.0000 | 5.8695 | 7.4290 | 0 | [15627, 40, 201] |
| p1-b | b | p1 | 1.3313 | 3.8614 | 5.2709 | 0 | [15627, 40, 201] |
| p2-a | a | p2 | 2.1386 | 4.8310 | 5.4727 | 0 | [15627, 40, 201] |
| p2-b | b | p2 | 1.5248 | 4.0610 | 5.4430 | 0 | [15627, 40, 201] |
| p3-a | a | p3 | 3.0513 | 5.8805 | 6.4858 | 0 | [15627, 40, 201] |
| p3-b | b | p3 | 1.6730 | 4.0567 | 5.1134 | 0 | [15627, 40, 201] |
| p4-a | a | p4 | 1.8511 | 4.2140 | 6.4481 | 0 | [15627, 40, 201] |
| p4-b | b | p4 | 1.1760 | 2.9885 | 3.9660 | 0 | [15627, 40, 201] |
| p5-a | a | p5 | 2.0679 | 4.3513 | 5.8141 | 0 | [15627, 40, 201] |
| p5-b | b | p5 | 1.4698 | 4.5201 | 6.1232 | 0 | [15627, 40, 201] |
| p6-a | a | p6 | 2.1395 | 4.4795 | 6.0545 | 0 | [15627, 40, 201] |
| p6-b | b | p6 | 1.1728 | 3.0279 | 3.9877 | 0 | [15627, 40, 201] |
| p7-a | a | p7 | 1.9783 | 5.2904 | 8.0688 | 0 | [15627, 40, 201] |
| p7-b | b | p7 | 1.4371 | 3.7515 | 5.1415 | 0 | [15627, 40, 201] |
| p8-a | a | p8 | 2.0194 | 5.1469 | 7.8289 | 0 | [15627, 40, 201] |
| p8-b | b | p8 | 1.3400 | 3.5898 | 5.0479 | 0 | [15627, 40, 201] |

**逐对 p50（B/A）**：[0.6657, 0.713, 0.5483, 0.6353, 0.7108, 0.5482, 0.7264, 0.6635]；
中位 **0.66461**（≤0.90），CI [0.59174, 0.71298]。
**合并 1000 样本/侧 p95**：A 5.2710 ms、B 3.8840 ms，
比值 **0.73686**（≤1.10），CI [0.68485, 0.79194]。
响应模态 15,627B/40 rows 在 16 腿一致；R72 的 C4 不一致（1.175 vs 0.717）在本轮 8 对中未复现。
**PASS**。

### X1.concurrency（json-pipeline，2 轮 × A/B/B/A，100 请求/级）

| leg | side | round | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c8 errors | c16 429（诊断） |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| r1-a1 | a | r1 | 2564.1 | 2.31 | 5.50 | 0 | 0 | 22 |
| r1-a2 | a | r1 | 1388.9 | 4.97 | 10.91 | 0 | 0 | 26 |
| r1-b1 | b | r1 | 1785.7 | 3.16 | 10.46 | 0 | 0 | 47 |
| r1-b2 | b | r1 | 1538.5 | 4.27 | 10.85 | 0 | 0 | 54 |
| r2-a1 | a | r2 | 1886.8 | 3.34 | 10.21 | 0 | 0 | 46 |
| r2-a2 | a | r2 | 1785.7 | 3.60 | 8.17 | 0 | 0 | 24 |
| r2-b1 | b | r2 | 2439.0 | 2.93 | 5.35 | 0 | 0 | 37 |
| r2-b2 | b | r2 | 2325.6 | 3.07 | 5.90 | 0 | 0 | 76 |

- c8 合并：A 中位 **1836.3 qps**、B 中位 **2055.6 qps**，
  **B/A = 1.11948**（≥0.90），bootstrap 95% CI **[0.74686, 1.52091]**（下界 <0.90，
  非回归结论不具统计强度）。轮内比值：r1 0.8409、r2 1.2974——100 请求/级的短样本方差大，按冻结口径用合并值，不做单轮结论。
- c8 每腿 429=0、errors=0；c16 的 429/errors 是默认配额下的已知诊断项，不用于豁免 c8。**PASS**。

### 持续 c8（json-pipeline，8 客户端 × 500/腿，100 warmup + 400 timed）

| leg | side | round | timed | qps | p50 ms | p95 ms | failures |
|---|---|---:|---:|---:|---:|---:|---:|
| r1-a1 | a | r1 | 3200 | 1457.2 | 3.32 | 14.99 | 0 |
| r1-a2 | a | r1 | 3200 | 1429.9 | 3.25 | 15.64 | 0 |
| r1-b1 | b | r1 | 3200 | 1523.4 | 2.88 | 14.35 | 0 |
| r1-b2 | b | r1 | 3200 | 1387.8 | 2.86 | 16.86 | 0 |
| r2-a1 | a | r2 | 3200 | 1428.8 | 3.26 | 15.16 | 0 |
| r2-a2 | a | r2 | 3200 | 1385.1 | 3.18 | 15.89 | 0 |
| r2-b1 | b | r2 | 3200 | 1583.1 | 2.69 | 13.56 | 0 |
| r2-b2 | b | r2 | 3200 | 1557.0 | 2.97 | 14.14 | 0 |

- 合并：A 中位 **1429.3 qps**、B 中位 **1540.2 qps**，
  **B/A = 1.07755**（≥0.90），bootstrap 95% CI **[0.97093, 1.12406]**；轮内 r1 1.0083、
  r2 1.1159。8 腿各 3,200 timed、0 失败、0 worker error。**PASS**。

### W1.insert（json-pipeline，c1，20 timed/腿）

| leg | side | round | timed | p50 ms | errors |
|---|---|---:|---:|---:|---:|
| r1-a1 | a | r1 | 20 | 18.64 | 0 |
| r1-a2 | a | r1 | 20 | 18.41 | 0 |
| r1-b1 | b | r1 | 20 | 15.93 | 0 |
| r1-b2 | b | r1 | 20 | 23.88 | 0 |
| r2-a1 | a | r2 | 20 | 24.83 | 0 |
| r2-a2 | a | r2 | 20 | 16.48 | 0 |
| r2-b1 | b | r2 | 20 | 18.20 | 0 |
| r2-b2 | b | r2 | 20 | 18.73 | 0 |

- 有序对 p50（B/A）：[0.8546, 1.2972, 0.7332, 1.1364]，
  中位 **0.99552**（≤1.10），bootstrap 95% CI [0.73315, 1.29718]。写会话不走延迟 close（B 腿
  `deferred_session_close.enqueued_total=0` 是预期，见不变量）。**PASS**。

### H1.content-20mb（json-cursor，可选，c1，3 timed/腿）

| leg | side | round | timed | p50 ms | errors | mode (bytes, rows, rows_read) |
|---|---|---:|---:|---:|---:|---|
| r1-a1 | a | r1 | 3 | 59.72 | 0 | [20608525, 8625, 0] |
| r1-a2 | a | r1 | 3 | 61.05 | 0 | [20608525, 8625, 0] |
| r1-b1 | b | r1 | 3 | 61.88 | 0 | [20608525, 8625, 0] |
| r1-b2 | b | r1 | 3 | 61.02 | 0 | [20608525, 8625, 0] |
| r2-a1 | a | r2 | 3 | 64.46 | 0 | [20608525, 8625, 0] |
| r2-a2 | a | r2 | 3 | 61.58 | 0 | [20608525, 8625, 0] |
| r2-b1 | b | r2 | 3 | 60.89 | 0 | [20608525, 8625, 0] |
| r2-b2 | b | r2 | 3 | 62.60 | 0 | [20608525, 8625, 0] |

- 有序对 p50（B/A）：[1.0362, 0.9996, 0.9446, 1.0166]，
  中位 **1.00802**（≤1.10），bootstrap 95% CI [0.94456, 1.03624]。**PASS**。

## 5. 不变量（64 条 campaign 腿）

- **64/64** 腿：平台/worker SHA 与冻结值一致；镜像 digest 与 launcher SHA 一致；
  观测器开关均为 `0`；`errors_total`/`rejection_reasons_total` 无禁止增量；
  审计链连续且 head 匹配 tail；结束 `session_seats=0`、`active_permits=0`、
  `unknown_held_permits=0`、worker sessions=0、pending close=0；
  launcher-stop `outcome=clean`、`reason=terminal_proof_written`、
  `unresolved_operations=false`；docker stop exit 0。
- **响应形状**：F1 模态 312B/1 row；L1 模态 15,627B/40 rows/rows_read=201；
  32 条 F1/L1 腿一致（`RESPONSE-SHAPE: PASS`）。
- **A 臂**无 `deferred_session_close` 键；**B 臂**默认发布该键，settle 后
  `pending=0`、`failed_total=0`、`unresolved_total=0`。W1 B 腿因写会话不走延迟 close，
  `enqueued_total=0` 为预期（任务判据明确豁免）；复用 R72 collector 因此报 4 条
  “flag on enqueues closes” 失败，属口径差异，不是缺陷。
- 容器日志扫描：0 条 `outcome_unknown` / `session_moved` / `audit_unavailable` /
  `unresolved platform operation` / `deferred session close unresolved`；
  0 条 502/503 token。

## 6. T8 / T1（release-blocking smoke）

### T8：显式关闭延迟会话关闭

| leg | 进程 env | deferred 键 | immediate enqueued | immediate pending | worker sessions | cleanup pending |
|---|---|---|---:|---:|---|---:|
| t8-b1 | `PERI_DEFERRED_SESSION_CLOSE=0` | 无 | — | — | worker-a/b = 0 | 0 |
| t8-b2（对照） | unset（默认） | 有 | 6 | 0 | worker-a/b = 0 | 0 |

显式 0 时无指标键、close 走同步路径（无 deferred 账户）；默认时键存在且 6 个 close
入账。**PASS**。

### T1：2s close 延迟 + SIGTERM 优雅 drain

- 平台：main + `probe/r71-g3-fault-injection`（`090aad5`）cherry-pick 到 `03d9b87`，
  构建 SHA `831d647c052a817e374cae87ea75a906afcbb384adf12b8ab35ca753cc18ac1f`；
  launcher 用 R71 `dev-probe.py`（SHA `74ef196…`）转发探针 env。
- 注入：`PERI_R71_G3_CLOSE_DELAY_MS=2000`；4 个并发 Hrana pipeline `[SELECT 1, close]`。
- 结果（t1-b4）：burst 4/4 HTTP 200（≈11.8ms）；stop 前 metrics
  `enqueued_total=4, pending=4, pending_max=4, failed_total=0, unresolved_total=0`；
  `docker stop` 用时 **2.147s**，平台 stage `waited_ms=1916.7`；
  launcher-stop `clean / terminal_proof_written / unresolved_operations=false`，
  stop exit 0；审计 464 条连续、head 匹配，close 记录 5 条（1 条 seed + 4 条 burst）；
  0 条 `deferred session close unresolved`。**PASS**。
- 过程记录：t1-b1 因脚本 case 分支错误在 driver 前失败；t1-b2 用 stock `dev.py`
  未转发探针 env，注入未生效（pending=0、stop 0.324s）；t1-b3/t1-b4 用
  `dev-probe.py` 后注入生效。失败尝试保留在 `legs/smoke/t1/`。

## 7. 判决与未闭合项

**判决：PASS（冻结判据口径）。**

未闭合项（不改变本判决，但发布前应处理/知悉）：

1. **镜像二进制血缘**：v1.2.7 镜像内 `peri-platform` 为 Go 1.23.12 / CGO=1
   （`3286df64…`），worker 为 `9e9f2fd6…`；与发布证据/本轮 A 臂的
   Go 1.27.3 / CGO=0 构建（`95716e4d…` / `b17a0708…`）不同。
   v1.2.8 若仍用 `golang:1.23-bookworm` + `CGO_ENABLED=1` 构建，则该镜像的
   实际二进制未被本轮性能数据直接覆盖。
2. X1 c8 的 100 请求/级短样本方差大：r1 B/A 0.841、r2 1.297，合并 1.119。
   合并口径 PASS，但不宣称每一轮都无回归。
3. 本报告是受控单机 A/B，不是容量/SLA 证明；H1 只使用 3 timed/腿。

## 8. 证据文件

- `criteria.json` / `criteria-freeze.txt`：冻结判据与哈希。
- `environment.txt`、`build/`：环境、工具链、构建 SHA 与 `go version -m`。
- `legs/p2/<scenario>/<leg>/`：每腿 `leg.txt`、`binaries.txt`、`seed.txt`、
  `metrics-before.json`、`metrics-after.json`、`driver.txt`、`harness/`、
  `settle.json`、`container.txt`、`launcher-stop.json`、`audit/`、`audit-verify.json`。
- `legs/smoke/t8/`、`legs/smoke/t1/`：release-blocking smoke 证据。
- `summary.json`、`metrics.json`、`raw.jsonl`、`analysis/`：聚合统计与逐请求数据。


## 9. 补充观察（不改变判决）

- **F1 p6 单对**：B/A = 1.2378（A 0.9404ms、B 1.1640ms），是 8 对中唯一显著反向的一对；
  其余 7 对为 0.34–0.92，合并 p50 比值 0.4643、p95 0.7023。单对反向不构成“可复现 >10% 回归”。
- **W1 轮内方向**：pair1/pair3 B 更快（0.8546/0.7332），pair2/pair4 B 更慢（1.2972/1.1364），
  中位 0.9955；W1 p50 本身在 15.9–24.8ms 间波动，方向在同一轮内混合，按冻结口径 PASS，
  但不宣称写路径有稳定改善。
- **H1**：pair1/pair4 B 略慢（1.0362/1.0166），pair2/pair3 B 略快（0.9996/0.9446），
  中位 1.0080，低于 1.10 阈值；每腿仅 3 个 timed 样本，只作为非劣检查，不作容量/尾延迟结论。
- **X1 c8**：r1 B/A 0.841、r2 1.297，合并 1.119；100 请求/级的 elapsed 仅 39–72ms，
  轮间方差大，按冻结的合并口径 PASS，单轮不作为结论。
- **X1 driver exit=1**：c16 默认配额 429 使 harness 返回 1；c1/c4/c8 均零 429/零错误，
  按冻结判据 c16 仅诊断，故 8 条 X1 腿仍为有效腿。
- **W1 B enqueued=0**：写会话不走延迟 close（任务判据明确说明）；重复使用的 R72
  collector 因此对 4 条 W1 B 腿报 “flag on enqueues closes” 失败，属口径差异。
- **连接文件**：全部 `connection.json` 已移出证据目录到 `/tmp/r73-private-state/`（0700），
  证据目录中 0 个 connection.json；扫描确认没有证据文件包含 API Key。

## 10. 复跑命令（摘要）

```bash
# 冻结判据（已在任何腿之前完成）
sha256sum /tmp/r73-evidence/criteria.json   # 77bdd4bc...

# 单腿（示例：F1，B 臂，端口 21700）
cd /tmp/r73-evidence && ./run_leg.sh f1 p1-b 21700

# 场景腿序列
# f1/l1: p1-a p1-b p2-b p2-a p3-b p3-a p4-a p4-b p5-b p5-a p6-a p6-b p7-a p7-b p8-b p8-a
# x1/c8/w1/h1: r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2
# T8: IMMEDIATE=1 SAMPLES=5 ./run_leg.sh t8 t8-b1 21800 -e PERI_DEFERRED_SESSION_CLOSE=0
# T1: T1_PLATFORM=.../peri-platform-t1probe SKIP_SETTLE=1 IMMEDIATE=1 \
#     DEV_OVERRIDE=/tmp/peri-r71-g3/dev-probe.py ./run_leg.sh t1 t1-b4 21804 \
#     -e PERI_R71_G3_CLOSE_DELAY_MS=2000
```

## 11. 入库说明（体积策略、排除项与隐私扫描）

本节描述证据从 `/tmp/r73-evidence/` 入库到
`docs/performance-review/2026-11-05-r73-v1.2.8-release-ab/` 时所做的变换。
**`/tmp/r73-evidence/` 中的原始副本未做任何删除、移动或修改，仍是唯一完整副本。**

### 11.1 压缩（gzip）

为满足入库体积预算（目标 ≤ 60MB、单文件 ≤ 20MB），以下机器生成的大体量产物
在本仓库副本中改为 `*.gz` 提交；`/tmp` 中保留未压缩原件：

| 路径模式 | 数量 | 未压缩体积 | 压缩后体积 | 再生成方式 |
|---|---:|---:|---:|---|
| `legs/**/audit/events.json` | 71 | 约 56MB | 约 8.9MB | 各腿运行时由平台审计文件复制；见 `run_leg.sh` 与第 10 节 |
| `legs/**/harness/raw.jsonl` | 68 | 约 11MB | 约 1.5MB | 各腿 harness 逐请求输出；见 `run_leg.sh` |
| `raw.jsonl` | 1 | 10.3MB | 0.55MB | `analyze_r73.py` 的聚合输入；由各腿 `harness/raw.jsonl` 汇总 |

压缩命令：`gzip -9 <file>`（保留原文件于 `/tmp/r73-evidence/`）。全部 `.gz` 已用
`gzip -t` 校验通过。读取方式：`gzip -dc <file>.gz`。

### 11.2 排除（不入库）

以下文件是构建产物而非可复核证据，且体积占总量约 117MB（压缩后仍约 53MB），
为守住 60MB 预算未纳入提交。`/tmp/r73-evidence/` 中原件保留，SHA256 与
`go version -m` 摘录见 `build/platforms.txt` 与本报告第 2 节。

| 路径 | 字节数 | SHA256 | 说明 / 再生成方式 |
|---|---:|---|---|
| `bin/peri-loom-v1-main` | 32,371,720 | `8e62601274ca06a6503ba6eb41c4f20c358b4432c928f456970372e1e21bbd5d` | B 臂 worker；`rust:1.98-bookworm` + `RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`，`cargo build --release --locked --offline --bin peri-loom-v1`（main 只读挂载） |
| `bin/peri-platform-56dc914-dirty` | 15,023,724 | `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` | R73 B 臂平台（复现任务预期 SHA）；`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/peri-platform`，源码 `56dc914` + 未跟踪标记 |
| `bin/peri-platform-main` | 15,023,724 | `88aa249f0e2a3cc4dc310b6800d2152a344427bb77bc0beada015c102f746c99` | 干净 `03d9b87` 重建的交叉核对平台 |
| `bin/peri-platform-t1probe` | 15,025,005 | `831d647c052a817e374cae87ea75a906afcbb384adf12b8ab35ca753cc18ac1f` | T1 故障注入探针平台（main + `090aad5` cherry-pick） |
| `image-binaries/peri-loom-v1-image` | 32,366,904 | `9e9f2fd6b46fc89d7c8321bc950c789f9d1860cb251535893738ae54bc01cc4e` | v1.2.7 镜像自带 worker；`docker create ghcr.io/konghayao/peri-loom:v1.2.7`（digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`）后 `docker cp` 提取 |
| `image-binaries/peri-platform-image` | 13,212,960 | `3286df6480e855d4b7e16806701755f54725e22fd4e7e5a0319e38d40fb1832a` | v1.2.7 镜像自带平台（go1.23.12 / CGO=1）；同上方式提取 |

### 11.3 重命名

`build/worker-build.log` → `build/worker-build.txt`。仓库根 `.gitignore` 的 `*.log`
会静默忽略该文件；内容未改动，仅改扩展名以便入库。

### 11.4 隐私扫描（方法与结果）

在提交前对全部已暂存文件执行（BSD/macOS 兼容）：

```bash
cd docs/performance-review/2026-11-05-r73-v1.2.8-release-ab
git add -A
git ls-files -z | xargs -0 -n1 sh -c '
  f="$1"
  case "$f" in
    *.gz) gzip -dc "$f" ;;
    *)    cat "$f" ;;
  esac | grep -nE "sk-[A-Za-z0-9]{16,}|Bearer [A-Za-z0-9._-]{20,}|(\"|'"'"')(api_key|global_api_key|project_api_key)(\"|'"'"')[[:space:]]*:[[:space:]]*\"[^\"]{8,}\"|Authorization:[[:space:]]*(Bearer|Basic)[[:space:]]+[A-Za-z0-9._-]{8,}"
' sh
```

结果（2026-11-05，本轮入库复扫）：

- 扫描文件数：**1,767**（含 140 个 `.gz`，均先 `gzip -dc` 解压后扫描）。
- API Key 值、`Bearer`/`Authorization:` 实际值：**0 命中**（`/tmp/r73-privacy-scan.txt` 为空）。
- `connection.json` **文件数 = 0**；原件在 `/tmp/r73-private-state/`（0700，未入库）。
- 文本中出现 `connection.json` 路径引用 **82 处 / 75 个文件**（主要是各腿
  `container.txt` 里的 `"connection_file": "/data/peri/connection.json"` 与 README 说明），
  仅为路径字符串，不含凭据，属已知良性引用。
- 已知误报模式（本轮未出现）：凭据拼接代码 `"Bearer " + target.global_api_key`、
  指标名 `owner_rate_tokens`。
