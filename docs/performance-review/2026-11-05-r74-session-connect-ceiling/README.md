# R74：worker `open_session` 非 authority 部分的上限探针

**性质**：独立上限探针。无生产代码合入 `main`，探针只存在于 `probe/r74-session-connect-ceiling` 分支。
不是容量或 SLA 证明。

**结论：不足门槛，方向关闭。** 即使把 worker `open_session` 的**全部**非 authority 工作移出请求路径，
F1 的端到端改善上限约为 **1.9%**（Stage A 客户端 p50 1.0968ms 的 1.88%）；实测连接池上限臂两轮都是
负值（−1.36% / −6.22%），远低于冻结的 20% 门槛，也低于 10%。

---

## 1. 判据冻结

- `criteria.json` SHA256：`6e185c8b28a16a1e3f8956caace9447bae47a26b6a6fce40dfc1285b09f62830`
- `frozen_at_utc`：`2026-11-05T11:28:35Z`；哈希时刻：`2026-11-05T11:28:39Z`
- 冻结时测量腿：**0**；Stage B 未开始。冻结前只拉取了 v1.2.8 镜像（非测量），未启动任何栈。
- 冻结后未改判据、样本量、配额或统计口径。分析脚本 `analyze_stage_b.py` 曾把默认配额 429 误判为
  作废条件，已按冻结的 `invalid_leg` 条文修正（429 不在条文内）；这是分析脚本修正，不是判据变更。
- 文件：`criteria.json`、`criteria.sha256`、`criteria-freeze.txt`。

## 2. Stage A：发布镜像零代码测量

镜像 `ghcr.io/konghayao/peri-loom:v1.2.8`，digest
`sha256:2efe4f813b43ac3b8146296eac206b862c4cd207cd00a73a864b3db96928fe9d`（与任务给定一致）。
容器内二进制：`peri-platform 1c7b3825…`、`peri-loom-v1 65e5cc16…`。

> 任务书写的镜像内 worker 为 `614e005f…`；本机按该 digest 拉取的镜像里是 `65e5cc16…`。平台 SHA 与任务
> 给定一致，digest 也一致，故按实际观测记录 worker SHA。此差异**未归因本轮**。

全新卷 `peri-r74-probe-f1-stagea-data`、端口 21900、默认配额、观测器全 0。seed fixture
`threads=201 messages=8625 chars=20177069 focus=8625`。F1.select-1 json-pipeline，1 warmup + 125 timed。

| 动作 | 次数 | 均值 |
|---|---:|---:|
| open | 127 | **0.6196ms** |
| statement | 126 | 0.1074ms |
| close | 127 | 0.0649ms |
| query（seed/校验，非 F1） | 4 | 19.74ms |

客户端 F1：p50 **1.0968ms**、p95 4.353ms、mean 1.704ms、n=125，全部 `200/ok`。

**判定**：`open` 动作均值 619,592ns ≥ 143,000ns → 进入 Stage B（按冻结判据）。

不变量：driver exit 0；审计链 836 条 `seq=1..836` 连续、head 匹配；`launcher-stop`
`clean / terminal_proof_written / unresolved_operations=false`；`session_seats=0`、
`active_permits=0`、`unknown_held_permits=0`；`deferred_session_close` pending=0、failed=0。

证据：`stage-a/`（`analysis.json`、`metrics-before/after.json`、`harness/raw.jsonl`、`audit/`、`audit-verify.txt`）。

## 3. Stage B：探针 worker 与分段

### 3.1 分支与二进制血缘

| 角色 | 源码 | worker SHA256 |
|---|---|---|
| A（v1.2.8 源） | `main@518c40d` | `8a5a7d539468f16543c8aed289de54567b8a32916d2cf29c1c26dcf096a86150` |
| B（探针） | `probe/r74-session-connect-ceiling`，探针代码最终提交 `674aa14` | `56fc06d6cd1fcb86016a406198524a912be09be1cdd4a01d9714d1c216ffe0f1` |

- 两侧同一平台：镜像内 `peri-platform 1c7b3825…`；同一 launcher `dev_r74.py`
  `706ba07d…`（仅比镜像 dev.py 多转发 `PERI_R74_SEGMENTS` / `PERI_R74_CONNECT_POOL`）。
- 构建：`rust:1.98-bookworm`、`RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`、
  `cargo build --release --locked --bin peri-loom-v1`。**偏离**：本地 `--offline` registry 卷缺少索引项，
  改为在线 `--locked` 从 rsproxy 拉取；A/B 同法构建。`git diff 674aa14..HEAD -- prototypes/v1` 为空，
  二进制与当前探针源码一致。
- 每腿：全新卷、同镜像、同 launcher、默认配额（`requests_per_second=500`、`max_sessions=16`、
  `max_concurrent=100`）、观测器全 0。

### 3.2 分段计时（B worker，`PERI_R74_SEGMENTS=1`，pool off，n=1000）

端口 21901 的独立诊断栈；`R74WORKER` 输出在 worker 日志（`/data/peri/logs/worker-a.log`），
不在 `docker logs`。计数断言：每个分段 n=1000 = completed=1000，全部一致。

| 分段 | p50 (µs) | p95 (µs) | mean (µs) |
|---|---:|---:|---:|
| `validate_authority` | **151.293** | 446.172 | 190.767 |
| `connect` | **10.375** | 32.833 | 22.156 |
| `configure` | 0.166 | 0.375 | 0.181 |
| `lane_new` | 1.083 | 2.792 | 1.328 |
| `register` | 0.083 | 0.375 | 0.125 |
| `attach` | 0.042 | 0.292 | 0.080 |
| `urandom` | 2.166 | 5.875 | 2.824 |
| `is_readonly` | 0.500 | 1.416 | 0.637 |
| `entry_new` | 2.334 | 6.000 | 2.962 |
| 其他（pre_open/session_entry/operation_gate/engine_lock/validate_local/session_open_total/registry_insert） | ≤1.125 | ≤3.125 | ≤2.265 |
| **`open_total`** | **171.919** | 498.923 | 226.234 |

推导（p50）：

- authority `validate_session` 往返 = **151.3µs，占 open_session 的 88.0%**；该部分**未授权移除**
  （R41/R44 会放宽 lease-validation 语义）。
- 全部非 authority 工作 = 171.9 − 151.3 = **20.6µs**，仅为 Stage A F1 p50 的 **1.88%**。
- 连接池能移除的 `connect + configure` = **10.5µs**，仅为 F1 p50 的 **0.96%**。

> 任务书的估计“剩余 ≤116µs 是 connect/configure + lane/register/attach”被实测否证：这些合计约 14µs
> （p50），不是 116µs。R38 的 251.4µs open_session 里除 authority 外的残差包含了本窗口不存在的排队/
> 其他开销；本轮 c1 直接分段给出 20.6µs。

### 3.3 直接上限臂：连接池 A/B

池设计：每个 `RemoteDatabase` 一个后台 refiller，维持 1–2 条**预先 connect + configure** 的 native
连接；`Database::session()` 有货就取、无货回退内联 connect；每条连接只服务一次会话，用后丢弃、不重置。
`PERI_R74_CONNECT_POOL=1`。冒烟（600 opens）：**takes=498 / misses=2（99.6% 命中）**；正式 B 腿：
**takes=2998 / misses=2**，8b 因追加 L1/X1 为 3496/4。上限臂确实生效。

块协议：1 块 = 100 个连续 F1 请求的墙钟 / 100；每腿 1 个预热块（不计）+ 30 个计时块；轮序
`A/B/B/A` 与 `B/A/A/B`；每腿独立容器 + 全新卷 + 同 fixture。默认配额 500 req/s 下，块与块之间
间隔 1000ms（块内仍为连续请求），否则会触发 429；块定义未变。

| 轮 | 腿 | 臂 | 块中位 (ms) | 块 min / max (ms) | 请求 p50 (ms) | 失败 | 池 takes/misses |
|---|---|---|---:|---:|---:|---:|---:|
| 1 | 1a | A | 1.8085 | 1.1556 / 2.3414 | 0.9846 | 0 | — |
| 1 | 2b | B | 1.7557 | 1.2943 / 2.4862 | 0.9967 | 0 | 2998 / 2 |
| 1 | 3b | B | 1.8893 | 1.1758 / 2.5812 | 1.0083 | 0 | 2998 / 2 |
| 1 | 4a | A | 1.7876 | 1.0516 / 2.7026 | 1.0294 | 0 | — |
| 2 | 5b | B | 1.9117 | 1.1072 / 2.8209 | 0.9968 | 0 | 2998 / 2 |
| 2 | 6a | A | 1.8521 | 1.1689 / 2.4466 | 0.9988 | 0 | — |
| 2 | 7a | A | 1.8562 | 1.4019 / 2.5308 | 1.0961 | 0 | — |
| 2 | 8b | B | 2.0274 | 1.2887 / 2.6414 | 1.1204 | 0 | 3496 / 4 |

轮统计（每侧两条腿中位的中位）与改善（正值为 B 更快）：

| 轮 | A 中位 (ms) | B 中位 (ms) | 改善 |
|---|---:|---:|---:|
| 1（A/B/B/A） | 1.7981 | 1.8225 | **−1.36%** |
| 2（B/A/A/B） | 1.8542 | 1.9696 | **−6.22%** |

**主结果 = min(两轮) = −6.22% → 冻结判据 `<10%` → 上限不足，方向关闭。**

替代诊断（不作判据）：合并 60 个块值的中位，r1 A 1.8021 / B 1.8485（−2.6%）、
r2 A 1.8527 / B 1.9571（−5.6%）；合并请求 p50，r1 A 1.0059 / B 1.0028（+0.3%）、
r2 A 1.0401 / B 1.0563（−1.6%）。逐请求 p50 接近持平，与“可移除量只有约 10µs”一致；块中位噪声
（单腿块 min–max 约 1.05–2.82ms）大于效应本身。B 在两种统计、两轮中都不优于 A。

### 3.4 不变量（8/8 腿）

- 8/8：driver `failures=0`，`exit=0`；审计链连续（6 条纯块腿 9,752 条；7a 11,245、8b 11,271），
  head 匹配 tail；`launcher-stop` 全部 `clean / terminal_proof_written / unresolved_operations=false`；
  结束 `session_seats=0`、`active_permits=0`、`unknown_held_permits=0`。
- 8/8：镜像 digest、平台 SHA、launcher SHA、默认配额、观测器 0 与冻结值一致；A/B worker SHA 分别为
  `8a5a7d53…` / `56fc06d6…`。
- 无 `outcome_unknown` / 502 / 503 / `audit_unavailable`。7a/8b 的 `quota_exceeded`（63/46）全部来自
  可选 X1 c16 的默认配额 429（单列观察，判据未将其列为作废条件）；X1 c8 两侧均 0 个 429。

### 3.5 回归观察（不作判据）

- `L1.list-20`（单腿各 125 样本）：A 1.3726ms / B 1.2953ms（B 快 5.6%），无错误。
- `X1` c8（单腿各 100 请求）：A p50 3.06ms / p95 6.34ms；B p50 5.05ms / p95 16.45ms；两侧 429=0。
  单样本、且 B 腿的 X1 在块测量与 L1 之后运行，不作退化结论；方向与块结果一致（B 未更快）。
- 容器内存（`docker stats`）：A 腿结束 174–190MiB；B 腿 171–210MiB（7a 含 L1/X1，266.8MiB）。
  未见随请求数单调增长，池上限 2 条连接。

## 4. 结论

**不足门槛、关闭方向。** 依据：

1. 分段直接测量：`open_session` p50 171.9µs 中 authority `validate_session` 占 151.3µs（88.0%）；
   全部非 authority 仅 20.6µs（F1 p50 的 1.88%），连接池可移除的 connect+configure 仅 10.5µs（0.96%）。
2. 直接上限臂（99.9% 池命中）两轮均未跑赢基线：−1.36% / −6.22%；替代统计同向。
3. 因此即使后续安全地实现“connect 移出请求路径”，也无法接近 F1 的 20% 门槛；唯一占大头的
   authority 校验属于未授权方向（R41/R44 已证会放宽 lease-validation 语义）。

**建议**：按冻结判据关闭该方向；不实现安全版。若未来重新打开，必须先获得 authority 本地校验的
明确授权，或改变每请求的 session 生命周期（会话复用/协议级融合），而不是优化 connect。

## 5. 偏离与环境事件（未归因本轮）

1. 镜像内 worker SHA 为 `65e5cc16…`，与任务书写的 `614e005f…` 不符；平台与镜像 digest 均匹配。
2. `--offline` registry 卷（`peri-v1-cargo-registry-r2`）缺少索引项，改为在线 `--locked` 构建。
3. 默认配额 500 req/s 使 100 连续请求的块在首次分段尝试中于第 4 块触发 429（16 条 `quota_exceeded`）；
   该次运行未进入本轮证据（driver 输出被重跑覆盖），仅其卷 `peri-r74-probe-seg2-data` 与日志保留，
   不作测量腿。改为块间 1000ms 间隔后（全新栈、卷 `peri-r74-probe-seg3-data`）通过；块定义与样本量未变。
4. 探针开关需要额外转发：新增 `dev_r74.py`（仅白名单加两个 `PERI_R74_*`），两侧使用同一 launcher。
5. 连接池首版 refiller 只在 100ms 超时后补货（takes=176 / misses=2824，约 5.9% 命中），该轮
   A/B 作废并保留在 `stage-b/aborted-pool-refill-bug/`；加 `notify_all` 后冒烟 99.6%、正式腿 99.9%。
6. 分析脚本首版把 X1 c16 的默认配额 429 计为腿作废；按冻结 `invalid_leg` 条文修正为只记录、不作废。
7. `stage-a` 的 `open` 计数 127 比 F1 请求数 126 多 1，来自 harness 的 fixture 校验/open 探针；
   动作均值按 delta 计算，不影响判定。

## 6. 证据路径

- 判据：`criteria.json` / `criteria.sha256` / `criteria-freeze.txt`
- Stage A：`stage-a/`（`analysis.json`、`metrics-*.json`、`harness/`、`audit/`、`binaries.txt`）
- Stage B：`stage-b/legs/r1/…`、`stage-b/legs/r2/…`（每腿 `driver.json`、`metrics-*.json`、
  `audit/`、`audit-verify.txt`、`probe.txt`、`worker-*.log`、`launcher-stop.json`、`stats-*.txt`）
- 分段：`stage-b/legs/segmentation/`、`stage-b/segmentation.json`
- 汇总：`stage-b/analysis.json`（含逐腿原始块中位、不变量与判定）
- 作废的第一轮池 A/B：`stage-b/aborted-pool-refill-bug/`
- 构建与运行脚本：`run_stage_a.sh`、`run_stage_b.sh`、`run_segmentation.sh`、`run_pool_smoke.sh`、
  `probe_block_driver.py`、`analyze_stage_b.py`、`summarize_segmentation.py`、`dev_r74.py`
- 探针源码：`probe/r74-session-connect-ceiling` 分支（`prototypes/v1/src/probe_r74.rs` 等），
  不合入 `main`。`main` 保持 `518c40d`。

> 持久性说明：原始连接文件（含私有测试 API Key）只保留在 `/tmp/r74-probe/state/`（`chmod 600`），
> 按仓库约定不写入 evidence、不打印。其余证据均在仓库 `docs/performance-review/2026-11-05-r74-session-connect-ceiling/`
> 并已提交到探针分支；`/tmp/r74-probe/` 只是工作副本，不是唯一副本。
