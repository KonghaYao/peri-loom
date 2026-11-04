# v1.2.7 发布性能报告冻结协议（v1.2.6 vs v1.2.7）

日期：2026-11-04
状态：**本文件在第一条 A/B 腿之前冻结；测量开始后不得修改判据。**
轮次性质：发布级 endpoint A/B 的 **smoke 子集**，不是全量基准，也不是容量/SLA 证明。

## 目的

比较上一版 `v1.2.6` 与新版本 `v1.2.7` 的端到端 endpoint 延迟，逐场景给出
PASS/FAIL/IMPROVED，确认本版合入的性能改动没有带来 >10% 的可复现回归，并如实报告
可观测的改善与未测项。

## 对比对象与构建

| 角色 | tag | 源码 commit | 平台 sha256 | worker sha256 |
|---|---|---|---|---|
| A 基线 | `v1.2.6` | `292507ffff06f273db91b4355e6a38313a8409ae` | `2a4a41d84011021e7c7b4d556f737a9582c5d7a48dec6a6c3887f6fbbae8294d` | `61f742362c04d095b6f23754e22bf6aa2d807ac7154feaf4af211406282faeff` |
| B 候选 | `v1.2.7` | `640ae7e3ce035f1d2b02d9367dc6aa755a018789` | `95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912` | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |

- 平台：Go 1.27.3，`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`，从本地
  clone 的 tag 工作树构建；`go version -m` 的 `vcs.revision` 分别为
  `292507f` / `640ae7e`，`vcs.modified=false`。
- worker：`rust:1.98-bookworm`（rustc 1.98.1），
  `RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`，
  `cargo build --release --locked --bin peri-loom-v1`，源码 commit 同 tag。
  R24 记录的离线 registry 卷已不完整（`peri-v1-cargo-registry` 为空），本次改用同一
  镜像内的 rsproxy 在线解析；两侧使用完全相同的工具链、命令与 target 卷，只有源码 tag
  不同。构建告警（`turso_core` 9 条）不参与判决。
- 运行镜像：`ghcr.io/konghayao/peri-loom:v1.2.7`，
  manifest digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`
  （linux/amd64 `ea6bd571…`、linux/arm64 `8243b821…`）。**两侧使用同一镜像**，
  只把平台与 worker 二进制分别挂载覆盖；镜像自带的二进制不参与测量。
- 运行镜像的 `PERI_IMAGE_VERSION` 仍为 `v1.2.7`（镜像元数据），两侧一致，不构成偏差。

## 环境

- Docker linux/arm64（OrbStack），`--network host --init --stop-timeout 60`，
  `--security-opt no-new-privileges --cap-drop ALL`。
- 每条腿一个全新 named volume，挂载到 `/data/peri` 作为 run dir；端口 20401–20408。
- `PERI_HRANA_EXPERIMENTAL=1`；无 TLS；**无观测器**
  （`PERI_DURABILITY_OBSERVER` / `PERI_SQL_OPEN_OBSERVER` 均未设置）；
  未设置 `PERI_COMPLETION_JOURNAL`（默认 legacy v1）。
- 默认配额（每条腿 fixture 实测）：`max_sessions=16`、`max_concurrent=100`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304`。
  不改配额、不跳过审计/持久化。
- fixture：`threads=201`、`messages=8625`、`content_chars=20177069`，
  每条腿通过 `seed_fixture.py` 重新导入并校验 `complete=true`。

## 场景与协议

| 场景 | 协议 | 每腿 timed 样本 | 备注 |
|---|---|---:|---|
| `W1.insert` | `json-pipeline`、`protobuf-pipeline` | 5（warmup 1） | 写入路径 |
| `F1.select-1` | `json-pipeline`、`protobuf-pipeline`、`json-cursor`、`protobuf-cursor` | 5（warmup 1） | 固定开销小查询 |
| `X1.concurrency` | `json-pipeline` | 1/4/8/16 各 50 请求 | 报告 c8、c16 |
| `H1.content-20mb` | `json-cursor`、`protobuf-cursor` | 3（warmup 1） | 20.2MB 大结果 |

- 只跑上述 smoke 子集；不跑 L1/B1/C1/R1/H2/H3，也不跑 protobuf X1（合同仅定义
  `json-pipeline`）、TLS、多租户或容量实验。
- 每场景 **2 轮 × A/B/B/A = 8 条腿**，共 32 条腿；每条腿独立栈、全新卷、重新导入
  fixture。A/B 腿逐轮交替，端口按 leg 固定。

## 主指标与判据（冻结）

- 主指标：每条腿的 `summary.json` 中位数（ms）。主协议：
  `W1.insert=json-pipeline`、`F1.select-1=json-pipeline`、
  `H1.content-20mb=json-cursor`；`X1.concurrency` 分别看 c8 与 c16 的
  `json-pipeline` 中位数。
- 每轮比值 = 候选两腿中位的中位 / 基线两腿中位的中位；合并比值 = 候选 4 腿中位的
  中位 / 基线 4 腿中位的中位。比值为「候选 / 基线」，大于 1 表示候选更慢。
- **FAIL（回归）**：同一场景（X1 为同一档）两轮比值都 `> 1.10` 且合并比值 `> 1.10`。
- **IMPROVED**：两轮比值都 `< 0.90` 且合并比值 `< 0.90`。
- 其余情况记 **PASS**（无回归；不宣称改善）。
- 少于两轮有效数据记 **UNCONFIRMED**，不得写成 PASS。
- p95、p99、次协议、`response_bytes` 与 harness 自带的绝对目标判定全部作为报告项，
  不参与发布 A/B 判决；不把累计倍率当作结论。

## 腿有效性

- 每条腿在启动后、seed 前执行容器内
  `sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1`，与该侧预期
  sha256 比对；不匹配则该腿无效。同时记录容器 image id 与镜像 RepoDigest。
- 每腿 0 错误、0×429、HTTP 200、fixture `complete=true`、`warnings`/`fatal` 为空。
- `launcher-stop.json` 为 `outcome=clean`、`unresolved_operations=false`，
  `stop-exit.txt=0`。
- X1 每档 `requests=planned`、`verdict=PASS`、`status_429=0`、`errors=0`。
- 任一腿无效：整腿重跑一次；仍无效则记录并排除，该场景判 UNCONFIRMED。
- 不变量：`rows` 符合 `expected_rows`（W1 不检查行数）、`response_bytes` 按协议核对；
  审计/操作计数由每条腿的 metrics 快照核对（只报告，不改变判决）。

## 证据目录

`legs/<scenario>/<leg>/`：`summary.json`、`summary.md`、`raw.jsonl`、
`metrics-before.json`、`metrics-after.json`、`launcher-stop.json`、`stop-exit.txt`、
`binaries.txt`、`image-id.txt`、`image-digest.txt`、`harness.log`、`seed.log`、
`container.log`。

`legs.txt`（完整运行日志）、`aggregate.py`、`run_leg.sh`、`run_all.sh`、
`seed_fixture.py`、`snap_metrics.py`、`perf-protocol.md`、`README.md`。

`connection.json` 含私有 API Key，**不进入仓库**，只保存在 `/tmp/peri-release-ab/`
并在每条腿结束后保持 chmod 600。

## 范围与未测

- 本报告只覆盖上述 smoke 子集，不能外推为全量 benchmark、容量、公平性或长期稳定性结论。
- E1（HTTP v3 写 cursor abort 无可确认回滚证明）仍是显式 known-gap，本报告不覆盖。
- 绝对数字只对本次二进制、工具链、镜像与宿主窗口负责；不得与历史 Issue 28 快照或其他
  测量窗口的绝对值混用。
