# v1.2.7 发布性能报告（v1.2.6 vs v1.2.7）

日期：2026-11-04
状态：发布已完成；受控 A/B 已完成。**本报告是 smoke 子集，不是全量 benchmark，也不是
容量/SLA 证明。**
协议：`perf-protocol.md`，在正式窗口第一条腿之前提交（`271e096`），判据未修改。

## 发布产物

| 项目 | 值 |
|---|---|
| 版本 | `v1.2.7` |
| release commit | `640ae7e3ce035f1d2b02d9367dc6aa755a018789` |
| annotated tag | `v1.2.7`（tag 对象 `b9ad1b63934fade16f5b243f9cff124bbd5d8fcc`，peeled `640ae7e`） |
| `v1-image` run | `41829439898`，success，13m8s |
| 工作流 job | `check`、`baseline / baseline`、`baseline / p0 / p0`、arm64 build、amd64 build、`publish` 全部 success |
| manifest list | `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| linux/amd64 | `sha256:ea6bd57150b658cbb044735e8643777ec8785c589a07acbbfdb98d20cdce2ed2` |
| linux/arm64 | `sha256:8243b8216c394680f074faa6ce4ba87fdbd0537029dc2b699760754833d56ab9` |
| GitHub Release | `v1.2.7`（preview / prerelease），https://github.com/KonghaYao/peri-loom/releases/tag/v1.2.7 |
| main CI（发布提交） | `v1 SQL and SDK baseline` `41829401647` success（4m57s）；`Issue 8 performance harness` `41829401626` success（23m55s） |

核对命令：

```bash
docker buildx imagetools inspect ghcr.io/konghayao/peri-loom:v1.2.7
gh release view v1.2.7 --json name,tagName,isPrerelease,isDraft,url
gh run view 41829439898 --json status,conclusion,jobs
git ls-remote git@github.com:KonghaYao/peri-loom.git 'refs/tags/v1.2.7*'
```

## 结论摘要

| 场景 | 主协议 | 基线中位 | 候选中位 | 合并比值 | 判决 |
|---|---|---:|---:|---:|---|
| `W1.insert` | json-pipeline | 21.956ms | 20.765ms | 0.946 | **PASS** |
| `F1.select-1` | json-pipeline | 2.433ms | 2.891ms | **1.188** | **FAIL** |
| `X1.concurrency` c8 | json-pipeline | 6.079ms | 3.830ms | **0.630** | **IMPROVED** |
| `X1.concurrency` c16 | json-pipeline | 9.047ms | 4.604ms | **0.509** | **IMPROVED** |
| `H1.content-20mb` | json-cursor | 151.425ms | 61.419ms | **0.406** | **IMPROVED** |

- **H1 是最大改善**：json-cursor 合并中位 151.425→61.419ms（−59.4%），protobuf-cursor
  119.792→55.773ms（−53.4%）。
- **X1 并发显著改善**：c8 −37.0%、c16 −49.1%；c1/c4 也分别为 0.902 / 0.648（报告项）。
- **F1 按冻结判据 FAIL**：两轮比值 1.228 / 1.541、合并 1.188，方向两轮一致；绝对中位
  2.4→2.9ms，仍远低于 harness 的 13ms 绝对目标。样本噪声大，回归幅度约 0.5ms，未归因到
  具体代码路径；需要后续定向归因。
- W1 按判据 PASS：两轮 1.041 / 0.904、合并 0.946；protobuf-pipeline 合并 0.815。
- 不把累计倍率当结论；逐场景判决如上。

## 对比对象与二进制

| 角色 | tag | 源码 commit | 平台 sha256 | worker sha256 |
|---|---|---|---|---|
| A 基线 | `v1.2.6` | `292507ffff06f273db91b4355e6a38313a8409ae` | `2a4a41d84011021e7c7b4d556f737a9582c5d7a48dec6a6c3887f6fbbae8294d` | `61f742362c04d095b6f23754e22bf6aa2d807ac7154feaf4af211406282faeff` |
| B 候选 | `v1.2.7` | `640ae7e3ce035f1d2b02d9367dc6aa755a018789` | `95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912` | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |

- 平台：Go 1.27.3，`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath`；
  `go version -m` 的 `vcs.revision` 为 `292507f` / `640ae7e`，`vcs.modified=false`。
- worker：`rust:1.98-bookworm`（rustc 1.98.1），
  `RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`，
  `cargo build --release --locked --bin peri-loom-v1`。两侧使用相同工具链、相同 target
  卷和同一构建命令，只有源码 tag 不同。
- 运行镜像：`ghcr.io/konghayao/peri-loom:v1.2.7`（manifest digest
  `sha256:8a8ba43…`），**两侧使用同一镜像**，平台与 worker 二进制分别挂载覆盖。
  镜像自带二进制不参与测量。

## 环境与协议

- Docker linux/arm64（OrbStack），`--network host --init --stop-timeout 60`，
  `--security-opt no-new-privileges --cap-drop ALL`；无 TLS。
- 每条腿一个全新 named volume（`peri-loom-release-v127-b-*`），run dir `/data/peri`，
  端口 20401–20408。
- **无观测器**（`PERI_DURABILITY_OBSERVER` / `PERI_SQL_OPEN_OBSERVER` 未设置）；
  未设置 `PERI_COMPLETION_JOURNAL`（legacy v1）；`PERI_HRANA_EXPERIMENTAL=1`。
- 默认配额（fixture 实测）：`max_sessions=16`、`max_concurrent=100`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304`。
  没有为跑分放宽配额，没有跳过审计/持久化。
- fixture：`threads=201`、`messages=8625`、`content_chars=20177069`，每条腿重新导入并
  校验 `complete=true`。
- 每场景 2 轮 × A/B/B/A = 8 条腿，共 32 条腿；A/B 逐轮交替，端口按 leg 固定。
- 每条腿在 seed 前记录容器内 `/usr/local/bin/peri-platform` 与
  `/usr/local/bin/peri-loom-v1` 的 sha256，并与该侧预期值比对；32/32 匹配。

### 预跑窗口与正式窗口

第一次 32 腿窗口在 20:51–20:59 完成，判据与脚本相同，但当时脚本未记录**每腿容器内
sha256**，不满足执行要求。该窗口保留为 `pre-run-legs/` + `pre-run-aggregate.txt` +
`pre-run-legs.txt`，**不用于本报告判决**。补上逐腿 sha 校验后，正式窗口在 21:01–21:08
重新跑完，正式证据为 `legs/` + `aggregate.txt` + `invariants.txt` + `legs.txt`。
预跑窗口的 F1 结果本身也是混合方向（round1 0.480、round2 1.512、合并 0.727），进一步
说明 F1 在该宿主上噪声较大；正式窗口按冻结判据判 FAIL。

## 主指标：逐轮与合并

单位为每腿 `summary.json` 的 `median_ms`；每轮取同侧两腿中位的中位，合并取 4 腿中位。
比值为候选 / 基线。

| 场景 | 指标 | 轮 | 基线中位(ms) | 候选中位(ms) | 比值 |
|---|---|---|---:|---:|---:|
| W1.insert | json-pipeline | round1 | 19.942 | 20.765 | 1.041 |
| W1.insert | json-pipeline | round2 | 22.027 | 19.918 | 0.904 |
| W1.insert | json-pipeline | 合并 | 21.956 | 20.765 | **0.946** |
| F1.select-1 | json-pipeline | round1 | 3.159 | 3.880 | 1.228 |
| F1.select-1 | json-pipeline | round2 | 1.706 | 2.629 | 1.541 |
| F1.select-1 | json-pipeline | 合并 | 2.433 | 2.891 | **1.188** |
| X1.concurrency | c8 | round1 | 4.955 | 3.939 | 0.795 |
| X1.concurrency | c8 | round2 | 6.933 | 3.830 | 0.552 |
| X1.concurrency | c8 | 合并 | 6.079 | 3.830 | **0.630** |
| X1.concurrency | c16 | round1 | 8.855 | 5.071 | 0.573 |
| X1.concurrency | c16 | round2 | 9.682 | 4.303 | 0.444 |
| X1.concurrency | c16 | 合并 | 9.047 | 4.604 | **0.509** |
| H1.content-20mb | json-cursor | round1 | 148.632 | 61.419 | 0.413 |
| H1.content-20mb | json-cursor | round2 | 152.552 | 63.614 | 0.417 |
| H1.content-20mb | json-cursor | 合并 | 151.425 | 61.419 | **0.406** |

判决规则（冻结）：两轮都 >1.10 且合并 >1.10 判 FAIL；两轮都 <0.90 且合并 <0.90 判
IMPROVED；其余 PASS。因此 W1=PASS、F1=FAIL、X1 c8/c16=IMPROVED、H1=IMPROVED。

### p95 / p99（报告项，不判）

| 场景 | 指标 | 轮 | 基线 p95 | 候选 p95 | 比值 | 基线 p99 | 候选 p99 | 比值 |
|---|---|---|---:|---:|---:|---:|---:|---:|
| W1.insert | json-pipeline | round1 | 38.974 | 31.505 | 0.808 | 40.469 | 34.154 | 0.844 |
| W1.insert | json-pipeline | round2 | 29.634 | 36.800 | 1.242 | 31.437 | 37.297 | 1.186 |
| F1.select-1 | json-pipeline | round1 | 6.450 | 6.644 | 1.030 | 8.010 | 7.626 | 0.952 |
| F1.select-1 | json-pipeline | round2 | 6.255 | 5.687 | 0.909 | 7.969 | 7.879 | 0.989 |
| X1.concurrency | c8 | round1 | 10.338 | 10.364 | 1.003 | 11.938 | 11.579 | 0.970 |
| X1.concurrency | c8 | round2 | 11.382 | 8.056 | 0.708 | 12.860 | 11.618 | 0.903 |
| X1.concurrency | c16 | round1 | 11.906 | 9.846 | 0.827 | 13.495 | 11.464 | 0.850 |
| X1.concurrency | c16 | round2 | 14.034 | 11.734 | 0.836 | 16.643 | 13.865 | 0.833 |
| H1.content-20mb | json-cursor | round1 | 154.346 | 72.423 | 0.469 | 155.273 | 76.562 | 0.493 |
| H1.content-20mb | json-cursor | round2 | 157.003 | 66.833 | 0.426 | 157.198 | 68.274 | 0.434 |

F1 的 p95/p99 没有同步变差（p95 比值 1.030 / 0.909，p99 0.952 / 0.989），说明正式窗口
判 FAIL 的信号主要在中位数附近，而不是尾部；这与 F1 每腿只有 5 个 timed 样本、单样本
跨度 0.8–8ms 的噪声特征一致。

## 次协议（报告项，不判）

| 场景 | 协议 | 轮1 比值 | 轮2 比值 | 合并比值 | 基线字节 | 候选字节 |
|---|---|---:|---:|---:|---:|---:|
| W1.insert | json-pipeline | 1.041 | 0.904 | 0.946 | 252 | 252 |
| W1.insert | protobuf-pipeline | 0.847 | 0.819 | 0.815 | 31 | 31 |
| F1.select-1 | json-pipeline | 1.228 | 1.541 | 1.188 | 312 | 312 / 318 |
| F1.select-1 | protobuf-pipeline | 1.205 | 1.613 | 1.296 | 40 | 40 |
| F1.select-1 | json-cursor | 0.541 | 2.001 | 1.487 | 252 | 252 |
| F1.select-1 | protobuf-cursor | 1.267 | 0.664 | 0.752 | 57 | 57 |
| H1.content-20mb | json-cursor | 0.413 | 0.417 | 0.406 | 20,608,525 | 20,608,525 |
| H1.content-20mb | protobuf-cursor | 0.478 | 0.459 | 0.466 | 20,272,006 | 20,272,006 |

- F1 的 json-pipeline 与 protobuf-pipeline 都是两轮 >1.10，方向一致；cursor 两个协议
  方向混合（json-cursor 0.541/2.001、protobuf-cursor 1.267/0.664）。这提示回归更可能
  在 pipeline 路径而不是 cursor 路径，但本轮没有做代码级归因，不能当作结论。
- F1 json-pipeline 的 response_bytes 在 312–324 之间变化，差异来自
  `query_duration_ms` 数字位数（历史已记录）；`rows=1`、`outcome=ok`、`status=200`
  在全部样本一致。H1/W1 的字节数两侧完全一致。

## 有效性与不变量

`invariants.py` 输出（`invariants.txt`，退出码 0）：

- **32/32 腿二进制 sha256 匹配**（A：`2a4a41d8…` / `61f74236…`；B：`95716e4d…` /
  `b17a0708…`）。
- **32/32 腿镜像 digest 匹配** `sha256:8a8ba43…`。
- **32/32 腿 fixture `complete=true`**（threads=201、messages=8625、chars=20177069）。
- **32/32 腿 `launcher-stop.json` 为 `outcome=clean`、`unresolved_operations=false`，
  `stop-exit=0`**。
- 96 个 summary 行、288 个非并发 timed 样本、X1 4 档 × 50 请求 × 8 腿；
  **0 错误、0×429、0 rejected**；`session_seats=0`、`active_permits=0`、
  `unknown_held_permits=0`、`in_flight_count=0`、`queue_count=0`。
- `durability_source=audit_file_v1`；`dropped_total=0`、
  `head_checkpoint_failures_total=0`、`operation_journal_compaction_failures_total=0`。
- harness 自带的绝对目标判定：96/96 行为 `PASS` 或 `—`（W1 无 target），没有 harness
  绝对目标 FAIL。F1 的 FAIL 是发布 A/B 的相对判据，不是 13ms 绝对目标失败。

### 机制计数器（harness 窗口增量中位）

| 场景 | 侧 | audit fsync | journal appends | journal syncs | coalesced | batch_max |
|---|---|---:|---:|---:|---:|---:|
| F1.select-1 | A | 34.0 | 50.0 | 38.0 | 12.0 | 3.5 |
| F1.select-1 | B | 37.0 | 50.0 | 40.0 | 10.0 | 3.0 |
| W1.insert | A | 113.0 | 146.0 | 143.0 | 3.0 | 2.0 |
| W1.insert | B | 113.5 | 146.0 | 118.0 | 28.0 | 2.0 |
| X1.concurrency | A | 119.5 | 404.0 | 184.5 | 219.5 | 15.5 |
| X1.concurrency | B | 94.0 | 404.0 | 139.0 | 265.0 | 23.0 |
| H1.content-20mb | A | 293.0 | 18.0 | 18.0 | 0.0 | 1.0 |
| H1.content-20mb | B | 271.5 | 18.0 | 18.0 | 0.0 | 1.0 |

- X1 的改善与 journal 合并增强一致：syncs 184.5→139.0、coalesced 219.5→265.0、
  batch_max 15.5→23.0。
- W1 写路径的 journal syncs 143.0→118.0、coalesced 3.0→28.0，但端到端中位仍为
  0.946（PASS，不是稳健改善）。
- H1 的 journal appends/syncs 两侧完全相同（18/18），改善来自 cursor 页路径而不是
  审计/日志批处理。
- F1 的 journal appends 两侧相同（50），syncs 38→40、coalesced 12→10；方向与 F1 轻微
  变慢一致，但差异太小，不能单独解释 0.5ms 的中位回归，**未归因**。

## 失败项、已知缺口与未测项

**失败项**

- `F1.select-1` json-pipeline 按冻结判据 **FAIL**（合并 1.188）。绝对中位 2.4→2.9ms，
  仍远低于 13ms 绝对目标；p95/p99 没有同步变差；5 个 timed 样本的跨度 0.8–8ms，
  预跑窗口方向也不稳定。需要后续用更深的探针或更多样本做定向归因，不能把本轮
  0.5ms 的差异直接归因到某个具体改动。

**已知缺口**

- E1（HTTP v3 写 cursor abort 无可确认回滚证明）仍是显式 known-gap；默认矩阵每轮
  26/27、2 轮 52 runs + 2 skips，本报告不覆盖、也不宣称完整 Hrana 兼容。
- 本报告只覆盖 smoke 子集：未测 `L1`、`B1`、`C1`、`R1`、`H2`、`H3`，未测 protobuf X1
  （合同只定义 json-pipeline），未测 TLS、多租户、容量/公平性/长期稳定性。
- 单机 macOS/OrbStack + 单次测量窗口；绝对数字不可与历史 Issue 28 快照或其他窗口混用。
- 运行镜像元数据 `PERI_IMAGE_VERSION=v1.2.7` 两侧相同；真正被测的是挂载的平台/worker
  二进制，镜像自带的二进制未参与。
- Rust 构建使用 rsproxy 在线解析（R24 记录的离线 registry 卷已为空/不完整）；两侧命令与
  工具链相同，但二进制字节与 CI 镜像内构建不保证逐字节一致。
- 第一次 32 腿窗口因缺少逐腿容器内 sha256 证据被保留为预跑、未用于判决；正式窗口是
  21:01–21:08 的 `legs/`。

## 证据与复现

```bash
cd docs/performance-review/2026-11-04-v1.2.7-release
python3 aggregate.py > aggregate.txt        # 退出码 1：F1 判 FAIL，属预期判决而非脚本故障
python3 invariants.py > invariants.txt      # 退出码 0：32/32 不变量通过
```

- 正式证据：`legs/`（32 腿 × summary/raw/metrics/stop/binaries/image）、`legs.txt`、
  `aggregate.txt`、`invariants.txt`。
- 预跑证据：`pre-run-legs/`、`pre-run-legs.txt`、`pre-run-aggregate.txt`（不用于判决）。
- 脚本：`run_leg.sh`、`run_all.sh`、`seed_fixture.py`、`snap_metrics.py`、
  `aggregate.py`、`invariants.py`、`perf-protocol.md`。
- `connection.json` 只在 `/tmp/peri-release-ab/` 使用（chmod 600），未进入仓库；证据目录
  已扫描，无 `Bearer`、`project_api_key` 或 launch token。
- 卷保留：正式窗口 32 个 `peri-loom-release-v127-b-*` 卷与预跑 32 个
  `peri-loom-release-v127-*` 卷均未删除；未触碰 `peri-loom` 生产容器/`peri-loom-data`。

## 后续更正（2026-11-05）

本节只追加，不修改上文任何判决记录与数字。

**F1.select-1 / json-pipeline 的 FAIL 未被更高功效窗口复现。** 独立窗口沿用本报告冻结协议
（同镜像 `sha256:8a8ba43b…`、同两组平台/worker 二进制、每腿全新卷、2 轮 × A/B/B/A、
无观测器、默认配额），把每腿 timed 样本从 5 提到 20，8/8 腿有效：

| 轮 | 基线中位 | 候选中位 | 比值 |
|---|---:|---:|---:|
| round1 | 2.631ms | 2.106ms | 0.800 |
| round2 | 2.384ms | 3.042ms | 1.276 |
| 合并 | 2.631ms | 2.413ms | **0.917** |

两轮方向相反，合并 0.917 低于 1.10 阈值。样本级（80/侧）Mann-Whitney 双侧
**p=0.997**、permutation p=0.386/0.484/0.787、bootstrap 95% CI 均跨 0；
10% trimmed mean 比值 0.912；>5ms 样本占比 A 21.2% / B 16.2%。

**本报告原窗口自身的样本级数据也无法区分该差异与噪声**：40 个 timed 样本
Mann-Whitney 双侧 **p=0.989**、permutation p=0.599。1.188 来自「每轮 2 条腿中位
再取中位」这一低功效估计量——在腿内 CV 45–91% 的分布上，它的方差远大于要检测的
0.24–0.5ms 效应。因此上面的 FAIL 判决按当时冻结规则仍然成立，但应理解为
**低功效估计量导致的判决**，不应被当作已证实的产品回归。

三条事实更正（详见新证据目录）：

1. `v1.2.6` tag（11-03 15:51）**没有 fused close**，也没有 `hrana_fused_open.go`；
   R43 `6d5c9da`（11-04 00:06）才引入，`17854a8`（11-04 18:00）撤销。因此
   「撤销 fused close」不是 v1.2.6→v1.2.7 的行为差异。
2. v1.2.6→v1.2.7 实际有 **17 个非 merge 代码提交**（R39/R42/R46–R50/R51/R52/
   R56/R57/R64 等），不是「R64 + 撤销 fused close」两类。若差异未来复现，
   **R39（worker 每库 open/close 闸门）**比 R64 更值得先测。
3. R64 在这组对比中**未生效**：v1.2.7 的 `deferred_durability.pending_max` 为
   17/35/18/22，远低于旧上限 256，`inline_total=0`。

**方法结论**：亚 5ms 场景不能用「腿中位的中位」做验收。按 σ≈2ms 估算，检测 0.5ms
差异、80% power 需要约 **248 样本/侧**；后续这类场景改用样本级合并统计或请求级
交错 A/B。

证据：`docs/performance-review/2026-11-05-f1-release-attribution/`（协议、脚本、
8 腿数据、机制证据与统计脚本）。
