# v1.2.0 发布级 A/B 与目标验收（2026-11-03）

本目录记录 v1.2.0 发版前的 endpoint A/B（上一版 v1.1.27 vs 本版）、机制计数器核验，
以及同窗口的 sqld 客户端路径对照。背景与授权见 issue #30。

## 环境与二进制

- 运行时镜像 `ghcr.io/konghayao/peri-loom@sha256:13ab3a908401c7447ce84109e098862ce3c4481dac7c82dba15fa7f073ffe1c1`
  （v1.1.27；两侧使用同一镜像作为运行环境）。
- A 侧（v1.1.27）平台：镜像自带 `/usr/local/bin/peri-platform`，
  sha256 `37fd35d5383110150923d7dfcbf6e4fb23778e5911fd39f4418d2fee39e35ff8`。
  用 `golang:1.23-bookworm`（go1.23.12，`CGO_ENABLED=1`，`-trimpath`）从 tag `v1.1.27`
  重建后与该二进制**逐字节一致**，确认构建配方可复现发布产物。
- B 侧（v1.2.0 候选）平台：同工具链从 `e04b0f3` 重建，
  sha256 `9085f03554bc81e1469e441f23c3c31a0601675b71d65b752b24f907861635e0`。
- worker 两侧均为镜像自带 `/usr/local/bin/peri-loom-v1`，
  sha256 `d6f1e6df55f894eff4e4ec447ff30d5273f79e43b58a4cd404be49f9839bee68`；
  `git diff v1.1.27..main -- prototypes/v1/src prototypes/v1/Cargo.toml prototypes/v1/Cargo.lock`
  为空，Rust 源码无差异。
- 默认启动配置：`--backend local --listen 127.0.0.1:8092 --no-build`，未设置
  `PERI_COMPLETION_JOURNAL`（即发布默认的 `audit_file_v1`）。配额
  `max_sessions=1024`、`max_concurrent=1024`、`requests_per_second=100000`、
  `max_sql_bytes=65536`、`max_result_bytes=4194304`。容器 `docker stop -t 60`。

## 方法

- 数据集：201 threads / 8,625 messages / 20MB content，每侧全新卷重新导入并校验。
- 仓库内 harness `scripts/perf-reliability/issue28/run.py`：`--rounds 3 --warmup 1
  --samples 5 --post-idle-seconds 30`，47 个 scenario×protocol 行 + c1/c4/c8/c16 并发。
- 顺序 A/B/B/A，每侧新卷；比较取每侧 6 个 round median 的中位数。
  回归判定：相对 A 侧 >10% 且两个 cycle 同向；同时检查 p95。
- 候选侧只挂载平台二进制；镜像、worker、dev.py、数据与客户端保持两侧一致。

## 结果

### endpoint A/B（默认 `audit_file_v1`）

`endpoint-ab-compare.txt` 为完整表。47 行中：

- **无 >10% 回归**（中位数与 p95）；p95 全部下降，最小降幅 9.1%
  （`F1.select-1` protobuf-pipeline 22.00→20.00ms）。
- 46 行在两个 cycle 同向改善：短查询/列表/按 id/投影 −83%～−95%，
  20MB content 读取 −27%～−33%，超限场景 −14%～−34%，写场景 −23%～−72%。
- `W1.delete` json-pipeline 中位数 −39%，但两个 cycle 方向不一致（候选 r1 受写路径
  方差影响），按不可确认处理，不宣称改善、也不是回归。
- 行数、状态码与错误语义两侧一致：`ok` / `expected_limit` 计数相同，
  无 409/5xx；`B1.batch-20` 与 `W1.insert` 的响应字节数存在 ±0.4% 的载荷内容差异，
  同一侧不同轮之间同样存在。

并发（json-pipeline，每级 100 请求）：

| level | A p50 | B p50 | Δ | A p95 | B p95 | Δ | B errors/429 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| c1 | 16.94 | 1.75 | −89.7% | 25.11 | 3.61 | −85.6% | 0/0 |
| c4 | 22.18 | 4.46 | −79.9% | 34.20 | 10.18 | −70.2% | 0/0 |
| c8 | 18.09 | 3.89 | −78.5% | 24.99 | 8.02 | −67.9% | 0/0 |
| c16 | 23.28 | 7.73 | −66.8% | 30.57 | 16.07 | −47.4% | 0/0 |

### 机制核验（Gate 0）

空库、c1、50 次客户端查询（connect→all→close）：

| | v1.1.27 | v1.2.0 |
| --- | --- | --- |
| qps | 48.74 | 166.33 |
| p50 / p95 / p99 (ms) | 20.39 / 27.67 / 28.12 | 5.79 / 7.33 / 8.38 |
| `operation_journal_syncs_total` / 请求 | 0.40 | 0.40 |
| `sync` 平均耗时 | 1.69ms | 1.25ms |
| audit `fsync_count`（同一窗口） | 222 | 174（后台合并） |

同步次数/请求不变、审计 fsync 仍然执行，说明变快来自把屏障移出回复路径，
而不是跳过持久化；这与 A/B 四侧审计链均为 10,605 条、逐条 hash/sequence 连续、
`launcher-terminal.json` 干净关闭证明齐全、无 `audit_unavailable` 一致。
`platform.log` 中 108 条 `result_limit` 是 R1 超限场景的预期结果。

### 同窗口 sqld 对照（目标验收）

3 轮交错、每轮每场景 9 次，取 3 轮均值的中位数（客户端 SDK，默认 `audit_file_v1`）：

| 场景 | sqld | v1.2.0 | 比值 | 结果 |
| --- | --- | --- | --- | --- |
| session_list_cellp | 4.66ms | 6.31ms | 1.36× | PASS（≤1.5×） |
| session_list_peri | 3.81ms | 5.37ms | 1.41× | PASS（≤1.5×） |
| session_by_id | 4.53ms | 4.89ms | 1.08× | PASS（≤1.5×） |
| history_content（20MB） | 918.84ms | 274.33ms | 0.30× | PASS |
| history_projection（8,625 行） | 141.01ms | 44.02ms | 0.31× | PASS |
| history_full | 940.99ms | 286.19ms | 0.30× | PASS |

行数两侧一致（20/0/1/8625）。SQL-only 时间上 peri 侧仍为 sqld 的 1.24×～1.74×，
端点目标已收敛；worker 侧查询耗时是下一轮候选方向。

## 限制

- 未做真实掉电/机器崩溃；R37 的掉电语义与故障注入结论见
  `docs/performance-review/2026-11-03-r37-crash-recovery/`。
  掉电丢失 close 尾会按现有规则阻止 clean restart 并隔离。
- unified journal（`PERI_COMPLETION_JOURNAL=1`）的 64 个 completion 槽仍限制
  dispatch 并发；本目录的发布 A/B 使用默认 `audit_file_v1`，未覆盖该配置。
- deferred 池饱和时会转为排队等待，p99 在持续高并发下可能抬升；本目录 c16 未饱和。
- sqld 对照使用本机同一窗口的单个 sqld 进程；绝对值只对本环境有效。

## 复现

```bash
# endpoint A/B（需先构建两侧二进制；脚本内为绝对路径）
OUT=/tmp/r37/rel-ab2 VOL_PREFIX=peri-loom-rel2 /tmp/r37/release-ab.sh
python3 /tmp/r37/analyze-release.py

# 同窗口 sqld 对照
OUT=/tmp/r37/target-ab3 VOL=peri-loom-rel2-target3 /tmp/r37/release-target.sh

# 审计链校验
docker run --rm -v <volume>:/data/peri --entrypoint cat alpine:3 /data/peri/audit/events.json \
  | python3 verify-audit.py
```

`release-ab.sh`、`release-target.sh`、`analyze-release.py`、`verify-audit.py` 均随本目录归档；
harness 本身在 `scripts/perf-reliability/issue28/`。
