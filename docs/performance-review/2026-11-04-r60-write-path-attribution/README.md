# R60：W1 写路径的耐久等待归因（测量轮，无性能改动）

日期：2026-11-04
结论：本轮不改平台/worker 代码；W1 单条写请求 ≈ worker 写执行 17.6ms + 耐久屏障 ≈20ms，
`intent` 的 12.6ms 是**排队**（上一条请求的延迟会话记录仍占用 journal/audit 屏障），
不是 checkpoint CPU、也不是 SQL 执行。两条 ≥10% 的方向都在授权边界上（耐久提交顺序、
审计 checkpoint 频率/成本），本轮不实施。

## 目的与背景

R59 把 X1 c16 尾部归因到单线程审计写者的屏障与 checkpoint 停顿，并给出下一轮提示：
授权前先做授权内场景（H1 平台页路径、W1 写路径）的归因。H1 已由 R58 归因（worker
turso VM ≈17–18ms、页序列化 ≈6.6ms、平台自身 ≈6.9ms、客户端 20MB 读取 ≈7ms），
本轮补 W1。

## 环境与二进制

| 项 | 值 |
|---|---|
| 仓库 | `main` @ `3671c1c`（二进制构建后只有 docs 提交） |
| platform | `/tmp/peri-perf/bin/peri-platform-r58-main`，sha256 `f4210c08…` |
| worker | `/tmp/peri-perf/bin/peri-loom-v1-r58-main`，sha256 `a648ecd9…` |
| 启动 | `dev.py --run-dir /tmp/peri-perf/run-r60-w1attr --listen 127.0.0.1:19681 --no-build`，`PERI_HRANA_EXPERIMENTAL=1`，无 TLS，默认配额 |
| 观察器 | `PERI_DURABILITY_OBSERVER=1` + `--sql-open-observer`（phase sample permille 1000、threshold 0） |
| harness | `run.py --scenario W1 --rounds 1 --warmup 1 --samples 3`，窗口 1.89s，16 个测量请求全部 200/ok |
| fixture | 201 threads / 8,625 messages / 20,177,069 chars（seed 校验通过） |

观察器会放大请求计数并压低吞吐，**本文件所有绝对数值只用于归因，不是可认证结果**。

## 请求相位（窗口内 1.89s）

窗口内共 48 open / 48 statement / 48 close：16 个测量请求（4 warmup + 12 timed）各产生
open→statement→close 三个动作，另有 32 条 harness 自身 setup/cleanup 语句（同为 Hrana execute）。

| action | n | total p50 / p95 / max (ms) | 主要相位 p50 (ms) |
|---|---:|---|---|
| statement | 48 | 37.85 / 46.68 / 48.12 | native 17.59、**intent 12.63**、audit 3.81、terminal 3.79 |
| open | 48 | 1.08 / 2.23 / 2.71 | native 0.93（延迟记录，回复前不等屏障） |
| close | 48 | 0.45 / 1.08 / 1.17 | native 0.29 |

## 关键证据

1. **intent 与 SQL 无关**：4 条语句的 native 只有 1.3–1.6ms，intent 仍 10.9–12.8ms
   （p50 11.27ms、total p50 21.26ms）；native >10ms 的 42 条为 native 17.95、intent 12.65、
   audit 3.79、terminal 3.81。即 intent 等待不取决于语句本身。
2. **客户端严格串行**：同一连接上 open 结束到 statement 开始只有 5–39µs（p50 13µs，n=48），
   两个请求之间没有任何重叠。R37 的延迟耐久账户让 open 的 completion/audit 在回复后才持久化，
   这些记录在 statement 到达时仍在途。
3. **不是 checkpoint CPU**：44/48 的 intent 同时与 `audit_batch` + `journal_batch` 重叠，
   4 条与 `audit_checkpoint` 重叠；三类的 intent 均值分别为 12.78 / 12.48 / 12.38ms
   —— 与 checkpoint 重叠的反而没有更慢。
4. **两条屏障链在窗口内几乎占满**：`audit_batch` 143 批（142 批 count=1，sync p50 3.87ms，
   sync 合计 669ms）、`journal_batch` 145 批（98 批 count=1，sync p50 6.64ms，sync 合计 867ms），
   两条链合计 1.54s / 1.89s 窗口。全run 计数：`audit_batch` 601（600 批 count=1）、
   `journal_batch` 1027（980 批 count=1）、`journal_member_wait` p50 2.70ms、
   `audit_member_wait` p50 3.92ms、`journal_prepare` p50 35µs、`journal_namespace_wait` p50 0.4µs。
5. **checkpoint 成本 = 4 次 fsync**：全run 18 次、p50 **21.00ms**（18.98–24.16）、合计 386ms；
   窗口内 4 次合计 87ms（4.6%）。代码路径：`checkpointAfterBatch` 在
   `sequence - head.revision >= auditBatchMaximum(32)` 时触发 `commitHead` →
   `commitAuditBatch` → `persist`，`persist` 写 `head.path` 与 `head.path+".floor"` 两个文件，
   各自 `atomicPrivate`（tmp 写 + fsync + rename + 目录 fsync）= 每次 checkpoint 4 次 fsync。

## 机制判断

`statement` 的 `intent` 相位覆盖 `beginDispatchOperation` → `registerOperation` →
`beginCorrelatedOperation`：把「可能提交」的意图写入 operation journal，并等待它 durable。
journal 是单写者、按命名空间有序，此刻队列里还有上一条 open 的延迟记录（其 audit 与 journal
屏障都在跑），所以 intent 的屏障落在它们之后。12.63ms ≈ 3 个屏障单位
（`journal_batch` sync p50 6.64ms 的窗口值 + 一次 audit 3.87ms），与「排队」一致。

## 判决

- W1 写请求的成本 = worker 写执行（native 17.6ms，SQL-only 16–18ms，属存储/引擎）
  + 耐久屏障（intent 12.6 + audit 3.8 + terminal 3.8 ≈ 20ms）。两部分都不在授权内的平台
  编码/分配路径上。
- **两条 ≥10% 的方向都在授权边界**：① 让提交意图不再排在上一条请求的延迟会话记录之后
  （合并或重排 durable 记录顺序）——耐久提交时点/journal 顺序；② 降低 `audit_checkpoint`
  的单次成本或频率（每 32 条审计记录一次、每次 4 次 fsync）——审计链与恢复语义。
- 因此本轮只记录，不实施；不在未授权时改动耐久语义。

## 附：R1 smoke 与 harness protobuf Error 字段修正

- 修正前的一次 R1 smoke 里 `R1.limit-5000 protobuf` 被判 `error`（json 为 `expected_limit`）。
  原因是 harness 解析 protobuf `Error` 时把 field 1 当 code、field 2 当 message；Hrana 3 规范
  是 `message = 1`、`code = 2`（平台 `hranaProtoError` 与之一致）。
- 修正 `scripts/perf-reliability/issue28/hrana.py`（pipeline 与 cursor 两处字段号）并更新
  `test_issue28_support.py` 的用例；离线 32 tests OK。修正后 protobuf 的错误被正确归为
  `expected_limit`。
- 修正后的 R1 smoke（1 轮 × 3 samples，**仅 smoke、不可认证**）：

| 场景 | json 中位 | protobuf 中位 | 结果 |
|---|---:|---:|---|
| R1.limit-100 | 1.92ms | 1.64ms | — |
| R1.limit-500 | 12.69ms | 11.53ms | — |
| R1.limit-1000 | 18.35ms | 19.28ms | — |
| R1.limit-2000 / 5000 / 8625 | 18.05 / 18.90 / 19.78ms | 20.77 / 20.08 / 18.90ms | `expected_limit`（4MiB 结果预算） |

- 结论：R1 的 100 行样本 p95 15.32ms、500 行 p95 20.83ms，3 样本内跨度大；R1 若要作为
  A/B 主指标需要多采样，不能据此 smoke 宣称任何变化。

## 证据文件

- `summary.md` / `summary.json` / `raw.jsonl`：W1 观察器运行的 harness 输出。
- `durability-observation.jsonl.gz` / `request-phase-observation.jsonl.gz`：原始观察器事件。
- `aggregate.py`：相位与耐久计数聚合脚本（`p50/p95`、窗口过滤、重叠分类）。
- `smoke-r1-prefix/`：修正前的 R1 smoke 输出（保留 protobuf 误分类证据）。
- `smoke-r1-postfix/`：修正后的 R1 smoke 输出。
