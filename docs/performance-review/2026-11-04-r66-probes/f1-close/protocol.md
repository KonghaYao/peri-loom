# R66 F1 close 成本 smoke 测量协议（测量轮，零代码）

冻结时间：2026-11-04T19:51:00+08:00（本文件在起栈前写入；测量开始后不再修改）
仓库：`/Users/kk/code/peri-loom`，`main` = `39e8e6cf9b41fbb32190c0b414a77edbe17e6535`
性质：**smoke 测量，不是对外性能结论，不构成容量/SLA 证据。** 不改生产代码、不改仓库脚本、不提交、不 push、不 merge。

## 1. 目的

在当前 main 上（`17854a8` 已撤销 R43 的适配器 fused close）测 F1.select-1 json-pipeline 中
`close` 动作的实际平台侧成本与相对 F1 端到端延迟的占比，为是否申请「减少协议帧数 / 重新引入
fused close」授权提供当前数字。R65 的 F1 数字（0.525ms）测于撤销之前，不作为当前基线。

## 2. 环境与二进制

| 角色 | 路径 | sha256 | 来源 |
|---|---|---|---|
| 平台 | `/tmp/peri-perf/bin/peri-platform-r66-main` | `44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104` | `go build ./cmd/peri-platform`，darwin/arm64，`vcs.revision=39e8e6cf…` |
| worker | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | 已发布 worker 二进制 |

- 本地 `dev.py` 栈（local backend，无 TLS），每次测量前用 `shasum -a 256` 复核两个二进制。
- 全新 run 目录与独立本地卷（每轮一个）：`/tmp/peri-r66-f1/run-round{1,2}`；测量前 `rm -rf`，
  测量后保留作为证据（含 `connection.json`，不复制到对外证据目录、不打印其内容）。
- 网关监听：round1 `127.0.0.1:20601`，round2 `127.0.0.1:20602`（20601–20606 范围内）。
  authority / broker / 两台 worker 的环回地址由 `dev.py` 用 ephemeral 端口分配，dev.py 不提供
  单独端口参数；不改 dev.py，因此这些内部端口不保证落在 20601–20606。报告中如实记录。
- 默认配额，不改 `max_concurrent` / `max_sessions` / `max_result_bytes` / `requests_per_second`。
- **不开启** `PERI_DURABILITY_OBSERVER` 或 `PERI_SQL_OPEN_OBSERVER`（保持关闭）。
- 不操作生产容器 `peri-loom` 与卷 `peri-loom-data`。

## 3. Fixture

使用仓库只读脚本 `docs/performance-review/2026-11-04-r65-r64-regression/seed_fixture.py`
（适配副本只写到 `/tmp`），每轮从空库重建同一形状：

- threads = 201
- messages = 8625
- sum(length(content)) = 20177069
- focus thread messages = 8625

用 `issue28.fixture.discover()` 校验并写 `fixture.json`；任一项不符即该轮无效。

## 4. 场景与请求数

| 项 | 值 |
|---|---|
| 场景 | `F1.select-1`（SQL：`SELECT 1`，want_rows=true，期望 1 行） |
| 协议 | `json-pipeline`（`/v3/pipeline`） |
| pipeline 形状 | `[execute SELECT 1, close]`，单个外部 HTTP 请求 |
| 每轮 timed 请求 | **50** |
| warmup | **0**（不设 warmup；第 1 个 timed 样本包含冷启动，2 个独立轮次结构相同） |
| 轮次 | **2 轮 A/A**，同一平台与 worker 二进制，每轮全新栈 + 全新卷 + 重新导入同一 fixture |

- 轮 1：全新栈 + seed + fixture 校验 → `/v1/metrics` 快照 → 50 请求 → `/v1/metrics` 快照 → clean stop。
- 轮 2：同样流程，使用 20602 端口与新 run 目录。
- `/v1/metrics` 快照由 `/tmp` 适配脚本取完整 `result`，使用 `global_api_key`；fixture discovery
  （open/query/close）发生在 metrics-before 之前，因此**指标窗口恰好是 50 个 timed 请求**。

## 5. 指标与计算口径

指标端点 `/v1/metrics`，当前 main 的相关字段（已核对 `prototypes/v1/platform/metrics.go`）：

- `actions_total`: map[action]count，已应答请求数（含 refusal）。
- `action_latency_ns_total`: map[action]累计纳秒，**包含 refusal**；语义字符串
  `all_answered_requests_including_refusals`。
- `successful_latency_ns_total`: map[action]仅成功请求的累计纳秒；用于交叉核对。
- Hrana 适配器的内部 `handler.call` 通过 `httptest.NewRequest` 调用 `Gateway.ServeHTTP`，
  因此内部 `open` / `statement` / `close` 都会进入上述 action 账本。

计算（每轮独立）：

1. `F1_p50`、`F1_p95`、`F1_mean` 来自 harness 同源 summary/raw.jsonl 的 timed 样本。
2. `close_count = Δactions_total.close`，`close_mean = Δaction_latency_ns_total.close / close_count`。
3. 同样计算 `open`、`statement` 的 count 与 mean（以及 `describe`/`query`/`cursor_*` 若出现）。
4. 占比 = 各 action mean / `F1_mean`（同一 50 请求窗口的均值，不使用 max 或生命周期峰值）。
5. 上限估计：
   - 乐观上限（整个 close 入口消失）= `close_mean / F1_p50`；
   - 同时报告 `close_mean / F1_mean`；
   - «仅省 worker close RPC、平台 close 入口保留» 的节省 ≤ `close_mean`，metrics 无法把
     worker RPC 从 close 处理器内部拆出；R43 的约 0.125ms RPC / 0.143ms 入口只作为外部先验，
     不作为本轮实测值。融合顺带省掉的内部 hop/编排成本也包含在 `close_mean` 内，与 RPC 成本
     一起构成上限；外部 HTTP 请求数在 `[execute, close]` 形状下本来就是 1，不会因 fused close 减少。

## 6. 有效性判据（每轮）

以下全部满足才算有效；否则记录并整轮重跑一次，仍无效则报告为无效：

- 50/50 个 timed 样本，`outcomes` 全部为 `ok`，`status_counts` 全部 200，`error_codes` 为空；
- `rows` 中位 = 1，`warnings` 为空，`fatal` 为空；
- fixture `complete=true` 且 threads/messages/content_chars/focus_messages 四项校验全 OK；
- 指标增量：`errors_total` 与 `rejected_requests_total` 增量为 0；`close` 增量 = 50；
  `open` 增量为可解释值（预期 50；若 fused open 关闭则为 50），`statement` 增量按实际记账；
- metrics-after 的 `session_seats = 0`、`active_permits = 0`、`unknown_held_permits = 0`；
- `launcher-stop.json` 的 `outcome` = `clean` 且 `unresolved_operations` = false；
- 无 429；不出现 `outcome_unknown` / `session_moved` / `audit_unavailable`。

## 7. 证据

`/tmp/peri-r66-f1/`：

- `protocol.md`（本文件）
- `round{1,2}/fixture.json`、`metrics-before.json`、`metrics-after.json`、`summary.json`、
  `summary.md`、`raw.jsonl`、`analysis.json`、`launcher-stop.json`、`run.log`
- `aggregate.json` / `report.md`（两轮汇总）
- 不复制 `connection.json`，不记录 API Key。
