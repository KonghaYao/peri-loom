# R66 F1 json-pipeline close 成本 smoke 报告

日期：2026-11-04（CST）
性质：**smoke 测量，零代码改动，不是对外性能结论，不构成容量/SLA 证据。**
仓库：`/Users/kk/code/peri-loom`，`main` = `39e8e6cf9b41fbb32190c0b414a77edbe17e6535`
冻结协议：`/tmp/peri-r66-f1/protocol.md`（2026-11-04T19:51:00+08:00）；
补充诊断协议：`/tmp/peri-r66-f1/supplementary-protocol.md`（2026-11-04T20:05:00+08:00）。
测量开始后未修改协议；补充诊断独立于主 2×50 A/A，不作为主判据。

## 1. 结论摘要

| 项目 | 当前 main 实测（2×50，smoke） |
|---|---:|
| F1.select-1 json-pipeline 合并 p50 / p95 / mean | **0.5449ms / 7.6276ms / 1.2820ms** |
| `close` 动作次数 / 累计 / 均值 | **100 次 / 24.8217ms / 0.2482ms** |
| `close` 均值 / F1 p50（整个 close 入口消失的上限） | **45.5%** |
| `close` 稳健估计（补充诊断中位数 / 截尾均值） | 0.2330ms / 0.2268ms |
| 稳健 close / F1 p50 | **42.8% / 41.6%** |
| open / statement / close 占 F1 mean | **51.6% / 19.9% / 19.4%**（residual 9.2%） |

- `action_latency_ns_total` 在当前 main 仍存在，键为动作名（`open`/`statement`/`close`/…），
  语义为 `all_answered_requests_including_refusals`；本窗口 0 refusal，故等价于成功请求。
- `close` 动作是**一次内部 `/v1/sql/close` 网关调用**：包含 worker `close_session` RPC
  + 平台 close 处理（准入、会话移除、完成记录入队、响应编码），**不包含** R37 延后耐久
  的 fsync 等待。它不是纯 worker RPC 计数器。
- 按 R43/R65 的跨窗口参照，R43 式融合会把 worker close 工作移入 open，close 动作账本
  相差约 **0.125–0.139ms**；但这不是 E2E 节省（R65 fused-close 的 F1 p50 0.465–0.602ms
  与当前 0.545ms 没有出现对应差异）。若融合进一步省掉整个 close 入口，机械上限是
  **0.248ms ≈ 45.5% of F1 p50**，同样不是预期 E2E 收益。两者都必须用 close-only 的
  受控 A/B 验证。
- 外部 HTTP 请求数：`[execute, close]` 本来就是一个外部 pipeline 请求，fused close
  不会减少外部 HTTP 帧；它省的是内部 close hop / 平台编排 / worker RPC。
- 所有数字为 smoke；不得作为对外性能结论。

## 2. 环境与二进制

| 角色 | 路径 | sha256 | 说明 |
|---|---|---|---|
| 平台 | `/tmp/peri-perf/bin/peri-platform-r66-main` | `44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104` | go1.27.3 darwin/arm64，`vcs.revision=39e8e6cf9b41fbb32190c0b414a77edbe17e6535` |
| worker | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | 与 R65 回归门同一 worker 二进制 |

- 本地 `dev.py` 栈，local backend，无 TLS，默认配额（`max_concurrent=100`、`max_sessions=16`、
  `max_result_bytes=4194304`、`requests_per_second=500`、`max_sql_bytes=65536`）。
- 未开启 `PERI_DURABILITY_OBSERVER` / `PERI_SQL_OPEN_OBSERVER`。
- 未操作生产容器 `peri-loom` 与卷 `peri-loom-data`。
- 网关端口：round1 `20601`、round2 `20602`、补充诊断 `20603`；authority/broker/两台 worker
  由 `dev.py` 分配 ephemeral 环回端口（dev.py 不提供单独端口参数，未改 dev.py）。
- 每轮独立 run 目录与独立本地卷：`run-round1`、`run-round2`、`run-supp`；每轮重新导入同一 fixture。

### 2.1 worker 能力核对

直接向 worker 发只读 `capabilities` 控制命令（临时栈，端口 20604；不打印令牌）：

```
worker-a/b: {"open_guarded_statement":2,"readonly_batch_protocol":1}
```

直接发 `open_guarded_statement`（`SELECT 1`）也返回 200、session 与 statement 结果，随后
`close_session` 返回 `closed=true`。因此本环境 worker 支持 fused open；`statement` 动作的
存在并不表示第二次 worker statement RPC（见 §5.2）。

## 3. 主测量：2 轮 A/A，每轮 50 请求

- 场景：`F1.select-1`，SQL `SELECT 1`，`want_rows=true`。
- 协议：`json-pipeline`（`/v3/pipeline`），pipeline 形状 `[execute SELECT 1, close]`。
- **warmup=0**；每轮 50 个 timed 请求。第 1 个样本包含冷启动，2 轮结构相同。
- 每轮全新栈 + 重新 seed + fixture 校验；metrics-before 在 fixture discovery 之后、
  第一个 timed 请求之前；metrics-after 在最后一个 timed 请求之后。因此指标窗口恰好是
  50 个请求。
- fixture 校验（两轮均 OK）：threads=201、messages=8625、content_chars=20177069、
  focus_messages=8625，`complete=true`。

### 3.1 有效性

| 检查 | round1 | round2 |
|---|---|---|
| timed 样本 | 50/50 | 50/50 |
| outcomes | `ok=50` | `ok=50` |
| HTTP 状态 | 200×50 | 200×50 |
| 429 | 0 | 0 |
| error_codes | 空 | 空 |
| rows（中位） | 1 | 1 |
| response_bytes（中位） | 312 | 312 |
| fixture complete | true | true |
| `session_seats`（after） | 0 | 0 |
| `active_permits`（after） | 0 | 0 |
| `unknown_held_permits`（after） | 0 | 0 |
| clean stop | `clean` | `clean` |
| `unresolved_operations` | false | false |
| 全部 stage exit_code | 0 | 0 |

- 两轮 `deferred_durability.pending` 在 metrics-after 时分别为 180；这是 R37 的“回复后异步
  持久化”账户尚未排空，不是泄漏；clean stop 已排空。`audit.queue_count=79`、`head_checkpoint_failures_total=0`。
- 第一次起栈尝试在发出任何请求前失败（run 目录被我预建为 755，`dev.py` 要求私有目录）；
  该失败日志保留在 `attempt1-startup-failed/`，不属于测量数据。

### 3.2 F1 端到端

| 轮次 | samples | p50 | p95 | mean | min | max |
|---|---:|---:|---:|---:|---:|---:|
| round1 | 50 | 0.5475ms | 8.5150ms | 1.2313ms | 0.4663ms | 16.7910ms |
| round2 | 50 | 0.5386ms | 7.5809ms | 1.3327ms | 0.4629ms | 16.7098ms |
| 合并 | 100 | **0.5449ms** | **7.6276ms** | **1.2820ms** | 0.4629ms | 16.7910ms |

- 两轮都在第 14 个请求附近出现约 16.7ms 的确定性停顿（round1 16.791ms、round2 16.710ms），
  另有 round1 的 8.515/9.967ms、round2 的 7.581/3.600/13.440ms。它们把 mean/p95 抬高，
  但 p50 稳定在 0.54–0.55ms。
- 该停顿落在哪个动作会直接改变该动作的累计均值；这是本轮 close 均值在 0.183–0.314ms
  之间跳动的原因（见 §4/§6）。

### 3.3 动作累计与均值（`actions_total` / `action_latency_ns_total` 增量）

| 轮次 | 动作 | 次数 | 累计 | 均值 |
|---|---|---:|---:|---:|
| round1 | open | 50 | 30.5601ms | 0.6112ms |
| round1 | statement | 50 | 9.3177ms | 0.1864ms |
| round1 | close | 50 | 15.6983ms | 0.3140ms |
| round2 | open | 50 | 35.5414ms | 0.7108ms |
| round2 | statement | 50 | 16.1719ms | 0.3234ms |
| round2 | close | 50 | 9.1235ms | 0.1825ms |
| 合并 | open | 100 | 66.1015ms | 0.6610ms |
| 合并 | statement | 100 | 25.4896ms | 0.2549ms |
| 合并 | close | **100** | **24.8217ms** | **0.2482ms** |

- `successful_latency_ns_total` 增量与 `action_latency_ns_total` 增量在上述三个动作上逐项相等
  （0 refusal / 0 error）。
- 其他动作（`query`/`describe`/`cursor_*`）在窗口内增量为 0；fixture discovery 的 `open`/`query`/
  `close` 在 metrics-before 之前，已排除。
- 两轮 close 次数都恰好 50，说明每个 F1 pipeline 的 close 条目都到达了网关 close 动作。

### 3.4 占 F1 E2E 的比例（同一窗口均值）

| 轮次 | open / F1 mean | statement / F1 mean | close / F1 mean | residual |
|---|---:|---:|---:|---:|
| round1 | 49.6% | 15.1% | 25.5% | 9.7% |
| round2 | 53.3% | 24.3% | 13.7% | 8.7% |
| 合并 | **51.6%** | **19.9%** | **19.4%** | **9.2%** |

- 这是 **mean/mean** 口径，符合任务要求；不使用 max 或生命周期峰值。
- residual 是外部 HTTP、Hrana 适配器编解码、pipeline 封装等未被内部 action 账本覆盖的部分。
- round1 与 round2 的 close 占比差异来自 16.7ms 停顿落在不同动作，不代表两轮 close 语义不同。

## 4. close 成本与上限

### 4.1 直接可测

- 合并 100 次 close：累计 24.8217ms，均值 **0.2482ms/次**。
- 分轮：round1 0.3140ms、round2 0.1825ms；差异由周期性停顿落在 close 的次数不同造成。
- 补充诊断（逐请求快照）close：中位数 **0.2330ms**、截尾均值 **0.2268ms**、p95 0.3411ms、
  min 0.1261ms、max 9.3282ms。它把“典型 close”与“停顿落在 close 的离群请求”分开。
- 因此当前 close 的合理区间是 **约 0.23–0.25ms**（典型/稳健），短窗口均值可因停顿落在 close
  而升到 0.31ms。

### 4.2 上限口径

| 口径 | 计算 | 结果 |
|---|---|---:|
| 整个 close 入口消失（最乐观） | 0.2482 / 0.5449 | **45.5%** |
| 稳健 close（补充中位） / F1 p50 | 0.2330 / 0.5449 | 42.8% |
| 稳健 close（补充截尾均值） / F1 p50 | 0.2268 / 0.5449 | 41.6% |

### 4.3 区分“close RPC”与“HTTP/编排”

`/v1/metrics` 只给每个动作的累计总时延，不能把 worker close RPC 从 close 处理器内部拆出。
可用同一 worker 二进制、不同平台版本的跨窗口参照给出估计（**不是本轮受控 A/B**）：

| 参照 | 值 | 来源 |
|---|---:|---|
| 当前 close 动作均值（含 worker RPC + 平台 close） | 0.2482ms | 本轮合并 100 次 |
| R65 F1 腿 close 动作均值（R43 fused close 生效，跳过 worker close RPC） | 0.1097ms | `docs/performance-review/2026-11-04-r65-r64-regression/legs/F1.select-1/r1a1/metrics-after.json`（close 2,852,749ns / 26） |
| 动作账本差值 | 0.1385ms | 0.2482 − 0.1097 |
| R43 形状探针的 close 入口 / 融合节省 | 0.143ms / 0.125ms | CLAUDE.md R43 记录 |

**但动作账本差值不能直接当作 E2E 节省。** 同一 R65 窗口的 8 条 F1 腿（fused close 生效）
p50 为 **0.465–0.602ms**，当前 100 样本 p50 为 **0.545ms**；两者没有出现与 0.14ms
close 动作差一致的系统性差异。原因至少有三点：

- R43 的 fused close 是把 worker close 工作**移入 open 请求**（`close_after_statement`），
  worker 的 close 工作本身不消失；它省的是 RPC 边界/内部 hop，不是 close 工作。
- 平台 close 入口（准入、完成记录、会话移除、响应编码）在 R43 设计中仍保留。
- R65 每条腿只有 5 个 timed 样本，p50 跨腿波动 0.465–0.602ms，足以掩盖 0.1ms 量级的差异。

因此：

- **机械上限**：如果整个 close 入口（含 worker close 工作与平台 close 编排）都消失，
  上限是 **0.2482ms ≈ 45.5% of F1 p50**。这是数学上限，不是预期收益。
- **R43 式融合**（保留平台 close 入口，worker close 工作移入 open）：可测的 close 动作差
  为 0.125–0.139ms，但它主要对应“worker close 工作 + RPC 边界”，其中 worker 工作会转移
  到 open，真正可能减少的 E2E 部分只是 RPC 边界/内部 hop。**本轮数据不能证明这部分
  有 23–25% 的 E2E 改善**；R65 vs 当前的跨窗口 p50 也不支持该幅度。
- **外部 HTTP**：`[execute, close]` 已经是 1 个外部 pipeline 请求；fused close 不会减少
  外部 HTTP 帧。它省的是内部 close hop、平台 close 编排和/或 worker close RPC。
- **延后耐久**：close 动作在回复前不等待完成记录的 fsync（R37），所以上面的 close 均值
  不含该屏障。若融合保留平台 close 记录，延后耐久成本仍然存在；这一点不会因 fused close
  自动消失。
- **结论**：要对“fused close 能为 F1 省多少 E2E”下结论，必须做 **close-only 的受控 A/B**
  （同栈、同 fixture、配对交替、至少 2 轮），不能用本轮的动作账本上限替代。

## 5. 指标字段、语义与限制

### 5.1 当前 main 可用字段

`/v1/metrics` 的 `result` 中仍包含：

- `actions_total`：`map[action]count`。
- `action_latency_ns_total`：`map[action]累计 ns`，语义字符串
  `all_answered_requests_including_refusals`。
- `successful_latency_ns_total`：仅成功请求的 `map[action]累计 ns`。
- `action_latency_semantics`、`successful_latency_semantics`、`deferred_durability`、
  `audit`、`session_seats`、`active_permits`、`unknown_held_permits`、`errors_total`、
  `rejected_requests_total` 等。

动作名包括 `open`、`statement`、`close`、`query`、`describe`、`cursor_open`、
`cursor_start`、`cursor_fetch`、`cursor_close` 等。Hrana 适配器的 `handler.call` 通过
`httptest.NewRequest` 调用 `Gateway.ServeHTTP`，所以内部 open/statement/close 都会进入账本。

### 5.2 动作语义（重要）

- `open`：内部 `/v1/sql/open`。本环境 worker 支持 `open_guarded_statement=2`，open 融合了
  `SELECT 1` 的 worker 执行；所以 open 的 0.661ms 均值不是纯会话 open。
- `statement`：内部 `/v1/sql/statement`。适配器在 pipeline 中仍调用它，但 gateway 的
  `dispatchLocked` 检测到 fused statement 后走 `fusedGuardedStatement`，**不再发第二次
  worker statement RPC**；该动作主要是平台侧完成（准入、记录、响应）。证据：
  `prototypes/v1/platform/hrana_http.go:1081`（适配器调用）、
  `prototypes/v1/platform/gateway_sessions.go:455`（fused 分支）、
  `prototypes/v1/platform/gateway_fused_open.go:286`（使用 fused 答案，不再 call worker）。
  机制核对：round1 metrics-before/after 的 worker-a `metrics.query.total` 增量 = **50**，
  与 50 个 F1 请求一一对应；若 statement 动作再发一次 worker statement RPC，该增量应为
  100。worker-b 增量为 0（本轮请求都路由到 worker-a）。
- `close`：内部 `/v1/sql/close`。适配器在 `17854a8` 后不再请求 `close_after_statement`，
  `session.fusedClose` 为 false，走 `gateway.callSession(... close_session ...)`
  （`prototypes/v1/platform/gateway_sessions.go:498-513`），包含 worker close RPC。

### 5.3 限制与不确定

1. `action_latency_ns_total` 只有累计值，没有分位数/单请求分布；本报告的 close 中位数来自
   补充诊断的逐请求快照，不是 `/v1/metrics` 直接给出的。
2. 没有 worker close RPC 的独立计数器/时延。§4.3 的 0.125–0.139ms 是跨窗口估计，
   不是本轮直接测量。
3. 主 2×50 窗口的动作均值对约 16.7ms 的周期性停顿非常敏感：停顿落在 close 时 close 均值
   升高，落在 open/statement 时它们升高。两轮 close 均值 0.314 vs 0.183ms 就是证据。
4. 补充诊断每个请求前抓一次完整 metrics，telemetry 的 `get_metrics` 控制调用会改变系统
   状态：补充的 E2E 中位数 1.18ms 明显高于主测量 p50 0.545ms，且 metrics-after 的
   `deferred_durability.pending` 只有 5（主测量为 180）。因此：
   - 补充的 E2E 不作为 F1 基线；
   - 补充的 close 中位数/截尾均值只作为 close 成本的稳健性检查，且应理解为“较空载”
     环境下的值；主测量的 0.248ms 更接近有 deferred 待办时的成本。
5. 主测量 metrics-after 时 deferred durability 仍有 180 条待办、audit queue 79；这是
   R37 的预期异步状态，不是泄漏。clean stop 的 `unresolved_operations=false` 与各 stage
   exit 0 是最终状态。
6. `audit.in_flight_count` 在快照中为 -79（有符号计数的观测值），本轮未用它做结论；
   需要单独确认该字段语义。
7. 本轮没有做 `PERI_DURABILITY_OBSERVER` 或请求相位观测（任务红线）；因此无法把 close
   动作内的 worker RPC / admission / 记录入队 / 响应编码进一步拆开。
8. 本轮没有跑全量 benchmark，没有做 429/容量压力测试；F1 p95 的 7.63ms 由周期性停顿
   主导，不代表稳定尾延迟。

## 6. 授权判断建议（仅基于本轮 smoke）

- **close 是否是 F1 的可观成本？是。** 当前 close 动作 0.248ms/次，占 F1 mean 的 19.4%，
  占 F1 p50 的 45.5%（整个 close 入口口径）；稳健 close 约 0.227–0.233ms。它不是边际项。
- **fused close 能否给 F1 E2E 带来 ≥20% 改善？本轮数据不能证明。** 机械上限是 45.5%
  （整个 close 入口消失），但：
  - R43 式融合会把 worker close 工作移入 open，worker 工作本身不消失；平台 close 入口
    也保留。真正可能减少的只是 RPC 边界/内部 hop。
  - R65（fused close 生效）8 条 F1 腿 p50 0.465–0.602ms，当前 0.545ms；没有出现与
    0.14ms close 动作差一致的系统性 E2E 差异。跨窗口噪声和 5 样本/腿不足以定论。
  - 因此不能把 23–25% 或 45.5% 写成“预期收益”。若要以 ≥20% 为授权依据，必须先做
    **close-only 的正式 A/B**（同栈、同 fixture、配对交替、至少 2 轮），并且候选实现要
    明确说明它省的是 RPC 边界、平台 close 编排，还是整个 close 入口。
- **如果目标只是减少协议帧数/内部 hop**：本轮可确认 close 是一次独立的内部网关动作
  （含 worker `close_session` RPC），并且每个 F1 pipeline 恰好触发 1 次；把它融合进 open
  在机制上可省一次内部 hop/RPC 边界，但绝对量级约 0.02–0.14ms（取决于实现是否保留
  worker close 工作与平台 close 入口），相对 F1 p50 约 4–25%。需要候选实现 + A/B 才能收窄。
- **绝对节省很小**：F1 p50 已经是 0.545ms；即使整个 close 入口消失，也只有 0.248ms。
  是否值得增加协议融合复杂度，是产品/维护成本判断；本轮只提供当前成本与上限。
- **外部 HTTP 帧数不会因 fused close 减少**（pipeline 本来就是一个外部请求）；如果
  “减少协议帧数”指的是外部帧数，本方向不解决该问题。

## 7. 证据与复现

证据目录 `/tmp/peri-r66-f1/`：

| 路径 | 内容 |
|---|---|
| `protocol.md` | 主协议（19:51 冻结） |
| `supplementary-protocol.md` | 补充诊断协议（20:05 冻结） |
| `round1/`、`round2/` | `fixture.json`、`metrics-before.json`、`metrics-after.json`、`summary.json`、`summary.md`、`raw.jsonl`、`analysis.json`、`launcher-stop.json`、`run.log` |
| `supp/` | 100 请求逐请求快照：`per-request/NNN-{before,after}.json`、`raw.jsonl`、`analysis.json`、`summary.json`、`launcher-stop.json` |
| `aggregate.json` | 两轮合并 + 补充诊断 + 跨窗口参照 |
| `capprobe/capabilities.txt`、`fused-open-probe.txt` | worker 能力/命令只读探针输出（不含令牌） |
| `attempt1-startup-failed/` | 首次起栈失败的日志（未发请求，非测量数据） |

复现命令（全部在 `/tmp`，不改仓库）：

```bash
/tmp/peri-r66-f1/run_round.sh round1 20601
/tmp/peri-r66-f1/run_round.sh round2 20602
/tmp/peri-r66-f1/run_supplement.sh
python3 /tmp/peri-r66-f1/aggregate.py
```

未提交、未 push、未 merge；未修改仓库文件；未操作生产容器/卷。
