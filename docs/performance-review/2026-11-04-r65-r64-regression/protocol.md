# R65 冻结协议：R64 合入后的回归门（W1 / F1 / H1）

日期：2026-11-04
状态：**本文件在第一条 A/B 腿之前提交并推送；测量开始后不得修改。**
轮次性质：测量轮（无生产代码改动）。

## 目的

R64 的冻结协议只覆盖 X1；W1、F1、H1 在主指标判决时没有复测，合入记录里已写明这是缺口。
本轮补做回归门：确认延后耐久账户待办上限 256→4096 没有让任何主场景的端到端中位数退化 >10%。

## 机制核对（A/B 之前）

改动只提高延后耐久账户的待办上限（`durability_deferred.go`），不改变耐久时点、审计链顺序、
隔离与 fail-closed 规则。写语句的 intent 与 terminal 仍同步 durable（R37 的用户授权边界），
所以 W1 预计中性；F1/H1 的只读 completion 可能因更多记录被合并而略优。任何 >10% 的退化按回归处理。

## 基线与候选

| 角色 | 来源 | 二进制 | sha256 |
|---|---|---|---|
| A 基线 | 合入前 `main` @ `1399f15` | `/tmp/peri-perf/bin/peri-platform-r62-main` | `fd959df7a0680f80f7b15e9b86c851b9ca24d555c673e69b2659f0d1f15159a1` |
| B 候选 | 合入后 `main` @ `1a6b9dd` 重新构建 | `/tmp/peri-perf/bin/peri-platform-r65-main` | `9214cd0c95ea636bc3d7898714d0eb884d2769957ece44b9063480f453aa3a6a` |

- B 用 `go build ./cmd/peri-platform` 从 `main` @ `1a6b9dd` 重新构建（go1.27.3、darwin/arm64）。
  与 R64 接受过的候选二进制 `98487e5e…` 相比：两侧平台源码 `git diff 530fcf7 1a6b9dd --
  prototypes/v1/` 为空，二进制同大小（15,525,762 字节）、仅 **362 字节** 不同（构建 ID 区），
  说明被验收的代码与合入后的代码一致。
- worker 两侧相同：`/tmp/peri-perf/bin/peri-loom-v1-r58-main`
  sha256 `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594`。
- 每次运行前用 `shasum -a 256` 复核两侧二进制。

## 场景与样本

| 场景 key | 协议 | 每腿 timed 样本 | 备注 |
|---|---|---|---|
| `W1.insert` | json-pipeline、protobuf-pipeline | 5（warmup 1） | 写入路径，intent/terminal 同步 durable |
| `F1.select-1` | json-pipeline、protobuf-pipeline、json-cursor、protobuf-cursor | 5（warmup 1） | 固定开销小查询 |
| `H1.content-20mb` | json-cursor、protobuf-cursor | 3（warmup 1） | 20MB 大结果 |

- 每场景 **2 轮 × A/B/B/A = 8 条腿**，共 24 条腿；每腿独立栈、全新 run dir、重新导入同一 fixture
  （threads=201、messages=8625、content chars=20177069）、默认配额、无 TLS、**无观测器**。
- 端口 20301–20308；每条腿一条 `run_leg.sh <scenario> <leg> <port> <binary> <samples>`。
- `/v1/metrics` 只在每条腿的计时窗口之外各取一次快照。

## 主指标与判据（冻结）

- 每场景主协议：**W1 = json-pipeline、F1 = json-pipeline、H1 = json-cursor**。
- 每轮取主协议的 `median_ms`；比值 = 候选 / 基线。
- **回归**：同一场景两轮比值都 >1.10 且合并（4 腿中位）比值 >1.10；否则记「无回归」。
- 主协议之外的协议、p95、`query_ms`、`response_bytes`、outcomes 全部报告，不参与判决。
- 少于两轮有效数据 ⇒ 判「不可确认」，不得写成 PASS。

## 腿有效性

- 任一计时样本 `outcome` 不属于 {ok, expected_limit}、`status_429>0`、fixture `complete=false`、
  `launcher-stop.json` 非 `clean` 或 `unresolved_operations=true` ⇒ 该腿无效，整腿重跑一次；
  仍无效则记录并排除。
- 不变量：每腿 0 错误、0×429、`rows` 符合 `expected_rows`、`warnings` 为空、审计链连续。

## 证据

`legs/<scenario>/<leg>/`（`summary.json`、`summary.md`、`raw.jsonl`、`metrics-before.json`、
`metrics-after.json`、`launcher-stop.json`）、`legs.txt`（完整运行日志，以 `.txt` 保存）、
`aggregate.py`、`run_leg.sh`、`run_all.sh`、`seed_fixture.py`、`snap_metrics.py`。
