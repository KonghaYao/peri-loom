# R66 补充诊断协议：逐请求 metrics 分解（非主判据）

冻结时间：2026-11-04T20:05:00+08:00（在补充测量起栈前写入）
性质：补充诊断，**不是**主协议的一部分，不改变已冻结的 2×50 A/A 主测量；smoke，不可作为对外结论。
原因：主测量的两轮都出现约 16.7ms 的确定性停顿（两轮都在第 14 个请求附近），
`action_latency_ns_total` 的均值会把停顿归到停顿发生时正在执行的那个动作，导致
close 均值在 round1=0.314ms、round2=0.183ms 之间跳动。需要逐请求分解来区分
「典型请求的 close 成本」与「周期性维护停顿」。

## 目的

用逐请求的 `/v1/metrics` 前后快照，得到每个 F1 请求的 open/statement/close 实际耗时，
从而给出：

- close 的逐请求中位数、均值、p95；
- open/statement/close 的稳健均值（中位数与 95% 截尾均值）；
- 周期性停顿落在哪个动作、幅度多少；
- close 成本占典型 F1 请求（p50 或稳健均值）的比例及 fused close 上限。

## 环境

与主协议相同：main `39e8e6c`、平台 `44448ed6…`、worker `a648ecd9…`、local backend、
无 TLS、默认配额、**无观测器**。全新 run 目录 `/tmp/peri-r66-f1/run-supp`，网关端口
`127.0.0.1:20603`；authority/broker/worker 仍为 dev.py 分配的 ephemeral 环回端口。
重新 seed 同一 fixture（threads=201、messages=8625、chars=20177069）并校验。

## 请求与快照

- 场景/协议：`F1.select-1` / `json-pipeline`（`[execute SELECT 1, close]`）。
- **1 个不计入窗口的 warmup 请求**（在第一次快照之前完成；用于建立连接并让 capability 探针落地）。
- **100 个 timed 请求**，逐请求执行：
  1. 抓完整 `/v1/metrics` 为 `per-request/NNN-before.json`；
  2. 发 1 个 F1 pipeline 请求（harness 同源 `_request_once`，phase=timed）；
  3. 抓完整 metrics 为 `per-request/NNN-after.json`；
  4. 计算该请求的 action 增量与 E2E `duration_ms`。
- 快照发生在请求与请求之间，使用独立 management HTTP 连接；不与 F1 请求重叠。

## 计算（冻结）

逐请求：
- `close_ns = Δaction_latency_ns_total.close`，`open_ns`、`statement_ns` 同理；
- `e2e_ms = measurement.duration_ms`；
- `close_ms = close_ns / 1e6`，等。

汇总：
- 逐请求中位数（median）、均值（mean）、p95；
- **稳健均值**：中位数，以及去掉 E2E 最高的 5 个样本后的截尾均值（`trimmed_mean_95`）；
- 占比：`median(close_ms) / median(e2e_ms)`、`trimmed_mean(close) / trimmed_mean(e2e)`；
- 同时报告原始均值口径，便于与主协议对齐；
- 周期性停顿：列出 `e2e_ms > 5ms` 的请求及其 `open/statement/close` 分解。

## 有效性

- 100/100 timed 请求 `outcome=ok`、HTTP 200、rows=1、无 error_code；
- fixture `complete=true`；
- 每个请求 `close` 增量为 1，`open` 增量为 1，`statement` 增量按实际记账；
- `errors_total` / `rejected_requests_total` 增量为 0；
- 结束时 `session_seats=0`、`active_permits=0`、`unknown_held_permits=0`；
- clean stop、`unresolved_operations=false`。

## 证据

`/tmp/peri-r66-f1/supp/`：`protocol.md`（本文件）、`fixture.json`、`summary.json`、`raw.jsonl`、
`per-request/*.json`、`analysis.json`、`launcher-stop.json`、`run.log`。
