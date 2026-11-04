# R58 归因：H1 只读 cursor 页剩余成本（探针轮，不合入）

目的：R56/R57 合入后，H1.content-20mb 的 cursor 路径已从「平台页转码/序列化」瓶颈移开。
本轮只回答一个问题——剩余 ~53ms 的端到端时间花在哪里，下一步动哪一段才可能过 10% 硬门。
本轮不含生产改动，探针代码保留在分支 `probe/r58-cursor-page-timing`（不合入 `main`）。

## 环境

- 原生 macOS 26.5.1 / arm64（M4），非容器；`dev.py` 单机栈，端口 19651/19652，
  每次运行全新 run dir，fixture 重灌并校验 `201 threads / 8625 messages / 20177069 chars`。
- 默认配额（`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_result_bytes=4,194,304`）；TLS 关闭；`PERI_HRANA_EXPERIMENTAL=1`。
- harness `scripts/perf-reliability/issue28/run.py`，H1.content-20mb，`json-cursor` /
  `protobuf-cursor`，`--rounds 1 --warmup 1 --samples 2–3`。
- 响应字节：json `20,608,525`、protobuf `20,272,006`。

平台探针二进制（`sha256`）：

| 二进制 | sha256 | 用途 |
|---|---|---|
| `peri-platform-r58-probe` | `4e9c8f89…e66a52` | 页相位（wait/process/first） |
| `peri-platform-r58-probe2` | `195e455f…6cf` | + upstream Do/read/decode |
| `peri-platform-r58-probe3` | `36147e06…afda` | + `dispatch` serve/reply |

worker 探针二进制（`sha256`）：

| 二进制 | sha256 | 用途 |
|---|---|---|
| `peri-loom-v1-r58-probe` | `c3dcbe9f…910a` | 页总时/rows/post |
| `peri-loom-v1-r58-probe2` | `3d7b658d…328e` | + lane/turso 分段 |
| `peri-loom-v1-r58-probe3` | `480611cd…bb76` | + 行级 guard/clock |
| `peri-loom-v1-r58-probe4` | `ea8508be…e30d9` | + lane settle |

完整十六进制哈希在同目录日志文件名对应的运行批次中；这些二进制只用于归因，
A/B 期间不使用（观测器路径不得进入验收测量）。

## 端到端（探针构建，只用于对齐数量级）

| 运行 | json-cursor 中位 | protobuf-cursor 中位 |
|---|---:|---:|
| r58 | 56.94ms | 52.60ms |
| r58b | 53.87ms | 51.12ms |
| r58c | 56.07ms | 50.71ms |
| r58d | 52.65ms | 51.19ms |
| r58e/f/g | 51.50 / 56.38 / 52.93ms | — |

## 平台侧（`R58CURSOR`，每次 H1 查询，34 页）

以 r58c json 稳定样本为例（聚合 35 次 upstream 调用）：

- `wait_ms≈38.6`（34 页里 prefetch=33、direct=0；R56 预取把页等待压到每页约 0.17ms 暴露）
- `worker_ms≈44.1`、`process_ms≈6.0`（页转码）
- `first_ms≈1.6`（首帧）
- `upstream_do_ms≈32.5`、`upstream_read_ms≈6.3`、`upstream_decode_ms≈5.0`
- `dispatch_serve_ms≈45.6`、`dispatch_marshal_ms≈0.21`、`dispatch_reply_ms≈0.001`

推论：`serve` 45.6ms 减去 adapter 的 ~38.7ms upstream 等待，平台自身约 **6.9ms/查询**
（鉴权、审计提交、会话状态、页转码编排）。R56 的预取只暴露约 0.17ms/页，已接近用尽。

## worker 侧（`R58WORKER`，每次 H1 查询，33 页）

r58e/f/g 稳定样本（首个样本的 `turso_step_ms=5886` 是首次采样伪影，已弃用）：

- `total_ms≈30.8–33.6`
- `rows_ms≈24.3–26.5`，其中 `turso_step_ms≈16.5–18.3`（VM stepping，占 rows 的 ~2/3）
- `post_ms≈6.4–7.1`（页行序列化/回写）
- `row_work_ms≈0.52–0.60`、`account_ms≈0.31–0.33`、`lane_*` ≈0（千分之一毫秒级）
- 行级检查（`R58WORKER2`，17,303 次调用/样本）：
  `guard_check_ms≈0.41–0.56` + `operation_until_ms≈0.85–1.12` +
  `clock_enter_ms≈1.23–1.67` + `thread_cpu_ms≈1.75–2.48`，合计 **≈4.3–5.9ms/查询**
- `lane_settle_ms≈0.002`（`R58WORKER3`）：准入 settle 不是成本

## 结论（假设级，未做 A/B）

1. worker 每查询约 31ms，其中 turso VM stepping 约 17ms、行级预算/时钟检查约 4.3–5.9ms、
   页序列化约 6.6ms；平台自身约 6.9ms；其余为客户端读取 20MB 与连接开销。
2. 现有可动项按「上限 ÷ 端到端 53ms」排序：行级检查 ≈8–11%、页序列化 ≈12%（内存拷贝已接近
   带宽上限）、平台自身 ≈13%（分散在鉴权/审计/编排）、turso stepping ≈32%（引擎层，需单独授权）。
3. 下一步先对「行级检查」做上限探针（跳过/批量化的不安全版本）拿真实 E2E 上限；
   若 <10%，该方向放弃，再评估平台自身的 6.9ms 结构。

## 限制

- 探针在每个测量点前后各取一次 `Instant::now()`，行级分段（4 次/行）本身被仪器放大；
  4.3–5.9ms 是上界而非真实成本，必须以「关掉检查」的上限探针为准。
- worker 首样本 `turso_step_ms=5886.0`（r58e/f/g 均出现）是首采样伪影，不作为机制证据。
- 探针构建的端到端数字（51.5–56.9ms）高于干净构建同窗口基线，只用于相对归因，
  不能用于验收或对外汇报；验收须用干净二进制重新配对测量。
- 本目录日志来自 `/tmp/peri-perf/run-r58*`，为一次性运行；fixture 每次重灌，未做跨轮配对。
