# R59：延迟耐久账户 worker 8→64 的上限探针（不合入）

目的：先归因 X1.concurrency 的尾部延迟落在哪一层，再用最小探针验证「延迟耐久账户的
worker 宽度是否限速器」。本轮只改一个常量（`deferredDurabilityWorkers` 8→64），
**不改耐久时点、不改审计链语义、不改配额**；探针分支 `probe/r59-deferred-workers`
（提交 `fbf7b98`）保留作证据，不合入 `main`。

## 环境

- 原生 macOS 26.5.1 / arm64（M4），非容器；`dev.py` 单机栈，两台加密 worker。
- 基线栈端口 `127.0.0.1:19672`（run dir `run-r59a-base`），探针栈端口 `127.0.0.1:19673`
  （run dir `run-r59b-probe`），各自全新 run dir，同批 fixture 重灌并校验
  `201 threads / 8625 messages / 20177069 chars`。
- 默认配额（`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_result_bytes=4,194,304`）；TLS 关闭；`PERI_HRANA_EXPERIMENTAL=1`；
  `completion_journal=false`、`writer_admission=false`（即 v1 审计文件 + 操作日志）。
- harness `scripts/perf-reliability/issue28/run.py`：`--scenario X1.concurrency
  --protocol json-pipeline --rounds 1 --warmup 1 --samples 3`，每档 100 个请求。
- 二进制：

| 二进制 | sha256 | 说明 |
|---|---|---|
| `peri-platform-r58-main` | `f4210c08d97cb84eb7eaa900af9689c5e9d1a688e909853e26a1927e5d6e626b` | 基线；构建于 `main` @ `0a51ab9`；`main` 其后仅有文档提交（`325207c`），代码相同 |
| `peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` | 基线 worker（与 R57 合入构建逐字节一致） |
| `peri-platform-r59-probe` | `56aa4aba203ef688d702f86ae8fb447c59bf5cc3261691609517dd9819b5e97d` | 探针；仅 `deferredDurabilityWorkers = 64`（原 8）+ 两行探针注释；`deferredDurabilityMaximum` 保持 256 |

探针 diff 见 `probe.txt`，harness 包装脚本见 `r59run.py` / `r59sum.py` / `metrics.py`。

## 判据（A/B 前确定）

- 主指标：X1 **c16 中位延迟**（候选 vs 基线）。
- 硬门：候选中位数相对基线改善 ≥10%，且两条 B 腿同向；其余档位不得出现 >10% 的回归。
- 探针轮的任何结果都不合入；探针失败只记录方向否证。

## 结果（A/B/B/A，两条独立栈）

每格为 `中位 / p95`（ms），每腿 1 warmup + 3 samples/档：

| 腿 | 二进制 | c1 | c4 | c8 | c16 |
|---|---|---:|---:|---:|---:|
| A1 | 基线 | 0.53 / 7.56 | 0.70 / 97.59 | 2.08 / 156.02 | 3.75 / 186.96 |
| B1 | 探针 | 0.51 / 7.58 | 0.86 / 34.71 | 8.96 / 142.09 | **165.62** / 250.42 |
| B2 | 探针 | 0.50 / 7.87 | 1.65 / 29.56 | 3.04 / 146.54 | **124.39** / 231.37 |
| A2 | 基线 | 0.46 / 7.37 | 0.64 / 97.77 | 1.98 / 144.31 | 16.56 / 180.02 |

- **c16 中位反向崩坏**：基线两腿 3.75 / 16.56ms，探针 165.62 / 124.39ms——相对较快基线腿
  回归 33–44×，相对较慢基线腿也有 7.5–10×；两条 B 腿同向变差，没有任何档位出现 ≥10% 改善。
- c1 基本不变（0.46–0.53ms）；c8 探针 8.96 / 3.04ms 对基线 1.98–2.08ms（B1 明显更差，B2 噪声内）。
- c4 的 **p95** 确有改善（基线 97.6–97.8 → 探针 29.6–34.7ms），但同档 c16 崩塌，不能算收益。
- 全部腿：`errors=0`、`status_429=0`、verdict 全 PASS；fixture `complete=true`；
  响应字节 312（json-pipeline），无 `warnings`。
- 单腿仅 1 warmup + 3 样本/档、运行时长 1.1–1.8s，属探针级测量；本轮只用于「方向成立/否证」，
  不作为发布级结论。

## 归因（基线二进制 + 观测器，独立栈）

观测栈端口 `127.0.0.1:19671`（run dir `run-r59-x1attr`），基线平台二进制 `f4210c08…`，
`PERI_DURABILITY_OBSERVER=1` + `--sql-open-observer`，
`PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS=20`、`PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE=0`，
同一 fixture、同一 X1 场景。观测文件覆盖 208 s 栈生命周期（含空闲），共 9,923 条事件；
下面是事件总量，不是速率。

耐久观测（`durability-aggregate.json`，原始 `durability-observation.jsonl.gz`）：

| 事件 | n | 关键量 |
|---|---:|---|
| `audit_batch` | 506 | 466 批 `count=1`（92.1%），34 批 `count=32`；`total_ns` p50 3.90 / p99 8.06 / max 42.19ms；`sync_ns` p50 3.84 / p99 7.23 / max 13.09ms；写者忙 2.114s |
| `audit_checkpoint` | 55 | p50 **21.01** / p90 31.19 / p99 37.08 / max 38.12ms；合计 **1.204s**（≈ 审计写者忙时的 57%） |
| `journal_batch` | 965 | 924 批 `count=1`（95.8%）；`total_ns` p50 3.97 / p99 8.87 / max 12.19ms；`sync_ns` p50 3.90 / p99 8.01 / max 9.69ms；写者忙 4.121s |
| `audit_member_wait` | 1,661 | 总等待 p50 56.9 / p99 119.5 / max 126.3ms；**排队** p50 46.7 / p99 115.5ms；屏障等待 p90 21.1 / p99 115.8ms |
| `journal_member_wait` | 3,368 | 总等待 p50 4.70 / p99 104.4 / max 111.0ms；排队 p50 0.016 / p99 12.7ms |

请求相位（只记录超过 20ms 的请求；`request-phase-aggregate.json`、原始
`request-phase-observation.jsonl`）：

- `query` n=436：总时长 p50 28.2 / p95 32.9 / max 73.6ms；相位 p50 为 `native` 15.4、
  `intent` 4.2、`audit` 3.9、`terminal` 3.7ms。最慢查询 73.6ms = `native` 50.2 + `intent` 16.2。
- `open` n=46：总时长 p50 32.1 / p95 148.2ms；其中 `audit` p95 126.2、`terminal` p95 31.1。
- `close` n=14：总时长 p50 102.0 / p95 148.2ms；其中 `audit` p95 126.3、`terminal` p95 21.7。
- 慢请求是**突发**而不是长尾分布：一批 open/close 同时停在 `audit` 126.0–126.3ms +
  `terminal` 20.6–21.7ms，`native` 仅 0.5–0.8ms。

机制解释（与探针假设一致的部分）：审计写者是单线程串行段，绝大多数批次只含 1 条记录
（`sync_ns` p50 3.84ms），每 32 条记录还要做一次 head checkpoint（p50 21ms，纯 CPU/IO，
`sync_ns=0`）；两者叠加时写者被长时间占住，延迟耐久账户（8 worker / 256 槽）在突发流量下
饱和，`settle` 回退为内联等待，于是请求侧看到 126ms 的 `audit` 相位。

**探针否证的正是「加宽账户即可缓解」这一点**：64 个 worker 让更多记录同时涌向同一个串行写者，
屏障被拆得更碎。两栈的累计计数器因工作量不等（基线栈另外承接了一次未计分的预备运行，
审计里能看到 15:33:04 的第三个突发）不能直接相比；按 session-open 归一后，
`operation_journal_syncs_total / open` 从 0.93（基线）升到 1.57（探针），方向与
「屏障碎片化」一致，但该归一化只作方向性证据。

## 判决

- **方向否证，不合入**：把延迟耐久账户 worker 从 8 提到 64 使 c16 中位从 3.75–16.56ms
  恶化到 124–166ms，无任何档位达到 ≥10% 改善。
- 队列宽度不是限速器；限速器是**单线程审计写者的屏障与 checkpoint 停顿**。这与
  R29/R31 的结论同构：屏障合并依赖串行化，增加并发只会让批更碎。
- 若要继续压 X1 尾部，只剩两条路：降低 `audit_checkpoint` 的单次成本/频率，或合并
  每条记录的独立屏障。两者分别触及审计链语义与耐久提交时点，属需用户显式授权的边界，
  本轮不实施。
- 在获得授权前，优先转向授权内的其它场景（H1 平台页路径、W1 写路径归因）。

## 复现

```bash
# 基线 / 探针栈（各自全新 run dir）
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY=<platform> PERI_RUST_BINARY=<worker> \
  python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-perf/run-r59a-base \
  --listen 127.0.0.1:19672 --no-build

# 每腿：等待空闲 → 跑 X1 → 读取前后计数器
python3 docs/performance-review/2026-11-04-r59-deferred-workers/r59run.py \
  <run-dir>/connection.json /tmp/peri-perf/r59probe-a1
```

`connection.json` 含私有测试凭据，不进入仓库。
