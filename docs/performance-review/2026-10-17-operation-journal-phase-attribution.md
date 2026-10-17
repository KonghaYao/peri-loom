# operation journal 六段耗时归因：本机诊断样本

日期：2026-10-17。状态：**诊断性观测；性能、容量、公平性与 SLO 均未测量**。
范围：`prototypes/v1` 本地全栈（Authority + 两台加密 Worker + Controller + Gateway + Key Broker），
单机 `127.0.0.1:19960`，TLS 开启，本地对象后端，维护调度关闭。
Go 平台二进制 `11ad02c`（sha256 `5a4a765594a2f38a75db9e9b391763fd4702fe31904a12455a17e0a7a2875bdd`）；
Rust 数据面沿用固定二进制 `4a92f85`（sha256 `1962e2ba0e0c66ce3df2336847ed6bb47c169f77c62ef4035c8121d33e81800e`）。
运行窗口 `2026-10-17T12:33:18Z`–`12:42:00Z`，环境变量 `PERI_WORK_METRICS=1`。

**本文不宣称 PASS、吞吐、延迟 SLO、容量或公平性，也不给出任何加速倍数。**
文中所有秒数、纳秒数都来自这一次本机运行；两臂使用不同项目与数据库，墙钟时间不能互相换算为加速比。
全文 `performance` 与 `capacity` 恒为 `NOT MEASURED`，`correctness` 为 `INCOMPLETE`。

---

## 1. 这次补了什么

此前 operation journal 只能看到批次总量、队列峰值、`wait_ns` 与 `syncs_total`，
一次 barrier 内部的成本无法归因。本次提交把一次批次拆成六段并各自累计：

| 计数器 | 覆盖范围 |
|---|---|
| `operation_journal_encode_ns_total` | `submit` 内 `json.Marshal(mutation)` 与 `json.Marshal(journalEntry)` |
| `operation_journal_verify_ns_total` | barrier 前对路径与已打开文件的身份校验（`Lstat` + `SameFile`） |
| `operation_journal_assemble_ns_total` | 把批内各行拼成一次 `WriteAt` 的连续缓冲 |
| `operation_journal_write_ns_total` | `WriteAt` 本身 |
| `operation_journal_sync_ns_total` | `file.Sync()` |
| `operation_journal_witness_ns_total` | 见证行 `WriteAt`（不单独 fsync） |

失败路径也会回填已经花费的段。计数器只在 `PERI_WORK_METRICS=1` 时由 `/v1/metrics` 的
`state_operation_cost` 暴露；关闭时提交路径不增加计时调用。

## 2. 实验方法

两个测量臂，串行执行：

- **串行臂**：内置 `local_default/demo/main` 数据库，1 个 session，逐条
  `INSERT INTO probe_<id>(id, v) VALUES(?, 'x')`，共 300 次。每次都在收到响应后才发下一次。
- **并发臂**：新建项目 `conc<id>`，显式 `max_concurrent=8`、`requests_per_second=500`，
  在该项目内新建数据库；预先打开 8 个 session，用 8 个线程各发 25 次 INSERT，共 200 次。
  这次不再受默认 `max_concurrent=4` 的准入拒绝干扰。
- 两臂都在动作前、动作后读取 `/v1/metrics`，逐项做差。串行臂的差从
  `baseline`（建表后）到 `serial`；并发臂的差从 `serial` 到 `concurrent`。
- 随后关闭会话、读取 `closed` 快照并干净关栈。

证据目录：`~/.peri-e01/journal-phase-20261017T123316Z/attempt2/`，包含
`probe.json`（两臂与差量）、`summary.json`（含进程级绝对快照与边界说明）、
`probe.stdout`、`probe.stderr`。关栈结果：`outcome=clean`、
`reason=terminal_proof_written`、`unresolved_operations=false`、四个 stage 的
`exit_code=0` 且 `ack=true`。

## 3. 结果

| 观测 | 串行臂 | 并发臂 |
|---|---:|---:|
| 请求数 / 失败数 | 300 / 0 | 200 / 0 |
| 墙钟 | 89.370 s | 6.370 s |
| 请求延迟均值 / p50 / p95 | 297.900 / 253.874 / 308.270 ms | 248.097 / 241.928 / 353.067 ms |
| journal append | 600 | 418 |
| journal barrier（Sync 次数） | 600 | 418 |
| `coalesced_total` 增量 | 0 | 0 |
| 累计 Sync 时间 | 2.406 s | 1.561 s |
| 单次 barrier 均值 | 4.010 ms | 3.734 ms |
| 六段累计时间 | 2.622 s | 1.650 s |
| 六段累计 / 请求墙钟 | 2.93% | 25.90% |
| 每次 append 的六段均值 | 4.370 ms | 3.947 ms |

六段占比（同一臂内）：

| 段 | 串行臂 | 并发臂 |
|---|---:|---:|
| Sync | 91.76% | 94.62% |
| WriteAt | 6.98% | 4.40% |
| Witness | 0.57% | 0.49% |
| Verify | 0.43% | 0.24% |
| Encode | 0.26% | 0.25% |
| Assemble | 0.01% | 0.01% |

进程级绝对快照（两臂结束后、关栈前读取）：`appends_total=4691`、
`syncs_total=4691`、`coalesced_total=0`、`batch_max=1`、`queue_max=1`、
`queued=0`、`wait_ns_total=9,527,907`、`wait_max_ns=3,382,833`。
该快照还包含此前一次中止尝试产生的 append，因此它只用于证明**整个进程里**
`batch_max` 与 `queue_max` 从未超过 1；不能把它当作某一臂的独立样本。

## 4. 解释

1. **每个已派发写请求在稳态下产生 2 次 append**：一次 durable intent、一次 terminal。
   串行臂 `600/300 = 2.00` 正好对应这条路径；并发臂 `418/200 = 2.09` 还包含少量
   session/settle 记录。每次 append 都单独付一次 barrier，`syncs=appends`。
2. **Sync 是 journal 内部的主要成本**：占六段累计的 91.8%–94.6%。
   Encode、Verify、Assemble、Witness 合计约 1.0%–1.3%，其中 Encode/Verify/Assemble
   都在 1% 以下，单独优化它们不会改变这里的请求成本。
3. **journal 不是请求墙钟的主要成本**：串行臂六段累计只占 2.93%；
   并发臂升到 25.9%，但每次请求仍有约 240 ms 落在 SQL 执行、Worker 持久化与网络路径上。
   在这组样本里，把两次 barrier 合成一次最多只能解释每次请求约 4 ms，不能解释其余延迟。
4. **没有观察到合批**：8 路并发下 `batch_max=1`、`queue_max=1`、`coalesced=0`。
   并发臂实际到达率约 31 次请求/秒，两个 journal 提交点平均相隔约 32 ms，
   远大于约 4 ms 的 barrier；队列在绝大多数时间为空，合批没有形成的窗口。
   这不是“合批已失效”的证据，而是“在这个到达率下它没有被触发”的观测。
   要检验合批收益，必须把请求到达率提高到与 barrier 同量级，或让多个数据库的提交
   在时间上真正重叠；本实验没有做到这一点。

## 5. 边界与不宣称

- 单次本机运行，没有 pre-fix 负对照；不能据此归因某项历史改动，也不能外推为
  生产吞吐、容量、公平性或 SLO。
- 两臂使用不同项目与数据库，且并发臂包含建库与建表后的首次写入；两臂墙钟不可直接比较。
- `batch_max` 与 `queue_max` 是 gauge，不是累计计数。`probe.json` 对它们做差得到 0，
  这是取差方式的限制；第 3 节的绝对快照才是有效读法，`summary.json` 已注明。
- `wait_ns_total` 只覆盖等待 barrier 的时长，不包含等待 state 锁、SQL 执行或网络。
- 此前的失败尝试（`journal-phase-20261017T122908Z`）在默认 `max_concurrent=4` 下
  产生了 1696 次 `concurrency_limit` 拒绝；这些请求不在两臂差量内，但会计入进程级绝对快照。
- 本机磁盘、文件系统与负载未受控；Apple SSD 的 `fsync` 行为不等于远端对象存储或云盘。
- 本文没有测试正常重启、Unknown 后恢复、快照/长读保护或数据页一致性。

## 6. 下一步

如果继续优化，优先级应放在 SQL 提交路径与 Worker 持久化，而不是 journal 的
编码或身份校验：后者在本样本里合计不到 1%。若要检验 journal 合批，需要单独设计
高到达率实验（更多客户端、多个项目/数据库同时提交），并在同一到达率下对照
`syncs_total / appends_total`；在拿到该对照之前，不应改动“每次 append 一次 barrier”
的现有语义，也不应宣称合批会带来加速。
