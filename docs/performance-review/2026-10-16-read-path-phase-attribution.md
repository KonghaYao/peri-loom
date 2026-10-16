# 读路径分段归因：一次读要付三道 fsync

日期：2026-10-16。状态：**已实测**（插桩服务端分段计时 + 客户端复核），单机 loopback，共享主机。
被测对象是完整 peri-loom，全部经公开 URL + token；未走内部模块。

## 1. 结论

只读 `/v1/sql/query` 的服务端耗时里，**70% 是三道 fsync 的串行等待**，SQL 执行只占 29%：

| 段 | mean | 占服务端 |
|---|---|---|
| intent（dispatch 前的操作日志 + fsync） | 3.93 ms | 22.0% |
| worker（实际 SQL 执行） | 5.19 ms | 29.1% |
| terminal（终态操作日志 + fsync） | 4.00 ms | 22.4% |
| audit（审计追加 + fsync） | 4.40 ms | 24.6% |
| admission / respond / decode / 其余 | 0.32 ms | 1.8% |
| 未覆盖（unaccounted） | 0.008 ms | 0.04% |
| **合计** | **17.84 ms** | 100% |

即：**读一次 = intent fsync + terminal fsync + audit fsync**。这与源码证据一致
（`beginDispatchOperation` → `journal.submit`+`file.Sync`；`settleOperation` → `journal.submit`+`file.Sync`；
`Audit.Record` → `recordBatch` → `audit.file.Sync`），并由 `nonCommittingOperation`
只覆盖 `describe`/`session_state`/只读 cursor 解释：`query`/`execute`/`statement`/`open`/`close`
都走"提交型"路径，即使语句是纯读。

### 1.1 正交对照（同一会话内交替采样，n=40/组）

| 动作 | 动作性质 | 服务端 mean | intent | worker | terminal | audit |
|---|---|---|---|---|---|---|
| `query`（点查） | 提交型 | 18.98 ms | 4.30 | 5.55 | 4.27 | 4.76 |
| `describe`（只 prepare） | **非提交型**（跳过 intent） | 10.68 ms | **0.03** | 0.79 | 4.62 | 5.14 |
| `session_state` | **非提交型**（跳过 intent） | 13.48 ms | **0.04** | 4.84 | 4.35 | 4.16 |
| `open`（开会话） | 提交型 | 23.25 ms | 7.28 | 5.78 | 5.18 | 4.87 |
| `close` | 提交型 | 23.70 ms | 7.01 | 0.41 | 8.25 | 7.96 |

三点可直接读出的量：
1. **分类路径本身有效**：非提交型动作的 intent 段降到 0.03 ms，说明"可证明不提交 → 免 intent"是既有机制，只是没覆盖读查询。
2. **terminal + audit 是每个请求都付的固定税**：连 `describe`（不执行任何 SQL）也要 4.6 + 5.1 ms；这是"每请求固定成本"的直接证据。
3. **worker 往返本身很便宜**：`describe` 的 worker 段仅 0.79 ms，而 `query` 是 5.55 ms —— 差额约 4.8 ms 是**语句执行/取数**而非 IPC。

## 2. 客户端复核（同一冻结二进制，另一轮）

100k 行数据集、C1、`per-request` 连接、四族各 1000 次：

| 指标 | 值 |
|---|---|
| 成功/计划 | 4000 / 4000（mismatched 0、missing 0） |
| 正确性 | ORACLE_VERIFIED |
| 吞吐（查询阶段墙钟） | **49.81 ops/s** |
| 族内 p50 | point 17.88 / index_list 18.82 / keyset 18.89 / offset 18.92 ms |
| 服务端同时段 query mean | 17.84 ms（与客户端 p50 一致，误差 <1%） |

族间 p50 相差不到 1.1 ms（1.06×），而各族的 SQL 形状、索引命中、扫描行数差异很大 ——
**读延迟主要由与语句形状无关的固定成本决定**，而不是扫描量。这与第 1 节的分解互为印证。

对照原生 TursoDB（本地文件、进程内、同族查询 p50 0.038–0.947 ms、C1 584.2 ops/s）：
平台 49.81 ops/s，**约 11.7×**。差值中约 70% 是上述持久化屏障，而不是 SQL 引擎能力。

## 3. 由此得到的优化阶梯（按风险从低到高）

| 层级 | 做法 | 语义代价 | 预估收益 | 依据 |
|---|---|---|---|---|
| **A** | terminal 与 audit 两道屏障**重叠等待**（两者都在回复前确认，各自 fsync 语义不变） | 无 | −0 … −4.3 ms | 两文件互不依赖，目前是同一 goroutine 串行等待 |
| **B** | 语句被 **worker 面证只读**（`describe` 的 `is_readonly=true`，引擎判定）时跳过 intent，保留 terminal + audit | 无数据语义变化：不可能提交的操作本就不需要对账对象；无法面证时 fail-closed 回退 | −4.3 ms | 探针证实跳过 intent 的路径已存在且有效（0.03 ms） |
| **C** | 可证明只读的请求连**操作台账终态记录**也省掉（只留 audit），在内存台账里体现 | **改变读操作的持久化台账语义**，需明确批准 | 再 −4.3 ms | terminal 对非提交型的唯一作用是对账与在途清点 |
| **D** | Authority 侧：`ownership_view()` 按 root 摘要**免掉重复解码**（每次校验仍读 root、仍验租约） | 无：租约校验与 fencing 语义不变，只是不再对同一份文档重复解析与哈希 | **实测 −0.4 ms/次校验**（head 95 KB：validate 613 → 196 µs；head 945 KB：6,058 → 1,782 µs）。原估「−1…−4 ms」被实测否掉 | root 文档 68% 是幂等台账、随续期增长；`admitted::holds` 已有"按摘要只验一次"的先例（`authority.rs:261-272`） |

A+B 预计把读查询从 18.98 ms 降到约 10.4 ms（−45%，折算约 91 ops/s）；
A+B+C 约 6.1 ms（−68%，约 155 ops/s）。**这些是投影，不是实测。**

实测结果见 `2026-10-16-read-path-barrier-acceptance.md`：**只保留 B**，
服务端读查询 17.855 → 14.357 ms（−19.6%）、客户端 49.81 → 60.17 ops/s（+20.8%）。
A 实测只值约 0.54 ms，且带来审计 `Outcome` 语义与 goroutine 恢复边界两处代价，
已按外部评审回退；C 未获批准，未做。

剩余 29% 在 worker 段，已单独归因（见 `prototypes/v1/platform/worker-read-path-cost.md`，
worktree `perf/worker-readpath-3d5e9b21`）：**那 4–5ms 里几乎全部与 SQL 无关**。

| 动作/族 | 返回行数 | worker 段 mean |
|---|---|---|
| `describe`（只 prepare） | — | 0.787 ms |
| `point`（等值主键） | 1 | 4.549 ms |
| `index_list` | 49 | 5.077 ms |
| `keyset` | 50 | 5.331 ms |
| `offset`（深分页扫描） | 50 | 5.785 ms |
| `session_state`（**不含任何 SQL**） | — | 4.838 ms |

读一行与读五十行只差 0.5–0.8 ms，而**完全不执行 SQL 的 `session_state` 比点查更贵** ——
说明 worker 段存在一个约 4.0–4.8 ms、与语句形状无关的固定底噪。源码对照给出结论：
`describe` 与 `session_autocommit` 路径只差一处 —— `src/session/lane.rs:528` 的
`validate_session()`（describe 不调用），因此 `session_state − describe ≈ 4.05 ms`
就是**一次权威租约校验往返**。

该往返在 Authority 侧的成本来源值得单独记录（`authority/ownership_view.rs:17-37`）：每次校验都
重新 `store.get("authority/root")` 并对整篇做解码（解码含"解析信封 + 全篇 blake3 + 再解析一遍"）。
本次 run 量到 root 文档 **148,970 B（解码后 112,182 B）**，其中 `ownership.commands` 幂等台账
占 **76,638 B = 68%**；栈空转期间该台账仍随租约续期增长（稍后再读已是 **160,338 B / 147 条**）。
即：**每请求固定成本随运行时长与命令条数单调增长**，属于算法/缓存问题（同一 root 摘要无需重复解码），
而不是 fencing 语义所必需。这条与既有的 root 内嵌结构只增不减（`pins`/`WalMetadata`）是同一类问题。

**实测剖分（后续补做，release、`LocalObjectStore`、p50 µs，见 `perf/authority-ownership-6b1f4e02`
提交 `0d95202`）**：

| head bytes | read | decode(冷) | decode(摘要命中) | validate | session（worker 形状，读两次 head） |
|---|---|---|---|---|---|
| 1,446 | 18 | 8 | 1 | 22 | 45 |
| 95,374 | 128 | 485 | 57 | 196 | 570 |
| 944,974 | 1,151 | 4,907 | 564 | 1,782 | 5,484 |

两个更正：① 增长性成立——head 每长 10 倍，校验成本约长 9 倍，decode 在未做复用时是最大项
（约为 read 的 4 倍）；② **量级不成立**——Authority 侧一次校验在生产文档尺寸下只有约 0.2 ms，
不是上面 4.05 ms 的来源。那 4 ms 应在 worker↔authority 的 RPC 往返与 worker 自身（本测量不含网络），
下一步要测的是那一段；解码复用把 decode 降到约 1/12 之后，read 成为 Authority 侧的主导项。

被否定的假设（同样重要）：**读路径并不是每次去对象存储拉整段**。数据集实际是 10 个
3.5–7.1 MB 的 WAL blob，worker 在 `open` 时把整幅 WAL 重放进内存镜像
（`io.rs:88-91`），之后页读直接从内存镜像取（`io/files.rs:88-96`）；
`offset` 与 `point` 只差约 1 ms 即为旁证。

## 4. 测量条件与适用边界（不可外推的部分）

- 插桩构建 `request-phase-997abbd-r2`：peri-platform sha256 `b24cebea50b1ce…`，
  相对 r1 只改了 `requestPhaseLimit` 64 → 8192 与其注释（64 条预算会被任何数据导入耗尽，
  同一进程就读不到读阶段）。profile 关闭时路径不变。
  Rust 侧复用冻结构建 `frozen-997abbd-r1`（sha256 `e0f0bf95c705fb…`）。
- 采样期主机负载：导入前 load 4.62，查询后 8.99（同机上还有别的 owner 的任务在跑）。
  绝对毫秒数受此影响；**分段占比**比绝对值稳健，但也不保证在空闲机器上完全一致
  （屏障等待会随设备队列压力变化）。
- 观察者开启时每条请求多一次内存记录与编码（有界、非阻塞）；未测其对绝对值的影响量级，
  但四族 p50 与本轮之前未插桩的 C1 结果（49.10 ops/s）一致，未观察到量级偏移。
- 单机 loopback、开发 CA、单 Authority；远程 RTT、真实 S3、多租户并发均未测。
- 导入为独立合成参考（`SYNTHETIC_REFERENCE`），不冒充真实 Peri 2× 负载。

## 5. 证据位置

- run-dir：`/Users/konghayao/.peri-e01/phase2-20261016T142415Z`
  （`platform/request-phase-observation.jsonl` 4434 条记录，single stream，`complete` 关闭行在进程停止时写）
- 客户端：同目录 `queries-c1/queries.json`、`import/import.json`、`probe-client.json`
- 归因脚本与产物：`/Users/konghayao/.peri-e01/phase2/{analyze.py,attribution.json,probe.py}`
- 插桩构建记录：`/Users/konghayao/.peri-e01/request-phase-997abbd-r2/BUILD_RECORD.txt`
