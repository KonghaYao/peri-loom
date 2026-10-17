# 九臂阶梯压测：观测记录与口径（Rust 数据面 + Go 平台，本地全栈）

日期：2026-10-17。状态：**观测完成，未申请 SLO**。
范围：`prototypes/v1` 的本地全栈（Authority + 两台 Worker + Controller + Gateway + Key Broker），
数据集 100k 行，单机 `127.0.0.1`。
被测二进制：main `1f6da75` 的 debug 构建，sha256 前 16 位 `1dc1c3ee1f2ec548`
（launcher 启动行原文：`=== stress final-stress rust_sha256=1dc1c3ee1f2ec548 ===`）。

**本文不宣称 PASS、容量、公平性或 SLO，也不给出任何加速倍数。**
产物内 `performance` 与 `capacity` 两字段恒为 `NOT MEASURED`，`correctness` 恒为 `INCOMPLETE`——
这是 `e01-benchmark` skill 未获批 SLO 时的固定口径（`arms/summary.json` 的 `note`：
「仅转述各臂自报数据；performance/capacity 恒 NOT MEASURED，不构成容量、公平性或 SLO 判定」）。
本文只转述各臂自报数据与网关计数器的差分，并说明两者的对应关系与未覆盖的部分。

---

## 0. 一句话结论

在 9 个臂、每臂 ≤60 s、并发 ≤100、单臂预算 4000 次操作的阶梯里，**`timeout` 与 `unknown` 全为 0**；
所有被计入的失败（`known_failure`）都是**平台准入拒绝**，客户端看到的错误码统一为 `quota_exceeded`，
在 `/v1/metrics` 里区分为 `concurrency_limit`（并发准入）与 `session_limit`（会话上限）。
过载臂（w7）之后回落到低并发的 w8 与重复臂 w9 都能继续服务，且在 w7 后与序列末尾各做一次
「打开会话 + 只读查询」探针，均成功。**这些只说明该序列里未出现数据面超时或未知结果，
不说明容量、公平性或任何 SLO 已被满足。**

---

## 1. 方法与边界

- 序列由本地脚本 `stress.sh` 驱动（本机 `~/.peri-e01/stress-20261017/`，**不在仓库内**）：
  一个新 run 目录、一个分支、一个 100k 行导入，随后按固定阶梯跑 9 个臂，最后 `sleep 30` 做一次
  「会话全关 → 空闲 → 再开」探针。
- 导入：`run_platform_import.py`，`--batch-size 250 --commit-rows 10000 --max-seconds 600`，
  结束行 `planned=100000 attempted=100000 insert_ack=100000 commit_ack=100000 readback=100000 stop=planned_rows_reached`。
- 每臂参数与意图：先闭环保底（c=1/8/32），再开环逐级加码到 c=100，越界制造过载（r=200），
  然后回落到 c=8 看是否恢复，最后重复一个关键开环臂看波动。臂标签自带参数，例如
  `w6-open-c100-r100-60s` = 100 并发、100 ops/s、60 s。
- 客户端**不重试任何 SQL**：`run_platform_queries.py` 在 `unknown` 或超时后按契约停止全部派发
  （`timeout`/`unknown` 一旦出现就 `stop.set()`），会话打开失败也不重试。本序列里两者都未触发。
- 凭据只从 run 目录私有文件读取，未进入命令行、日志或本文档。本序列**没有删除任何对象**。
- 产物位置：逐臂原始 JSON 与 summary 在 `~/.peri-e01/stress-20261017/arms/`，
  日志在 `.../logs/final-stress.out`（含每臂启动行与 metrics 快照指针）、
  `.../logs/final-stress-probe-{a,b}.log`、`...-recovery.log`、`...-idle-reopen.log`。
  这些路径在本机私有目录，仓库内不可复现该路径。

---

## 2. 操作结果（`arms/summary.json`）

`attempted` 是**实际派发**的操作数（有事件的操作），不是预算数；预算恒为 4000，
未派发部分见 `oracle.missing_ops`。

| 臂 | 并发 | 速率 | 秒 | attempted | success | known_failure | timeout | unknown | not_dispatched | peak_in_flight |
|---|---|---|---|---|---|---|---|---|---|---|
| w1-ramp | 1 | 0 | 60 | 3482 | 3481 | 0 | 0 | 0 | 1 | 1 |
| w2-ramp | 8 | 0 | 60 | 4000 | 1206 | 2794 | 0 | 0 | 0 | 8 |
| w3-ramp | 32 | 0 | 60 | 4000 | 627 | 3373 | 0 | 0 | 0 | 16 |
| w4-ramp | 100 | 0 | 60 | 4000 | 605 | 3395 | 0 | 0 | 0 | 16 |
| w5-open | 100 | 50 | 60 | 317 | 301 | 0 | 0 | 0 | 16 | 1 |
| w6-open | 100 | 100 | 60 | 444 | 428 | 0 | 0 | 0 | 16 | 2 |
| w7-overload | 100 | 200 | 45 | 540 | 518 | 6 | 0 | 0 | 16 | 8 |
| w8-recover | 8 | 0 | 60 | 4000 | 1186 | 2814 | 0 | 0 | 0 | 8 |
| w9-repeat | 100 | 100 | 60 | 444 | 428 | 0 | 0 | 0 | 16 | 4 |

逐臂事件文件的分类与此表逐项一致（对 `arms/<臂>/query-events.ndjson` 重新计数得到）。
所有 `known_failure` 的 `error_class` 均为 `quota_exceeded`；所有 `not_dispatched` 的
`error_class` 均为 `time_budget_reached`。

会话与规模：

| 臂 | 会话 opened/requested | 打开的会话失败码 | 打开失败数 | oracle checked / mismatched / missing_ops | scale_coverage | 脚本自报吞吐 |
|---|---|---|---|---|---|---|
| w1 | 1/1 | — | 0 | 3481 / 0 / 518 | VERIFIED_BY_COUNT | 58.011 |
| w2 | 8/8 | — | 0 | 1206 / 0 / 0 | VERIFIED_BY_COUNT | 104.135 |
| w3 | 16/32 | `quota_exceeded` | 16 | 627 / 0 / 0 | NOT_VERIFIED | 87.088 |
| w4 | 16/100 | `quota_exceeded` | 84 | 605 / 0 / 0 | NOT_VERIFIED | 85.003 |
| w5 | 16/100 | `quota_exceeded` | 84 | 301 / 0 / 3683 | NOT_VERIFIED | 4.557 |
| w6 | 16/100 | `quota_exceeded` | 84 | 428 / 0 / 3556 | NOT_VERIFIED | 6.662 |
| w7 | 16/100 | `quota_exceeded` | 84 | 518 / 0 / 3460 | NOT_VERIFIED | 10.874 |
| w8 | 8/8 | — | 0 | 1186 / 0 / 0 | VERIFIED_BY_COUNT | 85.39 |
| w9 | 16/100 | `quota_exceeded` | 84 | 428 / 0 / 3556 | NOT_VERIFIED | 6.652 |

「脚本自报吞吐」是 `throughput_ops_per_second`，即**客户端成功操作数 ÷ 臂墙钟**，
受客户端侧准入拒绝与时间预算截断影响，**不是服务端容量或 goodput 保证**。

---

## 3. 读法

### 3.1 字段语义（避免把三类计数混为一谈）

- `success` / `known_failure` / `timeout` / `unknown` / `not_dispatched` 是对**已派发操作**的分类；
  `not_dispatched` 表示客户端在自己的时间预算内没有发出该操作（`time_budget_reached`），
  它既不是服务端拒绝，也不是失败。
- `known_failure` 表示平台**明确拒绝**并返回了错误码（本序列全部为 `quota_exceeded`）——
  与 `unknown`（结果未知）是两件不同的事，本序列 `unknown` 为 0。
- `oracle.missing_ops` = 计划操作数 − 已派发操作数（本序列即 `4000 − attempted`），
  它衡量的是**没被测到多少**，不是错误数。开环臂只有 317–540 次派发，因此 `missing_ops` 在 3460–3683。
- `oracle.mismatched` 全为 0：所有被检查的成功操作在离线清单上都能对上。
- `scale_coverage=NOT_VERIFIED` 出现在开环臂，原因是该臂在时间预算内没有覆盖完整操作清单，
  不是数据不一致。

### 3.2 失败全部来自平台准入，且与网关计数器逐位对应

逐臂读 `/v1/metrics` 的**累计**计数器并做差分（`arms/<臂>.metrics-after.json`，
`metrics.py` 每次采样覆盖同一文件，故差分才是一次臂的量），与同臂 `known_failure` 对照：

| 臂 | 网关 `concurrency_limit` 增量 | 网关 `session_limit` 增量 | 同臂 `known_failure` |
|---|---|---|---|
| w2 | 2794 | 0 | 2794 |
| w3 | 3373 | 17 | 3373 |
| w4 | 3395 | 85 | 3395 |
| w5 | 0 | 85 | 0 |
| w6 | 0 | 85 | 0 |
| w7 | 6 | 85 | 6 |
| w8 | 2814 | 0 | 2814 |
| w9 | 0 | 85 | 0 |

两点读法：

1. **每条 `known_failure` 都是并发准入拒绝**：闭环臂（w2/w3/w4/w7/w8）的 `known_failure`
   与同臂 `concurrency_limit` 增量逐位相等（2794/3373/3395/6/2814），不存在数据面错误。
2. **`session_limit` 不产生 `known_failure`**：它拒绝的是**打开会话**这一步，不是查询操作，
   因此只体现在会话打开失败数与 `missing_ops` 里。w1 与 w2 的 `concurrency_limit` 差异说明：
   1 并发时无拒绝，8 并发起开始出现——**本文不由此推断网关的并发阈值具体是多少**。

### 3.3 会话上限：16 是观测到的接受数，不是容量结论

w4、w5、w6、w7、w9 都请求 100 个会话，**每次都恰好 16 个成功、84 个以 `quota_exceeded` 失败**
（w3 请求 32 个，16 成功、16 失败）。这与网关 `session_limit` 的增量（85 ≈ 84 + 1）一致。
可以说的只是：**在这套配置下，单次请求里被接受的最大会话数是 16**。
不能说的：这是容量上限、是公平分配、或换负载/换配置后仍然成立。

### 3.4 重复臂与过载后恢复

- w6 与 w9 参数完全相同（c=100、r=100、60 s）：`success` 428/428、`not_dispatched` 16/16、
  自报吞吐 6.662/6.652。两个重复臂在本序列里一致。
- w2 与 w8 参数完全相同（c=8、r=0、60 s）：`success` 1206/1186（差 1.7%），
  且 w8 是在 w7 过载之后跑的——**过载没有让服务进入不可用状态**，但也没有恢复出比 w2 更好的结果。
- 过载臂 w7 结束时（`logs/final-stress-recovery.log`）与序列末尾空闲 30 s 后
  （`logs/final-stress-idle-reopen.log`）各做一次「打开会话 + 只读查询」，两次都 `open: ok`
  （0.021 s / 0.023 s）。**这是两次单点探针，不是稳定性或恢复时间的测量。**

**以上都不能作为稳定性、容量或公平性结论**：两个重复臂一致只说明这两次一致，
主机负载、后端（本地磁盘）、并发剖面都只覆盖了这一次。

---

## 4. 修复验收与基线对照（单次观测）

同一序列内的探针（`arms/summary.json` 里的 `probe` 条目来自更早的一次独立探针运行，
不属于本序列；本序列的探针见下列日志）：

| 探针 | 条件 | 结果 |
|---|---|---|
| stress probe A | 导入结束后立即打开 | `open: ok` 0.018 s |
| stress probe B | 空闲 3.4 s 后打开 | `open: ok` 0.021 s |
| w7 后 recovery | 过载臂结束后打开 + 只读查询 | `open: ok` 0.021 s |
| idle-reopen | 会话全关、空闲 30 s 后打开 | `open: ok` 0.023 s |
| accept probe A/B | 同一 accept 序列（另一 run 目录） | `open: ok` 0.019 s / 0.020 s |

基线对照：同一 accept 序列在不含加载预算修复的 `0ecb117` 二进制上 **FAIL**——
探针 A 成功 0.021 s，但空闲 3.4 s 后的探针 B 报 `sql_timeout` 16.019 s
（`execution_completed=false`、`may_have_committed=false`、`timed_out=false`）。
落盘证据：`~/.peri-e01/stress-20261017/logs/base-probe-b.log`。

口径：这些都是**单次观测**，只用于判定「成功 / 失败」，**不是延迟基线**，
不支持任何加速倍数、百分比或 SLO 结论。故障机制、修复边界与回退理由见
`docs/performance-review/2026-10-17-open-load-stall.md`。

---

## 5. 未测量与不可外推

- **未做**：多进程 RSS 采样与「修复前/后」内存对照（采样被误杀、对照栈未跑完），
  因此本文没有内存或驻留相关的任何数字。
- **未做**：多档数据集规模（10k/1M 行）、不同 `commit-rows`、真实 S3 后端、
  多 Worker 故障切换、跨网延迟。
- 所有数字来自**单机、本地磁盘后端、一次性 run 目录**，主机上同时有其他构建/测试进程；
  每臂只有一次（两组重复臂各两次）。
- `performance` / `capacity` 在产物内恒为 `NOT MEASURED`，`correctness` 恒为 `INCOMPLETE`；
  本文不改变这两个字段的含义，也不把本文的任何数字当作它们的替代。
- 过载行为（`concurrency_limit` 拒绝）来自**客户端侧共享的准入路径**，
  它不代表数据面在真实并发下的表现——本次 9 臂里，数据面看到的并发从未超过 16 个会话。

---

## 附：与其它文档的关系

- `docs/performance-review/2026-10-17-open-load-stall.md`：本次栈所验证的故障与其结构性修复（§3、§4.2）。
- `docs/performance-review/implementation-status.md`：F06（固定缓冲预算）等未完成项。
- `docs/performance-review/2026-10-17-overload-rejection-reasons.md`：拒绝原因的既有记录。
