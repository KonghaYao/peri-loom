# 2026-10-17 WAL 回放有界预算（F06 固定缓冲预算）

基线：`db06c38`（main，v1.1.5）。
范围：Rust `prototypes/v1/src/wal.rs`、`src/authority_rpc/client.rs`（一处常量可见性）、`tests/remote_database.rs`。
不改 Go、不改 E01 skill、不改持久格式、不改数据库表结构、不改 fencing/审计/Unknown 语义。

本文只记录**已由当前代码与用例证明**的性质。第四节之后的一切墙钟数字都不是本次的测量结果。

## 1. 为什么现在可以重新落地预取

[冷加载停摆](./2026-10-17-open-load-stall.md) §4.1 回退了那版并发预取，并写下两个重新落地条件：

1. 先把 F06 的**有界缓冲预算**做出来——窗口**按字节**计而不是按块数计；
2. 再把那条钉「恰好 1 条在飞」的用例改成断言「在途条数与字节 ≤ 预算 + 顺序 + 提前停止不抓完整历史」。

两者现在都在代码里：预算是 `PERI_WORKER_WAL_REPLAY_BYTES` 的字节数（第 2 节），
用例分工见第 5 节，其中端到端用例 `wal_replay_reads_ahead_within_its_window_and_stops_with_the_visitor`
断言的是顺序、提前停止和「读前距离是小常数而非历史长度」。

**本次没有重新测量任何延迟**：§4 的观测属于被回退的那版实现，不能转移给本次实现（见第 6 节）。

## 2. 预算账本

| 项 | 值 | 来源 |
| --- | --- | --- |
| 单块计费 `WAL_CHUNK_CHARGE` | `MAX_RPC_BYTES / 2` = 16 MiB | 一次 append 的载荷原始上限（`WalAppendRequest::validate`），不是历史样本里的最大块 |
| 默认预算 | `4 * WAL_CHUNK_CHARGE` = 64 MiB | 被回退实现的「4 块」，改成字节后原样保留 |
| 窗口上限 `WAL_REPLAY_WINDOW_MAX` | 8 | 调用闸门的三分之一（第 3 节） |
| 窗口 | `(budget / WAL_CHUNK_CHARGE).clamp(1, 8)` | `wal_replay_window_for` |

因此在途字节上界是 `window * WAL_CHUNK_CHARGE`——按索引里的**原始上限**计费，而不是按读到的块估算：
后一块可能比之前任何一块都大，估算可能被它放行的那次读取本身超过。预算小于一个计费单位时仍至少
买一块（回放不能什么都不取），此时上界是 `1 * WAL_CHUNK_CHARGE` 而不是预算值。

**这不是常驻内存实测**：回放本身最多持有 `window` 块，而索引（`WalMetadata.entries`）与
WAL 镜像（`WalImage.bytes`）仍随历史线性增长，见第 6 节。

## 3. 上限 8：从注释变成编译期约束

预取的每一个在途块都占用 Authority 客户端的一个调用槽。把上限写成 `8` 的理由原本只在注释里
（`LIVE_CONNECTIONS = 24`），现在是一条编译期断言，就写在常量旁边：

```rust
const _: () = assert!(
    WAL_REPLAY_WINDOW_MAX * 3 <= crate::authority_rpc::client::LIVE_CONNECTIONS,
    "a WAL replay may hold at most a third of the authority client call gate"
);
```

`LIVE_CONNECTIONS` 为此从 `const` 改为 `pub(crate)`；取值的唯一权威仍在 `authority_rpc/client.rs`，
`wal.rs` 不再复述数字。上限被抬高时会**编译失败**（见第 5 节 M1），而不是等某条用例去发现。

## 4. 环境变量语义

`PERI_WORKER_WAL_REPLAY_BYTES`：正整数字节。非法值 fail-closed——回放位于打开路径，
把一个坏预算静默降级成另一个值，只会把配置错误藏在一次更慢或更饿的打开后面。
子进程 re-exec 用例覆盖 `unset`（默认） / `1`（窗口 1） / `3 * CHARGE`（窗口 3） / `0` / `abc` / `-1`（三种均拒绝），
拒绝时断言「一条记录都没回放、一块都没读」。

## 5. 覆盖分工与变异证据

| 性质 | 用例 | 位置 |
| --- | --- | --- |
| 预算 → 窗口的算术（含上限 8） | `the_replay_window_is_the_budget_divided_by_the_per_chunk_charge` | `wal.rs` 单测 |
| 环境变量语义与 fail-closed | `wal_replay_window_follows_the_byte_budget`（子进程三臂 + 三个拒绝臂） | `wal.rs` 单测 |
| 在途字节 ≤ 预算 | `replay_bounds_in_flight_bytes_to_the_budget`（假 store 统计并发峰值 × 计费 ≤ 默认预算） | `wal.rs` 单测 |
| 乱序完成仍按 LSN 交付 | `replay_delivers_records_in_lsn_order_when_chunks_finish_in_reverse` | `wal.rs` 单测 |
| 报最低 LSN 的失败 | `replay_reports_the_lowest_lsn_failure_and_stops_delivering` | `wal.rs` 单测 |
| 访问者拒绝即停止交付 | `replay_stops_delivering_when_the_visitor_rejects_a_record` | `wal.rs` 单测 |
| 端到端：顺序 + 提前停止 + 读前距离是小常数 | `wal_replay_reads_ahead_within_its_window_and_stops_with_the_visitor`（32 条记录） | `tests/remote_database.rs` |

变异实验（每次只改一处，跑独立目标）：

| 变异 | 预期 | 观测 |
| --- | --- | --- |
| M1 `WAL_REPLAY_WINDOW_MAX` 8 → 64 | 编译失败 | `error[E0080]: a WAL replay may hold at most a third of the authority client call gate`（`wal.rs:167`） |
| M2 绕过预算，`for_each_record` 固定窗口 32（= 记录数） | 端到端用例失败 | 账本用例仍 `ok`（它只钉算术），端到端 `Error: replay ran 32 chunks ahead of its visitor; the ceiling is 9` |
| M3 上限 8 → 64 **且**预算设到上限之上（旧形态的「窗口=历史长度」，在加编译期断言之前做的） | 端到端用例失败 | 当时 `replay ran 32 chunks ahead of its visitor`；加上 `const _` 断言后这条变异已经编译不过，即退化成 M1 |

上限被**抬高**时编译失败（`9 * 3 > 24`）；被**调低**时账本用例的 `wal_replay_window_for(usize::MAX) == 8`
失败。两个方向都不会静默通过。

**顺带修掉一个我自己写错的常数**：端到端用例原本写 `READ_AHEAD_CEILING = 8`，注释说这是
「任何字节预算能买到的最宽窗口」。但投递循环是在一块**被接受之后、访问者看到它之前**补位，
所以访问者拒绝第一条记录时观测到的是 `window + 1` 次读取：把预算真的调到上限（窗口 8）时该用例
自己失败（`replay ran 9 chunks ahead of its visitor`）。常数改为 9 并在注释里写明推导——
窗口上限本身不约束这个计数，本条用例也不替代账本单测。

## 6. 明确不能说的

- **没有加速倍数或百分比**。本次没有做任何墙钟测量；[§4](./2026-10-17-open-load-stall.md) 的
  5.3–7.6 s 与 15.9–17.7 s 是被回退实现的**单次观测**，不转移给本次实现，也不外推到其他 WAL 长度、
  其他规模、其他 `sql_max_duration_ms`、真实 S3 后端或并发场景。
- **内存上界是算术上界**（`window * WAL_CHUNK_CHARGE`），不是被测量的 RSS；进程级内存账本仍缺席（F05）。
- **不随历史增长只覆盖回放窗口**：索引条目表与 WAL 镜像仍随历史线性（F06 的剩余项，W1 第二阶段
  的 `previous.image.clone()` 也未做，仍需批准）。
- **自动 compact 仍未做**，T06 三档规模数据仍未产出。
- 同文件里被 `#[ignore]` 的 `wal_replay_wall_clock_against_serial_replay` 只跑假 store（sleep 代替
  Authority 往返），是手工对照，不是平台性能证据，数字不得引用。
