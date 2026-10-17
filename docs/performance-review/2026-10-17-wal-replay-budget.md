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
| 单块计费 `WAL_CHUNK_CHARGE` | `MAX_RPC_BYTES` = 32 MiB | 一次 fetch 的**单一编码形态**上界（见下），不是 append 校验器量到的原始载荷，也不是解析期间的峰值 |
| 默认预算 | `5 * WAL_CHUNK_CHARGE` = 160 MiB | 被回退实现的「4 块」，再加上交付给访问者的那一块 |
| 窗口上限 `WAL_REPLAY_WINDOW_MAX` | 8 | 调用闸门的三分之一（第 3 节） |
| 窗口 | `(budget / WAL_CHUNK_CHARGE - 1).clamp(1, 8)` | `wal_replay_window_for` |

回放最多同时持有 `window + 1` 个计费单位：`window` 次在飞 fetch，加上「补位发生在块被接受之后、
访问者看到它之前」时手里的那一块。所以算术上界是 `(window + 1) * WAL_CHUNK_CHARGE`，按常数计费而
不是按读到的块估算：后一块可能比之前任何一块都大，估算会被它放行的那次读取本身超过。预算小于一个
计费单位时仍至少买一次 fetch（回放不能什么都不取），此时上界是**两个**计费单位（窗口 1 + 交付 1 =
64 MiB）而不是预算值；同理，要买到窗口 8 需要 9 个计费单位 = 288 MiB，而不是 8 个。

**计费单位为什么是 32 MiB 而不是 16 MiB**：一次 append 的载荷原始上限是 `MAX_RPC_BYTES / 2`
（`WalAppendRequest::validate`），但一次 fetch 持有的字节是那些载荷**编码两次**的结果——chunk 落盘
是 JSON + base64（≤ 4/3 原始载荷），`Get` 的响应体又是这一份的 base64（≤ 16/9 原始载荷 ≈ 28.4 MiB）。
两种形态各自都在一次计费之内，而这条关系由 `wal.rs` 的编译期断言钉住：把计费改回 `MAX_RPC_BYTES / 2`
会**编译失败**（第 5 节 M4）。断言只钉「计费不小于两层 base64 的**载荷**」这个下界（任何 `≥ 28.44 MiB`
的值都能通过），常数本身取整个 `MAX_RPC_BYTES`，把 chunk 信封（≤ 16384 条 `Write` ≈ 0.7 MB，同样被
第二层放大）与余量一起算进去：最坏响应体 ≈ 30.7 MB，余量 ≈ 8.5%（见
[§2.6](./2026-10-17-open-load-stall.md)）。

> **订正记录**：先前 `5f8281b` 用 `MAX_RPC_BYTES / 2` 计费，并把 `window * WAL_CHUNK_CHARGE` 称为
> 「在飞字节的可证上界」。那个上界小于它要约束的对象（漏掉两层 base64 与交付那一块），本次订正为
> `(window + 1) * MAX_RPC_BYTES`。**默认窗口没有变，仍是 4**（预算从 4 个 16 MiB 单位改为 5 个
> 32 MiB 单位），所以 §4 验收与压力观测所指的行为不受影响。

**账本不覆盖什么**——写在这里，避免把 `(window + 1) * WAL_CHUNK_CHARGE` 读成进程 RSS：

- Authority 客户端每次调用的编码临时量：`AuthorityClient::call_with_deadline` 一次大 `Get` 会同时
  持有响应体文本与两棵解析出的 JSON 树（`serde_json::Value` 与 `RpcResponse.result` 各一份 base64
  字符串），`RemoteStore::get` 再把 `result` 解成 `StoredObject.bytes`。按常量算：内层 chunk 文本
  ≤ `4/3 × 16 MiB` + 信封（≤ 16384 条 `Write`，每条约 40 B）≈ 23 MB，响应体是它的 base64 ≈ 30.7 MB，
  两棵树各持一份 23 MB 的内层文本 ⇒ 同活 ≈ 77 MB ≈ **2.3 个计费单位**。它是**任何**大 `Get` 的成本，
  不是回放特有的，也没有计入本账本。
- 回放自己解析一块时的瞬时：`load_chunk_record` 把 chunk 文本（≈ 23 MB）交给 `serde_json::from_slice`
  生成 `WalRecord`（载荷解码后 ≤ 16 MiB，另加每条 `Write` 一个 `Vec` 头 ≈ 16.9 MB），解析期间两者同活
  ≈ 40 MB ≈ **1.2 个计费单位**。所以单块计费钉的是「同一时刻只有一种编码形态」，解析瞬间会短暂超过它。
- 索引（`WalMetadata.entries`）与 WAL 镜像（`WalImage.bytes`）仍随历史线性；compact 路径
  （`DurableWal::read` → `Vec<WalRecord>` 全量）不受本预算约束。两条都见第 6 节。

以上三条是**按常量推算**的上界，本次没有实测（尤其是解析瞬时第 2 条——本次复核发现后按常量算出来的）。

**这不是常驻内存实测**：本次没有对回放做 RSS 测量（F05 的进程级内存账本仍缺席）。

## 3. 上限 8：从注释变成编译期约束，并从「每次回放」改为「每 worker」

预取的每一次在途 fetch 都占用 Authority 客户端的一个调用槽。把上限写成 `8` 的理由原本只在注释里
（`LIVE_CONNECTIONS = 24`），现在是两条编译期断言加一层**进程级许可**：

```rust
const WAL_REPLAY_FETCH_PERMITS: usize = crate::authority_rpc::client::LIVE_CONNECTIONS / 3;
static WAL_REPLAY_PERMITS: FetchPermits = FetchPermits::new(WAL_REPLAY_FETCH_PERMITS);

const _: () = assert!(
    WAL_REPLAY_WINDOW_MAX <= WAL_REPLAY_FETCH_PERMITS,
    "one WAL replay has to be able to fill its window from the shared fetch permits"
);
const _: () = assert!(
    WAL_REPLAY_FETCH_PERMITS * 3 <= crate::authority_rpc::client::LIVE_CONNECTIONS,
    "WAL replays may hold at most a third of the authority client call gate"
);
```

第一条断言与第二条的关系就是这次改动的原因：**只钉「一次回放 ≤ 1/3 闸门」是不够的**。不同数据库的
冷开是并行的（注册表只合并同键打开，见 `runtime.rs` 的 `Entry::Loading` 分支与锁外加载），所以 N 个库
同时冷开时，每个回放都可以各自吃掉整个窗口，单回放的那条断言对第二个库起不到约束作用。许可把这一维
补上：任何时刻**所有**回放的并发 fetch 合计 ≤ 8 个，单一回放仍能填满自己的窗口（默认 4、上限 8）。

`LIVE_CONNECTIONS` 为此从 `const` 改为 `pub(crate)`；取值的唯一权威仍在 `authority_rpc/client.rs`，
`wal.rs` 不再复述数字。上限被抬高时会**编译失败**（见第 5 节 M1），而不是等某条用例去发现。
许可由 fetch 线程在往返期间持有（往返返回后的块解码也在同一段里），交付那一块**不在**其中——它属于
字节账本。**等着拿许可的线程不占调用槽**：闸门被回放占住的那部分因此不超过 8 个槽。

**聚合口径**（写清楚上界各自归属谁）：

- 许可约束的是**在飞 fetch**：任何时刻所有回放的并发 Authority 调用 ≤ 8 次，每次调用自己的客户端临时量
  按第 2 节单独算。
- 许可**不**约束「已经取回、还排在队列里等交付」的记录：线程返回即释放许可，而那块记录仍占一个计费
  单位。所以每个正在回放的库的字节驻留仍是自己的 `window_d + 1` 个单位（默认 5，上限 9）。
- 因此 N 个库并发冷开的字节上界是 `Σ_d (window_d + 1)`（默认 5N；每库都开到上限时 9N），再加各库索引
  与镜像——后者才随历史线性增长，是 F06 的剩余项（第 6 节）。**`8` 不是回放内存的聚合上界**，它只是
  调用槽的聚合上界。

## 4. 环境变量语义

`PERI_WORKER_WAL_REPLAY_BYTES`：正整数字节（默认 `5 * WAL_CHUNK_CHARGE` = 160 MiB，窗口 4）。
非法值 fail-closed——回放位于打开路径，把一个坏预算静默降级成另一个值，只会把配置错误藏在一次更慢
或更饿的打开后面。子进程 re-exec 用例覆盖 `unset`（默认 → 窗口 4） / `1`（窗口 1） /
`4 * CHARGE`（窗口 3） / `0` / `abc` / `-1`（三种均拒绝），拒绝时断言「一条记录都没回放、一块都没读」。

## 5. 覆盖分工与变异证据

| 性质 | 用例 | 位置 |
| --- | --- | --- |
| 预算 → 窗口的算术（含交付那一块与上限 8） | `the_window_is_the_budget_less_the_delivery_over_the_fetch_charge` | `wal.rs` 单测 |
| 环境变量语义与 fail-closed | `wal_replay_window_follows_the_byte_budget`（子进程三臂 + 三个拒绝臂） | `wal.rs` 单测 |
| 在飞块数 + 交付 ≤ 预算 | `replay_bounds_the_window_plus_the_delivery_to_the_budget`（假 store 统计并发峰值 `peak`，断言 `(peak + 1) × 计费 ≤ 默认预算`。`peak` 是**并发 get 数**，管的是「几块」，管不了「每块多少字节」——后者由下一行的编译期断言钉住） | `wal.rs` 单测 |
| 并发回放共享同一份许可 | `concurrent_replays_share_one_fetch_budget`（两个回放各用最宽窗口 8，断言聚合峰值 ≤ 8 且都按 LSN 收齐） | `wal.rs` 单测 |
| 计费覆盖两层 base64 编码 | `const _` 编译期断言（`WAL_CHUNK_CHARGE * 9 >= 16 * (MAX_RPC_BYTES / 2)`） | `wal.rs` 常量旁 |
| 乱序完成仍按 LSN 交付 | `replay_delivers_records_in_lsn_order_when_chunks_finish_in_reverse` | `wal.rs` 单测 |
| 报最低 LSN 的失败 | `replay_reports_the_lowest_lsn_failure_and_stops_delivering` | `wal.rs` 单测 |
| 访问者拒绝即停止交付 | `replay_stops_delivering_when_the_visitor_rejects_a_record` | `wal.rs` 单测 |
| 端到端：顺序 + 提前停止 + 读前距离是小常数 | `wal_replay_reads_ahead_within_its_window_and_stops_with_the_visitor`（32 条记录） | `tests/remote_database.rs` |

变异实验（每次只改一处，跑独立目标；下表是**本次修订**（含共享许可）重跑的观测，工作区文件在每次变异后按 sha256 校验恢复）：

| 变异 | 预期 | 观测 |
| --- | --- | --- |
| M1 `WAL_REPLAY_WINDOW_MAX` 8 → 64 | 编译失败 | `error[E0080]: evaluation panicked: one WAL replay has to be able to fill its window from the shared fetch permits`（`wal.rs:183`）——上限被抬高时先撞到的是这条，而不是「1/3 闸门」那条（后者只看许可数，与窗口无关） |
| M2 绕过预算，`for_each_record` 固定窗口 32（= 记录数） | 端到端用例失败 | 账本用例仍 `ok`（它只钉算术），端到端 `Error: replay ran 32 chunks ahead of its visitor; the ceiling is 9 (window cap plus the refill started before the visitor answered)` |
| M3 上限 8 → 64 **且**预算设到上限之上（旧形态的「窗口=历史长度」，在加编译期断言之前做的） | 端到端用例失败 | 当时 `replay ran 32 chunks ahead of its visitor`；加上 `const _` 断言后这条变异已经编译不过，即退化成 M1 |
| M4 计费改回先前的口径 `MAX_RPC_BYTES / 2`（= 把第 2 节的订正撤销） | 编译失败 | `error[E0080]: evaluation panicked: the fetch charge has to cover the twice-encoded payload ceiling`（`wal.rs:195`） |
| M5 删掉 `load_chunk_record` 里的 `let _permit = WAL_REPLAY_PERMITS.acquire();` | 并发用例失败 | `two replays held 16 chunks at once, over the 8 permits the worker shares between them`（`concurrent_replays_share_one_fetch_budget`，`wal.rs:1157`）——没有它，两个回放各自吃满窗口 |
| M6 许可数从 `LIVE_CONNECTIONS / 3` 抬到 `LIVE_CONNECTIONS` | 编译失败 | `error[E0080]: evaluation panicked: WAL replays may hold at most a third of the authority client call gate`（`wal.rs:187`） |

窗口上限被**抬高**（M1）、许可数被**抬高**（M6）、计费被**调低到载荷上限**（M4）都会编译失败；
窗口被**调低**时账本用例的 `wal_replay_window_for(usize::MAX) == 8` 失败；许可被删掉时 M5 失败。
五个方向都不会静默通过。

**顺带修掉一个我自己写错的常数**：端到端用例原本写 `READ_AHEAD_CEILING = 8`，注释说这是
「任何字节预算能买到的最宽窗口」。但投递循环是在一块**被接受之后、访问者看到它之前**补位，
所以访问者拒绝第一条记录时观测到的是 `window + 1` 次读取：把预算真的调到上限（窗口 8）时该用例
自己失败（`replay ran 9 chunks ahead of its visitor`）。常数改为 9 并在注释里写明推导——
窗口上限本身不约束这个计数，本条用例也不替代账本单测。

## 6. 明确不能说的

- **没有加速倍数或百分比**。本次没有做任何墙钟测量；[§4](./2026-10-17-open-load-stall.md) 的
  5.3–7.6 s 与 15.9–17.7 s 是被回退实现的**单次观测**，不转移给本次实现，也不外推到其他 WAL 长度、
  其他规模、其他 `sql_max_duration_ms`、真实 S3 后端或并发场景。
- **内存上界是算术上界**（`(window + 1) * WAL_CHUNK_CHARGE`），不是被测量的 RSS；它也不覆盖第 2 节
  列出的三处（Authority 客户端每次调用的编码临时量 ≈ 2.3 个单位、回放解析一块时的瞬时 ≈ 1.2 个单位、
  随历史线性的索引与 WAL 镜像）。三条都是按常量推算、没有实测。进程级内存账本仍缺席（F05）。
- **不随历史增长只覆盖回放窗口**：索引条目表与 WAL 镜像仍随历史线性（F06 的剩余项，W1 第二阶段
  的 `previous.image.clone()` 也未做，仍需批准）。
- **本次复核发现两条相邻上限，与回放预算无关，也还没有用例**（算术推断，未实测，不在本次改动范围内）：
  compact 走 `RemoteWal::truncate` → `DurableWal::read(namespace, 0)`，把整段历史收进一个
  `Vec<WalRecord>` 再拼出整份镜像，都不受本预算约束；而 `truncate` 写出的 baseline 是**绕开**
  `WalAppendRequest::validate` 的（`chunk()` 只做 `check_writer` + `put`），唯一的界是客户端请求体
  32 MiB ⇒ 镜像超过 ≈ 18 MiB（`16/9 × M ≤ 32 MiB`，两层 base64）时这次 `Put` 必然超限，`io.rs` 随即
  置粘性 `wal_failed`。同理 `wal/index` 由 Authority 本地写入、无体量检查，读回只能走单次
  `Get ≤ 32 MiB` ⇒ 索引 JSON 超过 ≈ 24 MiB 后打开 / 追加 / compact 全部硬失败。
- **自动 compact 仍未做**，T06 三档规模数据仍未产出。
- 同文件里被 `#[ignore]` 的 `wal_replay_wall_clock_against_serial_replay` 只跑假 store（sleep 代替
  Authority 往返），是手工对照，不是平台性能证据，数字不得引用。
