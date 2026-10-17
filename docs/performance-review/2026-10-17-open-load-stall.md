# 数据库重新打开停滞：症状、归因与修复边界（Rust 数据面 + Go 失败契约）

日期：2026-10-17。状态：**已定位、已复现、已定案到失败产生点；加载预算的结构性修复已落地（§3），
另有一项"并发预取 WAL chunk"的修复经单变量对照验证有效、但**未落地**（§4 记录了回退理由）**
范围：`prototypes/v1/src` 的 Worker 加载路径与 `prototypes/v1/platform` 的 SQL 失败分类。
基线：main `0ecb117`；§1–§3 的观测全部来自该基线的 debug 二进制（run3/run4：sha256 `9c73a5e38b83eee4…`，
run3 本身的启动输出未记录 sha，见 §5）；§4 的对照还用了含预取的二进制（sha256 `e226b37dc1b0b151…`）。
相关研究文档：`peri-loom-tuning-research-2026-10-16.md` 的 W2（"已加载句柄的生命周期"）与 W5（读路径）。

**本文不宣称任何性能或容量结论。** 文中所有耗时都是单次观测，且实验期间主机负载为 17–24（其他 worktree 正在构建），
只能用来判定"成功/失败"与定位路径，不足以作为延迟基线。

---

## 0. 一句话结论

导入 100k 行后关闭全部会话，**空闲约 1 秒**就会把数据库从 Worker 卸载（`PERI_WORKER_DB_IDLE_UNLOAD_MS`
默认 1000 ms）——这是基线 `0ecb117` 的默认值，本文"空闲约 1 秒就卸载"的描述都指该基线默认；当前 main 已把该默认
改为 **60000 ms**（`7c3adab`，见 §3）。
在基线默认下，其后**第一次**打开会话必须重新回放整个远端 WAL，实测耗时 16.1–16.6 s；而这次打开所用的语句预算被请求级
截止点夹在 **10 s**（`2 × PERI_SQL_MAX_DURATION_MS`）以内，于是加载完成后立刻被判为
`writer admission deadline`，Go 平台按既有短语表把它报成 **504 `sql_timeout`**——
尽管该语句**从未进入执行**（`execution_completed=false`）。

采样显示这次加载的代价主要不在网络，而在**同一份响应体被反复解析**：字节负载按 JSON 整数数组编码
（实测 3.36 字符/字节，见 §2.4），客户端先把响应体解析成 `serde_json::Value`，再对同一批字节解析成 `RpcResponse`，
`Get` 路径上还有第三遍从 `Value` 树再物化一次——三次反序列化合计 42.2% 样本（§2.3）。

---

## 1. 症状与复现

### 1.1 稳定复现的四探针序列

同一栈、同一分支，唯一变化的量是"两次打开之间是空闲还是保持活跃"：

| 探针 | 条件 | 结果 |
|---|---|---|
| A | 导入结束后立即打开 | **成功 0.019 s**（旧顺序运行；条件与顺序无关） |
| C | 保持活跃 3.4 s 后打开 | **成功 0.021 s**（默认预算下的修正顺序对照见 §4：`keepalive` 9–10 次全部成功，随后被测 open 0.025–0.039 s 成功） |
| B | 空闲 3.4 s 后打开 | **失败 16.133 s** |
| D | 再空闲 3.4 s 后打开 | **失败**（同一路径重付一次代价） |

上表的 B/C/D 取自**旧顺序**运行（A → B → C → D，见 §1.3）；其中 16.137 s 那次"C 失败"是旧顺序的产物，
**不能**读成"修正顺序下保持活跃也会失败"。真正按修正顺序、默认预算采集的对照在 §4。

失败体的关键字段：`code=sql_timeout`、`message=statement did not finish within the worker per-statement deadline
(PERI_SQL_MAX_DURATION_MS)`、`execution_completed=false`、`may_have_committed=false`。
Worker 侧原文是 `SQL execution not entered: writer admission deadline`，`rejected=true, executed=false`——
**语句没有进入执行**，所以这是"确定未提交"，不是 Unknown。

同一序列在更早的一次运行（run3，同一代码族）上也复现：A 成功 0.019 s、B 失败 16.649 s；
run3 的 authority 日志显示三次失败对应三次完整的 11-chunk 回放，即**每次重试都以相同方式再付一次加载**。
（探针 C/D 那次旧顺序里的失败即为此现象的第二次观测。）

这是**默认配置下的不动点，不是库损坏**：把语句预算从 5 s 提到 20 s
（`--sql-max-duration-ms 20000`，请求级截止点随之变成 40 s）后，同一位置的"空闲 3.4 s 后打开"
在 **15.873 s 成功**。"每次重试都重付代价"的原因是重试间隔超过了空闲卸载阈值，见 §2.5。

### 1.2 触发条件

`src/worker_rpc/sessions.rs:111` 把空闲卸载阈值定为 1000 ms——下段取自基线 `0ecb117`：

```rust
database_idle: duration_env("PERI_WORKER_DB_IDLE_UNLOAD_MS", 1_000)?,
```

`sweep()`（同文件 `:389`）末尾调用 `self.runtime.evict_idle(self.database_idle)?`（`:414`），
由 watchdog 周期调用（`src/worker_rpc/server.rs:202`）。所以"关闭全部会话约 1 秒后卸载"是**基线 `0ecb117` 默认值下的行为**，
与 30 s 的 lease TTL 无关：3.4 s 的空闲窗口已足够越过 1 s 阈值。当前 main 已把该默认值改为 **60000 ms**
（`7c3adab`，见 §3）；本节与 §1.1 的失败观测都出自基线默认。

### 1.3 一次实验设计缺陷的更正

最初把"保持活跃"探针排在失败探针**之后**，结果它自己就成了第三次失败观测：
数据库早已被卸载，保持活跃段无法先建立"已加载"状态。修正后的顺序是 A → C → B → D，
即**保持活跃必须在仍是已加载状态时开始**。上表按修正后的顺序描述；已按旧顺序采集的那次运行
（C 失败 16.137 s）只用来佐证"卸载后每次重试都重付代价"。

---

## 2. 归因

### 2.1 请求级截止点夹住了加载

`src/worker_rpc/server.rs:380-381`：请求受理时建立一个线程局部的截止点

```rust
let deadline = accepted + Budget::trusted_default().wall_duration();
let _deadline_scope = crate::session::RequestDeadline::scope(deadline);
```

`Budget::trusted_default().wall_duration()` 是 `2 × max_query_duration`（`src/session/budget.rs` 的用例
明确钉住 5 s → 10 s 这条关系）。随后 `RequestDeadline::new(&budget)` 取
`now + wall_duration` 与这个外层截止点的**较小者**（`src/session/budget.rs:135`：

```rust
let end = RPC_DEADLINE.get().map_or(end, |outer| outer.min(end));
```

于是无论语句自身的预算是多少，**这次打开的可用时间都不超过"请求受理时刻 + 10 s"**，
而加载需要 16.1–16.6 s。

**产生点已定案（2026-10-17 补）：失败来自加载路径自己发的那条语句。**
`RemoteDatabase::open_configured` 在非只读打开时执行
`database.query("SELECT name FROM sqlite_master LIMIT 1")`（`src/database.rs:152`），
它走 `SessionLane::query`（`src/session/lane.rs:167`）→ `RequestDeadline::new(budget)`（`:174`）→
`deadline.check()`（`src/session/budget.rs:147`）→ `AdmissionError::Deadline`；
而 `lane.rs:177` 的 `.map_err(|error| deadline.evidence.failure(error))` 会在语句
`entered == false` 时补上 `SqlEvidence::NotExecuted`（`src/session/evidence.rs:44-57`）。
这条链一次解释了此前两组不自洽的事实：

- 为什么文本是 `SQL execution not entered: writer admission deadline`（带证据前缀）——
  因为失败点是一次**从未进入执行**的语句检查，而不是 `RemoteDatabase::session()` 的会话判定；
- 为什么它发生在"同一请求已经花了 16 s"之后——那 16 s 花在**同一函数体内**的 WAL 回放上，
  回放完成后才轮到这条 open 期语句，此时请求级截止点（受理 + 10 s）早已过期。

（此前本节把它记为"未解释矛盾"，依据是"`RemoteDatabase::session()` 的 `deadline.check()` 不带证据"。
那个否证本身成立，缺的是**加载路径内部也有一条取截止点的语句**这一环。）

### 2.2 Go 平台把它报成了语句超时

`prototypes/v1/platform/sql_failure.go:52` 的短语表里有

```go
{"writer admission deadline", sqlFailureDeadline},
```

它在 `:94` 是**子串匹配**，于是落在 `:120-121` 的 504 `sql_timeout` 分支，客户端看到的是
"语句未在每条语句截止时间内完成"。语句其实从未执行。这条映射对"语句执行超预算"是正确的，
但**对"数据库还没加载完"是错误的**：两者都不该被重试，但前者可能已提交，后者确定未提交。

### 2.3 加载代价：一次有效的 native 采样

`sample(1) pid … 5ms`，25 s 窗口，覆盖整个失败窗口（pid 54729，栈文件随本次实验保留）。
服务线程共 **3806** 个采样（同一进程主线程 4201，其中 4182 停在 `__semwait_signal` 的 100 ms 睡眠；
两条 `reqwest-internal-sync-runtime` 线程停在 `kevent`，主线程停在 acceptor 的 `poll`）。

下表**前四行是同一符号在同一线程内的帧总和**（该符号可能作为内联副本出现多次），
其余行是 `AuthorityClient::call` 之下最大单节点的采样数：

| 帧 | 采样数 | 占服务线程 |
|---|---|---|
| `WorkerRuntime::load_native` | 2775（4 个帧） | 72.9% |
| `RemoteIO::open` | 2750（3 个帧） | 72.3% |
| `RemoteWal::for_each_record` | 2726（5 个帧） | 71.6% |
| `AuthorityClient::call` | 2413（6 个帧） | 63.4% |
| ├ `call_with_deadline` → `reqwest` → `park_timeout` | 942 | 24.7% |
| ├ `serde_json::from_slice::<Value>` | 719 | 18.9% |
| │ └ 其中 `SeqAccess::next_element` | 634 | 16.7% |
| ├ `serde_json::from_slice::<RpcResponse>` | 714（其下另有 `next_element` 642） | 18.8% |
| └ `serde_json::from_value::<Option<StoredObject>>` | 175 | 4.6% |

峰值物理占用 1.9 GB。

**读法**：这一次加载里，"等 Authority 回包"占约四分之一，而**反序列化合计 42.2%**（1608/3806），
比网络等待还多。其中两遍解析的是**同一批字节**：`client.rs:283` 的 `from_slice::<Value>`
（只用于校验 `result` 字段存在）与 `:291` 的 `from_slice::<RpcResponse>`；
而 `RpcResponse.result` 是 `Option<serde_json::Value>`，所以 `Get` 路径上还有第三遍
`serde_json::from_value::<Option<StoredObject>>`（`src/authority_rpc/provider.rs:327`）
从第一遍物化出的 `Value` 树再物化一份字节。三遍都无条件执行。

### 2.4 为什么解析这么贵：字节负载按整数数组编码

`RemoteWal::for_each_record` 对 `wal/index` 的每条 entry 各发一次同步 `Get`。
对该分支对象存储的只读普查（离线解析 run 目录的 `.obj` 信封）给出规模：

- `wal/index`：**13 条 entry**（`last_lsn=13`, `epoch=2`）；
- 每个 WAL chunk 对象的**解码字节**：10 个大的为 **2.64–5.31 MB**，另有 3 条小记录（11,279 B、11,281 B、292 B）；
  内容就是 `serde_json::to_vec(WalRecord)`（顶层键 `append_id/batch/commit/epoch/lsn/namespace/timestamp_millis`）；
- 同一对象在磁盘信封（base64）里是 **3.52–7.08 MB**。

而 `StoredObject { bytes: Vec<u8>, .. }`（`src/object_store.rs:19-24`）**没有**任何 serde 字节编码属性，
`SessionOperation::Get` 的回复由 `src/authority_rpc/server.rs:603-605` 直接
`serde_json::to_value(store.get(&key)?)` 产生，`Vec<u8>` 在没有 `serde_bytes` 的情况下序列化成
**整数数组**（仓库自己的 `src/object_store/local/payload.rs` 把这种形式称作 `legacy byte array`）。
这条线格式**没有任何测试或协议向量钉住**，目前只有三处旁证：`payload.rs` 的措辞、
Go 侧 `platform/p6_verify_artifacts_test.go` 同时接受 base64 与整数数组、
以及采样叶子侧 `parse_integer` 197 样本与 `Vec<Value>::push_mut` 122 样本而**没有任何字符串扫描叶子**
（`skip_to_escape` 仅 4 个样本，且属 `WalRecord` 那条小分支）。

**实测（离线逐字节计算，非抓包）**：数组文本的放大倍率是 **3.36 字符/字节**
（不是整数数组"最坏 3.9–4"的上界）。据此：

- 单个 chunk 的回复体文本 **8.9–17.8 MB**；
- 13 个 chunk 合计载荷 39.5 MB → 文本 **132.8 MB**；
- 采样窗口里最大的那份回复体 17.8 MB，**未触及** `MAX_RPC_BYTES`（见 §2.6）。

这解释了 §2.3 里两遍 `from_slice` 各占约 19%。`RemoteWal::for_each_record` 在该基线上对
`wal/index` 的每条 entry 各发一次**同步** `Get`、没有批量取回（该基线与当前 HEAD 都是如此：
曾经落地的并发预取已按 §4.1 回退）。

### 2.5 放大：空闲清扫反复驱逐实例（失败路径本身不丢弃它）

`src/runtime.rs:251` 的失败路径 `drop(database); self.release(&identifier);` 只丢掉**一个** `Arc` 克隆：
注册表里的 `Entry::Loaded`（`src/runtime/registry.rs:35-41`）持有同一个 `Arc<Instance>`，
而 `release`（`src/runtime.rs:442`）对 `Loaded` 条目只递减 `sessions`、在归零时把
`idle_since` 置成 `Some(now)`（`:452`），只有 `draining && sessions == 0` 才转 `Entry::Retiring`。
**所以失败的打开并不会把已加载实例丢掉**；把它丢掉的是空闲清扫：watchdog 每 100 ms 走一遍
（`src/worker_rpc/server.rs:223`），`sweep()` 末尾按 `PERI_WORKER_DB_IDLE_UNLOAD_MS`
调用 `evict_idle`（`:414`），把 `Loaded` 换成 `Retiring` 并释放那份强引用。
这里引用的是基线 `0ecb117` 的默认值 1000 ms（`src/worker_rpc/sessions.rs:111`），当时观测到的行为是
**约 1.0–1.1 s 空闲之后实例才消失**；当前 main 已把该默认改为 60000 ms（`7c3adab`，见 §3）。

探针 B/D 之间隔着 3.4 s 空闲，因此（在基线默认下）D 必然重付一次冷加载；当前 main 的 60000 ms 默认不再满足这个前提。但**这不能推出"空闲清扫是唯一机制"**：
把 `--sql-max-duration-ms` 压到 2000（请求级截止点 = 受理 + 4 s）后实测，与上一次失败相隔 **0 ms**
的重开同样重付了一次完整加载（9.26 s）并再次失败——即那次失败发生时条目**已经不在注册表里**。
能产生这一状态的有两条路径，且都无需"加载成功"：

- `src/runtime.rs:161-165`：加载本身失败（`opened = Err`）时 `registry.entries.remove(&identifier)`；
- 空闲清扫把 `Loaded` 换成 `Retiring`（上一段）。

两条都与"每次重试都重付完整加载"一致。**具体是哪一条，本文没有钉死**：
`writer admission deadline` 这句文本在非测试代码里至少有 9 个产生点
（`AdmissionError::Deadline` 的 `Display` 全是这一句，`src/session/admission.rs:25`），
要区分需要 worker 内部计数或日志。所以这里只记录可观测事实，不把机制归到单一原因上。

这不改变结论方向：本例中加载本身越过了截止点，**仅靠保留实例并不能让这次打开成功**，
修复动作是给加载独立界限并在加载后重新锚定语句预算；把实例保留下来只是附带效果，
不能写成"修好了某次打开失败"。

### 2.6 顺带发现的上限风险（推算，未实测）

`MAX_RPC_BYTES = 32 MiB`（`src/authority_rpc/protocol.rs:10`）。写方向的 WAL 负载上限是
`MAX_RPC_BYTES / 2`（16 MiB **原始字节**，`src/wal.rs:139` 对 `payload.len()` 求和后比较——
这正是给 base64 的 1.33 倍膨胀留出的余量：16 × 1.33 ≈ 21 MiB < 32 MiB），
但**读方向的 `Get` 回复没有按编码放大单独设限**。按 §2.4 的实测倍率推算：

| 读方向编码 | 用满 16 MiB 写入预算的 chunk 的回复体 | 对 32 MiB 上限 |
|---|---|---|
| 整数数组（`9fed133` 之前） | 3.36 × 16 MiB ≈ **54 MB** | **超限约 1.7 倍**，报 `authority response body limit exceeded` |
| base64（当前，**两层**） | 4/3 × 4/3 × 16 MiB ≈ **28.4 MB** | 占上限约 89%，余量约 11% |

即：改成 base64 能把当前这条**会直接超限**的读路径降到安全区，但余量只剩约九分之一，
**这是修复后仍然存在的剩余限制**。这条路径与写方向同构——`Put` 请求体是 base64(chunk) 加
`key`/`condition`，`Get` 回复体是同一份 base64(chunk) 加 `etag`，信封同量级（几百字节）——
所以写进去的 chunk 一定读得回来，会先撞上限的是"把载荷上限抬高"这类改动。

**2026-10-17 订正**：本表初版把读方向按**一层** base64 算（1.33 × 16 MiB ≈ 21 MB，"余量约三分之一"），
漏掉了 chunk 自身已经对载荷做过一次 base64；正确值是表里的 ≈28.4 MB（余量约 11%）。
同一处漏算出现在 [WAL 回放有界预算](./2026-10-17-wal-replay-budget.md) §2 的单块计费上，已在那份文档订正。
本节全部是常量与倍率的算术，没有构造越界用例。

---

## 3. 需要改什么（与不需要改什么）

按证据，缺口分布在两处，且互相独立；两处各对应一项已落地的改动（见下）：

| 缺口 | 位置 | 为什么必须单独处理 |
|---|---|---|
| 加载没有独立界限，且加载后的预算没有重新锚定 | `worker_rpc/server.rs` 的截止点作用域、`database.rs` 的会话预算 | 只有它能让"加载慢"不再等价于"语句超时" |
| 字节负载的线格式放大 3.36 倍，且同一份字节被反序列化三遍 | `authority_rpc/protocol.rs`、`object_store.rs`、`authority_rpc/client.rs`、`authority_rpc/provider.rs` | 它决定加载本身的代价；改它不动任何语义 |

**已落地**（不改变任何语义，只改编码与界限）：

- `perf(authority_rpc)：对象字节改走 base64`（`9fed133`）——`Put.bytes`、`UploadSegments.segments`、
  `StoredObject.bytes` 三处复用既有的 `object_store::payload` 约定，读方向继续接受旧整数数组。
  实测同一负载的 JSON 文本：4 MiB 对象 14,977,748 → 5,592,489 字节；16 × 128 KiB 段批次
  7,490,140 → 2,796,316 字节（单元测试打印，不是抓包）。
  （该提交原在分支 `perf/authority-payload-20261017` 上，hash `be7d0dce`，已 cherry-pick 到 main。）
- `fix(worker)：冷加载独立预算并在加载后重新锚定语句预算`（`7c3adab`）——加载段用
  `RequestDeadline::suspend()` 摘掉请求级截止点，加载界限改为 `PERI_WORKER_LOAD_MS`
  （默认 `max(2×语句墙钟预算, 60 s)`，非法值 fail-closed），加载后重新锚定语句预算；
  超预算报 `database load budget exceeded (PERI_WORKER_LOAD_MS)` 且带 `SqlEvidence::NotExecuted`，
  平台侧归入 **503 `sql_unavailable`**；基线 `0ecb117` 的 `PERI_WORKER_DB_IDLE_UNLOAD_MS` 默认 1000 ms
  已改为 60000 ms。
  （该提交原在分支 `perf/open-load-budget-20261017` 上，hash `4761183`，已 cherry-pick 到 main。）

**并发打开的裁决边界**（同一提交引入的行为，一并记录）：超预算的裁决只由**实际执行加载**的那一个打开者承担——
`src/runtime.rs:216-223` 先 `completion.finish(Ok(()))` 放行等待者，随后才判 `loaded_for > load_budget()`，
超了才 `release` 并返回 `database load budget exceeded (PERI_WORKER_LOAD_MS)` + `SqlEvidence::NotExecuted`。
同一时刻等在完成信号上的打开者（`src/runtime.rs:143-149` 只 `completion.wait()` 后 `continue`）醒来会落到
`Entry::Loaded` 快路径**成功**拿到已驻留实例；这不是漏洞，而是与"超预算时实例保留、供后续复用"同一个意图。
因此并发冷库上的调用方可能看到"一个 503 `sql_unavailable`（带 `execution_completed=false`），其余成功"，
本文与 commit message 的措辞不应被读成"对所有打开者都报超预算"。这条边界已由 `src/runtime.rs` 的注释与
`src/runtime/tests.rs` 的用例 `concurrent_openers_split_on_the_load_budget` 钉住。

**不需要改**：`writer admission deadline` 对**语句执行**超预算的映射仍然正确；`rejected=true`
表示确定未提交，客户端可以安全重发，这个契约不能动。

**本次不动的相邻项**（记录在案，不在本次改动范围内）：

- worker→platform 方向的 **SQL 结果 BLOB** 仍走整数数组：`src/worker_rpc/sessions.rs:758` 的
  `json!(bytes.as_slice())`，Go 侧 `platform/hrana_http.go:760` 按 `[]any`→`byte` 解码；
  实测 4 MiB blob → 14,977,669 字节（约 3.57 字符/字节）。它与 authority 入参方向相反、属另一条协议
  （worker_rpc），要动必须同时改 Go 解码器与协议版本协商。
- 由此产生的上限关系：SQL 结果字节预算默认 4 MiB（`PERI_SQL_MAX_RESULT_BYTES`），编码后约 14.3 MiB，
  仍低于 Go 侧 32 MiB 传输上限（`platform/sql_upstream.go:23` 的 `sqlWireLimit`）；若把
  `PERI_SQL_MAX_RESULT_BYTES` 抬到约 9 MiB 以上，编码后就会撞上 32 MiB 传输上限，
  而当前没有任何校验把这两个上限绑在一起。

---

## 4. 已验证但**未落地**的修复：WAL 并发预取

`RemoteWal::for_each_record` 在基线上是**同步串行**的：13 个 chunk 就是 13 次串行往返。
曾一度移植了一个有界预取窗口（`PERI_WORKER_WAL_PREFETCH`，默认 4、上限 8、`=1` 退化为串行、
非法值 fail-closed，仍按 lsn 顺序投递），实测有效，但**已从 main 回退**——原因见本节末。

对照设计：**两个二进制只差这一个提交**（`git diff --name-only 0ecb117 2f729e2 -- prototypes/` 只列出
`prototypes/v1/src/wal.rs`（+489/−18）；区间内另一个提交 `f8f036f` 只加 markdown），
交替跑同一套四探针（A→C→B→D，同一 sequence 脚本、同一 100k 导入、默认预算 5 s ⇒ 请求级截止点 10 s），共两轮：

| 轮次 | 二进制（sha256 前 16 位） | 探针 B（空闲 3.4 s） | 探针 D（再空闲 3.4 s） |
|---|---|---|---|
| ab-old1 | `9c73a5e38b83eee4`（`0ecb117`） | **失败 17.690 s** | **失败 16.002 s** |
| ab-old2 | `9c73a5e38b83eee4` | **失败 15.910 s** | **失败 17.145 s** |
| ab-new1 | `e226b37dc1b0b151`（`2f729e2`） | **成功 5.260 s** | **成功 7.583 s** |
| ab-new2 | `e226b37dc1b0b151` | **成功 7.610 s** | **成功 5.615 s** |

四轮里探针 A（导入后立即）与探针 C（保持活跃，`keepalive` 9–10 次连续 open/close）**全部成功**
（0.013–0.050 s），失败体一律是 §1.1 的 `sql_timeout` 形态。

**能说的**：在这个数据集、这套主机负载下，预取把冷加载从"越过 10 s 截止点"压到 **5.3–7.6 s**，
于是同一序列从 2/2 失败变成 2/2 成功（同侧散布 1.1–2.4 s，未出现跨侧交叉）。
**不能说的**：这不是延迟基线，**不支持任何加速倍数或百分比**（单次观测、每臂仅 2 次、
主机自报负载 6.2–14.6），也不能外推到其他 WAL 长度、其他规模、其他 `sql_max_duration_ms`、
真实 S3 后端或并发场景。四轮关栈均已 `clean` / 终态证明落盘，端口与进程无残留。

### 4.1 为什么回退

预取本身不违反顺序、也不改变错误归属（按 index 顺序 join、按 lsn 交付，注释与用例都在），
但那版实现把**在途块数**从 1 提到 `window`（默认 4、上限 8），而"回放不在访问者结束前抓取下一条 chunk"
是 `2ed2e2d`（"replay cold records without collecting all chunks"）刻意钉住的内存上界，
由 `tests/remote_database.rs:177` 的 `wal_replay_visits_a_record_before_fetching_the_next_chunk`
断言。带着预取的 main 上该用例实际失败：

```
---- wal_replay_visits_a_record_before_fetching_the_next_chunk stdout ----
Error: replay fetched later records before the visitor finished
test result: FAILED. 14 passed; 1 failed
```

这不是可以顺手改测试的细节：每块原始上限是 `MAX_RPC_BYTES / 2` = 16 MiB，
`window` 块的在途量意味着最坏 128 MiB（按 2026-10-17 订正的两层编码口径与交付那一块，最坏是
`(window + 1) × 32 MiB`，见本小节末段与[WAL 回放有界预算](./2026-10-17-wal-replay-budget.md) §2）；而 F06
（`docs/performance-review/implementation-status.md:12`）本身还写着"**需完成固定缓冲预算**"——
这条路径的内存账本尚未建立，在账本缺席时放宽上界，方向是反的。

更关键的是：**它修的故障已经被 §3 的结构性修复取代**。加载有了独立预算、加载后重新锚定语句预算、
空闲卸载默认 1 s → 60 s 之后，"空闲后重开"不再每次重付冷加载，预取的收益只剩"真冷加载更快"，
不再是"能不能打开"。所以这是**用一条已量化的延迟收益换取一条未记账的内存上界**，
在账本补齐前不值得。

**重新落地的条件**（写在这里以免下次又从"扫一遍代码"开始）：先把 F06 的有界缓冲预算做出来
（窗口按字节而不是按块数计，块大小从索引或首次读取处获得），再把该用例改成断言
"在途条数与字节 ≤ 预算 + 顺序 + 提前停止不抓完整历史"，而不是回退成"恰好 1 条"。
在此之前，预取保留在分支 `perf/wal-prefetch-20261017`（`5fd8be1`）与本次回退提交之前的 `2f729e2` 上。

**2026-10-17 更新**：两个条件都已满足并在 main 落地——预算改为字节
（`PERI_WORKER_WAL_REPLAY_BYTES`，默认 `5 * WAL_CHUNK_CHARGE` = 160 MiB；**旧名
`PERI_WORKER_WAL_PREFETCH` 不再是开关**，按块数计的窗口已随回退删除）、窗口 = 预算 / 单块计费 − 1，
其中单块计费取 Authority RPC 上限 32 MiB（覆盖 chunk 落盘与 `Get` 回复两层 base64，以及交付给访问者的
那一块），**默认窗口仍是 4**；上限 8 由编译期断言绑定调用闸门，那条用例已改成"顺序 + 提前停止 +
读前距离是小常数"。落点、覆盖分工与变异证据见
[WAL 回放有界预算](./2026-10-17-wal-replay-budget.md)（§2 有这次计费口径的订正记录）。
**本次重新落地没有重测墙钟**：上面两行的 5.3–7.6 s / 15.9–17.7 s 属于被回退的那版实现，
是单次观测，不转移、不外推。

### 4.2 修复后的验收观测（默认配置，单次）

验收链（stress 序列）：导入 100k 行 → 探针 A（导入后立即打开）**成功 0.018 s** → 空闲 3.4 s 的探针 B
**成功 0.021 s**（新默认下不再触发冷加载）；随后 9 臂压测跑完、空闲 30 s 后重开**成功 0.023 s**。
所用二进制为 main 的 debug 构建，sha256 前 16 位 `1dc1c3ee1f2ec548`；同参数的另一次 accept 序列
（另一 run 目录）给出探针 A 0.019 s、探针 B 0.020 s。

基线对照：同一 accept 序列在 `0ecb117` 二进制上 **FAIL**——探针 A 成功 0.021 s，但空闲 3.4 s 后的
探针 B 报 `sql_timeout` 16.019 s（`execution_completed=false`、`may_have_committed=false`、`timed_out=false`）。
落盘证据在 `~/.peri-e01/stress-20261017/logs/base-probe-b.log`（本机私有目录，不在仓库内）。

**口径**：以上都是**单次观测**（每个形态 1 次），只用于判定"成功/失败"，**不是延迟基线**，
不支持任何加速倍数或百分比，不构成性能或容量结论。

---

## 5. 未测量与不可外推

- 16.1–16.6 s 与 §4 的 5.3–7.6 s 都是**单次观测**，且主机负载有明显漂移
  （§4 四轮自报 load 14.62 / 6.22 / 7.50 / 9.43，实验开始时宿主 1 分钟负载 16.85，
  同机另有其他 worktree 的 `cargo test` 与遗留栈在跑）；§4 每臂只有 2 次，**不足以**作为延迟基线或加速比结论。
- 没有测量 RPC 回复体的真实字节数（§2.4 的 132.8 MB 是离线逐字节计算，不是抓包；回复体还含 `etag` 等字段未计）；
  没有测量端到端字节吞吐。
- 没有在多档数据集规模（10k/1M 行）或不同 `commit-rows` 下重复，因此**不能外推**到其他 WAL 长度。
- 采样窗口只有 25 s，且只有一个 Worker 进程；没有与控制组做同窗口对比。
- §2.6 的上限风险只有常量与倍率的算术，没有构造越界用例。
- §2.5 的"失败路径不丢弃实例、约 1.0–1.1 s 空闲后由清扫驱逐"（后者的窗口是基线 `0ecb117` 的 1000 ms 默认）
  来自代码路径与常量；由此推出的"失败后 1 s 内重开可复用驻留实例"**尚未实测**，不能当作重试建议的依据。
- §2.1 的产生点由代码链定案（`database.rs:152` → `lane.rs:167/174/177` → `budget.rs:147`），
  **没有**来自生产运行的 worker 内部日志；run3 也没有记录当时二进制的 sha256。
- §4.2 的验收链与基线 FAIL 对照同为**单次观测**（各 1 次、无重复臂），与 §4 的对照一样
  只用于判定"成功/失败"，**不能外推**为延迟或容量结论。
- 容量、公平性、SLO 一律 **NOT MEASURED**。
