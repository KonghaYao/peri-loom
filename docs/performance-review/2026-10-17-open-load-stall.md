# 数据库重新打开停滞：症状、归因与修复边界（Rust 数据面 + Go 失败契约）

日期：2026-10-17。状态：**已定位并复现；归因基于一次有效的 native 采样与离线对象普查；其中一条修复（WAL 并发预取）已落地并有单变量对照，见 §4**
范围：`prototypes/v1/src` 的 Worker 加载路径与 `prototypes/v1/platform` 的 SQL 失败分类。
基线：main `0ecb117`；§1–§3 的观测全部来自该基线的 debug 二进制（run3/run4：sha256 `9c73a5e38b83eee4…`，
run3 本身的启动输出未记录 sha，见 §5）；§4 的对照还用了含 `2f729e2` 的二进制（sha256 `e226b37dc1b0b151…`）。
相关研究文档：`peri-loom-tuning-research-2026-10-16.md` 的 W2（"已加载句柄的生命周期"）与 W5（读路径）。

**本文不宣称任何性能或容量结论。** 文中所有耗时都是单次观测，且实验期间主机负载为 17–24（其他 worktree 正在构建），
只能用来判定"成功/失败"与定位路径，不足以作为延迟基线。

---

## 0. 一句话结论

导入 100k 行后关闭全部会话，**空闲约 1 秒**就会把数据库从 Worker 卸载（`PERI_WORKER_DB_IDLE_UNLOAD_MS` 默认 1000 ms）。
其后**第一次**打开会话必须重新回放整个远端 WAL，实测耗时 16.1–16.6 s；而这次打开所用的语句预算被请求级
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

`src/worker_rpc/sessions.rs:111` 把空闲卸载阈值定为 1000 ms：

```rust
database_idle: duration_env("PERI_WORKER_DB_IDLE_UNLOAD_MS", 1_000)?,
```

`sweep()`（同文件 `:389`）末尾调用 `self.runtime.evict_idle(self.database_idle)?`（`:414`），
由 watchdog 周期调用（`src/worker_rpc/server.rs:202`）。所以"关闭全部会话约 1 秒后卸载"是**默认行为**，
与 30 s 的 lease TTL 无关：3.4 s 的空闲窗口已足够越过 1 s 阈值。

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

**失败发生在加载之后的"执行前"判定**，这一点由报文确定：worker 文本是
`SQL execution not entered: writer admission deadline`，平台记为 `rejected=true, executed=false`，
即确实**带** `SqlEvidence::NotExecuted`。**但具体产生点未能由现有产物确定**，遗留一处未解释的矛盾：

- `RemoteDatabase::session()` / `operation_until` 内的 `deadline.check()` 会给出同一句文本
  （`src/session/admission.rs:25`，非测试代码里至少有 9 个产生点），但它**不带** `NotExecuted`；
  平台对缺证据的失败判 `ambiguous` 并打印 `SQL outcome unknown`，与实测的 504 / `rejected=true` 不自洽。
- 能同时给出 `NotExecuted` 与该文本的 `src/worker_rpc/server.rs:469`（`prepared` 闭包失败）与
  `:538-540`（dispatch 之前的截止点检查）都在加载之前求值，与"同一请求已花 16 s 加载"冲突。

采样侧只能确认这 16 s 的加载确实发生在这个请求里（栈是
`State::serve → State::handle → State::dispatch → Sessions::open → OwnershipRuntime::open_session →
WorkerRuntime::open → load → load_native`，2775 个样本），且响应之后该连接只进入等待，没有第二个请求。
**定案需要 worker 侧带证据链的日志或一次带 trace 的复现**；本文不替它圆场，
也不把站点写进结论——下面只沿用"执行前被截止点判掉、且确定未提交"这一可观测结论。

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
`wal/index` 的每条 entry 各发一次**同步** `Get`、没有批量取回；当前 HEAD 已改为有界并发预取，见 §4。

### 2.5 放大：空闲清扫反复驱逐实例（失败路径本身不丢弃它）

`src/runtime.rs:251` 的失败路径 `drop(database); self.release(&identifier);` 只丢掉**一个** `Arc` 克隆：
注册表里的 `Entry::Loaded`（`src/runtime/registry.rs:35-41`）持有同一个 `Arc<Instance>`，
而 `release`（`src/runtime.rs:442`）对 `Loaded` 条目只递减 `sessions`、在归零时把
`idle_since` 置成 `Some(now)`（`:452`），只有 `draining && sessions == 0` 才转 `Entry::Retiring`。
**所以失败的打开并不会把已加载实例丢掉**；把它丢掉的是空闲清扫：watchdog 每 100 ms 走一遍
（`src/worker_rpc/server.rs:223`），`sweep()` 末尾按 `PERI_WORKER_DB_IDLE_UNLOAD_MS`
（默认 1000 ms，`src/worker_rpc/sessions.rs:111`）调用 `evict_idle`（`:414`），
把 `Loaded` 换成 `Retiring` 并释放那份强引用，即**约 1.0–1.1 s 空闲之后实例才消失**。

探针 B/D 之间隔着 3.4 s 空闲，因此 D 必然重付一次冷加载。但**这不能推出"空闲清扫是唯一机制"**：
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
| 整数数组（当前） | 3.36 × 16 MiB ≈ **54 MB** | **超限约 1.7 倍**，报 `authority response body limit exceeded` |
| base64 | 1.33 × 16 MiB ≈ **21 MB** | 占上限约三分之二 |

即：改成 base64 能把当前这条**会直接超限**的读路径降到安全区，但余量只剩约三分之一，
**这是修复后仍然存在的剩余限制**。本节全部是常量与倍率的算术，没有构造越界用例。

---

## 3. 需要改什么（与不需要改什么）

按证据，缺口分布在三处，且互相独立：

| 缺口 | 位置 | 为什么必须单独处理 |
|---|---|---|
| 加载没有独立界限，且加载后的预算没有重新锚定 | `worker_rpc/server.rs` 的截止点作用域、`database.rs` 的会话预算 | 只有它能让"加载慢"不再等价于"语句超时" |
| 字节负载的线格式放大 3.36 倍，且同一份字节被反序列化三遍 | `authority_rpc/protocol.rs`、`object_store.rs`、`authority_rpc/client.rs`、`authority_rpc/provider.rs` | 它决定加载本身的代价；改它不动任何语义（预取已把"串行 RTT"这一层拿走，见 §4） |
| 空闲卸载默认 1000 ms 与 W2 的验收场景冲突 | `worker_rpc/sessions.rs:111` | 它决定"什么时候要付这份代价" |

**不需要改**：`writer admission deadline` 对**语句执行**超预算的映射仍然正确；`rejected=true`
表示确定未提交，客户端可以安全重发，这个契约不能动。

---

## 4. 已落地的一处修复与单变量对照：WAL 并发预取（`2f729e2`）

`RemoteWal::for_each_record` 在该基线上是**同步串行**的：13 个 chunk 就是 13 次串行往返。
`2f729e2` 引入有界预取窗口（`PERI_WORKER_WAL_PREFETCH`，默认 4、上限 8、`=1` 退化为串行、
非法值 fail-closed），仍然按 lsn 顺序投递。

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
真实 S3 后端或并发场景；它没有触碰"加载与语句预算共用请求级截止点"这个结构问题——
预取只是让这一次加载**恰好**落在界限内。四轮关栈均已 `clean` / 终态证明落盘，端口与进程无残留。

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
- §2.5 的"失败路径不丢弃实例、约 1.0–1.1 s 空闲后由清扫驱逐"来自代码路径与常量；
  由此推出的"失败后 1 s 内重开可复用驻留实例"**尚未实测**，不能当作重试建议的依据。
- §2.1 的**失败产生点未定案**（报文带 `NotExecuted` 与已知产生点冲突），这是本文唯一遗留的未解释矛盾；
  run3 也没有记录当时二进制的 sha256（该次构建已被 07:20 的重建覆盖）。
- 容量、公平性、SLO 一律 **NOT MEASURED**。
