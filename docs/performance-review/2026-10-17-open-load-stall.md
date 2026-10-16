# 数据库重新打开停滞：症状、归因与修复边界（Rust 数据面 + Go 失败契约）

日期：2026-10-17。状态：**已定位并复现；归因基于一次有效的 native 采样与离线对象普查；修复尚未落地，本文不含任何修复后的数量**
范围：`prototypes/v1/src` 的 Worker 加载路径与 `prototypes/v1/platform` 的 SQL 失败分类。
基线：main `0ecb117`，Rust debug 二进制 sha256 `9c73a5e3…`（本文实验用同一二进制重建）。
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

采样显示这次加载的代价主要不在网络，而在**同一份响应体被解析两遍**：字节负载按 JSON 整数数组编码
（约 4 字符/字节），客户端先把响应体解析成 `serde_json::Value`，再对同一批字节解析成 `RpcResponse`。

---

## 1. 症状与复现

### 1.1 稳定复现的四探针序列

同一栈、同一分支，唯一变化的量是"两次打开之间是空闲还是保持活跃"：

| 探针 | 条件 | 结果 |
|---|---|---|
| A | 导入结束后立即打开 | **成功 0.019 s** |
| C | 保持活跃 3.4 s 后打开 | **失败 16.137 s**（见 §1.3 说明） |
| B | 空闲 3.4 s 后打开 | **失败 16.133 s** |
| D | 再空闲 3.4 s 后打开 | **失败**（同一路径重付一次代价） |

失败体的关键字段：`code=sql_timeout`、`message=statement did not finish within the worker per-statement deadline
(PERI_SQL_MAX_DURATION_MS)`、`execution_completed=false`、`may_have_committed=false`。
Worker 侧原文是 `SQL execution not entered: writer admission deadline`，`rejected=true, executed=false`——
**语句没有进入执行**，所以这是"确定未提交"，不是 Unknown。

同一序列在更早的一次运行（run3，同一代码族）上也复现：A 成功 0.019 s、B 失败 16.649 s，
且此后**该数据库再也打不开**——每次重试都在同一处再花约 16 s 并失败（探针 C/D 转为失败即为此现象的第二次观测）。

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

于是无论语句自身的预算是多少，**这次打开的可用时间都不超过"请求受理时刻 + 10 s"**。
加载需要 16.1–16.6 s，加载完成时预算早已耗尽，`RemoteDatabase::session()` 的
`RequestDeadline::new(&Budget::trusted_default())` 报 `AdmissionError::Deadline`。

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
服务线程共 **3806** 个采样（同一进程主线程 4201，其余停在 acceptor park）：

| 帧 | 采样数 | 占服务线程 |
|---|---|---|
| `WorkerRuntime::load_native` | 2767 | 72.7% |
| `RemoteIO::open` | 2726 | 71.6% |
| `RemoteWal::for_each_record` | 2580 | 67.8% |
| `AuthorityClient::call` | 2405 | 63.2% |
| ├ `call_with_deadline` → `reqwest` → `park_timeout` | 942 | 24.7% |
| ├ `serde_json::from_slice::<Value>` | 719 | 18.9% |
| │ └ 其中 `SeqAccess::next_element` | 634 | 16.7% |
| └ `serde_json::from_slice::<RpcResponse>` | 714 | 18.8% |

峰值物理占用 1.9 GB。

**读法**：这一次加载里，"等 Authority 回包"占约四分之一，而**两次解析同一份响应体合计 37.7%**，
比网络等待还多。`from_slice::<Value>` 与 `from_slice::<RpcResponse>` 解析的是**同一批字节**
（`src/authority_rpc/client.rs:280` 与 `:283`，前者只用于校验 `result` 字段存在），
而 `RpcResponse.result` 是 `Option<serde_json::Value>`，所以第二遍又完整物化了一份。

### 2.4 为什么解析这么贵：字节负载按整数数组编码

`RemoteWal::for_each_record` 对 `wal/index` 的每条 entry 各发一次同步 `Get`。
对该分支对象存储的只读普查（离线解析 run 目录的 `.obj` 信封）给出规模：

- `wal/index`：**13 条 entry**（`last_lsn=13`, `epoch=2`）；
- 每个 WAL chunk 对象的**解码字节**为 3.5–5.0 MB（内容就是 `serde_json::to_vec(WalRecord)`）；
- 同一对象在磁盘信封（base64）里是 4.5–7.1 MB。

而 `StoredObject { bytes: Vec<u8>, .. }`（`src/object_store.rs:21`）**没有**任何 serde 字节编码属性，
`SessionOperation::Get` 的回复由 `src/authority_rpc/server.rs:603-605` 直接
`serde_json::to_value(store.get(&key)?)` 产生。`Vec<u8>` 在没有 `serde_bytes` 的情况下序列化成
**整数数组**（仓库自己的 `src/object_store/local/payload.rs` 把这种形式称作
`legacy byte array`，即为此形态的旁证）。

**推算（非实测）**：按 3.9–4 字符/字节计，单个 chunk 的回复体约 14–20 MB，13 个 chunk 合计约 200 MB JSON 文本。
这解释了 §2.3 里两次解析各占约 19%。

### 2.5 放大：失败的加载把已加载实例丢掉

`src/runtime.rs` 的 `make_handle` 在失败路径上 `drop(database); release();`。
由于本例中加载本身越过了截止点，**仅靠保留实例并不能让这次打开成功**；
但它决定了"偶尔侥幸加载成功的实例是否会因为后续检查失败而白扔"。
这两件事必须分开判断，不能合并成"修好了某次打开失败"。

### 2.6 顺带发现的上限风险（推算，未实测）

`MAX_RPC_BYTES = 32 MiB`（`src/authority_rpc/protocol.rs:10`）。写方向的 WAL 负载上限是
`MAX_RPC_BYTES / 2`（16 MiB），但**读方向的 `Get` 回复没有按编码放大单独设限**。
整数数组编码的放大系数约 4，所以一个合法的、接近 16 MiB 的 chunk 其 JSON 文本会触及 32 MiB 上限，
报 `authority response body limit exceeded`。改成 base64 后放大系数降到约 1.33，
同样大小的 chunk 约 21 MB，仍接近上限——**这是修复后仍然存在的剩余限制**。

---

## 3. 需要改什么（与不需要改什么）

按证据，缺口分布在三处，且互相独立：

| 缺口 | 位置 | 为什么必须单独处理 |
|---|---|---|
| 加载没有独立界限，且加载后的预算没有重新锚定 | `worker_rpc/server.rs` 的截止点作用域、`database.rs` 的会话预算 | 只有它能让"加载慢"不再等价于"语句超时" |
| 字节负载的线格式放大 4 倍，且被解析两遍 | `authority_rpc/protocol.rs`、`object_store.rs`、`authority_rpc/client.rs` | 它决定加载本身的代价；改它不动任何语义 |
| 空闲卸载默认 1000 ms 与 W2 的验收场景冲突 | `worker_rpc/sessions.rs:111` | 它决定"什么时候要付这份代价" |

**不需要改**：`writer admission deadline` 对**语句执行**超预算的映射仍然正确；`rejected=true`
表示确定未提交，客户端可以安全重发，这个契约不能动。

---

## 4. 未测量与不可外推

- 本文没有测量任何**修复后**的数字；16.1–16.6 s 是修复前的单次观测，且主机同时有 17–24 的负载。
- 没有测量 RPC 回复体的真实字节数（§2.4 的 200 MB 是推算）；没有测量端到端字节吞吐。
- 没有在多档数据集规模（10k/1M 行）或不同 `commit-rows` 下重复，因此**不能外推**到其他 WAL 长度。
- 采样窗口只有 25 s，且只有一个 Worker 进程；没有与控制组做同窗口对比。
- §2.6 的上限风险只有常量与编码形式的推算，没有构造越界用例。
- 容量、公平性、SLO 一律 **NOT MEASURED**。
