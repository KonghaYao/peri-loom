# R7 联网调研席：小查询最小安全优化

日期：2026-10-29（Asia/Shanghai）。本次联网只依赖官方源码和官方文档。报告与源码快照均为研发输入；没有改生产代码、没有执行性能负载。既有 1.5× 交付门槛及 1.1× 研发目标保持原样。

## 裁决

本轮仅落地**进程内 HTTP/JSON 适配消重**；将**guarded cursor-first 消除 describe**保留为后续需独立验证的候选。不要一次同时改鉴权缓存、审计范围、SQL 只读判定和结果分页。每个候选必须分别证明机制收益，再判断是否改善同一 SDK 的 connect→all→close。

## 五个具体结论

### 1. sqld 的内部执行直接消费 typed program，不重复绕 HTTP

官方 `libsql-server/src/hrana/http/mod.rs` 的 cursor 路径先解码 HTTP 请求，然后取得 stream、构造 program，直接传入 `CursorHandle.open(db, ctx, pgm,...)`。cursor 最终调用 `db.execute_program`；program VM 直接 prepare/bind/iterate 并写入 result builder。[S1][S2][S3]

**迁移推断**：peri 可以让 REST 和 Hrana 都调用同一 typed Gateway 核心，将 HTTP wrapper 留在最外层，减少内部 Marshal→httptest request→Recorder→Decode。保留同一鉴权、额度、撤销、fencing、operation/audit 和错误分类函数，不能另外复制一条“快速但弱校验”路径，也不能无条件缓存认证或会话状态。

**最小验证**：旧内部 HTTP 路径与新 typed 入口喂同一有效请求及权限失败、额度失败、关闭会话、过期 incarnation、取消、native Unknown；比较业务结果、HTTP/SDK 错误、operation/audit 顺序、执行次数和持久化屏障数。机制指标为 alloc/op、CPU/op、JSON编解码次数；业务仍计完整 connect/all/close。这个改动主要节省 CPU/分配，不能凭源码就承诺消除20–25ms差额。

### 2. readonly 不是无副作用，也不是能跳过控制路径的充分条件

SQLite 官方 `sqlite3_stmt_readonly` 只表示该语句不会直接改变数据库文件；事务控制、ATTACH/DETACH可返回true，自定义函数或虚表仍可能有间接副作用，EXPLAIN也有专门判定规则。[S4]

更关键的是部分 PRAGMA 在 prepare 阶段就会改变设置，未 step 并不自动证明没有副作用；EXPLAIN不屏蔽这些prepare副作用。[S5]

**迁移推断**：guarded cursor-first 必须沿用固定 Turso 引擎的 native 分类和既有允许/禁止规则，而不是 SQL 前缀或单一 readonly flag。进入快速路径的写/控制候选必须按既有协议处理；NotExecuted 必须描述实际保证，不能将“没step”偷换成“连接状态没变”。SQLite 文档给的是反例方向，不能未经验证就声称冻结 Turso 版本具有完全相同行为。

**必要反例**：BEGIN/COMMIT/ROLLBACK/SAVEPOINT、PRAGMA 设置与读取、ATTACH、EXPLAIN UPDATE、带注释/CTE的写入、INSERT RETURNING、无效参数、准备错误、带副作用扩展（如未支持则验证拒绝不回归）。需要证明SQL最多执行一次，Unknown不得重试。

### 3. 一次 prepare 与紧邻执行有价值，但缓存跨上下文会改变语义

sqld 的 program VM 当前调用 `conn.prepare`，随后读取metadata、bind、遍历结果；源码并不是 `prepare_cached`。它保留 program auth 和 statement-kind 检查，不能把其速度归因于神秘的跨连接预编译缓存。[S1]

SQLite v2/v3 prepare 的语句在schema变化、部分参数影响计划时可能在step重新编译。[S6]

**迁移推断**：能安全复用的是同一 session/lane 中本次语句的真实 prepared instance，且要处理实际引擎的reprepare和readonly再验证。先把冗余describe/RPC移除，暂不引入跨session缓存。不同tenant/database/schema、临时表、PRAGMA、事务和epoch不能共享未经证明有效的判定。

**最小验证**：首个prepare成功后schema或连接设置改变、事务状态变化、cursor分页中断/close、结果0/1/20行、首页即done与多页分别测试；记录prepare和RPC次数下降，而不是只测空函数微基准。

### 4. 少一次结果转换比“sqld零拷贝”更符合真实源码

sqld cursor 是容量为1的mpsc channel；result builder逐行生成typed entry；HTTP CursorStream对每个entry进行serde_json序列化并作为Bytes交给hyper。[S2][S3]

**迁移推断**：可以借鉴typed entry直接到wire writer，避免同一页多次JSON树构造，但不能宣传sqld完全没有逐行调度、复制或JSON编码。peri先保留256行分页和全部审计，只减少重复对象转换，避免把分页/持久化变化混入同一候选。整数精度、BLOB、NULL、UTF-8、大小/深度限制、重复字段与未知字段处理应由原信任边界负责。

**最小验证**：0/1/20行 + 8625窄行 + 长文本，按最终结果摘要及协议事件顺序逐项比较；同时看首行、all完成、close ACK、CPU、累计分配和峰值内存。慢客户端和取消必须保持背压与有限缓冲。

### 5. 当前文档和SDK不是旧测试版本，不能照搬新协议行为

本次读取的当前 serverless `protocol.ts` 仍使用/v3/cursor、逐行JSON解析和async generator，但这是当前默认分支blob，不是1.4.1发布代码。[S7]

当前 Turso `serverless/PROTOCOL.md` 还记录了某些stream自动回收与重复store_sql ID替换规则，并明确提到predecessor差异。[S8] 不能将它直接当旧libSQL Hrana 3或原基准serverless@1.4.1的合同。

**迁移推断**：先固定实际SDK包及hash、HTTP轨迹和服务端协议；本轮不换SDK、不把持久连接复用替换新会话场景，也不因当前新规范允许某行为就改变旧close/stream生命周期。源码借鉴只用于内部实现，公开返回类型和ACK边界保持本轮冻结合同。

## 大型服务对照：往返、会话连续性与连接初始化

本轮补充 Cloudflare D1 和 Neon，采用三个官方文档页面。以下是机制参照，不是它们与 peri/sqld 在同一机器、同一 SDK、同一一致性合同下的性能对比。此补充不改变上面的 SQL readonly 边界。

### A. Cloudflare D1 batch：一次调用传入多个已知语句，仍保留执行顺序和失败语义

**官方事实**：D1 的 `batch()` 把多个语句放进一次调用，减少网络往返；语句按顺序而非并发执行。官方将批处理定义为事务：其中一个语句失败时，整个序列中止并回滚，结果按输入顺序返回。[S9]

**对 peri 的迁移推断**：可以测量一次既有业务请求内部的适配次数、worker/RPC 往返及重复 prepare，将已经确定的工作在同一个受保护执行单元内传递 typed 数据。只有当现有接口已经承诺相同顺序、事务与失败边界时，才考虑合并内部调用。这个方向与本轮 JSON 适配消重相容，但尚未证明额外 RPC 合并有收益。

**不可类比边界**：不能为了降低数字把原基准独立请求改成一个 batch，不能合并两个本来独立提交的业务事务，也不能提前返回写入/close ACK。批量请求节省公网 RTT 的收益不能直接算成当前本机固定小查询开销的收益。后续实验需分别报告完整业务 p99 和内部往返数，保持同一 workload。

### B. Cloudflare D1 Sessions：用可见版本约束选择副本，而非默认允许旧读

**官方事实**：D1 读取副本异步复制；Sessions 通过 bookmark 使后续查询读取的版本不早于已有上下文。`first-primary` 从主实例开始取得最新上下文；默认 `first-unconstrained` 可从任意可用版本开始。副本收到 bookmark 后可能等待追上该版本才回答。[S10]

**对 peri 的迁移推断**：未来若复用 worker/session 或缓存初始化结果，应把 tenant、database、incarnation/epoch 以及需要的可见版本作为有效性条件，先证明版本和所有权连续性，再尝试省 setup。应拆开“逻辑会话初始化”“物理执行器初始化”“等待可见/持久化屏障”的耗时，避免把屏障成本误当成可删的包装成本。

**不可类比边界**：D1 的 session sequential consistency 不是 peri 既有 fencing、durable audit 或全局最新读合同的替代品，bookmark 也不能替代授权。不能将 first-unconstrained 的宽松起点迁移到原本要求最新状态的读；跨地域读路由也不能解释本机 20–25 ms 差距。任何有效性不确定、撤销或 epoch 变化都必须走原校验和失效路径。

### C. Neon：物理连接复用与逻辑会话语义必须分开

**官方事实**：Neon 的 PgBouncer 将多个客户端连接复用到更少的 Postgres 连接，按用户与数据库分池；transaction 模式在事务完成后归还后端连接。官方列出持久会话状态限制，例如 session `SET` 和 SQL 级 `PREPARE`；协议级 prepared statements 是不同机制，当前文档明确支持。[S11]

**对 peri 的迁移推断**：先确认 connect→all→close 中究竟哪一段反复新建物理引擎/worker、哪一段仅建立逻辑会话。未来可以单独评估受控常驻执行器或同租户数据库连接复用，前提是事务、临时表、PRAGMA、取消、游标和 epoch 状态完全隔离，close ACK 的资源释放合同仍成立。记录物理创建次数、排队时间和复用命中率，才能区别减少 setup 与新增池排队。

**不可类比边界**：PgBouncer/Postgres 不是 Hrana/SQLite 的即插即用实现；不能跨 session 偷渡 PRAGMA、临时表或事务状态，不能通过改用持久客户端让固定新会话基准变快。Neon 的连接容量数字也不代表同等并发查询吞吐，更不证明 peri p99。故障重连机制不能成为重放 Unknown SQL 的理由。

**本轮范围裁决**：本轮只落地 JSON/内部 HTTP 适配消重；batch、bookmark 和物理连接池仅作为后续需要独立验证的研究方向，未修改这些产品行为，未发起性能负载。1.5× 硬门槛、1.1× 研发目标及鉴权、准入、durable audit、fencing、ACK、SQL 不重放保持不变。

## 官方来源与身份

下列源码通过GitHub connector直接读取，身份是**文件Git blob SHA**，不是仓库commit或镜像digest。快照保存在同目录，不能当作issue当时镜像的精确源码。

| 编号 | 来源 | 文件blob SHA |
| --- | --- | --- |
| S1 | [sqld program VM](https://github.com/tursodatabase/libsql/blob/main/libsql-server/src/connection/program.rs) | `08dd9526f3119c0351c6814b849c11eaf23f61c4` |
| S2 | [sqld HTTP handler](https://github.com/tursodatabase/libsql/blob/main/libsql-server/src/hrana/http/mod.rs) | `c336a3e1f0c9a8a02ed95b89716d31cbd1ed01f7` |
| S3 | [sqld cursor](https://github.com/tursodatabase/libsql/blob/main/libsql-server/src/hrana/cursor.rs) | `33a255216d7561370c10fb027d247716f12d3d41` |
| S7 | [当前serverless protocol](https://github.com/tursodatabase/turso/blob/main/serverless/javascript/src/protocol.ts) | `a612fbbdb34ca3df298e9e0be956f6b0378325c1` |

- S4 [SQLite readonly API](https://www.sqlite.org/c3ref/stmt_readonly.html)，web ref `turn16view0`。
- S5 [SQLite PRAGMA](https://www.sqlite.org/pragma.html)，web ref `turn16view2`，开头说明prepare副作用。
- S6 [SQLite prepare API](https://www.sqlite.org/c3ref/prepare.html)，web ref `turn16view1`，自动recompile说明。
- S8 [当前Turso SQL-over-HTTP规范](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md)，web ref `turn15search3`。
- S9 [Cloudflare D1 Database API — batch](https://developers.cloudflare.com/d1/worker-api/d1-database/)，当前官方文档，页面显示更新于 2026-06-22；本轮读取于 2026-10-29，web ref `turn20view0`，对应正文第149–153行。此页没有用于本报告的产品 build/commit 身份。
- S10 [Cloudflare D1 read replication — Sessions](https://developers.cloudflare.com/d1/best-practices/read-replication/)，当前官方文档，页面显示更新于 2026-08-10；本轮读取于 2026-10-29，web refs `turn20view1`、`turn21view2`、`turn21view3`，对应正文第187–190、204–208、217–252、386–400行。该版本说明不代表历史 D1 行为。
- S11 [Neon connection pooling](https://neon.com/docs/connect/connection-pooling)，本轮读取于 2026-10-29 的当前官方文档，web refs `turn20view2`、`turn21view0`、`turn21view1`，对应正文第392–404、508–519、567–571行。未获取运行服务 build/commit，不将文档当前设置归因于历史版本或 sqld。
- 当前Turso的compat/change-log搜索仅用于发现版本差异，不将其功能直接归因于固定 `24a3a460...` 引擎。

## 本轮应拒绝的论证

- “readonly=true，所以不用intent/audit/事务状态保护”。
- “没调用step，所以prepare一定无副作用”。
- “sqld有缓存/零拷贝，所以快四倍”，没有对应源码或本地归因。
- 把libsql-client-ts的单HTTP pipeline行为归给@tursodatabase/serverless@1.4.1。
- 用吞吐微基准、少一次JSON或static RPC计数直接宣称完整业务p99或sqld倍率已经达标。
