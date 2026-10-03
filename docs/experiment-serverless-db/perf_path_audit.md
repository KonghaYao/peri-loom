# Simple 模式查询路径审计

范围：`POST /data/v1/databases/{db_id}/query`，依据当前源码静态审计。本文的“可能耗时”表示代码路径存在等待或同步点，不是该点已测得的耗时。原压测使用管理员 JWT、HTTP 持久连接、单行主键点查和逐行 `INSERT`（[脚本](bench_simple.py) 第 20–24、63–67、81–115 行）。

## 每请求路径

| 阶段 | 精确位置与机制 | 每请求？ | 对 SELECT / INSERT 的影响 |
|---|---|---|---|
| HTTP 解析与认证 | `services/db-server/src/api/data/query.rs:53–66` 提取 JSON、`Principal`，检查 `db:write`，转换参数。`services/db-server/src/auth.rs:306–366`：压测用 JWT，每次校验签名，查用户，解析权限。 | 是 | 相同。JWT 路径有 Catalog 读。 |
| 资源授权 | `services/db-server/src/api/mod.rs:495–514`：每次 `get_database` 并核对租户。`crates/catalog/src/sqlite.rs:220–226`：SQLite 读出 JSON record 并反序列化。 | 是 | 相同。 |
| Simple 准入 | `services/db-server/src/simple/execution.rs:43–44`：先取 `gate` 读锁，再 `ensure_available`。`simple/mod.rs:37–84`：每次查 pending mutation、等待全局 `starting` 锁、再次读取 DB record；仅非 serving 状态才执行启动和状态写入。`crates/catalog/src/sqlite.rs:140–148,220–226` 为两次 SQLite 查询。 | 热库也发生前半段 | 相同；并发请求争用单个 `starting` 锁和 Catalog 单连接池。冷启动只在非 serving 状态发生。 |
| 宿主入队 | `simple/execution.rs:64–75` 调用 `LocalHost::execute`；`crates/database-host/src/lib.rs:144–169` 查 worker、创建容量为 2 的响应 channel、向数据库命令队列发送。`lib.rs:366–403` worker 已存在时复用；未存在时创建目录、同步父目录并启动专属线程。Simple 模式没有跨进程 IPC 或远程 WAL RPC。 | 入队是；建线程仅首次或重建 | 相同。每库单执行线程，队列等待随同库并发上升。 |
| 宿主取队列 | `crates/database-host/src/lib.rs:432–465`：线程 `try_recv()` 为空时 `sleep(10 ms)`，下次轮询才取新命令；`lib.rs:470–515` 同步执行命令。 | 线程空闲后可能 | 空闲后的请求会遭受最多一个轮询周期的排队等待（调度实际耗时还需实测）。同库并发被串行化。 |
| 无会话连接 | `crates/database-host/src/lib.rs:614–671`：本端点使用 stateless target，执行时每次 `adapter.connect()`，设 `Full` / `FullFsync` / retry；`crates/engine-adapter/src/engine.rs:94–102,164–172`。 | 是 | 相同。不能把连接创建开销归给 SQL 引擎点查本身。 |
| SQL 执行与响应帧 | `crates/database-host/src/lib.rs:673–765`、`crates/engine-adapter/src/engine.rs:196–225`：校验 SQL、设置取消与超时、编译/绑定/运行语句、通过有界 channel 发列/行/结束帧。发送缓冲满时 `lib.rs:912–938` 每次睡 5 ms 再试。 | 是 | INSERT 的 autocommit WAL 同步由引擎写路径承担；SELECT 不应有事务提交，但两者均进入相同的帧发送流程。小结果通常不会触发背压睡眠，需实测。 |
| 目录同步 | `crates/database-host/src/lib.rs:751–764`：**每条成功语句结束前**调用 `sync_dir(db_path.parent())`；`lib.rs:906–910` 对数据库目录调用 `File::sync_all()`，然后才发结束帧。注释原意是确保新建 WAL 的目录项落盘，但代码没有仅首次新建 WAL / 仅写操作的条件。 | 是 | **SELECT 也付目录 `fsync` 成本。**这可能解释毫秒级热读，实际耗时需单独计时。INSERT 除此之外还有 FULL 同步写路径。 |
| 响应整形 | `services/db-server/src/api/data/stream.rs:49–118`：默认非 NDJSON，等待全部帧、把结果转换为 JSON、构造 HTTP 响应。 | 是 | 小结果相同；大结果可能切 NDJSON。 |

API Token 认证路径与压测不同：`services/db-server/src/auth.rs:369–412` 每请求除查 token/用户、解析权限外还同步等待 `touch_token_last_used`；`crates/catalog/src/sqlite.rs:1056–1072` 对 Catalog 开事务、查 token、更新 JSON record、提交，Catalog 配置 `WAL + FULL` 且池最大连接数为 1（`sqlite.rs:86–100`）。因此 API Token 热查询可能多出一次元数据持久写及全局串行点；不能拿本次 JWT 压测数字验证它的影响。

## `elapsed_micros` 的边界

Simple `LocalExecutor::open_stream` 在 **`host.execute(...).await` 返回以后** 才建立 `started = Instant::now()`（`services/db-server/src/simple/execution.rs:64–75`）。收到宿主 `End` 帧时，把 `started.elapsed()` 写进 trailer（第 76–88 行），HTTP 默认 JSON 路径原样复制该值（`api/data/stream.rs:72–118`）。它包含等待帧、宿主执行及目录同步，但**不包含**认证、资源授权、`ensure_available`、命令入队等待或最终 JSON 编码 / 传输。尤其 `host.execute` 只等待命令发送进入 channel，命令的排队时间可能仍在 `elapsed_micros` 内；若 worker 在 `started` 前已执行部分工作，此字段可能漏掉该部分。不能把它称为纯引擎执行时间。

原压测脚本从发出 HTTP 请求前到读完整个 JSON 后计时（`bench_simple.py:110–116`），所以报告的 10–100 ms 是客户端端到端延迟，包括以上所有阶段和本地 HTTP 客户端调度。脚本没有保存响应中的 `elapsed_micros`，原始数据无法直接拆分耗时。

## 可验证的优先归因

1. **10 ms 级单请求：** 宿主空闲轮询睡 10 ms 是明确的代码等待点；每请求目录 `fsync` 也是明确的阻塞点。两者哪个贡献更大，需要阶段计时。现有数字不能断定 TursoDB 引擎本身慢。
2. **高并发 100 ms 级尾延迟：** 同库单执行线程、每次连接、每次目录同步，以及前置 `starting` 锁和 Catalog 单连接池，均能形成队列。哪处队列主导，尚无分段指标。
3. **写入：** 使用 `FullFsync`，autocommit 写会等待本地持久化；它叠加了与读路径共享的目录同步。Simple 宿主 `run_worker` 用原生 `UnixIO` 且 `durable_io: None`（`crates/database-host/src/lib.rs:406–420`），因此不能把本次写延迟归因于 Remote WAL quorum。

建议在不改变行为的诊断构建中，分别记录认证、授权、准入（含 `starting` 等待）、命令入队、worker dequeue、连接创建、`stream_with_params`、目录 `sync_dir`、HTTP 响应组装的单次耗时；用同一请求 ID 对齐客户端总耗时及 `elapsed_micros`。现有代码没有这些 Simple 分段埋点。
