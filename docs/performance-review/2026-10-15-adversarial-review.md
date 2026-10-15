**peri-loom 性能对抗审查与优化建议**

审查日期：2026-10-15
适用对象：架构负责人、Go 控制面维护者、Rust 运行时维护者、性能验证负责人
审查基线：[KonghaYao/peri-loom](https://github.com/KonghaYao/peri-loom) 的 `main@3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0`
引擎基线：Turso 子模块 `d0c23d6a16f457d382059a84f84f9c4463c85a84`

**一、交接结论与使用方式**

当前最需要解决的性能设计问题，是热路径的成本缺少稳定上界：历史提交、完整平台目录、WAL 回放镜像、长期回执和失联会话，都可能把与当前请求无关的工作带入请求路径。与此同时，Worker 全局锁、控制面串行轮询及共享持久化，把局部慢操作扩散到其他数据库。

这些问题直接影响三个目标：

| 目标 | 当前主要阻力 | 需要建立的约束 |
|---|---|---|
| Serverless | 空闲卸载与首请求唤醒尚未闭环；失联原生会话长期占额；恢复依赖历史规模 | 成本随活跃集合增长；冷库不占 native 实例；唤醒与清理有明确状态机 |
| 高并发 | 全平台状态持久化、跨库全局锁、namespace root CAS、控制与数据争用容量 | 无关数据库独立推进；共享持久化可批处理；排队有界且公平 |
| 低资源 | Pager、TEMP、中间值、WAL、结果聚合不受统一预算约束 | 分配前准入；进程总预算；有界重放、编码和后台维护 |

本报告由六路并行源码审查、交叉复核和 JSON 字节模型组成。当前环境未运行 Go/Rust 原生压测，也未运行 Docker 故障注入；没有修改源代码。调用次数、复杂度和容量模型不等同于实测吞吐、实际 RSS 或事故复现。所有验收数字均为建议目标，须在冻结环境后验证。

代码链接固定到上述提交，接手后应先比较目标分支差异。报告中的问题编号可直接用于拆分 issue；本报告本身不代表已创建或发送任何 issue。

**分级口径**

- **P0**：足以证明灾难性数据损坏、严重隔离失效或全面不可恢复故障。本轮性能审查没有新增可确认 P0。
- **P1**：在明确配置或负载下，会威胁可用性、资源边界或事务推进，应作为相应容量承诺前的阻断项；不表示已在线复现。
- **P2**：能力缺口、扩展性限制或维护效率问题，应纳入设计与验收。
- 分级表达影响程度，修复先后还需考虑依赖关系。模型推导单独标注，不因数值大就升级为已发生事故。

| 编号 | 结论 | 分级与条件 | 主要责任 |
|---|---|---|---|
| F01 | 完整历史元数据校验在特定负载下呈二次增长 | P1，累计历史保留 | Rust Authority |
| F02 | root、回执与命令历史缺少在线规模上界 | P1，长期运行 | Rust Authority；Go 提供续租策略 |
| F03 | 单次成功 SQL RPC 反复克隆并持久化整个平台状态 | P1，生产状态存储与文件审计 | Go Gateway |
| F04 | 全 Worker 刷新锁阻塞无关数据库 | P1，冷启动或慢清理 | Rust Worker |
| F05 | 查询与进程内存预算未覆盖主要分配 | P1，低资源或大中间结果 | Rust 引擎与 Worker |
| F06 | WAL 全量驻留、回放、扫描和复制 | P1，当前 WAL 镜像增长 | Rust 存储与恢复 |
| F07 | 失联原生会话长期占额；自动停启未闭环 | 会话 P1；自动停启能力 P2 | Go 生命周期；Rust 清理执行 |
| F08 | 控制热路径依赖全目录与历史 | P2，目录及恢复规模扩大 | Go Controller；Rust Authority |
| F09 | 慢 Worker 与数据请求影响健康租约续期 | P1，局部故障或拥塞 | Go Controller；Rust Authority |
| F10 | 拒绝流量仍消耗同步审计容量 | P1，请求实际到达公开 Gateway | Go Gateway |
| F11 | 可选协议与调度配置存在聚合边界缺口 | P1，仅开启对应配置时 | Go Hrana；Rust admission |
| F12 | checkpoint 放大与 namespace GC 长独占 | P2，大量离散脏页或历史对象 | Rust 存储与维护 |

**二、问题证据、对抗场景与修改方向**

**F01  历史元数据校验随累计索引长度增长**

**场景。** 一个分支持续更新同一小表，每笔提交产生一次独立 WAL append；不 compact、reset、truncate 或裁剪历史。数据库有效数据很小，仍保留 n 份逐渐变大的累计 index。

**证据。** [authority.rs:189](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority.rs#L189) 在 snapshot 读取 root 后执行完整校验；[semantic.rs:174](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/semantic.rs#L174) 遍历分支历史，[semantic.rs:278](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/semantic.rs#L278) 解析 WAL index 并遍历 entries；[semantic.rs:248](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/semantic.rs#L248) 将不同物理 index 的原始字节保留在 Walker 中。[wal.rs:305](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/wal.rs#L305) 生成新的完整累计 index，[branch.rs:285](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/branch.rs#L285) 增加恢复点。

一般成本取决于所有保留历史点所引用 index 的长度之和。在上述模型中，该和为 1+2+…+n，故单次完整校验可达 Θ(n²)；逐笔提交累计校验工作可达 Θ(n³)。相同物理 index 的读取可以去重，这不消除不断新增的累计 index。

**字段形状模型，非 RSS 测量：**

| 提交数 | 最新 index 字节 | 历史 index 原始 JSON 总字节 | 换算 |
|---:|---:|---:|---:|
| 1,000 | 220,748 | 110,372,945 | 105.26 MiB |
| 2,000 | 445,748 | 443,733,445 | 423.18 MiB |
| 10,000 | 2,245,753 | 11,210,617,450 | 10.44 GiB |

模型使用 5 位 PID、19 位时间戳和递增序号构成 append ID，hash/digest 为 64 个 ASCII 字节。不含 root、WAL chunks、初始额外 markerless append、解码结构和临时副本；不能据此断言 2,000 次提交必然 OOM。请求级元数据缓存的上限不约束 Walker 自己保留的全部字节。

**修改方向。** 热路径验证变化部分、新依赖及其可信身份；完整图审计放到明确的启动或后台任务。历史改为分页、分段或持久树，root 保存有限指针。对已验证不可变内容建立有界缓存，避免仅通过扩大缓存推迟问题。

**验收。** 固定有效数据，累计 100/1k/10k/100k 提交，记录每次 metadata GET 数与字节、校验 CPU、分配量及 Authority 峰值内存。引入固定在线保留窗口后，1k→100k 历史规模下，热提交元数据读取量与 p95 延迟增长建议不超过 20%，单请求临时元数据内存有明确上限。

**F02  root 与回执缺少长期在线上界**

**场景。** 一个持续 active 但没有 SQL 的数据库反复续租；多个分支共享 namespace 并发提交；执行 compact 后继续长期运行。

**证据。** [authority.rs:222](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority.rs#L222) 对整个 root 读取、克隆、验证、编码和 CAS；root 包含分支、历史、回执、pins、上传意图及 tombstones。[wal.rs:423](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/wal.rs#L423) 的 compact/truncate 会改变 entries 和 baseline，但不清除 receipts/receipt_digests。[lease_commands.rs:137](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/lease_commands.rs#L137) 追加续租回执，[ownership_catalog.rs:54](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/ownership_catalog.rs#L54) 禁止直接删除既有命令回执。

按默认 30 秒租约、约半 TTL 续租且循环及时运行推导，一个 active 分支约产生 5,760 条续租回执/天，30 天约 172,800 条。实际频率受循环耗时与故障影响。

普通 WAL append 发布链包含 root 预约 CAS、chunk PUT、完整 index PUT、root 发布 CAS，形成四次顺序对象写。共享 namespace 的分支同时竞争同一 root；这构成扩展性风险，但未测量实际 CAS 冲突率和吞吐上限。

**修改方向。** 将当前 ownership、分支头、历史提交、回执和 GC 元数据拆为职责明确的记录；建立完成水位、命令序号或重放边界后分段归档回执。最终 fencing 仍由存储权威执行，不能直接用普通 TTL 删除去重和未知结果证据。

**验收。** 模拟 30 天零 SQL 续租，以及固定保留窗口下 100k 次提交；在线 root 与单次续租读写字节应进入稳定区间。比较 1/8/32 个分支共享 namespace 与独立 namespace 时的 ACK/s、CAS 重试分布和 p99。

**F03  Gateway 请求反复克隆和持久化整个平台状态**

**场景。** 大量租户执行简单 SELECT 或 cursor fetch；SQL 工作量固定，平台数据库、密钥和 operation records 不断增加。

**证据。** [state.go:71](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/state.go#L71) 的 Snapshot 在全局锁内 JSON 序列化和反序列化整个 State；Update 又执行全量克隆、验证与持久化。[auth.go:55](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/auth.go#L55) 鉴权先取完整状态，再扫描 APIKeys。

生产 stateStore、fileAudit、已打开 session、正常完成 operation 登记与结算且无 audit rotation 时，一次原生 Gateway SQL RPC 的同步调用为：

| 路径 | Sync 调用数 | 依据 |
|---|---:|---|
| registerOperation → State.Update | 7 | [gateway_sessions.go:199](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_sessions.go#L199) |
| settleOperation → State.Update | 7 | [gateway_http.go:109](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_http.go#L109) |
| Audit.Record | 8 | [audit.go:306](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/audit.go#L306) |
| 合计 | **22** | 响应写出前发生 |

每次状态提交的 7 次来自 intent、floor、payload 的文件与目录同步，以及删除 intent 后的目录同步，见 [state_disk.go:142](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/state_disk.go#L142) 和 [state_disk.go:264](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/state_disk.go#L264)。若 State 序列化大小为 S，两次提交写入的逻辑文件字节约为 6S，尚未计审计及其他 IO。

22 是 Go 文件与目录 Sync 调用数，不是 22 次独立物理刷盘或固定时延；不含 Rust 和对象存储。一个业务操作或 Hrana 请求可以对应多个 Gateway RPC。operation retention 压缩发生在原 Update 内，不必额外增加一次状态提交；audit rotation 则会增加成本。

**修改方向。** operation durable intent/结果日志与组织、项目、密钥、数据库注册表分离；使用追加日志、索引和受控 checkpoint。只读状态采用不可变快照及窄查询，密钥建立索引。group commit 必须让每个请求等待覆盖自己的持久化屏障，保留撤销即时可见、Unknown 隔离及反回滚语义。

**验收。** 固定查询与活跃数，扩大非热点目录；记录 alloc/op、JSON 编解码量、逻辑写字节、Sync/s、状态锁和审计锁等待。证明热请求不再重写完整平台注册表，并验证密钥撤销后的首个请求立即失败。

**F04  全 Worker 刷新锁阻塞无关数据库**

**场景。** A 库冷启动、长 WAL 恢复或慢关闭；同 Worker 上已预热的 B、C 库持续查询和 COMMIT。

**证据。** [sessions.rs:26](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/worker_rpc/sessions.rs#L26) 只有一把 image_refresh mutex。open 在第 72 行持锁跨 runtime.open_session，begin 在第 124 行每次 SQL 入场取锁，close 在第 172 行持锁执行清理。[server.rs:299](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/worker_rpc/server.rs#L299) 的 sessions.begin 先于 lane 分配，锁等待不受后续 SQL 执行预算完整覆盖。

因此，A 的慢打开或慢清理可以阻塞无关数据库的新请求入场。不能将此描述为所有查询的执行过程始终持有同一锁；问题发生在共用入场与生命周期协调路径。

**修改方向。** refresh/rebind 按 DatabaseId 协调；全局注册表仅保留短临界区；远端 IO 和 native close 避免占用全 Worker 锁。deadline 自请求入口覆盖准入和锁等待；保留同库导入、代际切换与 session rebinding 的原子性。

**验收。** 注入 A 的打开延迟 1/5/20 秒，同时对 B、C 保持稳定负载；B、C 的入场等待不能随 A 的冷启动时长增长。记录锁等待 p99、队列长度、健康库最大进展间隔。

**F05  内存限制未覆盖查询主要分配**

**场景。** 1 CPU/512 MiB 或 2 CPU/2 GiB，叠加多 session、排序、扫描、大中间值和恢复。大分配用例仅在隔离测试容器运行。

**证据。** [types.rs:70](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/runtime/types.rs#L70) 默认允许 32 DB、128 session、每 DB 16 session、每 DB 8 MiB segment RAM cache。每 session 创建独立 Pager；引擎默认 2,000 页，且为软上限。按 4 KiB 页且缓存充分填充，128 个用户 Pager 的页载荷为 1,000 MiB，32 个 segment cache 为 256 MiB，合计 1,256 MiB。该数字是容量推导，不是创建连接立即分配，也不是 RSS 实测，尚未计内部连接、WAL、TEMP 和 JSON。

[database.rs:161](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/database.rs#L161) 使用 TempStore::Memory，排序临时文件可落到 MemoryIO。[execution.rs:687](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/session/execution.rs#L687) 在行生成后检查结果字节，不能约束执行中的中间对象。引擎 [Turso core/vdbe/value.rs:342](https://github.com/KonghaYao/turso/blob/d0c23d6a16f457d382059a84f84f9c4463c85a84/core/vdbe/value.rs#L342) 中的 randomblob 可先尝试分配 1,000,000,000 字节；例如 length(randomblob(...)) 最终只返回整数。VM step 或时间预算也不能保证及时打断一个标量函数内部的分配与执行。

**修改方向。** 建立 Worker→DB→query 总字节预算；大 scalar/string/blob 分配前检查，Pager、TEMP、排序、WAL、结果均计费。结果直接有界编码，避免完整 Value 集合、JSON DOM、编码缓冲同时存活。磁盘 TEMP 若要启用，应独立验证清理和非提交生命周期，不直接撤销现有约束。

**验收。** 持续采集 cgroup memory.current/peak/events、RSS、anon/file、CPU throttling、PSI、FD 和线程数。在冻结负载下峰值建议不超过容器预算 90%，无 OOM；超预算请求有明确拒绝或背压。容器上限是最后防线，不能替代查询预算。

**F06  WAL 回放与增量验证仍依赖完整镜像**

**场景。** 固定 1–10 MiB 有效数据，持续更新同一行；将当前 WAL 累积到 16/64/256 MiB，比较热提交与冷恢复。

**证据。** [io.rs:46](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/io.rs#L46) 保存完整 WAL bytes 和 pending batch；第 80 行起冷打开读取 WAL 再重放。[wal.rs:397](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/wal.rs#L397) 串行读取 chunks 并先累计完整 Vec<WalRecord>。[session.rs:256](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/session.rs#L256) 禁用 native 自动 WAL actions；owned-worker 默认路径未发现按 WAL 字节或年龄调度的后台压缩。

[io.rs:293](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/io.rs#L293) 同步时扫描 committed_position；[recovery.rs:34](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/recovery.rs#L34) 从头计算校验链，增量验证仍在第 132 行复制完整 replay image。因此每笔小提交存在随当前 WAL 镜像 B 增长的 O(B) 扫描或复制。compact/reset 可降低 B，不能表述为 WAL 永远不可缩减。

10 万个 4 KiB frame 连同每帧 24 字节头，约为 393 MiB，尚未计回放结构、副本与索引；它与有效数据库大小并不等价。

**修改方向。** 有界流式重放；按 WAL 大小、记录数和年龄触发经 fencing 的 checkpoint/compact；维护可验证的 generation、frame 高水位和滚动校验，避免逐笔全量复制。覆盖写、rollback、header reset 必须正确使相关增量证明失效。

**验收。** 对同一小事务比较三档 WAL 的校验 CPU、copied bytes、commit p99；冷启额外内存遵守明确缓冲预算。分别记录 lease、key、manifest、下载、回放、首 SQL 时间；不能把 checkpoint 等同于远端历史已经完成压缩。

**F07  原生会话回收与 Serverless 生命周期不完整**

**场景。** 函数 open 后超时退出、open 响应丢失、客户端崩溃或 key 被撤销；大量数据库低频访问。

**证据。** [gateway_sessions.go:74](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_sessions.go#L74) 将原生 session 保留在 map 与配额中；[gateway.go:25](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway.go#L25) 没有统一 last-used/expiry。Worker 同样依赖显式关闭等生命周期路径回收。Hrana 有 idle timer，不应将原生问题扩大为所有协议均无回收。

[runtime.rs:295](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/runtime.rs#L295) 要求 sessions==0 才能空闲卸载。最后一个 session 关闭仅记录 idle_since；evict_idle 的现有调用位于开发适配器，未接入 owned-worker 自动调度。[management_databases.go:57](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/management_databases.go#L57) 创建数据库后立即 Place；[gateway_routing.go:29](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_routing.go#L29) 对 suspended/无 placement 拒绝，不做首请求 Resume。

这是有限配额被长期占用，不是已证明的无限内存泄漏。默认项目 16 session、Worker 128 session；显式关闭、受控清理或 shutdown 可以释放。Unknown 为安全保留，应与普通失联的已知空闲 session 区分。

**修改方向。** 将冷态目录、活跃 placement 和已加载实例分离；实现同库唤醒合并、idle-in-transaction 策略与 session lease。后台清理取得 native joined cleanup ACK 后再归还配额；不能因超时直接把未知结果视为已取消。为管理员提供受控关闭已知空闲会话的能力。

**验收。** 10,000 冷库+32 热库；设 idle=60 秒，满足安全关闭条件后建议 5 秒内释放实例；冷库不续租。同库 32 个并发首请求只加载一次。循环杀客户端与撤销 key，验证已知空闲占额回落，并单独验证在途写入不被错误回收。

**F08  控制热路径依赖全目录，恢复依赖历史**

**场景。** 固定 32 个活跃库，将冷库和历史规模从 100 扩大到 10,000；再执行控制面和 Worker 重启。

**证据。** [store.go:123](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/store.go#L123) Snapshot 在全局 mutex 下 JSON 深拷贝完整 Metadata；[reconcile_loop.go:19](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/reconcile_loop.go#L19) 每轮扫描、排序 placements，并对每个 active placement 再取全量快照。总目录 N、活跃数 A 时，该部分已有 O(A×N) 克隆工作。

[reconcile_worker.go:69](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/reconcile_worker.go#L69) 每次 Worker 心跳更新完整 metadata；普通持久化提交同样约 7 次文件/目录 Sync。[gateway_routing.go:36](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_routing.go#L36) SQL 路由使用相同 Snapshot。冷库目录因此影响活跃库路由和续租。

首次触达 namespace 时，[migration.rs:25](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/migration.rs#L25) 为 epoch floor 读取当前分支、历史与 namespace snapshots 的 WAL。B 个分支、每分支 H 个历史点、S 个均有 WAL 的快照时，该循环可达约 B×(1+H+S) 次读取；这是条件计数，非实测启动耗时。

**修改方向。** 将可重建 heartbeat/load 与必须持久化的 epoch/intent 分离；提供单库、单 Worker 索引读。活跃控制集合与历史 context 分离。维护可直接验证的单调 epoch floor，避免常规启动从完整历史重算；恢复队列有预算并优先热库。

**验收。** 100→10,000 冷库、固定 32 热库时，控制 CPU 与热 SQL 路由 p99 增幅建议不超过 20%；纯心跳不产生强持久化提交。固定当前数据、扩大历史后，首次打开的历史 GET 不再线性增长。

**F09  控制续租与故障隔离不足**

**场景。** Worker A 正常，B 的控制接口挂起 60 秒；另测慢对象存储使 Authority 数据请求占满容量。

**证据。** [reconcile_loop.go:10](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/reconcile_loop.go#L10) 在全局 operations 门内先处理串行心跳，再处理 placements；[reconcile_worker.go:52](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/reconcile_worker.go#L52) 按 ID 排序。[transport.go:67](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/transport.go#L67) 响应头超时 10 秒，而 [reconcile_loop.go:69](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/controller/reconcile_loop.go#L69) 健康心跳窗口为 5 秒。A 先成功、B 等待 10 秒后，A 的样本已陈旧；若反复发生，健康 A 也可能错过续租。drain 等待也争用全局生命周期执行队列。

[server.rs:112](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority_rpc/server.rs#L112) 对数据与控制共用 128 个 inflight 槽，没有独立续租预留。namespace gate 等待及同步后端 IO 可占用请求线程，持续饱和时控制请求可能被拒绝。线程总数有上限，不是无限创建线程。

**修改方向。** 独立并发心跳；按最早到期时间调度续租；按库串行生命周期操作。Authority 在可信身份校验后为续租、控制与清理保留容量；namespace 排队在占 OS 线程前有界化。

**验收。** B 挂起 60 秒时，A 无 lease expiry、无由该故障引起的 session_moved，最大续租间隔建议小于 10 秒。后台 drain 不阻碍其他库续租。对象存储拥塞下仍能取得控制进展，且不能以放宽 fencing 换可用性。

**F10  拒绝流量进入同步持久化审计**

**场景。** 健康租户低频查询，同时产生 missing-auth、invalid-key 和配额触发 429 的噪声；请求实际穿过外层代理到达 Gateway。

**证据。** [gateway_http.go:102](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/gateway_http.go#L102) 在认证前注册 defer，拒绝响应仍进入 Audit.Record；[audit.go:257](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/audit.go#L257) 使用全局审计锁，常规拒绝事件约 8 次 Sync。missing-auth 401 增加审计成本；invalid-key 与鉴权后 429 才另外经过相应状态快照等工作，不能混为同一完整鉴权成本。

**修改方向。** 昂贵鉴权与审计前增加全局有界入口准入和来源级限速；业务审计与拒绝事件明确各自预算。若策略要求每次拒绝均持久化，入口需按持续审计容量背压；若允许聚合，需显式定义聚合语义，不能静默丢失必须留存的事件。

**验收。** 分别注入三类噪声，记录 victim p99、ACK/offer、最大无进展间隔、goroutine、审计队列与 Sync/s。建议 victim p99≤独立运行基线的 2 倍，ACK/offer≥95%；固定 victim 负载与噪声速率，不能靠降低 victim 请求量取得通过。

**F11  可选 Hrana 与 writer admission 的聚合边界**

**Hrana，仅在 PERI_HRANA_EXPERIMENTAL=1 时适用。** [hrana_http.go:129](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/platform/hrana_http.go#L129) 允许最多 64 个 pipeline request，第 185 行允许每 batch 64 steps，第 296 行起累计结果，最后整体编码。内部请求逐个归还 admission permit，因此单请求配额不覆盖外层保留结果。每条 1 MiB 的 64-step batch 可保留约 64 MiB 原始结果，尚未计对象和编码；这不是 RSS 测量。4096 是协议结构组合上限，不能由此宣称实际必然产生 4 GiB 或必然 OOM。

外层 socket WriteTimeout 不等价于聚合过程的内存限制或总执行 deadline。修改应加入 pipeline 总 steps、总结果字节、总时长和租户输出预算，并支持分页与背压。超限时保留已经执行、部分执行和未知结果语义，不自动重放写入。

**writer admission，显式开启时适用。** [admission.rs:55](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/worker_rpc/admission.rs#L55) 的默认启用配置为 Control 16、General 110、Owner 1、Cleanup 1。[server.rs:322](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/worker_rpc/server.rs#L322) 将相应 v2 owner 调用放入全 Worker 唯一 Owner 槽，不能借 General。两个独立数据库的 owner SQL/COMMIT 可能互相拒绝，即使普通容量空闲。

修改应区分 owner 普通工作与事务结束/清理，建立受总预算约束的跨库公平推进；保留同库单 writer。默认关闭路径也需单独验证普通槽拥塞时事务结束能否及时推进。

**验收。** Hrana 从 1/8/32/64 steps 逐步增加，监测峰值内存与取消回收；writer profile 开/关分别做跨库事务测试。不能把 profile 开启的容量结果直接用于默认部署。

**F12  checkpoint 与 GC 需要受控维护预算**

**checkpoint 场景。** 每个 128 KiB segment 只改一个 4 KiB 页。[manifest.rs:5](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/manifest.rs#L5) 定义固定 segment；[storage.rs:175](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/storage.rs#L175) 修改整段 staging，[storage.rs:250](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/storage.rs#L250) 顺序上传 dirty segments、manifest 和 HEAD。该稀疏模式的数据上传放大为 32 倍；连续合并写入不一定如此。

若 D 个 dirty segments 内容互异且均为新内容，manifest 也为新内容，无重试冲突，当前发布链的基础计数约为 3(D+2) 次对象 PUT，其中 2(D+2) 次为 root CAS。D=1,024 时，4 MiB 页面变化对应 128 MiB segment 上传和约 3,078 次 PUT。已有内容可去重，实际 PUT 会减少；GET、校验和重试会增加总工作。不能将此计数直接当作实测时延。

**GC 场景。** 同一长驻数据库反复写、compact、retain、GC；历史 tombstones 持续增加。[ownership_maintenance.rs:22](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/ownership_maintenance.rs#L22) GC 使用 namespace 独占屏障；[gc.rs:183](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/gc.rs#L183) 扫描历史 tombstones，具体处理受安全窗口约束。尚未完成删除的对象逐个确认；已完成项通常检查物理对象是否重现，不能写成所有 tombstone 每轮都 DELETE+CAS。[branch.rs:183](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/src/authority/branch.rs#L183) 的长驻 store pin 集合可增长，最终 store 释放后正常 pins 可释放；不能表述为所有对象永久不可回收。

**修改方向。** checkpoint 批量预约、有界并行上传、批量注册、最终统一 fenced publish；dirty segment 首次 COW 后减少整段重复复制。GC 采用固定对象数、字节和时间预算，缩短独占窗口；基于明确无旧 handle 的证明推进 pin generation，并归档已完成删除身份。

**验收。** 随机修改 32/256/1,024 segments，与相同字节的连续写比较；记录上传放大、root CAS 和前台 p99。GC 建议独占窗口 p99≤100 ms，固定保留策略下 pins、在线 root 和空闲 GC 工作量进入稳定区间。活跃 reader、恢复点和未知上传意图不得被错误回收。

**三、统一对抗测试矩阵**

以下为建议验收合同，当前均待运行。每次测试同时冻结 commit、配置开关、quota、容器限制、对象存储 RTT/吞吐、缓存状态和数据历史规模。

| 编号 | 场景与规模 | 关键输出 | 建议通过条件 |
|---|---|---|---|
| T01 | 10k 冷库+32 热库；idle=60s | loaded DB、续租 RPC、目录规模、控制 CPU | 冷库零续租；满足清理条件后 5s 内释放实例 |
| T02 | 同一冷库 32 并发首请求；另测不同冷库 | load 次数、排队时间、首 SQL 分段时延 | 同库仅一次加载；排队有界 |
| T03 | 同库与跨库 C=1/8/32/128 | ACK/s、p50/p95/p99、quota 429、运行时拒绝、队列 | 所有已准入独立库持续推进；排队不无限增长 |
| T04 | 1 CPU/512MiB；2 CPU/2GiB；大值/排序/扫描/恢复 | cgroup 峰值、OOM、PSI、CPU throttle | 无 OOM；峰值建议≤预算90%；分配前拒绝超额 |
| T05 | 小表 100/1k/10k/100k 提交，固定在线保留窗口 | metadata GET/bytes、校验 CPU、alloc/op | 1k→100k 热路径关键量增幅建议≤20% |
| T06 | 16/64/256MiB 当前 WAL，三种缓存状态 | 下载、重放、首 SQL、峰值额外内存 | 重放有固定缓冲预算；增量提交不全量复制历史 |
| T07 | 2/8/32 租户；10%稳定 victim+90%噪声 | victim ACK/offer、p99、最大无进展间隔 | ACK/offer≥95%；p99≤独立基线2倍；间隔≤5s |
| T08 | 存储额外延迟20/50/200ms；暂停 Worker B 60s | A 的续租间隔、lease expiry、Unknown | A 无租约过期；续租间隔<10s；Unknown 全部可归类 |
| T09 | open 后杀客户端、不 close、撤销 key | session/permit/loaded DB、writer 状态 | 已知空闲会话在 TTL+5s 内完成清理并归还额度 |
| T10 | Hrana 1/8/32/64 steps，每条约1MiB | pipeline 总结果、峰值内存、取消回收 | 聚合预算生效；部分执行结果不被误报为未执行 |
| T11 | 32/256/1,024 稀疏 dirty segments+10k GC 对象 | 上传放大、CAS、独占窗口、前台 p99 | checkpoint 批次化；GC 独占 p99≤100ms |
| T12 | 8 项目、32 DB，24h，50%已验证安全吞吐 | 内存/FD漂移、p99、回执、root、恢复校验 | 无资源单调泄漏；稳定窗口末尾p99≤首窗口1.2倍 |

三种缓存状态应分开：已加载且缓存热、进程内存冷但本地缓存热、全新本地缓存。冷启动测试每档建议至少 100 次，不能以一次打开代表分位数。

T03 的 C=128 需要足够租户与适当声明的 quota。默认单项目并发与 session 配额会先拒绝请求，不能把正常配额 429 误判为 Worker 性能退化，也不能通过隐藏配置变更声称默认支持 128 并发。

T12 的内存漂移应在清理完成、相同负载与缓存稳态窗口比较；可先以额外 RSS≤64MiB、FD 增长≤12 为告警线，再根据明确的有界缓存行为校准。不能将缓存正常填充自动归类为泄漏。

**共同硬门槛**

- 已确认成功的数据不得丢失；所有测试运行独立数据校验器。
- 旧 term/epoch 不得恢复写权限，存储仍为最终 fencing 权威。
- 不确定结果不得被自动认作未执行，也不得自动重放 SQL。
- 内存、排队、恢复缓冲和维护批次均有可观察上限。
- 达到停止阈值即中止压力提升并保留现场，不为追求 100k 提交或最大并发把共享环境压垮。
- 延迟从请求到达入口计时，包含准入和排队；同时记录 offered、accepted、ACK、rejected、timeout 和 Unknown，不能只统计成功请求掩盖饥饿或拒绝。
- 固定到达率测试与最大吞吐测试分开，避免闭环客户端自动降压掩盖尾延迟。

**四、责任边界与修复依赖**

保留 Go 控制面与 Rust 正确性运行时的长期分工：

| 领域 | Go 的责任 | Rust 的责任 |
|---|---|---|
| 生命周期 | 全局停启策略、placement、唤醒合并、配额与管理员入口 | native open/close、清理完成确认、代际切换 |
| 租约 | 发行/续租调度、健康判定、迁移协调 | ownership 状态、lease/epoch 执行、最终 fencing |
| 性能预算 | 租户入口预算、请求/协议聚合预算、全局策略 | 查询实际分配、WAL、Pager、存储与维护预算 |
| 持久化 | 平台注册表、operation 协议状态、业务审计 | 数据提交、恢复、历史索引、pins、GC 正确性 |

建议按以下依赖安排工作，而非同时扩大所有并发参数：

1. **建立可测量的资源与复杂度上界。** 先处理 F01/F02/F05/F06，给元数据、回执、WAL 和内存设明确边界。否则并发提升会放大历史与内存问题。
2. **移除跨租户共享慢路径。** F03/F04/F09/F10 可按 Go/Rust 边界并行推进，分别验证共享持久化、入场锁、续租与审计隔离。
3. **补齐按活跃集合付费的生命周期。** F07/F08 验证冷库密度、会话回收、自动停启、重启恢复与控制成本。
4. **统一可选配置和后台维护的验收。** F11/F12 在功能开启条件下单独测试，再纳入默认部署能力说明。

每项交接至少提交：对应问题编号、冻结配置、源码变更说明、前后同环境数据、正确性验证结果、未覆盖场景。单独的“测试通过”或平均耗时下降不足以关闭问题。

本轮不要求引入行级 MVCC，不改变现有 WAL 快照与单 writer/多 reader 基线。优化不能通过移除 durable intent、放松 fencing、超时删除未知记录、强制过期 pins，或自动重放事务实现。

**五、现有证据与尚缺验证**

[CAPACITY.md](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/prototypes/v1/stress/CAPACITY.md#L1) 描述的是容量工具和验收合同，包含 performance_pass=false、G02=NOT PASS；当前矩阵以 C=1/2/4/8 为主，不能代表 32/128 并发、10k 冷库、低资源或长历史已经验证。这也不反向证明所有当前负载必然不合格。

历史 [09_experiment_report.md](https://github.com/KonghaYao/peri-loom/blob/3eb7e5d68941fe7bb6c6bac6ff114c763f03cab0/docs/experiment-commercial-db/09_experiment_report.md#L9) 中的少量串行写与缓存读来自不同实验条件，不经过当前完整 Gateway→Worker→Authority 并发路径，不能作为本报告的当前版本性能基线。

优先补齐的可观察量为：metadata GET/PUT 与字节、root 大小、回执/历史数量、CAS 重试、每 RPC Sync 与逻辑写字节、全局/每库锁等待、准入队列、续租剩余裕量、WAL 大小及复制字节、cgroup 峰值内存、清理完成时间、租户最大无进展间隔。

**附录  历史 index 字节模型复现**

下面的 Python 脚本只构造字段形状，计算原始 JSON 字节；不启动数据库，不访问 S3，不测量内存或延迟。递推值在各报告点与完整序列化交叉校验。

```python
import json

def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode("utf-8")

index = {
    "epoch": 1, "last_lsn": 0, "replay_baseline_lsn": 0,
    "entries": [], "receipts": {}, "receipt_digests": {}, "commit": None,
}
previous_size = 0
historical_bytes = 0

for n in range(1, 10_001):
    append_id = f"12345-1792020000000000000-{n - 1}"
    entry = {"lsn": n, "hash": "a" * 64}
    index["entries"].append(entry)
    index["receipts"][append_id] = n
    index["receipt_digests"][append_id] = "b" * 64
    index["last_lsn"] = n
    index["commit"] = {
        "lsn": n,
        "position": {"salts": [1234567890, 1234567890], "frame_count": n},
    }
    if n == 1:
        size = len(encoded(index))
    else:
        size = (
            previous_size
            + len(encoded(entry)) + 1
            + len(encoded(append_id)) + 1 + len(str(n)) + 1
            + len(encoded(append_id)) + 1 + len(encoded("b" * 64)) + 1
            + 3 * (len(str(n)) - len(str(n - 1)))
        )
    previous_size = size
    historical_bytes += size
    if n in (1_000, 2_000, 10_000):
        assert len(encoded(index)) == size
        print(n, size, historical_bytes, round(historical_bytes / 2**20, 2))
```

预期输出：

```text
1000 220748 110372945 105.26
2000 445748 443733445 423.18
10000 2245753 11210617450 10691.28
```
