# H3 / H4 / H5 独立源码对抗审查

日期：2026-10-17。仅阅读 `identity.json` 指定的冻结源码快照，V1 为 `c8990c406588a23a203a46033cf10103681d188c`，Turso 为 `24a3a4609683b73935888f8bc30b47727d100aa8`。没有运行构建、计时实验或修改业务源码。以下 V1 路径均相对 `prototypes/v1/`；Turso 路径相对 `prototypes/p0/turso/`。行号取自冻结快照。原报告基线为 e8beae4；本审查不假定旧测量在 c899 仍成立。

## 判断摘要

| 假设 | 源码审查能保留什么 | 最强削弱或反例 | 尚不能声称什么 |
| --- | --- | --- | --- |
| H3 | 一个真实主库 File 冷读在 Shared guard 存活期间同步等待段 GET；同 RemoteIO 的另一个 File 操作会竞争该 guard | native pager 的已加载页缓存命中不调用 File；不同 RemoteIO 无此共享；checkpoint 另外受更高层 operations 写锁保护 | 慢 GET 会阻塞该库所有 SQL；去掉 Shared 即可消除 checkpoint 前台停顿；该修改已被证明语义安全或应排第 2 |
| H4 | 命中 Authority 增量验证分支时，`WalImage` 的 `Vec<u8>` 是深复制；完整 index/receipts 成本也存在 | 冷会话、缓存失配、root CAS 重试走全量重建；无 pending WAL 的 sync 直接返回；成功 truncate+compact 可把镜像重置为空且 entries 收缩 | 每条 SQL / 每次 sync 都复制全镜像；镜像字节和累计历史永远同步增长；全量重建路径更便宜 |
| H5 | 最终不同且此前不存在的段按 128 KiB 上传；storage 的段级调用逐个完成 | 已有内容 GET 验证后跳过 PUT；同哈希跨段复用；dirty 按段索引合并；native checkpoint 已按页取最新帧并对连续页 writev；Authority 内部已有 put_group | 1024 个逻辑脏段必然有 1024 次新段 PUT；所有 checkpoint 都 32 倍；整个后端完全没有 batch |

这三个机制均不能单独建立“全局主要瓶颈”或跨工作负载的落地次序。建议将原顺序视为待测研究方向，特别是把“缩短外层锁”从普通中等改动改成需要边界证明的并发协议修改。

## H3：尝试推翻外层 guard，没有找到 File 慢读的提前释放

`src/io/files.rs:83–102` 的闭包先取得 `let state = self.shared.lock()?`，主库分支再同步调用 `storage.read_at`。虽然主库分支没有读取 `state` 字段，`state` 是含 `MutexGuard` 的 `ObservedLock`，并且有自定义 `Drop`（`src/work_metrics.rs:350–376`）。借用的非词法结束不等于自动提前析构这个 guard。没有显式 `drop(state)` 或分支重构，因此不能用“编译器可能提前放锁”反驳这条机制。

`storage.read_at` 经 `read_segment → fetch_segment → load_remote → store.get` 同步走到 GET（`src/storage.rs:178–198,208–263`）。File 的 `finish` 在闭包返回后才执行。因此真实 File 冷读中的等待受 Shared 覆盖，storage/cache 内层锁已缩短不改变这一点。

### 真正削弱范围的路径

1. **没有发现主库/WAL File 的 native 绕锁入口。** `RemoteIO::open_file` 在匹配虚拟 database / database-wal 时构造 `NativeFile::wrap_synchronous(RemoteFile { shared: self.shared.clone(), ... })`（`src/io.rs:450–472`）；`NativeFile::pread` 只是把请求转给 inner（`src/io/native_file.rs:83–85`）。native gate 是读票据，可以容纳多个调用，不能误把它当成替 Shared 自动提供并行的路径。临时文件允许走 host PlatformIO，但它们不是 authoritative database/WAL（`src/io.rs:474–480`）。
2. **真实 native 缓存命中可能完全不进入 File。** Turso `core/storage/pager.rs:3675–3705` 对已加载页缓存命中直接返回；miss 才进入 `read_page_no_cache`。所以“该库所有读被一个冷段拖住”过强。这个源码反例只保证该次 page read 不竞争 Shared；完整 SQL 是否还需要别的 File 调用，必须逐条观测，不能由此声称任意热 SQL 都能完成。
3. **Shared 属于一个 RemoteIO。** `RemoteIO::open` 每次构造新的 `Arc<Shared>`（`src/io.rs:103–113`），File 在同实例内克隆它。跨库没有这把共享锁；同数据库另一个独立快照/实例也不能因为逻辑数据库名称相同就假定共享它。反过来，同数据库两个 File handle 或两个 session 不能作为“独立 IO”对照，除非证明它们的 Shared 身份不同。
4. **常规 checkpoint 的前台排队有更上层充分原因。** `checkpoint_once` 与 `compact_wal_once` 在调用 native checkpoint 前取得 `maintenance_operation`（`src/database.rs:195–208,285–305`）；该函数取 `operations.try_write()`（`:364–375`），普通执行的 `operation_until` 取同一 RwLock 的读锁（`:331–347`，调用例 `src/session/execution.rs:34,93,137`）。因此仅缩短 Shared/storage 锁，常规 checkpoint 期间前台 SQL 仍会在 operations gate 等待。File 级实验不能把“允许另一个 File pread 推进”报告成“允许 checkpoint 时另一个 SQL 推进”。

### 为什么 storage revision 还不是删除 Shared 的证明

`src/storage.rs:14–17` 明确将“request path 经 IO gate 与 writer 串行”作为当前前提，READ_ATTEMPTS=8 的重试主要保护直接 storage API 的竞争调用。放开主库 File 外层锁后，高频 writer 可以使合法 SQL 进入此前通常不会到达的 8 次失败分支；这是错误表面变化，必须纳入验收。

revision 只跟踪 RemoteStorage 内部变化，不能直接证明一个 File 读与下列 Shared 状态在同一代：`image`、`batch`、`lsn`、`position`、`last_commit`、`wal_failed`、`publication_failed`。WAL write/reset 修改 image 与 pending batch（`src/io/files.rs:36–57`）；主库 write 在同 guard 内标 dirty 并修改 storage（`:58–61`）；`sync_wal` 根据 image 选择 commit position 并安装 lsn/清 batch（`src/io.rs:339–415`）；主库 publish 将 `position`、`state.lsn` 和 storage manifest 一起发布，再清 position/dirty/failure（`:419–437`）。允许 WAL reset 的前提还关联 dirty/failure/position、pending batch 和 storage.applied_lsn（`:320–335`）。

这些关系不自动说明每种读都必须持 Shared 全程；它们说明原锁也是顺序协议的一部分。只对主库 `pread` 放锁，可能比对 sync/publish 放锁容易证明，但仍需建立 native reader 的 snapshot、checkpoint/truncate/reset 互斥和 File completion 的可见性边界。sync 的锁外 I/O 方案更需要冻结版本、失败归属、核验代际、拒绝旧结果安装的协议。当前源码无法证明仅凭 storage.revision 就足够。

### 最小可证伪实验 H3

- 用实际 `RemoteIO::open_file` 返回的两个 File：A 读主库冷段，ObjectStore 在该段 GET 到达时发通道信号并等待释放；收到信号后启动 B 的 WAL File pread，再启动不同段的主库 File pread。用进入/完成事件判定先后，避免以调度睡眠推断持锁。若任一 B 在 A 放行前完成且确实共享同一 Shared，外层串行解释被推翻。
- 同时做两项对照：独立 RemoteIO、直接 storage.read_at。预期可推进，说明阻塞域在 Shared，不能据此宣称 native 已并行。
- 再从两个真实 session 发 SQL：B 对预热的 pager 页与 B 对另一个冷页各一条，记录 File 调用次数。热页查询能推进会直接推翻“同库所有 SQL 都阻塞”；若它仍等待，定位等待点后才能归因。
- 独立注入 checkpoint 段 PUT 的等待，观察前台 SQL 的 operations gate 与 Shared wait。只有同时改变 operations 互斥协议的实验，才有资格评估 checkpoint 并发 SQL 的改善。
- 如准备去锁，最小正确性门槛覆盖主库 read 与 write/truncate 的交错、WAL reset 前后相同 page 的读取、checkpoint position 发布与 failed publish、撤权/冷恢复，并记录 revision 重试次数与合法读失败。以上不是本次已验证结论。

## H4：clone 是深复制，但“每次提交”与“历史大小”需分开

`src/recovery.rs:355–358` 为 `#[derive(Clone)] struct WalImage { bytes: Vec<u8>, scan: ... }`。`validate_index_append:511–513` 克隆 `previous.image` 后再应用新记录，且记录 copied bytes。这里没有 Arc、共享块或写时复制；`Vec` 的已有字节会复制。浅拷贝假设不能保留。

### 真实分支反例及其解释边界

- **Authority 增量 clone 不是 Worker 的 image.apply。** Worker 在 File write 时直接修改自己的 image（`src/io/files.rs:53`）；本节 clone 在 Authority `BranchStore::commit_point` 中发生（`src/authority/branch.rs:255–283`）。两者不能算成一个镜像或把微测直接外推为 Worker commit CPU。真实网络部署需要计入 Authority 进程的 CPU/内存。
- **空 pending batch 的 sync 不追加。** `src/io.rs:345–347` 直接返回。只读 SQL、不产生 pending WAL 的 sync 不走这条 clone；一个显式事务里的多条 SQL 也不能逐条按持久提交计数。既有 append receipt 命中在 `src/wal.rs:478–480` 返回，不进入 publish/clone，但仍读完整 index 并验证请求内容。
- **冷会话/缓存失配与 root CAS 重试没有此 clone。** 增量只用于事务第一次尝试且 `view.validated_wal` 对应当前 branch wal entry 的情况（`src/authority/branch.rs:397–404`）。新 View 初始化为 None（`:210–217`），refresh 在 wal entry 变化时清缓存（`:221–224`）；重试令 previous=None。随后执行 `validate_index_state`，从空 image 重放全部 entries（`src/recovery.rs:449–473`）。这能反驳“每次提交都有这个 clone”，**不能证明这些提交更快**：重放、读块和解码可能更贵。第一笔与后续稳态必须分开统计。
- **普通 wal/index put 也全量验证。** `BranchStore::put` 对 `wal/index` 调用 `commit_point(..., None, ...)`（`src/authority/branch.rs:535–536`），不复用候选 clone。fence/compact 等非 append 发布不应套用增量 clone 的成本模型。
- **裸 Memory/LocalObjectStore 是真实低层配置，却不代表商业服务提交。** ObjectStore 默认 append_wal 返回 None（`src/object_store.rs:83–85`），`RemoteWal::append_commit` 退回 append_local（`src/wal.rs:446–449`），没有 Authority clone。若实验只跑裸 Memory RemoteWal，再把“没有 clone”外推到 Worker→Authority，证据会失效。应明确这一对照用于隔离 index 成本。

### compact、页复用和增长模型

成功 `RemoteDatabase::compact_wal_once` 会先 native truncate checkpoint，再 `io.compact_wal`（`src/database.rs:301–318`）；IO 还要求 image 已为空（`src/io.rs:247–253`）。`RemoteWal::truncate` 重建 baseline，收缩旧 entries（`src/wal.rs:643–699`）。因此“只要累计提交数增加，镜像 clone 就单调增长到历史总字节”的断言有明确反例：定期成功 compact 的镜像增长是周期性的。

但这不是证明现状已自动有界。`session::configure` 调用 `wal_auto_actions_disable`（`src/session.rs:273–276`），Turso 明确连 auto-checkpoint 与 header restart 一起关闭（`core/connection.rs:2614–2619`）；维护调度默认关闭（`src/maintenance/policy.rs:103–111`）。长期只改同一 SQL 页，仍可以不断追加 WAL frame；“逻辑页复用自动限制 WAL image”不成立。Full checkpoint 和 truncate+compact 也必须区分，前者不能直接当作 image 清零事件。

index 还有独立增长轴：truncate 只改 entries 与 replay_baseline_lsn，没有删除 receipts/receipt_digests（`src/wal.rs:686–695`）。所以即使 compact 控住镜像与 entries，累计回执仍能推动完整 index 的 decode/encode/校验成本。增量校验旧 receipts 做 BTreeMap 查找（`src/recovery.rs:495–504`），不能未经测量把所有成本简单称为线性 memcpy。应分别记录 `image_bytes`、entries 数、receipts 数、index JSON 字节、decode/validation/copy/publish 时间。

### 最小可证伪实验 H4

1. 保持 Authority branch session 不变，连续真实 append，记录第一次 full validation、第二次以后的 incremental、CAS retry 次数和 observed_copy；固定新增字节，改变上一镜像大小。只有测到实际增量分支才讨论 clone 曲线。
2. 与“每 K 次成功 truncate+compact”对照，记录每次的 image length 与复制字节。若镜像随周期归零，推翻“与终身累计 WAL 同步增长”；若 receipts/index 仍增长，把它列为另一机制。
3. 固定活跃镜像和 entries，仅改变历史 receipts 数，测 index 总字节与端到端 Authority append。反过来固定 index 大小、改变 image 大小隔离 copy；无法保持这些控制变量时不要称为单因素 clone 实验。
4. 记录真实 SQL 提交路径的 clone 耗时占提交总墙钟与 Authority CPU 的比例。若显著小于网络/持久发布等待，即使 memcpy 线性增长也不足以支持当前“主要瓶颈”或优化优先级。冷 session 的全量重建不要混进 clone 样本。

## H5：有真实去重和 native batch，但没有自动把所有段合成一个上传批次

### 可以直接击穿简单 32 倍/请求数模型的反例

**已有内容跳过 PUT。** `src/storage.rs:515–529` 的 put_immutable 先 GET；若 existing.bytes 与新 bytes 相等立即返回。对于回到旧内容的段、无实际变化的写、多个逻辑段形成相同内容，一次 publish 可以有大量 dirty 索引而只有一个甚至零个新段 PUT。dirty 本身是按索引的 `BTreeMap<u64, Vec<u8>>`（`:27–33`），多次写同段只留下最终内容（`:291`）。因此 1024 个 dirty 索引只能给出上传候选数；128 MiB 是“1024 个不同且此前不存在的最终内容”的段 PUT payload。

这个反例不是免费吞吐优化：逐段调用仍执行 GET，并返回完整段字节比较。重复同哈希可能省上行 PUT，却仍反复支付 128 KiB 下行 GET、RPC/base64/解析与完整性校验；manifest/HEAD 仍需发布。必须分别记 uploaded bytes、downloaded bytes、RPC 次数与底层后端请求数。受控全零/同内容段构造的是边界反例，真实 SQL 页包含页头和业务内容，不能把其去重率当成常规负载去重率。

**native checkpoint 已减少输入工作。** Turso `core/storage/wal.rs:5028–5032` 使用 iter_latest_frames；单进程实现为每个 page 选该范围最后一帧（`:1004–1017`），共享协调实现明确按 distinct page 去重（`core/storage/shared_wal_coordination.rs:2481–2522`）。同一页写 100 次不是 checkpoint 100 个页版本。

native 层另有 `WriteBatch` 按 page_id 覆盖并合并连续页（`core/storage/wal.rs:2573–2618`），checkpoint 调 `write_pages_vectored`（`:5141–5155`）；后者把连续页组为一个调用（`core/storage/sqlite3_ondisk.rs:665–756`）。V1 的 RemoteFile::pwritev 把 buffers 合成连续 bytes 后一次 `storage.write_at`（`src/io/files.rs:18–35,114–120`）。因此逐页调用 storage.write_at 人工构造的“同段每页都 clone 整段”上界，可能高估真实连续 checkpoint；完整段对齐写甚至走 `vec![0; SEGMENT_SIZE]` 而不读取/clone 旧段（`src/storage.rs:282–285`）。稀疏页、batch 切边、非对齐情况下 clone 仍可能发生。

32 倍准确限定为：4 KiB 页、每个最终脏段仅一个不同页、每个最终段为新内容，则段 body / 最终变化页字节 = 128/4。若每段含 32 个变化页，该比值趋近 1；若分母使用累计 WAL 字节、SQL 输入字节或逻辑业务字节，比例会改变。32 不是普适写放大常数。

### 已有 put_group 的实际作用范围

storage 的 publish 循环在每次 put_immutable 返回后才处理下一个 dirty 索引（`src/storage.rs:351–359`），RemoteStore::put 每次单独发一个 SessionOperation::Put（`src/authority_rpc/provider.rs:351–361`）。没找到跨这些调用的隐藏 checkpoint 聚合器，常规这条路径也没有调用 upload_segments/register_segments。

但“底层全部逐对象串行、没有 batch”同样不成立。单次 BranchStore::put 调 authority.upload 和 authority.transact（`src/authority/branch.rs:490–546`）；Authority 每次事务默认以 put_group 发布待写元数据与 head（`src/authority.rs:435–449`），namespace backend 把 group 原样传给底层（`src/authority/namespace_backend.rs:166–174`）。因此同一个逻辑段 PUT 可以对应内部若干事务、每个事务一组对象写；S3 group body 并行或本地 group 共享屏障已存在。准确说法是“段之间的同步调用依赖仍串行”，无法据此推出“每个内部元数据对象独立一次 fsync”或按底层对象数乘 RTT。

现有 UploadSegments 的批次预约/登记可减少 root 事务次数，但其 body 上传实现仍为 entries.zip(segments).map(finish_upload).collect（`src/authority.rs:514–545`），不能把接口名字 batch 当成 body 并行承诺。其逻辑身份冲突检查（`:559–567`）也意味着设计新 checkpoint 批次时需要保留现有去重行为，不能机械把所有 dirty 内容当作新物理对象提交。

### 最小可证伪实验 H5

- 8 个段已足够；分别构造：8 个不同且未存在内容的稀疏段、8 个相同新内容、回到已存在旧内容、8 个完整覆盖的连续段。对每组测脏索引数、distinct hash 数、segment GET/PUT 数与字节、manifest/HEAD 次数。相同内容或旧内容若仍有 8 次新段 PUT，直接反驳上述去重解读；新内容稀疏臂若 PUT body 不接近 distinct hash ×128 KiB，反驳简单放大模型。
- 用对象存储事件追踪而非总墙钟证明段间顺序：第一个段操作未返回时，第二个段请求是否已进入后端。分别记录 Worker→Authority RPC、Authority logical put、底层 put_group 及 group 内对象，不混用计数口径。
- 一组实际 SQL 反复改同页，一组改相邻 32 页，一组每 32 页挑 1 页；通过真实 `RemoteDatabase::checkpoint` 进入 native 路径。对比最终 distinct pages、RemoteFile pwritev 次数、storage 分段调用和上传字节。直接 storage 小写试验只检验存储机制，不证明 native 路径工作量。
- 若做“批次版本”A/B，需同时守住前台 operations gate 与正确性契约，报告 checkpoint 总时间减少是否真的降低前台等待；不要把按比例减少的 PUT 或某段 memcpy 时间宣称成全服务吞吐的同等提升。

## 对原优先级推理的修正建议

原报告已经多处写明收益未测，应保留这些限制，但开头排序的语气比证据更强。H3 的机制证据最直接，却存在 native cache 覆盖率与 operations 排他锁这两个重要收益上界；去锁还有跨状态协议成本。H4 的深复制确定，但必须区别 Authority 稳态、冷验证和维护周期，持续回执增长比“镜像永远增长”更难被 compact 反例消除。H5 的段格式放大在特定新内容稀疏工作负载成立，去重/native batch/内部 group 会改变实际网络量与次数。

下一轮应先用最小事件实验确认等待域与真实分支覆盖，再以端到端分段计时排序。仅复现机制不构成优化优先级；只有实际出现频率、工作量规模、等待/CPU 占比和语义改动成本一起支持，才应将某项列为当前主要瓶颈。
