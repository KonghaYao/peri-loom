# Writer admission：作者接入记录

本文按阶段保留作者过程与失败记录，不作为当前独立验收结论。
最新独立结果、已验证范围与未完成项见 `WRITER-ADMISSION-VERIFICATION.md`。

## 当前阶段

1. DEV 作者候选，非可部署或功能完成结论。已接入 checked execution、noncommitting abort、
   真实同步 IO retirement provider 与本地重对象收集；不得以旧 interrupt/reset 替代。
2. 历史统一作者组76项已通过；随后发现 late-Stopped 回收缺口，新增真实20秒 safety
   RED→GREEN oracle。本次新77项已在同一源码统一重跑通过，不借历史76项宣称新源码通过。
   不是独立 ParentGate。未运行 release、真实服务 API、S3、负载或信号操作。
3. 原始 R7 Busy4 FAIL 证据保留。本候选不追加 busy timeout / query budget，不做 SQL
   retry、batch replay、自动 COMMIT 或 DB-global RPC mutex，也不声称已证明 R7 根因修复。
4. lawful BEGIN IMMEDIATE 的内部空 TEMP 旧冲突曾导致 busy_tuning 0/5真实失败，证据保留。
   Native TEMP 条件子批次已由F独立43项验收，child为ad892d4；它不等于整个Parent验收。

## 内部 ABI

1. `crate::session::SessionWriterState` 为 crate-private `{ owner: bool, sequence: u64 }`。
   RemoteSession / DatabaseHandle / OwnedHandle 的 `writer_state()` 使用单个 packed atomic
   快照，不获取 native connection mutex、执行 SQL 或验证租约，也不授予权限。
2. `begin_close() -> Result<()>` 仅 mark closing、取消 queued ticket、notify；
   不推进 sequence，不释放 owner。Worker active-RPC guard 必须覆盖 hint / frame snapshot。
3. `RemoteSession::request_close() -> Result<()>` 是 crate-private 清理接口；
   对外显式入口为 `#[doc(hidden)] RemoteSession::close() -> Result<()>`。
   RemoteSession / DatabaseHandle / OwnedHandle 的 `request_close_until(deadline: Instant)`
   使用调用者绝对 deadline，两个未使用的crate-private handle便利wrapper已删除，
   不从 active-RPC drain 之后重新领取五秒。只有实际清理、close 与 settle 确认才能 ACK。
4. `OwnedHandle::lease_binding_snapshot() -> Result<LeaseBinding>` 仅短 context lock clone。
   它不是健康或授权 proof，不取 connection mutex / IO；实际原 OP store guards 不变。
5. sequence 只在确认 settled 后推进。无法证明新 settled 状态时不得为满足 wire 单调性
   虚构 sequence；缺失可信终态应按私有 v2 的 fail-closed 合同处理，而不是解除 unknown。
6. `crate::session::UnsettledSqlOutcome` 是 crate-private typed error context。
   当前 operation 的 abort 返回 PendingIo / FinalizationAlreadyStarted、或清理失败无法
   确认 StatementStopped 时，用 `original_error.context(UnsettledSqlOutcome)` 保留原 source。
   RPC 必须用 `error.downcast_ref::<UnsettledSqlOutcome>()` 检查完整 anyhow context；
   所有 v1 / v2 response 与诊断均不得声称 execution_completed=true，v2 不提供 fresh hint。
   后续请求因已封闭数据库收到普通 AdmissionError::Quarantine，不等于当前 operation unknown。
7. StatementStopped 只证明该 statement 已停止，不代表整个 batch 无副作用，也不代表
   explicit write transaction 的锁已释放；owner / sequence 仍依赖实际 native transaction / locks。

## 本地只读观察接口

1. `RemoteDatabase::writer_admission_status() -> Result<AdmissionStatus>` 与
   `RemoteSession::admission_id() -> u64` 为新增 `#[doc(hidden)]` 的 unstable 本地观察。
2. `AdmissionStatus` 含 `owner: Option<u64>`、`queued: Vec<u64>`、`quarantined: bool`。
   owner 是 coordinator reservation / retained owner；不能仅凭它证明 native 已获写锁。
   queued 按真实 ticket 顺序且最多八项，便于 deterministic FIFO / cancel oracle。
3. ID 只在该实际 RemoteDatabase 生命周期内稳定，是 opaque 实例 ID，不是 worker
   opaque session、lease 或权限。不要输出到 public wire / logs；不暴露 engine / pager。
4. `session::AdmissionError` 为独立 typed error：Conflict、QueueFull、Deadline、Closing、
   Quarantine。它们不是 native Busy / BusySnapshot，不得为了兼容旧测试伪造 native cause。

## 状态机边界

1. 每实际 RemoteDatabase 的默认 connection 与独立 sessions 共享一个 coordinator。
   等待不持 coordinator mutex 或 database operation gate，最多每 10ms 检查本地 guard。
2. 未获 permit 的 ticket 取消采用 RAII；取消 FIFO head 必须 notify 后继。
   已获 permit 如果没有 settle 就析构，则 quarantine，不自动转交 owner。
3. atomic owner hint 不包含仅排队或刚获 reservation 的“将来 owner”承诺。
   Constraint、COMMIT、ROLLBACK 必须以 native 当前实际锁与 statement 清理结果决定 settle，
   不能根据 SQL 字符串、HTTP action、auto_commit 单一布尔或错误名字推断释放。
4. 当前 private model tests 只验证状态机，不是 native locks / Authority grant / physical IO
   的真实 proof；完整生命周期需 James 的独立测试与 F 的统一门禁。
5. 主控 P0 旧 interrupt/reset baseline 的行为 RED 证据在
   `/tmp/peri-native-statement-safety-red-v2.log`；首次 red.log 是 compile error，不能计作
   行为 oracle。conditional Native child `3264185e511befc95c8750751e588b18709cd996`
   的内部23与外部6由主控/F独立验收；作者未重跑，不能外推为新TEMP合同或parent验收。

## 本地 retirement provider 候选

1. RemoteIO 的 RemoteFile 与 host temporary File 均通过同一个 private NativeFile gate。
   本合同限于实际 Unix PlatformIO=UnixIO 的同步 syscall / callback 实现；不是 io_uring，
   不向其他异步 backend 泛化。非 Unix 不签发 retirement token，默认 IO 操作仍可使用。
2. 每次 File entry 持有 read ticket 至实际底层调用及其同步 callback 返回。
   open_file / remove_file / step / cancel / file_id 入口也受同一 gate 控制。
   File token 的 finished / cancelled 不用于推断物理停止；远端 timeout 仍可能 unknown。
3. 关闭 dispatch 为不可逆 atomic 标记。入口使用 try_read，失败后每轮重查该标记，
   已通过第一次检查的请求也不会因 retirement write token 被持有而无限阻塞。
4. token 需要同一 gate 的排他 write guard；DatabaseRetirementGuard 还持有 database
   operations 排他 guard，绑定与 engine 相同的 IO Arc。调用者必须先释放自己的 operation
   read guard，并继续独占当前 Statement / native session；不得在队列等待时持 operation gate。
5. 沿用原绝对 deadline，没有重新领取 SQL budget。已经到期时仅允许一次立即成功的
   非阻塞 try_write 清理，不允许等待。失败后再次尝试是独立显式本地清理调用，不是 SQL
   重试或原请求的新 budget；必须仍保留其原 UNKNOWN / error source / 不变 hint sequence。
6. runtime quarantine tombstone 独立于 sessions count / loaded entry 存在，零计数、idle
   eviction、drain 或 Instance Drop 不能移除 unknown 标记或允许新 instance reopen。
   native收集确认后可以释放所有登记connection/engine强引用，保留unknown metadata。
   已有真实Weak与directory lock作者oracle，但不等于所有错误路径均已证明可回收。
7. 同步后端 Err 且未调用 submitted leaf callback 时，adapter在ticket内补terminal error，
   保留原LimboError；已callback不重复，partial write不是无副作用，异步后端不套此规则。
   必须同时有真实IO排他与leaf terminal，不能forcefinish group冒充proof。

## 同步 cleanup coordinator 与验收边界

1. 每实际DB至多一个同步cleanup leader，无新thread。failed peer先登记实际Statement，
   释放connection lane与operation read，再等待leader；leader在DB与IO排他下重新收集。
   对尚未取得原abort读gate的Statement，先noncommitting abort；Stopped只允许drop该stmt，
   仍须逐一退休全部登记connection并验证锁/count。不能直接retire NotRequested statement。
2. 普通failed call附加20秒显式cleanup safety bound，不继续SQLstep、不重置原SQL预算。
   Close使用调用者原绝对deadline，不从等待结束处追加20秒。同步HTTP可能先阻塞15秒；
   五秒query budget是合作式预算，不是硬中断或RPC响应硬界限。
3. 失败保留原SQL source与Unsettled marker、固定CleanupFailure分类；不记录敏感原始内容。
   后续真实清理成功也不改原UNKNOWN、不推进旧hint、不转terminal Close/Detached ACK。
4. 作者真实lostACK fault分receipt可读/不可读对照。可读时P1正确恢复knownOk，同一native
   business call成功且无SQL replay；不可读时保持unknown，不能把readback解读为cancel。
5. 多peer oracle当前为真实writer ACK未知加真实reader IntegerOverflow：reader原cleanup
   gate被leader排他占用且原deadline到期，两个Statement同时登记后再收集，pending=2、
   actual lanes=3、Weak最终释放、无额外物理写。它不是两个并发native writer获锁的证明。
6. wire WorkerRuntime预占session/DB容量；pending至多每实际active lane一个，包含default，
   依赖同lane串行、failed batch停止与quarantine禁止新prepare，不等于FIFO queue上限8。
   trusted library直接RemoteDatabase.session入口无独立session硬cap，不宣称全library有界。
7. resident容量计entries与quarantine tombstone ID的并集，同ID只计一个，未知epoch即使
   native重对象消失仍占原max_open_databases slot；不得自动evict或新epoch抹除未知状态。
8. 新TEMP实际双pager与Weak回收作者oracle已通过；Native内部count由条件子批次证明，
   Parent不读取私有count或将Debug文本当proof。仍待生产wire maxquota/back-to-back failure
   与真实Native Unknown shutdown全链路验证，不能把shutdown的typed watchdog模拟当该证明。

## Fresh TEMP 政策与现有失败

1. 仅RemoteDatabase::connect成功创建fresh native connection后一次设置TempStore::Memory；
   configure与每SQLcall不改temp_store，不清空已存在TEMP，不依赖SQL字串分类。
2. 原 `/tmp/peri-parent-writer-author-v2.log` busy_tuning 0/5行为失败保留。因Cargo遇首target
   失败停止，writer18、worker4与其他legacy targets尚未执行；PITR亦未执行。
3. 新TEMP seam必须在native证明actual MemoryIO provenance、empty schema与运行时闭集。
   MAIN角色/P4物理fence/query预算不放宽。作者不改Native child；新source与新oracle须另验。
4. 双peer首版 `/tmp/peri-parent-simultaneous-peers-v1.log` exit101是真实parent cleanup接入
   缺口；v2修正排他下abort顺序后1PASS，不能将旧RED覆盖或称为性能收益。
5. `/tmp/peri-parent-shutdown-reclamation-v1.log` exit0记录shutdown unit7与reclamation7，
   但当时新Native source处于mutable编辑，无frozen输入证明，仅作为候选观察保留。
   shutdown单元的typed错误模拟不是实际native failed RPC全链路清理/clean exit验收。
   该日志仅为历史候选观察；后续统一源码重跑shutdown7，仍不改变其mock范围。

## 新 Native9 作者观察（仍非 ParentGate）

1. 主控确认Native9冻结与缓存清理完工后，作者串行执行新TEMP内部14与旧23，合计37
   distinct PASS；复制snapshot338文件执行后逐hash不变。作者未执行新P0external6，
   不把重复串并运行相同六项计为十二。F独立bounded门禁仍是另一验收阶段。
2. collector现在先处理pending与退休所有登记connection，再逐conn调用纯观察
   `verify_noncommitting_retired(exact_guard)`确认MAIN与certified实际Memory TEMP均已停。
   RetirementErrors仅保留固定分类，None不洗unknown，非None不自动阻止已证完成的回收；
   未满足真实proof仍禁止detach/drop重对象，不能根据closed单一布尔或cfg标记判断aux。
3. 作者TEMP fixture真实BEGIN IMMEDIATE后MAIN COMMIT lostACK保持unknown，双pager proof
   与0新增backend GET/PUT、Weak释放通过；另fresh once政策与固定retirement错误分类单位
   测试通过。首次1/2失败（误假定退休必有本地errors）与修后3/3日志均保留。
4. writer v3三prefix失败是plain duration错误抢先typedDeadline。仅同一firststart duration
   到期加typed context，未映射VM/row/byte/SQLcaps或扩预算；v4 writer18、busy5、legacy8过，
   worker3/4因C旧Writer fixture未建表失败，整个命令仍exit101。C修fixture后worker4已重跑
   通过；未删除SQL/断言，也未修改预算以取得GREEN。
5. Sleep helper改为保留guard/store原anyhow类型与source，不改变检查顺序与10ms片；新纯
   Sleep deadline/Closing单位2项通过，不代表真实租约撤销。原生Interrupt保原Limbo源，
   不宣称所有native CPU预算中断都已有typedDeadline分类。
6. Sleep新增live-local positive，三项单位测试一起重跑通过。Deadline与Closing测试是本地
   mock，不是真实Authority grant撤销。作者输入与RED/GREEN日志SHA在
   `/tmp/peri-parent-author-candidate.md`；统一Parent独立门禁必须另由F执行。

## Late-Stopped safety 缺口与修复

1. 真实reader在IntegerOverflow后、noncommitting abort前暂停；writer实际COMMIT lostACK且
   receipt不可读，登记pending后等待原20秒cleanup safety耗尽。writer原LimboError source、
   Unsettled marker与CleanupFailure::Deadline保留，不标执行完成、不提供fresh hint。
2. 旧finish_call只检查本次Unsettled marker。20秒后最后reader获得StatementStopped，原错误
   是已知IntegerOverflow而无marker，因此不触发收集：真实RED在20.03秒后发现pending仍1。
   `/tmp/peri-parent-late-stopped-peer-red-v1.log` 保留，exit101。
3. 最小修复只让execute/query释放connection mutex后，在DB已quarantine时也触发cleanup-only。
   它不再次step/reset/halt、不SQL重试、不把known reader改成Unknown或NoEffects成功；
   原writer的Unknown不会因后续本地收集成功而洗成ACK。
4. GREEN同样先真实等待20秒，之后reader原IntegerOverflow返回，pending清空、native Weak
   无法upgrade、没有新增backend GET/PUT、旧writer hint不变且Close仍返回Unsettled。
   `/tmp/peri-parent-late-stopped-peer-green-v1.log` exit0，20.03秒。
5. 20秒是独立显式本地cleanup safety，不是新SQL执行预算或RPC硬期限。Close路径未调用该
   finish_call，仍只使用原caller absolute deadline，不获得新20秒。原query五秒保持不变。
   该oracle不声称所有异步backend、远端物理停止或整体shutdown硬时限已证明。
6. 新77作者整组须重新运行、输入逐SHA前后核对后再交F。输入inventory只覆盖Rust source与
   manifests，不含Markdown/PROGRESS、工具链binary、registry或OS，不称hermetic构建证明。

## 作者 source freeze（2026-10-07）

1. 新统一77项实际PASS、wrapper exit0，locked/offline/debug/default features，所有组串行。
   lib分组为IO bridge6、IO gate5、reclamation11、registry6、admission4、drive3、shutdown7；
   integration为busy_tuning5、busy_wait6、concurrent_sessions2、writer18、worker4。
   每个test name仅计一次；多backend循环不翻倍，单独RED→GREEN重复不再追加数量。
2. `/tmp/peri-parent-author-focused-final-v2.log` SHA256为
   `1c4667b4aaf677794ada504fa9f3675679a204cfee59df3819da212e24f37fab`。
   历史76日志仍独立保留，不能改写成77。新源码均经rustfmt静态stdin对比，31个owned
   Rust文件全部小于500行，未执行full clippy或release build；Native原有8 warnings保留。
3. `/tmp/peri-parent-author-inputs-v9.json` 含1026输入，逐hash执行前后UNCHANGED；SHA256为
   `88b70ac71a07b7669e9bfd9abb82475938abf9206dc55d30a6adcccbc45af8cc`。
   parent HEAD为080c225，只有主控PROGRESS doc追加；Native ad892d4 clean、不作任何源码修改。
4. 全部owned路径、完整SHA、77 distinct names、scope与日志在
   `/tmp/peri-parent-author-freeze-v1.json`、`/tmp/peri-parent-author-candidate.md`交接。
   此为作者候选冻结，F WholeParentGate、真实S3与后续负载另验；不声称R7功能修复或性能收益。
5. drive与queue的local expiry/revocation单位mock不是真实Authority grant撤销；worker4是真实
   隔离loopback协议fixture，不是现有用户fixture。shutdown7实际join/panic覆盖与typed
   watchdog模拟不可替代真实Native Unknown全链路shutdown证明。生产wire maxquota反复失败
   oracle以及整体P4/PITR/migration/SDK/Go回归不在本次77作者组内。

## 独立 strict-clippy 首败后的最小修复

1. F首轮fmt通过、strict-clippy exit101，七项lint原日志保留。仅四个owned文件修复：
   maintenance去冗余借用、短路条件合并、immutable private ExecutionContext聚合四个引用、
   provider测试tuple私有alias。无lintallow，mutable Statement/guard/permit/terminal仍明确。
2. 条件仍按原顺序短路：marker或quarantine成立才cleanup，cleanup失败才查固定failure。
   Close绝对期限、20秒safety、原五秒预算、SQL source/Unsettled与hint规则均不改变。
3. 作者最初误移除fresh factory owned Arc借用导致compile101，原日志保留；精确恢复后，
   provider6+gate5+cleanup10+drive3+busy5+busy_wait6+writer18共53项实际PASS、exit0。
   历史77与真实20秒RED/GREEN保持历史事实；本次没有新增控制路径，不为lint重复20秒oracle。
4. 新1026输入前后逐SHA一致，四文件格式核对通过；最新freeze-v2另列，F继续独立strict-clippy
   与剩余WholeParent门禁。作者未声称strict-clippy、release、S3或性能已经通过。

## 独立全量三FAIL中的作者两项修复

1. F v2 fmt/clippy通过，但release全量exit101，三项FAIL原日志保留。作者本地两项原测试
   都真实RED；不以旧77或53覆盖该失败。第三项p4 watchdog由C另行静态定位。
2. compaction原来对已持read snapshot的peer也dispatch Truncate，失败后maintenance保护
   进入quarantine。最小修复在既有DB operation write gate下、dispatch前检查全部已登记
   非default lane：connection只try_lock，实际MAIN read lock或忙lane返回Conflict。
   不把任何已dispatch checkpoint Err洗成known成功，也不猜partial checkpoint为健康proof。
3. 新private oracle验证Conflict无Unsettled、DB未quarantine、reader事务/snapshot保留；
   Drop reader后compaction与读取成功。原sessions七项、remote_database十二项均实际通过。
4. 主控裁决迁移remote test合同：健康时TempFile成功/drop，并验证同hostpath拒绝+不存在；
   原PRAGMA/INSERT durability error及失败后host拒绝断言保留。退休后同actual IO的TempFile
   必须拒绝，并断言quarantined。不新增temp-only旁路API、不放松真实retirement gate。
5. 新v12输入封存后四项窄复验实际PASS exit0（新private、两原回归、原maintenance writer
   oracle），1026 before/after不变。最新freeze-v3记录路径/SHA；完整独立release剩余门禁由F
   重启，不宣称本次作者debug已解决第三FAIL或替代完整门禁。

## Watchdog 诊断与 partial 保守分类最终冻结

1. C仅两测试helper加入固定Context诊断，payload/SQL/断言/五秒期限不变。作者用1.98.1、
   F hotParent target执行release exact三独立fixture均exit0；随后仅一轮默认并发完整
   p4_runtime 20/20 PASS、exit0。没有失败stage，历史race未rootcaused，原F FAIL不能消除。
2. 主控授权补齐已dispatch但非full backfill的错误分类：保持原相等条件与fixed error source，
   仅添加Unsettled context，使finish_maintenance触发cleanup-only。原checkpoint Err路径与
   成功路径、阈值、预算、Close期限均不变；不把partial当可复用proof，不洗Unknown或ACK。
3. 私有bool分类测试仅验证false保fixed source+marker、true保持Ok，不制造真实native partial。
   它与真实pinned-reader preflight oracle两项debug实际PASS，v13输入1026前后不变。
   新freeze-v4记录全部SHA、C诊断与作者日志；无活动builder，交F下一独立完整门禁。
