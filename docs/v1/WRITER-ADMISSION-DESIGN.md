# Native writer admission — 有界架构提案

## 状态与证据
- Proposal，非实现、非性能结论；本次仅新增本文档，没有 build/test/API/load/信号操作。
- 只读 root HEAD：`a26b6acb802f3fa2e53f9cb90ce10b76c2d4ac10`；R7部署 native `237625...`、结果按请求方提供记录。
- 本地 Turso checkout HEAD 是 `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`；以下依据当前可见源码，不冒称与部署二进制逐字匹配。
- R7请求 `stress_a22db275b5536ddf_000000002711` 已关联 worker-b typed Busy，RPC约4.074s；holder、具体锁、等待链仍未证明。
- C4 smoke 20 attempts/19 ACK/1 Busy、peer自然ACK、unknown=0不等于完整通过；无final oracle，后续architecture/SQL未运行。
- 不再仅增加timeout/SQL预算，也不把SQL failure忽略或改类来获取绿色结果。

## 决策
- 候选：Rust每个实际 `Arc<RemoteDatabase>` 生命周期内共享一个 writer coordinator，在语句第一次执行前 admission；不是global namespace map、Go SQL分类、DB-global RPC mutex或SQL replay。
- 排队对象是新writer事务；原writer跨RPC保留owner，owner后续SQL/COMMIT/ROLLBACK/close不排在竞争者后面。
- 纯read不经过writer队列；native仍决定实际锁、snapshot有效性、提交及回滚。保留busy4s、整batch5s、RPC10s和现有guards。
- 仅当参与竞争的SQL写路径都经过该coordinator，才能消除这些路径间的writer获取竞赛；不能承诺解决checkpoint、read升级、未知native内部锁或历史树成本。

## 当前源码与可行seam
- `prototypes/v1/src/session.rs`：每session一个connection mutex；execute逐条prepare/step，失败不自动回滚batch；constructor设busy4s一次。
- `src/session/drive.rs`：同Statement推进，Sleep分片≤10ms，检查query budget和`validate_local`；不得reset/reprepare/replay SQL来重试。
- `src/database.rs`：`operations: RwLock<()>`保护checkpoint/teardown；现execute在整个call持read gate，不能在该gate内新增长队列等待。
- `src/worker_rpc/sessions.rs`：registry持`Arc<OwnedHandle>`；close移除entry不代表在途Arc已Drop或native锁已释放。
- `src/worker_rpc/server.rs`：所有SQL共享sql_inflight，control reserve不服务SQL holder；只加coordinator会留下入口槽位饥饿。
- Turso `Statement::{get_program,get_pager}`、`Program::is_readonly`、`PreparedProgram::{insns,write_databases,is_compatible_with}`可见。
- `Connection::{get_auto_commit,is_in_write_tx}`、`Pager::{holds_read_lock,holds_write_lock}`可见；`get_tx_state`是crate-private，不假装已有外部完整事务快照API。
- `core/vdbe/insn.rs:2379` 从编译opcode计算readonly，包含Transaction Write、sequence、trigger子program及多种PRAGMA/DDL；HTTP Query不是read标签。
- `core/translate/transaction.rs`：BEGIN DEFERRED只有AutoCommit；IMMEDIATE/EXCLUSIVE有Transaction Write；COMMIT走AutoCommit。
- `core/vdbe/execute.rs:4997` 写升级失败区分原None/Read状态；BusySnapshot不能靠重新取得permit使旧snapshot有效。

## 新接口和文件职责（拟议，不是现有API）
- 新 `src/session/writer_admission.rs`：`WriterAdmission`、`SessionId`、`WriterPermit`、FIFO ticket/cancel状态；常数有界queue，例如每DB最多8个等待者。
- `WriterAdmission::acquire(session, request_deadline, check_local) -> Result<WriterPermit>`：同owner重入，其他新writer FIFO，检查取消/预算/lease，condvar等待≤10ms。
- `WriterAdmission::try_promote(session) -> Result<WriterPermit>`：已有read snapshot的升级仅立即尝试，不排队等待；竞争存在时返回独立`AdmissionConflict`，不伪造native Busy，不释放/重开其read txn。
- `WriterAdmission::settle(session, native_observation, terminal_outcome)`：保留真实writer或未settled占位；确认无writer锁/未完成statement后才release并唤醒下一个。
- `WriterAdmission::{owner_hint,request_close,quarantine}`：owner_hint只用于入口资源预留，不授予SQL/lease权限；close和quarantine也不跳过native清理。
- `src/database.rs`持coordinator；包含内部默认connection、每RemoteSession、直接execute/query以及maintenance路径，不能只保护WorkerRPC Execute。
- `src/session.rs`加入稳定SessionId、closing标志、permit生命周期；execute/query共用同一statement admission/helper。
- `src/session/budget.rs`加入私有`RequestDeadline`及`install_with_deadline`；call入口开始计时，session mutex等待/prepare/admission/执行共享原5s，不新增5s。
- `src/session/drive.rs`接收已有permit/closing检查，保留Sleep、IO及native错误类型；不在Busy后重新提交整条SQL。
- `src/worker_rpc/{sessions,server}.rs`实现owner入口预留和可确认的close；`src/runtime/handle.rs`、`src/ownership/handle.rs`仅传递内部hint/close状态。
- 不改Turso core、pager算法、authority root/ownership view、WAL格式或Go控制权协议；完整holder进度保证需要下述Go Gateway/私有SQL响应改动，不能宣称纯Rust即可闭合。若公开native观察不足，停止扩大语义，另行审只读观察hook。

## 分类和事务状态机
1. 在session自身互斥下prepare当前statement，不预先prepare整batch；允许前statement的schema/事务改变影响后statement。
2. 使用native编译结果：`!program.is_readonly()`保守申请writer permit，包括implicit INSERT/UPDATE、RETURNING、DDL、trigger、sequence和IMMEDIATE/EXCLUSIVE；只有编译结果readonly的SELECT bypass，不能按SELECT词头放行。
3. BEGIN DEFERRED/SAVEPOINT不预占writer；后续首个write再申请。owner的COMMIT/ROLLBACK虽可readonly，仍绕过新writer FIFO并正常native执行。
4. 可扫描实际Insn辅助标记txn-control/savepoint与维护类别；不能用字符串前缀、regex、独立Go parser或Query/Execute区分写权限。
5. `readonly`并非完整“所有锁需求”API：maintenance、temp及reprepare需保守处理；unsupported journal/MVCC/extension模式继续拒绝，不能以分类取代fencing。
   实际Insn将Checkpoint/JournalMode/Vacuum、写PRAGMA及sequence/trigger列为nonreadonly；`write_databases`仅辅助，不能替代整program分类。COMMIT若启用CDC可含Transaction Write，按实际metadata处理而非一概当read。
6. 队列等待发生在native首次step前；唤醒后再次验证prepared compatibility、local lease和fresh `validate_transaction(in_transaction)`，再取得短期DB operation gate执行。
7. 排队中prepare-context失配不得执行旧分类；最小版本显式返回失败，不自行reset/replay。后续若支持reprepare，必须重做分类后才能step。
8. native内部SchemaUpdated/reprepare仍可能发生；write原本已持permit，readonly→write变化不能默许。对支持的模式建立分类稳定性测试；无法证明的路径不宣称覆盖。
9. 保留permit直至statement结束并完成其Drop/清理观察；Row/IO/Sleep/错误/客户端断线均不是自动release点。
10. 观察`is_in_write_tx || holds_write_lock`则session继续owner，即使call失败；显式事务跨RPC仍保留，Constraint retained txn不能release，不能按错误名字推断rollback。
    完整TransactionState仍非公开；pending IO/statement未settled时一律保留reservation，不拿两个bool冒充完整native状态。任何异常观察不一致先quarantine，不能抢占锁。
11. 只有确认当前statement已settled且没有native writer/未完成提交，才能release；`get_auto_commit()`单独不足，错误也不等于已rollback。
12. 有read snapshot且无writer的promotion不进入可能阻塞holder的FIFO；立即拿不到permit就AdmissionConflict，拿到仍可能native BusySnapshot，原样失败、不auto reopen。
13. 无read snapshot的DEFERRED事务首写可FIFO等待；implicit write在首次step前排队，避免先取得新read snapshot后才等writer。
14. 每次COMMIT/ROLLBACK真正释放writer后，新事务回到FIFO尾；同batch的`COMMIT; BEGIN...`不能连续抢占跳过已有ticket。
15. 不给交互式owner设新自动事务回滚策略；已有长期owner可能使所有waiter超时，公平仅指已释放owner后的新writer FIFO，不是强制抢占。

## 资源、屏障和owner进度
- 统一顺序：call deadline → session lane → prepare/admission等待（无DB operation gate）→短期operation read gate →原guard/native执行；release/snapshot均在coord短锁下。
- 等待期间不持coordinator mutex、DB operation gate、registry mutex、ownership mutex或authority namespace barrier；session自身mutex可以持有，不阻挡别session holder。
- 每次≤10ms唤醒检查deadline、closing和`validate_local`；获permit后和statement开始前执行原fresh physical ownership验证，durability/finalCAS guards完全不变。
- permit不是lease，不扩展deadline；本地generation/epoch失效取消ticket，禁止继承到新RemoteDatabase实例或新branch incarnation。
- checkpoint/compact不能在持operation write gate时等待SQL owner；先协调maintenance admission，再取得write gate并重验guard，发现活跃交互writer就有界失败。
- 排队call占线程/SQL槽位，必须同时限制总waiter；纯read没有writer队列等待，但仍受正常worker admission容量限制。
- 预留独立、有限owner/close SQL执行槽位；一般SQL队列不得占用该reserve，且总socket/thread admission也必须留空间，不能只有后端sql_inflight预留。
- reserve分配用server registry中的真实SessionId/owner hint，最多一个同session在途continuation；不相信客户端“我是COMMIT”，不用SQL文本判定优先级。
- owner hint在授权解析后重验；未知/stale hint走正常lane或拒绝，不能读取该session connection mutex才能辨认owner（否则先被等待者堵住）。
- 解析前socket预算仍有限；private trusted Gateway必须避免将所有upstream槽位耗在waiter，否则本地reserve也救不了holder。该依赖必须测，不承诺跨层绝对优先。
- 多DB owner reserve需有限公平调度；饱和允许失败但不能把owner请求排在依赖它解锁的waiter后面。

## close、Drop、unknown和lease失效
- 请求close先移除可发现session并标closing、唤醒/取消排队ticket；在途Arc继续追踪到terminal，不能移除registry就声称native已经closed。Rust Drop与SQL DROP分别处理：前者要确认native cleanup，后者按普通compiled write/txn状态处理。
- owner close走reserve，在同session串行lane中执行当前native cleanup；不能持registry/coord锁等待connection mutex，也不能在Busy callback里close连接。
- 已失效lease的清理不得flush/commit；保留当前ROLLBACK/`Connection::close`的本地abort路径，native lock观察与成功清理之后才release。
- 当前RemoteSession Drop忽略ROLLBACK/close错误；新增admission不能沿用“Drop无条件释放”假设：清理结果不明则quarantine该实例、拒绝新writer，待确认销毁/恢复。
- 当前native close有本地rollback路径（`core/connection.rs:2439`）；但API成功/失败与实际锁观察必须在fixture证明，不把远端未知提交说成已回滚。
- panic/断线/transport未知不能触发permit自动移交或重试；RAII取消未获准ticket，已获准permit必须settle，否则fail closed。
- native明确终态且锁已释放可以让后续事务继续，仍保留原请求unknown；任何backend uncertainty沿用P4 namespace block，coordinator不能清除此状态。
- revoke/watchdog clear_database与清理必须互相唤醒；新实例只能由现有ownership teardown/acquire barrier建立，不能靠旧permit timeout强制takeover。
- 应增加owner存续、waiter数/年龄、txn状态、permit占用、maintenance状态、close-pending与typed Busy site的有界诊断，不能记录SQL/密钥。

## 错误契约必须显式变更
- coordinator使用自身`AdmissionConflict/QueueFull/AdmissionDeadline/Closing`类型；实际LimboError::Busy/BusySnapshot只来自native，不合成旧typed原因。
- 已进WorkerRPC的batch排队失败是terminal execution failure，不自动宣称pre-dispatch HTTP429或may_have_committed=false；prefix可能已提交，unknown规则照旧。
- 原held-lock测试可在4s返回native Busy；启用queue后可能在原5s budget耗尽返回AdmissionDeadline，这是可见合同变化，需独立断言时间界、类型、prefix和无replay，不静默改旧期望。
- 取消FIFO head必须移除ticket并notify后继；queue容量包含已取消但尚未移除状态，不能靠重试或session/token轮换绕过。取消不能释放已获准owner。

## 独立验证计划与oracle（本次不运行）
1. 三session受控barrier：A跨RPC BEGIN IMMEDIATE持writer，B/C FIFO排队；A COMMIT/ROLLBACK可进入reserve，B先于C获取；每业务ID只落一条ledger。
2. implicit write、Query UPDATE RETURNING、trigger/sequence/DDL、多statement和BEGIN DEFERRED首写均覆盖；reads不占writer permit，native readonly grant仍拒绝写。
3. DEFERRED先SELECT，另一session提交后promotion：native BusySnapshot原样失败；snapshot不重开，prefix无重复、显式ROLLBACK后caller可自行新事务。
4. owner超过5s、queue-full、短budget：waiters有界失败；不追加预算，失败保留分类/prefix flags；无promise任意load全ACK。
5. `COMMIT; later_error`与`write_error_inside_txn`：前者已提交prefix保留，后者按真实native state保留owner；ROLLBACK TO不错误释放外层writer，RELEASE处理实际状态。
6. 填满一般worker/Gateway槽位后holder COMMIT/ROLLBACK/close仍可进展；duplicate同session请求不能耗尽reserve；checkpoint/teardown不与queue形成环等待。
7. wait中expire/revoke/renew、grant替换、慢native IO/finalCAS：原deadline/generation/physical guards仍拒绝stale mutation，续期不误报session_moved。
8. 断线丢失COMMIT ACK、panic、close在途Arc、清理失败、backend unknown：原unknown不变、零replay；未确认释放不移交，确认后不制造永久permit泄漏。
9. frozen catalog/history/epoch-floor/P4guards与真实S3回归保持；最后完整mixed oracle核ledger unique、ACK影响、balance与未解决unknown，不能只看ACK数。

## 不推荐替代与复杂度
- 不用DB-global RPC mutex、整batch重试、per-call busy setter、自动rollback/reopen BusySnapshot、增加SQL预算或将Busy算PASS。
- AST只能辅助识别txn-control，无法代替native状态/锁及隐藏write依赖；`!auto_commit`既不等于writer，也不能保证清理安全。
- 最小完整方案是coordinator＋per-statement接入＋真实owner清理＋入口reserve；只加FIFO或只改timeout都不是完整实现。
- 复杂度中等偏高，尤其cross-RPC生命周期/maintenance/资源饥饿；分阶段fixture证明后再实际mixed，不用负载偶然绿色替代安全oracle。
- 这是针对SQL writer竞争的可实施候选，不是R7 holder归因已完成；若typed Busy实际来自其他锁/路径，应先定位再决定，不强行应用admission。
- retention、root/catalog O(n)、metadata IO、worker/authority饱和仍独立存在，排队可能仅把Busy变成预算超限；只报告真实结果。

## Blocking补充：Gateway也是holder进度依赖
- 静态结论：`platform/admission.go:35` 的control lane仅一条/project且独立于active；`gateway_http.go:127`只有close使用它，COMMIT/ROLLBACK作为execute仍受普通MaxConcurrent/rate限制。
- `gateway_sessions.go:64` 对同session用TryLock；busy即拒绝，不排队。close还要通过该锁、poison检查，不能取消在途或从unknown中伪造closed。
- Gateway close已有control quota不等于worker控制RPC：它转发`close_session`到`/sql`，仍经过worker sql_inflight；所以原两层reserve不能保证close实际完成。
- 配额在解析body/session之前获取；仅加worker owner reserve，四条已queued SQL仍能让公共holder execute在Gateway先429，holder到不了native。
- `service.go:157`可见的是HTTP时间/header界，没有共享global inflight semaphore或owner入口reserve；不能假定已有global reserve，也不能宣称面对socket/CPU饱和必然进展。
- `sql_upstream.go`共用HTTP client，无MaxConnsPerHost槽位限制；upstream有时限但没有owner优先策略。外部proxy/global ingress上限若存在，必须独立纳入验证。

### 选项A：完整闭合，需要显式Go合同（推荐候选，未实现）
- 原MaxConcurrent=4内部划分general≤3、owner-continuation≤1，active总和（含unknown retained）始终≤4；reserve不借给general，否则新owner提示到达时无法收回已派发waiter。
- owner lane只接受已认证主体自己的opaque session、可信native响应记录的当前writer-owner及匹配binding/incarnation；既不相信客户端hint，也不解析SQL词头识别COMMIT。
- 对已有owner的任意合法continuation使用该lane，由native决定它是否释放writer；每session最多一个call，owner多个DB竞争时有限公平调度，不承诺恶意owner无限续事务下的公平。
- rate也可先拒绝holder：在原总RequestsPerSecond/burst内预留有限owner credit，general不能耗尽；这同样改变quota语义，不另加无限token或偷偷跳过总rate。
- 本profile要求MaxConcurrent及rate/burst足够划分两lane；原quota=1没有可同时容纳waiter和holder的空间，应明确拒绝启用此profile或另审非阻塞方案，不能默默放宽到2。
- close保留原独立control≤1/project合同；它是cleanup能力不是额外SQL execute通道，worker侧也给close有限cleanup入口。unknown-held/poisoned session保持fail closed，不能借reserve清空未知结果。
- Gateway前置阶段为有界auth/body/session识别，再分类执行permit；body仍用原MaxBytes/read deadline，前置parser容量也有界。不能在先耗尽general执行permit后才查看owner。
- trusted worker私有响应新增版本协商的`session_state {owner: bool, sequence: u64}`，由coordinator在native settled点生成，包含成功与terminal SQL error响应；Constraint可能仍owner=true。
- Gateway必须严格校验worker、request_id、upstream session、placement binding和单调sequence，更新opaque registry后才结束该RPC；public请求不能设置/覆盖session_state。
- 当前`sql_upstream_codec.go`严格只收5/6个envelope字段，Execute/Query result也严格解码；新hint需显式同步worker protocol、Go codec/SQLRPCError和gatewaySession，不可直接加字段假装旧decoder兼容。
- 超时/丢失响应不推断owner=false；session继续poison且permit retained。stale hint只影响资源分配，route/auth/epoch guards仍重验；旧worker未协商此协议则拒绝启用完整profile。
- shared public ingress若新增有限总G，应在原G内留owner/cleanup执行空间，普通已派发请求最多G减reserve；前置TLS/auth/parser另有有限容量，不能让普通慢请求永久占据分类入口。
- 现有单listener在任意连接洪泛下不能证明保活；部署需受控前置容量或独立有界且同等auth的continuation通道，且总预算仍守原G。没有此条件时仅声明正常有界admission下的进度。
- 锁序为auth/有限解析→短registry/hint快照→TryLock session→资源permit→route/auth复验→dispatch；不持Gateway全局mu或admission锁进行upstream IO/等待native锁。

### 选项B与作者scope
- 若保持Go完全不变，Rust FIFO仅能改善已进入worker的竞争；必须删除端到端holder-progress保证，不派DEV以“接口已闭合”为前提。不能把Gateway429改成ACK/自动retry解围。
- 另一候选是writer竞争时立即独立AdmissionConflict而不占长队列槽位；它缩短资源占用但仍不保证公共holder入口，也不解决mixed必须全成功的要求。
- Rust作者scope：coordinator/session/cleanup/worker reserve及私有响应；Go作者scope：`platform/admission.go`、`gateway{,_http,_sessions,_routing}.go`、`sql_upstream{,_codec}.go`、必要service ingress及各自测试；不是P5 controller改动。
- 协议/配额需一个集成作者统一审scope与版本激活，未来各自隔离开发；本次不创建分支或实现。共同oracle需Q=4时三general阻塞仍允许可信owner提交、close贯穿两层、general不得用伪hint占reserve。
- 还需rate耗尽、多DB owner、quota=1、恶意body/stale hint、成功/Constraint hint、response丢失、renew/move与unknown retained测试；有界drain后原ledger/balance/SQL-prefix oracle不变。
