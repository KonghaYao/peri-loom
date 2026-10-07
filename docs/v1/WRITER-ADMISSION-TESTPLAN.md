# Writer admission：native 作者测试计划

本文是作者阶段的测试计划与过程记录；其中待执行表述不代表最新门禁状态。
当前实际独立执行结果、覆盖范围和限制见 `WRITER-ADMISSION-VERIFICATION.md`。

## 状态与独占范围

1. 本批是测试作者工作，不是最终独立验收。baseline 为 `de7aec992b1b66ce0fb8be34b72a9f95b8161834`，契约来源为 `WRITER-ADMISSION-DESIGN.md`；R7 `0f8f991` 的真实 mixed FAIL / matrix PARTIAL 不修改、不回填原因。
2. James 只写 `tests/writer_admission.rs` 及新 helper、适配既有 `busy_wait.rs`、`busy_tuning.rs`、`concurrent_sessions.rs` 与本文。生产 API/hook 由 A 冻结；C 独占 `writer_worker_rpc.rs` 与 worker RPC，B 独占 Go，F 最终独立门禁。
3. 当前已落 18 个新 integration test 与 13 个既有测试函数的契约适配；这些是源码函数数，不是执行 PASS 数。8 个 Rust 文件通过本作者 `rustfmt --edition 2021 --check`，既有三文件通过限定路径 `git diff --check`；没有 Cargo/test 结果。聚焦 Cargo 由 A 唯一协调，完整 gates 由 F 执行；不自行编译、clippy/release，不发送 live API/SQL/S3、压力或信号。

## 原则与所需接口

1. 每个业务 SQL/batch 只调用一次；发生失败后若有新调用，必须是明确的显式事务控制或新的业务 ID，不重放原业务。
2. 公共 RemoteDatabase/RemoteSession 测试必须依赖真实 coordinator/native 状态。FIFO 顺序需只读队列观察 hook 证明 ticket 已入场，不能仅凭线程启动、sleep 或本地 barrier 声称 FIFO。
3. A 已确认接口：`AdmissionError::{Conflict, QueueFull, Deadline, Closing, Quarantine}`、`AdmissionStatus { owner, queued, quarantined }`、`RemoteDatabase::writer_admission_status()`、`RemoteSession::admission_id()` 与 `RemoteSession::close()`；queue 容量为 8。close 先取消/唤醒，再等待确认 native cleanup，不能把仅请求关闭当 release。没有 unsafe admission bypass；无法从公共层构造的 native 原 Busy/异常 cleanup 保真由 A 在私有 unit 层证明。
4. prefix、Constraint 与 BusySnapshot oracle 核事务状态、已提交/未提交行、fresh session 和 integrity，不把 get_auto_commit 单独当作成功清理证明。确认 release 必须有后继真实 SQL 进展或 native 观察，而不是超时推断。
5. 默认 5s 从 call 入口覆盖 session lane、prepare、queue 与整批执行；短预算覆盖前缀等待而不重置。AdmissionDeadline/Conflict/QueueFull 使用实际冻结类型，不伪装成 native Busy。

## 公共层候选用例

1. 持交互 writer，独立 reader 读已提交快照；多个新 writer 确认按 ticket 顺序等待。holder 的 COMMIT/ROLLBACK 不排在 waiter 后，完成后各 business ID 只产生一次 ledger 效果及完整性通过。
2. BEGIN DEFERRED 无读取快照的首写允许 FIFO；已读快照的 promotion 在其他 owner 存在时立即 AdmissionConflict 或 native BusySnapshot，不长排队、不重开事务。没有 owner 但 snapshot 已过期时要求真正 typed BusySnapshot、原事务/快照保留，显式 ROLLBACK 后看到新状态。
3. writer 事务内 Constraint 后保留实际 owner；reader 仍看不到未提交行，waiter 不提前进入。ROLLBACK TO/RELEASE 不错误释放外层 writer；显式 COMMIT/ROLLBACK 才确认释放，失败业务不 replay。
4. 单次 `COMMIT; BEGIN...` 的 prefix 已提交后重新回 FIFO，不跳过旧 ticket。suffix 的 admission deadline/conflict 不抹去 prefix，不捏造独立成功 HTTP ACK，fresh session 只见确切已提交行。
5. Query 写 `RETURNING`、触发器写、DDL、数据库直连 execute/query 与 RemoteSession 共用 coordinator；按编译/native 写语义而非 API 名或 SQL 正则判断。ReadOnly role/native flag 阻止写且不占 writer owner，不能把 mock role 说成真实 Authority grant。
6. 40ms/更短预算、默认整批 5s、等待同 session mutex 与 prepare 的延时计入同一预算；队头 timeout/cancel 后后继实际进展。先确认真实入队，再触发 mock revoke/expiry，要求有界失败、无新增 wrapper 物理 I/O 及 ledger 无失败行。
7. 填满有界 queue 后下一个请求 typed QueueFull、不派发 SQL；移除队头后 FIFO 不破坏。已确认 drop/close cleanup 释放后 waiter 进展；未确认 native cleanup 必须 quarantine，不授予后继 writer。
8. maintenance 在活跃 writer/queue 情况下有界失败或遵循冻结 gate 契约，不持 DB write gate 反等 holder；holder 仍可结束事务，随后 maintenance 与新 writer 进展，不能靠超时假称已清理。

## 旧测试契约适配与保留证明

1. 旧公共同 DB 竞争的 typed Busy 断言可能转为 typed admission deadline：这是新契约，不把新错误改名为 Busy，不只延长测试 timeout 取绿。默认 5s queue 与 native 4s cumulative backoff 分开标注。
2. 旧 BusySnapshot、Constraint retained transaction、prefix rollback、短预算与 local mock fence 的语义证明保留；原 native Busy 传播/4s backoff 如公共队列已遮蔽，明确移交 A 私有 unit，不静默删除证据责任。
3. 旧 prefix pause hook 需按新 statement admission 时序确认，不能再在已持 DB operation gate 时制造环等待。若旧 hook 阻塞点不适用，先与 A 对齐只读/受控观察 seam，再修改作者测试。
4. C 的 worker 入口 owner/cleanup reserve 与 B 的 Gateway v2/配额分割是独立覆盖；公共 native 测试不能宣称替代两层入口饥饿、unknown-held permit 或真实 owner lease/finalCAS 验证。

## 证据、限制与冻结要求

1. Local lease 测试只有明确的 mock check/clock/revoke，wrapper 计数不是 S3 completion oracle；没有跨进程、Authority namespace barrier 或实际 takeover 结论。
2. 只报告实际测试命令、执行者、结果数、日志和 source SHA。本文最终应补路径/SHA、测试方法及未覆盖项；没有执行记录的项仍为计划，不写 PASS。
3. 每个测试/helper 小于 500 行，无 inline comment；只用 apply_patch 编辑，不 commit/push，不触 user CLAUDE 或冻结历史报告。

## 作者执行准备与观测依赖

1. 新入口 `prototypes/v1/tests/writer_admission.rs` 包含 3 个测试；`writer_admission/transactions.rs` 5 个、`queue.rs` 6 个、`lifecycle.rs` 4 个；`support.rs` 仅共享 fixture/oracle。既有入口 `busy_wait.rs` 6 个、`busy_tuning.rs` 5 个、`concurrent_sessions.rs` 2 个，保留 Constraint、BusySnapshot、prefix 与 ledger 语义证明。
2. A 唯一作者聚焦命令候选：`cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test writer_admission --test busy_wait --test busy_tuning --test concurrent_sessions -- --test-threads=1 --nocapture`。当前实现仍由 A 落地，命令尚未由本作者执行；失败不能通过删断言、改 deadline 或重放业务掩盖。
3. FIFO 直接观察 actual owner/queued ID，再用 native `admitted_order`、ledger、余额及 integrity 交叉核对。prefix hook 只在 owner 真实 Some→None 转变后暂停，且 fresh reader 必须看见已提交 prefix，再引入另一 holder；不使用 statement-count 或 sleep 推断 release。
4. 同 session mutex 预算测试的命名线程 gate 位于 mock `validate_transaction`，要求实际 statement 入口在 native lane mutex 后调用该检查，包括纯 SELECT；该 seam 的时序必须与 A 实现核实，不能把入口前暂停假称 mutex 持有。
5. ReadOnly 用本地 mock role 打开实际 native readonly connection，不是实际 Authority grant。wrapper 物理计数仅计 get/put/delete/list 调用，不能证明远端物理完成。异常 cleanup quarantine 与原 native Busy4 传播等待 A 私有 unit 证据；本文不把公开 admission deadline 替代为上述证据。

## 必须补齐的 late/failure cleanup 闭环

1. 主控报告 native 四项实际 PASS，仅作为主控执行的阶段证据；本作者未执行或独立核验。late/failure cleanup 尚未闭合，不能推导 core 整体 PASS。以下是必须验证的验收条件，不是已执行结果；现有 queued local revoke mock 不能替代已经派发的 PendingIo/revoke 闭环。
2. 分别控制真实 native I/O 已派发但尚未 terminal、late completion、I/O failure 与随后 revoke 的次序。记录 native statement/transaction、owner/queue 及 pending-resource 状态；观察不到真实阶段时由 A/Mill 提供安全 oracle，不用 timeout、coordinator owner 字段或 mock callback 单独假称 native stopped/released。
3. `PendingIo`、`FinalizationAlreadyStarted` 或无法证明 Stopped 的 cleanup 必须保留 original negative/source，并按已确认的内部 `UnsettledSqlOutcome` 标记区分当前 UNKNOWN 与后续 Quarantine。不得报告 completed、成功 close ACK 或授予后继 writer；RPC flags/hint 与 close ACK 的验证由 C 独占，本文不声称公共 native 测试已覆盖 wire。
4. 必须给出 retained statement、buffer、pending callback、cleanup task 与 queue 的明确容量及饱和拒绝条件，并用可观测计数证明连续失败不无界累积，不以 RSS 推断资源正确性。定义最终可释放条件：已派发 I/O terminal，native stopped/finalization/rollback 或 close 得到确切确认，且 native writer/read lock 与 owner 清理一致；条件未满足持续 quarantine，不得强制 advance。具体 cleanup 机制及跨撤销后的安全退出途径仍待接口确认，不能把永远保留称作已闭环。
5. late completion 或 cleanup failure 不得 reset、reprepare、重放业务 SQL、重开事务，或为原调用生成新 deadline；原 call 入口预算耗尽后不能转 success。若已 COMMIT prefix，fresh reader/ledger 必须保留其确定效果；不确定 suffix 保持原 negative/UNKNOWN，禁止按超时推断 rollback 或擦除 prefix。
6. revoke 前后分别核对 no-new-physical-I/O 边界与既有 I/O 的 terminal 状态；既有合法派发可能晚完成，不声明物理 deadline cancellation。只有安全确认后才能解除 retained resource；解除本地保留不等于恢复已撤销 lease，新业务需新的合法授权与业务 ID，不能复活旧调用。
7. unsupported native/admission profile 必须在业务 SQL effect、writer ticket/owner 授予和物理 mutation 之前拒绝。用 unsupported profile 的明确 negative、前后 ledger/schema 与 dispatch/mutation 计数证明；fixture 初始化/只读探测另记，不将其当业务 effects，亦不添加 unsafe bypass 让测试取绿。
8. 实际 native owner 的证明必须跨过 Constraint、交互事务仍存活及 queued successor 尚未执行三个阶段；holder COMMIT 不被自身 waiter 阻塞，确认 native release 后旧 tickets 按 FIFO 进入。最终核唯一 ledger、余额、fresh session 与 integrity；coordinator 状态只是交叉 oracle，不能替代 native writer/transaction 状态或真实后继 SQL 进展。
9. A/Mill 负责安全 native oracle 与异常清理私有证据，C 负责 worker wire，James 只在既定公共测试 scope 适配经确认接口。新增用例、原失败、执行者、命令及 source SHA 必须分别记录；只有实际 late/failure/revoke 闭环完成且 F 最终独立门禁完成后，才能讨论整体验收。本次不修改 Core/worker 测试，不运行 Cargo。
