# Writer admission：两个安全 blocker 的静态结论

本文保留早期设计裁决与当时的 blocker 状态，不代表当前候选仍处于该阶段。
后续 Native API、作者接入和独立验收结果见 `WRITER-ADMISSION-VERIFICATION.md`；
不得仅凭本篇的早期建议或后续单项测试推出完整项目验收通过。

## 状态
- Proposal，非实现/测试结论；仅新增本文档，不改其他设计/源码，不build/test/API/load。
- 本次只读root HEAD为`de7aec992b1b66ce0fb8be34b72a9f95b8161834`；依据本地Turso源码，不推断运行中作者或部署二进制已包含以下方案。
- coordinator纯状态机可独立开发；涉及提前结束Statement和readonly bypass的接入仍是blocker，不扩展owner保证。

## 1. reset/Drop不是通用abort
- `prototypes/p0/turso/core/statement.rs:1478`：public reset调用reset_internal；`:1569`附近先wait pending IO，IO drain不是“尚未提交”证明。
- `:1582–1599`：Running＋Normal＋change_cnt_on＋has_returned_row进入`halt(..., 0, "", None)`，直接完成RETURNING的成功finalization。
- RETURNING在首个Row前可能已完成全部DML；剩余scan-back不能被当成“仅有结果、没有事务副作用”。
- `:1774` Statement Drop调用reset_best_effort；reset失败只日志，且仍可能已做成功halt。预算/row cap/localfence错误后普通`?`退出会触发该Drop。
- 同文件`:464` Statement.interrupt仅设state；`vdbe/mod.rs:566` Interrupting仍被is_running判true，因此`statement.interrupt(); reset()`仍满足RETURNING分支。
- `connection.rs:4863` Connection.interrupt只是active-root标志；reset直接调用halt，不走正常step的periodic interrupt判断。**interrupt＋reset不保证rollback。**
- `vdbe/mod.rs:1957` Halt开始后忽略新connection interrupt/deadline/progress请求（已有state interrupt另论）；`:2326`检查按指令countdown发生，不保证下一个opcode前立即abort。
- 因而`interrupt(); step()`也不能在此静态审查中承诺“绝不跨入Halt”；请求已进入finalization时更不能以取消撤销既成结果。

### 安全动作与接口界限
1. 区分：已Done；native自身terminal error；adapter提前停止的仍Running/IO/Row；已开始finalization；结果不明。不能仅以error或is_in_write_tx两个bool判断。
2. 普通native Constraint/FAIL/Busy保持原native错误处理，不额外全事务rollback；Constraint retained tx保留writer owner，已COMMIT prefix永远不回滚。
3. adapter停止时，必须在局部持有Statement处拦截错误，再选择明确cleanup；不能先unwind/Drop再诊断，不能resume结果扫描来“清空RETURNING”。
4. 没有安全abort入口时，最小fail-closed是把未终结Statement移交同session受控quarantine存储、保留connection/permit；禁止该session继续SQL，不能让该Statement再次隐式Drop。
5. 该park需有界容量和明确后续cleanup；它只是安全隔离，不是可交付的无限泄漏策略。保留running Statement期间不能直接执行另一ROLLBACK，避免两个root/cleanup状态干扰。
6. Connection.close（`:2439`）有本地rollback，但末尾（`:2472`）可能checkpoint_shutdown；它本身不能证明随后Statement Drop不会再走halt。不要以close→Drop作为已证明安全配方。
7. 显式close只可在与执行互斥后进入经验证的abort/close流程；lease失效不能触发新flush/commit，pending backend uncertainty继续P4 block，失败/quarantine不释放permit。
8. 不允许fork补丁时：不能宣称支持任意writer RETURNING的预算中断；可明确拒绝未支持profile中compiled write＋结果列的语句（第一次step前），这是公开能力收缩，需主控批准，不能悄悄实施。

### 推荐最小fork seam（需独立审批/验证，不能只加观察getter）
- 新`Statement::abort_noncommitting() -> AbortProgress`，明确跳过“RETURNING成功halt”分支，复用原pending-IO settlement和Program::abort事务规则，不改正常reset/Drop语义。
- `AbortProgress = PendingIo | Settled(AbortDisposition) | FinalizationAlreadyStarted | Unknown`；Disposition区分StmtRolledBackTxnRetained、TxnPoisonedRetained、TxnRolledBack、NoTxn。
- fork入口必须在内部状态仍Running时选择原abort规则，再设置可靠terminal/cleanup标记，保证之后Drop不会重复成功halt；不伪造Interrupt错误以强制全事务rollback。
- `vdbe/mod.rs:3389`原abort无err且unfinished explicit writer、无statement-savepoint时会mark_tx_poisoned；`:3602`依TxnCleanup处理。复用该语义，不能承诺所有显式txn都能继续COMMIT。
- 有statement-savepoint可回退当前statement；没有时明确poison并要求caller显式ROLLBACK/close，不暗中提交，也不无条件撤销整个交互事务。
- drain已经发出的IO可能完成已开始提交；FinalizationAlreadyStarted须保留原native completion/commit结果和may_have_committed/unknown，不返回“取消所以未提交”。
- 此入口是受控取消/清理API，超出纯read-only observation hook；若未获准或未证明，应继续block接入，不能给A“interrupt＋reset足够”的结论。

## 2. 前置readonly不能跨内部reprepare作永久证明
- `statement.rs:634` prepare_step可能因prepare-context不匹配reprepare；`:690` finish_step对SchemaUpdated reprepare后立即`program.step`，调用者未获重新分类机会。
- `:1060`附近reprepare调用reset_internal并换new_program；admission必须绑定执行program版本，而不只绑定原SQL/原Statement指针。
- `vdbe/mod.rs:1916` PreparedProgram.is_compatible_with只比较prepare_context；schema-cookie/shared schema更新另走native流程，**它不是完整schema-validity检查。**
- `vdbe/builder.rs:1203`按每个Insn聚合readonly；`vdbe/insn.rs:2379`写opcode含Transaction Write、Sequence*、JournalMode、Checkpoint、trigger子program等。
- `translate/sequence.rs:4`明确nextval写backing table；SELECT词头不保证只读。支持builtin/view/sequence的schema替换可能改变编译依赖，禁ATTACH/extensions不能排除它。
- 具体“readonly view重定义为含nextval后同SQL重编译”的反例需fixture证明合法性，本次未运行，不能冒称已复现；已确认的是API没有readonly稳定性保证。
- 写program已有permit时内部reprepare仍需检查类别/维护模式；最危险是无permit read路径在内部换为write并立刻执行。
- 查询后观察`is_in_write_tx/holds_write_lock`已经太晚，无法撤回首次写锁/副作用；单独增`schema_generation()`getter也有检查后变化的TOCTOU。

### 支持模式与推荐execution gate
- 明确限定既有legacy native WAL、non-MVCC、禁ATTACH/extensions/journal-mode切换；不在本patch新增MVCC、autonomous sequence或隐藏维护模式保证。
- 此profile仍允许schema变化和PRAGMA/sequence依赖，故**不能仅靠上述模式限制证明一次readonly检查充分**；完整SELECT bypass目前仍block。
- 固定literal/plain table SELECT小subset可另审，但需要同native AST限制view、函数、sequence、虚表、PRAGMA等和schema稳定条件；不是当前完整SQL能力的替代证明。
- 推荐新`Statement::step_with_admission_check(callback)`，callback读取本次实际PreparedProgram及非secret generation/token，返回Allow或AdmissionRequired；既有step路径默认Allow。
- hook必须覆盖prepare_step reprepare和finish_step每次reprepare之后、任何新program opcode执行之前；触发AdmissionRequired则返回专用非terminal暂停结果，不伪装Busy/SchemaUpdated或自动abort。
- adapter在native外有界申请permit，重进hook再次检查当前program；callback不持native/DB-global锁等待，不以缓存分类或原SQL字符串授权。
- hook只读元数据，但“可暂停执行”是显式API扩展；必须证明未改变PC/参数/原txn/错误规则，不能以观察hook名称掩盖算法改动。
- 初始writer/admitted owner Allow；readonly仅在该次program检查成立时bypass；read→write时带原snapshot进入promotion规则，不auto reopen/replay。
- 准备/内部helper若本身可能写，必须一起识别或在profile中明确拒绝；只拦root.program一次不能冒称覆盖全部native隐式路径。

## 必须独立证明的oracle（本次不运行）
1. UPDATE/INSERT RETURNING首Row后row cap、budget、localfence触发：abort前后ledger/balance符合声明，零新commit；先前已COMMIT prefix保持。
2. pending WAL/commit IO前、中、后取消：区分可abort、finalization-started和unknown；Drop不二次halt，owner不提前release，P4 namespace block保留。
3. 显式txn原Constraint语义保持；adapter取消有savepoint回退statement、无savepointpoison需显式rollback，不能被native后续COMMIT误接受。
4. 无RETURNING Running writer、pure read、Done statement、park/close失败都覆盖；所有资源计数、IO、native writer锁在确认settled后才释放。
5. 受控schema-cookie/reprepare发生在queued wait、首次step及内部SchemaUpdated循环：每个换program在opcode前经过hook，readonly→write先admit且零重复prefix。
6. BusySnapshot promotion、CDC/PRAGMA/sequence/trigger/EXPLAIN按声明profile验证；支持不足则显式拒绝，不扩大合同猜测。

## 给A的接入结论
- 不批准以interrupt＋reset/Drop作为通用abort，也不批准is_compatible前置检查作为跨reprepare的永久admission证明。
- 可以继续coordinator纯状态机；安全native abort与execution-gate契约闭合、独立oracle通过之前，不接入完整SQL生命周期。本文不改变其他作者scope或Go设计。
