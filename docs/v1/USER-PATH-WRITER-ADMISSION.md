# Writer admission：用户手工验收路径

## 证据与范围

1. Parent 冻结批次已由 F 独立 ACCEPT，提交 `09c478be663fff8c90da5bcc0dc697b378d17183`；依据为 [WRITER-ADMISSION-VERIFICATION.md](WRITER-ADMISSION-VERIFICATION.md)。报告记录 Rust 391 PASS / 0 FAIL / 5 ignored、Go normal/race 各109 root PASS / 2 SKIP、Python26 PASS及主控实际S3五项通过；重复运行不累计为新用例。
2. 本页是该批次后的**手工操作说明，以下各组未按本页运行**，不是新的用户路径、UI/SDK矩阵、部署、FIFO公平性或压力PASS。历史失败及watchdog未证明的归因保留在Parent报告；R7 mixed FAIL / matrix PARTIAL不被功能验收覆盖，R8独占实测由另行报告给出。
3. 本页不使用当前压力fixture。未来操作者须获授权，使用自己新建的私有目录、空闲端口、项目及数据库；每组换新库和业务ID。采集binary/source SHA、非秘密request ID、公共终态及ledger/integrity，不输出credential、Authorization、connection全文或私有worker hint。

## 一、启动显式profile的TLS实例

1. 沿用 [USER-PATH-OPERATIONS.md](USER-PATH-OPERATIONS.md) 的源码准备；未发布gitlink取不到时联系维护者，不修改gitlink/vendor掩盖。在仓库根目录设置 `PROJECT_ROOT="$PWD"`，由主控提供匹配已验收源码的绝对路径 `RUST_BIN`、`PLATFORM_BIN`，记录SHA；构建需另行协调。
2. 获授权后设置空闲私有 `LISTEN`（如 `127.0.0.1:<已确认空闲端口>`，不是可原样运行的地址），创建新目录并在前台启动：

   ```sh
   umask 077
   RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/peri-writer-path.XXXXXX")"
   test -x "$RUST_BIN" && test -x "$PLATFORM_BIN" && test -n "$LISTEN" || exit 1
   env PERI_PROJECT_ROOT="$PROJECT_ROOT" \
       PERI_RUST_BINARY="$RUST_BIN" PERI_PLATFORM_BINARY="$PLATFORM_BIN" \
       python3 prototypes/v1/platform/dev.py --run-dir "$RUN_DIR" \
       --backend local --tls --listen "$LISTEN" --no-build --writer-admission
   ```

3. 等真实READY，核配置 `writer_admission` 与匹配binary；`--writer-admission`使launcher给worker设置 `PERI_WORKER_WRITER_PROFILE=1`。独立worker部署需显式设置该环境变量；0或未设置关闭，其他值拒绝。已有run-dir不允许静默改profile，不通过重启旧目录覆盖配置。
4. 沿用 [USER-PATH-SDK.md](USER-PATH-SDK.md) 的Go私有loader、`sdk.NewClient`及正常RootCAs/hostname验证。凭据从受控私有配置加载，不放argv或源码；不使用TLS验证绕过。S3变体须另获授权，确认已有测试bucket及Authority环境凭据后才改 `--backend s3`，本页不创建bucket或给示例密码。
5. **预期/证据/STOP**：记录READY、profile、SHA与正常TLS握手；缺失或不匹配即停。已读launcher仅向Authority加入S3环境，worker采用allowlist/RPC/KMS环境且不传backend argv；实际配置权限及worker无backend keys由授权隔离审查确认，不能仅凭READY自证。

## 二、配额与两个独立session

1. 按既有平台管理路径为新项目设置至少2个session slots、`MaxConcurrent=4`、`RequestsPerSecond>=2`。Gateway内部general最多3、可信owner最多1，总active仍不超过4，含unknown-held；RPS在原额内分general R-1、owner1，不增加总配额。
2. 启用profile时Gateway自动做私有worker能力协商，用户不调用或编造公开profile API。worker的sqlTotal=maxInflight-controlReserve至少3，general=total-2、owner1、cleanup1，control独立；两层容量不混写。eligible Open的probe失败可为503 writer_profile_unavailable，其他Open错误按实际类别保留。
3. 使用两个独立 `Client.Open` session A/B，每session最多一个outstanding调用。调用 `Session.Execute/Query/Close` 时生成唯一request ID，每个业务只调用一次；不得跨独立CLI `sql` 命令拼事务，也不得提交public owner/hint字段来抢reserve或绕auth/route/epoch。
4. 低配 `MaxConcurrent=1` 或RPS1走legacy边界，没有holder reserve保证；不自动扩大到C2，不把竞争失败称作违反C4保证。该检查用另一个项目，勿中途改正在运行的配额。
5. **预期/证据/STOP**：保存实际quota、Open终态与request ID；不符或非预期拒绝即停。429是入口限额，不冒称nativeBusy/AdmissionDeadline。wire runtime caps与trusted直接library路径分开，用户无需自证私有状态。

## 三、纯读与holder COMMIT后的writer进展

1. 新库用一次Execute建 `ledger(id INTEGER PRIMARY KEY, delta INTEGER NOT NULL)`、`account(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)`，插入account(1,1000)。A执行一次 `BEGIN IMMEDIATE; INSERT INTO ledger VALUES(101,10); UPDATE account SET balance=balance+10 WHERE id=1;`，确认终态，不提交。
2. B在无显式事务的独立Query中核ledger为空、余额1000。确认完整读终态后健康Close B，再Open全新B作为contender，避免沿用读事务snapshot；另起goroutine让新B单次Execute：`BEGIN IMMEDIATE; INSERT INTO ledger VALUES(102,20); UPDATE account SET balance=balance+20 WHERE id=1; COMMIT;`。
3. A在自己的独立调用中COMMIT一次，收集A/B终态。仅在健康已知终态后Close两session，fresh reader核101/102各一次、余额1030、`PRAGMA integrity_check`为ok。
4. 若使用已BEGIN且已读取的B，应走下一组snapshot promotion，不无条件期待FIFO。纯读依据实际compiled program，不依据SQL词头或Query方法；SELECT中有可写依赖也不能直接宣称bypass。
5. **预期/证据/STOP**：两次ACK、公共行/余额/integrity；任一不符即停、不重放102。goroutine launched不证明native已入队；本组是进展烟测，确定性FIFO由作者actual queue/native oracle证明，不要求用户调用私有接口，也不以网络响应顺序证明公平性。

## 四、snapshot、Constraint与显式ROLLBACK

1. 本组各场景另建新库，先沿用第三组第1步的schema/account初值。验证B `BEGIN DEFERRED`且尚未读取snapshot，首写可等待A释放；A结束后B一次新业务得到明确终态并显式结束事务，不重放失败业务。
2. snapshot组让B `BEGIN`后先SELECT建立快照，再让A持writer；B一次promotion预期明确负终态，不长排队、不自动reopen。公共 `sql_error`不带typedAdmission/BusySnapshot详情，确切native事务/旧视图由独立oracle补证。
3. Constraint组A执行一次 `BEGIN IMMEDIATE; INSERT INTO ledger VALUES(301,10); UPDATE account SET balance=balance+10 WHERE id=1;`，再单次 `INSERT INTO ledger VALUES(301,10);` 触发主键错误。只有已知completed且session仍健康才继续显式控制；native owner跨Constraint保留及savepoint不误释放外层事务由Parent测试证据提供，不让用户自证private internals。
4. A显式ROLLBACK一次，再由B单次执行 `BEGIN IMMEDIATE; INSERT INTO ledger VALUES(302,20); UPDATE account SET balance=balance+20 WHERE id=1; COMMIT;`；fresh reader只见302、余额1020、integrity为ok。snapshot组也仅在已知健康终态后显式ROLLBACK，再以新业务ID建立新事务，不重放原INSERT。
5. **预期/证据/STOP**：原公共negative、控制终态及fresh oracle。eligible已知SQL错误为400 sql_error/completed=true/may_have_committed=true，且SDK outcome_unknown=false；这不证明自动rollback。falsecompleted、EOF、坏schema或session已poison即STOP，不继续ROLLBACK/Close。

## 五、committed prefix、RETURNING与整批预算

1. 本组各场景另建新库，先沿用第三组第1步的schema/account初值。单次Execute `BEGIN; INSERT INTO ledger VALUES(401,1); UPDATE account SET balance=balance+1 WHERE id=1; COMMIT; BEGIN; INSERT INTO ledger VALUES(402,2); INSERT INTO ledger VALUES(402,2); COMMIT;`。仅健康已知终态时显式ROLLBACK suffix；fresh reader必须保留401、不含402、余额1001、integrity为ok。禁止整batch或任一失败业务自动retry。
2. 在另一新库先Execute `CREATE TRIGGER ledger_balance AFTER INSERT ON ledger BEGIN UPDATE account SET balance=balance+NEW.delta WHERE id=1; END;`，再Query一次 `INSERT INTO ledger VALUES(501,10) RETURNING id,delta;`。明确完整成功应返回501/10，fresh ledger只有501、余额1010。Query写、trigger及DDL受实际compiled写分类，不因方法名绕admission；readonly grant拒写。
3. prefix后suffix重新排队、row cap首Row后清理、prepare/lane等待等确定性预算场景由授权作者oracle验证，不通过用户长递归/大结果做压力强造timeout。首Row不是Done或COMMIT ACK，cap错误不证明全部效果已回滚。
4. 默认whole5s从call入口覆盖sessionMutex、prepare、queue与batch，不每statement重置；nativeBusy4是累计backoff，不是FIFO/wall上限。客户端较长timeout只留RPC余量，不生成新服务器deadline；cooperative界不是硬抢占或物理IO取消保证。
5. **预期/证据/STOP**：公共prefix/RETURNING行、fresh ledger/余额与原negative。出现UNKNOWN或oracle不符即停，不replay、不按期限推断suffix rollback；细节由native清理证据核验，不将读回转成原UNKNOWN的success。

## 六、UNKNOWN、queue与清理限制

1. 对outcome_unknown、session_moved、context到期或缺失/损坏终态立即STOP：不retry、自动ROLLBACK/reopen或盲Close；保留安全request ID与公共终态交主控。旧业务不能因新session可读而重新提交。
2. 用户不得把Close请求已发送或等待结束当成功ACK；只有完整成功终态才算Close完成。未确认cleanup的namespace/admission范围保持fail closed，不能自己解除quarantine、复活旧lease/session或扩大为所有tenant失效。
3. queue8仅限native writer tickets；C4入口不能同时证明排满8。第9个QueueFull、队头取消后FIFO等由足够session/入口quota的专属作者测试证明，不在当前共享fixture追加试验。入口429和nativeQueueFull分开记录。
4. late/failure已有IO可晚完成；originalnegative保留、不reset/replay/重置预算、最终terminal/native清理确认的生产证据引用Parent报告。queue8或onecleanupLeader不证明all-library内存硬bound/actorjoin；pending/hooks Vec及trusted `RemoteDatabase.session()`绕runtimecaps的限制不能省略。
5. unsupportedprofile须在业务effects前拒绝；合法BEGIN内部emptyTEMP与用户真实TEMP/attached/MVCC的拒绝边界按最终native合同及Parent oracle区分。不从关闭worker profile推导取消native安全限制，不承诺未审SQL模式支持。
6. **预期/证据/STOP**：用户保存公共失败/flags，不发额外探测SQL取伪成功；多peer/failure、retained/锁终态、实际join/panic由独立oracle提供。没有确切完成证据记UNKNOWN/BLOCKED，不写clean/PASS。

## 验收记录与后续

1. 每组记录执行者、匹配binary SHA、实际操作顺序、request ID、预期与实际公共终态、fresh ledger/integrity；执行失败保留，不以retry或临时提高配额/预算覆盖。秘密与任意用户SQL内容不进入公开记录。
2. 本页新增profile/quota、snapshot与compiled写分类、whole5s及清理边界；既有 [USER-PATH-CONCURRENCY.md](USER-PATH-CONCURRENCY.md) 的no-replay/prefix规则保留。Parent ACCEPT只复用报告已验证范围，本页未运行不计新增覆盖。
3. 新页尚需F独立文档review后由主控决定提交；UI/SDK新矩阵、当前R8压力、性能与最终联合结论必须来自各自实际报告。本页不修改主索引、PROGRESS或既有报告，也不授权并发负载、构建或故障操作。
