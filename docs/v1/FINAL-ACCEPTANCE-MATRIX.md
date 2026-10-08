# 最终验收矩阵：历史完成与当前证明分开

本次仅对照已提交实施清单与独立报告更新矩阵；未 build/test/网络/API/SQL/读取凭据，不改PROGRESS。状态为本文冻结快照，不是全项目最终 PASS。
“历史完成”只适用于原报告冻结输入；任何后续生产源码改变都须新 gate，不能继承旧 artifact SHA 或把未运行项计为 PASS。
计数按报告的 unique scope引用，不把 normal/race、重复执行、helper/selfspawn或跨 harness共享 oracle相加。

## 1. P0–P6 实施/原始验收与现阶段覆盖

| 范围 / 原始要求 | 原始完成证据 | 当前可用 proof与限制 |
| --- | --- | --- |
| P0：分段、generation、lazy open、checkpoint/重启 | `prototypes/p0/VERIFICATION.md`；原36测试；`01-p0-core-prototype.md` | 后续 native安全/TEMP有独立子批；不是当前整个 Turso workspace重新全测。 |
| P1：远端唯一authority、删cache冷恢复、integrity/eviction | `P1-VERIFICATION.md`：63常规+2 RustFS live | 当前另轮S3含fresh-cache SQL recovery与object-store conformance；R16 whole11为独立smoke，不代替全P1 suite或所有冷故障覆盖。 |
| P2：metadata-only branch/snapshot、共享clone、PITR/GC | `P2-VERIFICATION.md`；121常规+3 live，见 `IMPLEMENTATION.md` | 当前S3包含refs/PITR/GC与empty-worker-cache case，R16另有snapshot/restore/readonly clone；非全历史P2矩阵重跑。 |
| P3：多DB、idle卸载、真实kill/cache删除、lazy迁移 | `P3-VERIFICATION.md`：165常规+4 live；`P3-ACCEPTANCE.md` | 当前S3 multi-DB/unload/fresh-cache case及R16 migration/suspend/resume通过；非全部SIGKILL、冷故障或大DB lazy读量证明。 |
| P4：epoch/lease、stale WAL/HEAD fence、退出quiescence | `P4-VERIFICATION.md`：254常规+5 live，原修正版PASS | 当前full Root含P4 25 PASS，另轮S3 fence通过；test completion合同接受，不证明新增产品completion barrier或历史失败根因修复。 |
| P5：registry/placement、失联/failover、graceful migration | `P5-VERIFICATION.md`：62顶层、1 helper skip，normal/race通过 | 历史8实际进程故障场景已签发；当前R16 lifecycle子集通过，不是重跑所有失联/reconciliation场景。 |
| P6：gateway/auth/quota、KMS/encryption、SDK/UI、metrics/部署 | `P6-VERIFICATION.md`：原冻结Rust/Go/live SDK/UI/Linux接受 | 新Go SDK4/UI功能、后阶段own-lock cleanup、Linux Local overlay与common分别接受；`FINAL-JOINT-DELIVERY-VERIFICATION.md`限定prototype功能/source audit已接受，非production或全项目PASS。 |
| 原联合：跨阶段native+S3 cold/fence | `JOINT-VERIFICATION.md`：325 unique native PASS/5 ignored；复用明确列出的P6证据 | 历史功能PASS保留；该fault fixture后续退出1明确非clean，且不覆盖后来的observer/client生产源码。 |

## 2. 最新增量证明与当前门禁

| 项目 | 已证明 / 当前状态 | 未证明或下一门禁 / 负责方 |
| --- | --- | --- |
| Native safety/TEMP、writer profile | `WRITER-NATIVE-TEMP-VERIFICATION.md` 接受43 unique子批；`WRITER-ADMISSION-VERIFICATION.md`后续parent full/真实profile集成有其冻结证明 | 当前Rust1029输入闭包/full gate另列；非全Turso workspace/all-features，旧子批不外推最新业务负载。Native作者A，独立F。 |
| Owner rate修复 | `../experiment-v1-stress/OWNER-RATE-VERIFICATION.md`追加v2接受：full normal/race、9 actual-native、fmt/vet/build | v1 race FIRSTFAIL原始保留；不是R8 quota错误归因或性能提升证据。Go作者B，独立F。 |
| 新Go/Py SQL Open observer | `FINAL-SQL-OPEN-OBSERVATION-VERIFICATION.md`（84014e5）：11路径独立接受；normal/race各覆盖union146 roots+254 subcases，Python7+5、fmt/vet/build通过 | 原full各144roots/4SKIPs与writer补跑2PASS分开，非单命令九native全开；141源码/1154依赖闭包，host12ad，Linux eed仅编译未部署；非Open根因fix。 |
| 两次新隔离单Open | `SQL-OPEN-DIAGNOSTIC-VERIFICATION.md`接受default Open200、不Close；`SQL-OPEN-MATCHED-VERIFICATION.md`接受原provision顺序后project Open200/显式Close200 | 两次各一次Open、零query/retry；第一会话仍保留，matched Close非整fixture关闭。observer0B非rootcause/完整观察证明；旧Unknown不重分类、不触碰。 |
| Caller safety / SDK | `../experiment-v1-stress/CLI-CALLER-SAFETY-VERIFICATION.md`独立mock；R15成功保留，当前R17四case见 `FINAL-SDK-CURRENT-VERIFICATION.md` | known-success不证明unknown恢复；Open unknown即使无句柄/无query仍停止、不Close/reopen/probe，caller协议见 `USER-PATH-CALLER-SAFETY.md`。 |
| 原full Rust失败与诊断绿 | `FINAL-RUST-VERIFICATION.md`：392 PASS/1 FAIL/5 ignored；`FINAL-RUST-DIAGNOSTIC-VERIFICATION.md`：399 source-distinct PASS/5 ignored | 绿轮未捕获Grant拒绝，NOT_REPRODUCED，不消除原watchdog失败或根因HOLD。 |
| P4 Stage1 / Native97e1 | `../experiment-v1-stress/P4-ORIGINAL-TRANSPORT-VERIFICATION.md`：8 diagnostics+20 P4 PASS，fmt/clippy/build；新Native97e1 artifact | same-receiver capture限定接受；未捕获失败、不是修复。生产client已变，旧Native64ec全量/S3不能作为当前全门禁。C作者，F验收。 |
| P4 Stage2机制 | `../experiment-v1-stress/P4-CLOSE-TRANSITION-VERIFICATION.md`：2 deterministic unique PASS | 接受local机制oracle，不证明status/detaching或revoke等于final Drop完成；后续test合同接受仍不消除产品lifecycle gap或历史cause未证明。 |
| P4 test-only同步合同 / 当前full Root | C三项helpers同步，生产算法未改；F独立full实际413 unique PASS/0 FAIL/5 ignored，见 `FINAL-RUST-CURRENT-VERIFICATION.md`（提交b8c5d3b） | 当前冻结测试门禁接受；不证明产品completion barrier已实现或历史失败根因已闭合。 |
| 当前真实S3 opt-in | Main另行实际5 PASS，F只读独立接受，见 `FINAL-S3-CURRENT-VERIFICATION.md` | 单列真实服务证明，不将full Root的5 ignored改成PASS或合并命令；bucket Drop忽略清理错误，不证明资源删除完毕。 |
| R16 proper-CA whole11 | Mainsafe evidence实际pass/exit0、11 checks；Native97e1/GoC252，fixture source root5ac850d；Gauss独立核前置binding | whole11不代表UI通过：后续UI独立FAIL/HOLD见 `UI-R16-INDEPENDENT.md`；不代表性能PASS。 |
| UI stage reporter / private v5 | `UI-STAGE-REPORTER-INDEPENDENT.md`：离线14PASS；`UI-V5-INDEPENDENT.md`：Gauss affected10独立PASS | v5 offline接受不代替actual browser/API证明，不追认旧失败实际cause。 |
| 现有UI实际失败 | `SDK-UI-ISOLATED-INDEPENDENT.md`：R15 wrapper/capture exit1、wire/model未证明，combined HOLD | 旧失败/潜在unknown原样保留、不操作；R17新隔离功能另列接受，不追认旧run成功或完整served lineage。 |
| R16唯一actual UI | `UI-R16-INDEPENDENT.md`：Gauss独立FAIL/HOLD；captureSchema、owned SIGTERM、diag/wire0、无oracle | 静态确定捕获器错拒GET catalog合法空response ID，是捕获器兼容缺陷而非生产BUG；不追认本次实际cause。 |
| R17新fixture / actual UI | proper-CA whole11通过；`UI-R17-INDEPENDENT.md`接受实际功能：helper/capture0、四查询、CloseACK、refs、storage0、八wire关联 | 原supervisor锁断言exit1保留；`UI-PROFILE-CLEANUP-VERIFICATION.md`接受后阶段delete1/lexists=false，不改原exit，非原子same-UID限制保留。 |
| 当前Go12ad SDK/UI功能 | `FINAL-SDK-UI-GO-OBSERVATION-VERIFICATION.md`（3aef6ab）：SDK4 known-success与UI四query/八wire/CloseACK1/storage0独立接受，59项lineage匹配 | 原PRE-SPAWN0dispatch失败及supervisor1保留；`UI-19457-PROFILE-CLEANUP-VERIFICATION.md`（ce5325b）接受后阶段cleanup0/delete1/lock absent，非原UI整体验收或原子same-UID保证。 |
| Linux Local subset | `../experiment-v1-stress/LINUX-CURRENT-INDEPENDENT.md`接受base9e image：UID10001/properCA/whole11/actual container0 OOMfalse/clean marker | 不包含后续observer；4109 regular限定context非completeRepoClosed，非LinuxS3；service逐退出码未由空logs单独证明。作者B，Gauss独立。 |
| 新Go Linux Local overlay | `FINAL-LINUX-GO-OVERLAY-VERIFICATION.md`（071a935）：image c534、Go eed payload、UID10001、properCA/whole11、stop/wait0/OOMfalse/clean marker限定接受 | 两文件Go overlay，继承base49e6d3/Linux Native60eb/CLI7b99及旧launcher adf658，不是current Root dev.py；observer=0，非重编全部产物、完整源码闭包、Linux S3/HA或逐服务exit证明。 |
| Metrics/checkpoint观察 | `../experiment-v1-stress/16_verification.md`实际标题/内容为R14：24 GET200/0 metrics403、checkpoint ACK1、unknown0 | 文件编号16不是R16；只接受该run功能/报告map，不证明每项仪表精度或native checkpoint算法修复。Main/E执行，F核验。 |
| 旧common Open未知 | `FINAL-COMMON-CURRENT-VERIFICATION.md`保留旧run：首次Open HTTP502/outcome_unknown，CLI自然exit1、UNRESOLVED/HOLD；13cases全部NOT RUN、零业务ACK/Query/Execute | metrics GET200先于失败、非后续健康证明；unknown无closed cause，namespace保留不重试/Close/reopen；external builder confound=true、performance_pass=false。 |
| 最新observer common实际 | `FINAL-COMMON-OBSERVED-VERIFICATION.md`（920f16a）：F FUNCTIONAL ACCEPT；13scenarios、72timed phases、1863 workflow ACK、38correctness PASS、ledger210、unknown/cleanup_unknown0；CLI/guard0、checkpoint ACK | 72phases来自12cases，constraint_error_reuse另行顺序验；ledger209显式+1源码重建，预期sql_error/rollback未过滤；功能仅本run，external builder confound/performance_pass=false，旧Unknown保留。 |

## 3. 已验版本与新诊断门禁边界

1. R16前置binding引用既有独立报告；本次仅读报告、不重新访问fixture。R16失败、R17成功与后阶段锁删除证据互不改写。
2. 既有R16独立报告核whole SHA `dc5f3e8d186c3c640a7bf11f0ff54f7ae9da1652573c7eed56a1b84a17a47704`；`external_s3_fixture` / `certificate_verified` / 11 checks PASS，不是本次重跑。
3. Rust test-only批未改变Native97e1生产bytes；旧common为97e1/C252/376及1214+3输入，新已验common为97e1/12ad/376及1219+3输入；功能ACCEPT不解决旧Open未知、不证明NativeRollback或空native cache。
4. 11file observer/common、Go12ad SDK/UI与Linux eed overlay分别有新独立实际报告；F最终joint核1222身份并限定接受，审计Root ce5325b，非整Root Git-clean/完整仓库闭包；PREP proof仍仅前置门禁，不代替实际验收或继承旧产物gate。

## 4. 限定交付与保留边界

1. **当前native测试门禁接受**：F full Root413 unique PASS/0 FAIL/5 ignored；Main另跑S3 opt-in5 PASS、F只读接受。本批仍不修生产completion；产品lifecycle completion架构边界与historical cause not proven保留。
2. **当前SDK/UI功能与后续cleanup接受**：Go12ad上的SDK4/UI功能独立接受；19457原supervisor1与R16/R17旧失败保留，另授权own锁cleanup0不洗绿原执行，无完整served-lineage/browser CA-chain或Unknown恢复证明。
3. **新Go Linux Local overlay限定接受**：image c534含Go eed，非root/TLS/whole11/退出clean已核；Native/CLI与旧launcher继承，不是完整current Root部署，observer关闭，S3/HA/逐服务exit边界不变。
4. **限定prototype功能/source audit ACCEPT；性能NOT PASS**：`FINAL-JOINT-DELIVERY-VERIFICATION.md`已冻结独立接受，非production READY、完整项目/all-features或新Main joint_pass；旧Open502未知/13未运行与历史cause/退役/rollback未证保留。未push/remote publication与clone可用性未证，完整cold-fault/长期HA未覆盖，不批准新执行。
