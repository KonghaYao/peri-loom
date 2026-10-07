# 最终验收矩阵：历史完成与当前证明分开

本篇仅只读核实施清单、已签发报告及获准 R16 safe evidence；未 build/test/API/SQL/读取凭据。状态为本次文档冻结时的快照，不是全项目最终 PASS。
“历史完成”只适用于原报告冻结输入；任何后续生产源码改变都须新 gate，不能继承旧 artifact SHA 或把未运行项计为 PASS。
计数按报告的 unique scope引用，不把 normal/race、重复执行、helper/selfspawn或跨 harness共享 oracle相加。

## 1. P0–P6 实施/原始验收与现阶段覆盖

| 范围 / 原始要求 | 原始完成证据 | 当前可用 proof与限制 |
| --- | --- | --- |
| P0：分段、generation、lazy open、checkpoint/重启 | `prototypes/p0/VERIFICATION.md`；原36测试；`01-p0-core-prototype.md` | 后续 native安全/TEMP有独立子批；不是当前整个 Turso workspace重新全测。 |
| P1：远端唯一authority、删cache冷恢复、integrity/eviction | `P1-VERIFICATION.md`：63常规+2 RustFS live | 最新 R16 whole11有真实 SQL/metrics，但未重跑删全cache、冷故障或全部 P1 suite；不得宣称新97e1已完成该全量。 |
| P2：metadata-only branch/snapshot、共享clone、PITR/GC | `P2-VERIFICATION.md`；121常规+3 live，见 `IMPLEMENTATION.md` | R16 snapshot/restore与readonly clone通过；不代替当前refs/GC全套或PITR历史矩阵。 |
| P3：多DB、idle卸载、真实kill/cache删除、lazy迁移 | `P3-VERIFICATION.md`：165常规+4 live；`P3-ACCEPTANCE.md` | R16 migration/suspend/resume通过；本次未新跑SIGKILL、整cache删除或大DB lazy读量证明。 |
| P4：epoch/lease、stale WAL/HEAD fence、退出quiescence | `P4-VERIFICATION.md`：254常规+5 live，原修正版PASS | 当前 completion/next-grant问题仍HOLD；Stage1观察和Stage2机制证明不能代替修复及latest full gate。 |
| P5：registry/placement、失联/failover、graceful migration | `P5-VERIFICATION.md`：62顶层、1 helper skip，normal/race通过 | 历史8实际进程故障场景已签发；当前R16 lifecycle子集通过，不是重跑所有失联/reconciliation场景。 |
| P6：gateway/auth/quota、KMS/encryption、SDK/UI、metrics/部署 | `P6-VERIFICATION.md`：原冻结Rust/Go/live SDK/UI/Linux接受 | caller/metrics/Linux各有新限定证据；当前UI及P4联合仍HOLD，原浏览器PASS不覆盖新UI/helper。 |
| 原联合：跨阶段native+S3 cold/fence | `JOINT-VERIFICATION.md`：325 unique native PASS/5 ignored；复用明确列出的P6证据 | 历史功能PASS保留；该fault fixture后续退出1明确非clean，且不覆盖后来的observer/client生产源码。 |

## 2. 最新增量证明与当前门禁

| 项目 | 已证明 / 当前状态 | 未证明或下一门禁 / 负责方 |
| --- | --- | --- |
| Native safety/TEMP、writer profile | `WRITER-NATIVE-TEMP-VERIFICATION.md` 接受43 unique子批；`WRITER-ADMISSION-VERIFICATION.md`后续parent full/真实profile集成有其冻结证明 | 旧Core子批不能覆盖所有library tests；后续P4 client变化须新parent闭包。Native作者A，独立F。 |
| Owner rate修复 | `../experiment-v1-stress/OWNER-RATE-VERIFICATION.md`追加v2接受：full normal/race、9 actual-native、fmt/vet/build | v1 race FIRSTFAIL原始保留；不是R8 quota错误归因或性能提升证据。Go作者B，独立F。 |
| Caller safety / SDK | `../experiment-v1-stress/CLI-CALLER-SAFETY-VERIFICATION.md`独立mock；`SDK-UI-ISOLATED-INDEPENDENT.md`接受4实际known-success SDK cases | unknown不Close/replay由限定源码/mock证明；真实成功不证明unknown恢复。新caller文档 `USER-PATH-CALLER-SAFETY.md`已独立接受。 |
| 原full Rust失败与诊断绿 | `FINAL-RUST-VERIFICATION.md`：392 PASS/1 FAIL/5 ignored；`FINAL-RUST-DIAGNOSTIC-VERIFICATION.md`：399 source-distinct PASS/5 ignored | 绿轮未捕获Grant拒绝，NOT_REPRODUCED，不消除原watchdog失败或根因HOLD。 |
| P4 Stage1 / Native97e1 | `../experiment-v1-stress/P4-ORIGINAL-TRANSPORT-VERIFICATION.md`：8 diagnostics+20 P4 PASS，fmt/clippy/build；新Native97e1 artifact | same-receiver capture限定接受；未捕获失败、不是修复。生产client已变，旧Native64ec全量/S3不能作为当前全门禁。C作者，F验收。 |
| P4 Stage2机制 | `../experiment-v1-stress/P4-CLOSE-TRANSITION-VERIFICATION.md`：2 deterministic unique PASS | 接受local机制oracle，不证明status/detaching或revoke等于final Drop完成，不关闭root contract HOLD。Main/A协调，F核验。 |
| P4 test-only同步合同 / 当前full Root | C三项helpers同步，生产算法未改；F独立full实际413 unique PASS/0 FAIL/5 ignored，见 `FINAL-RUST-CURRENT-VERIFICATION.md`（提交b8c5d3b） | 当前冻结测试门禁接受；不证明产品completion barrier已实现或历史失败根因已闭合。 |
| 当前真实S3 opt-in | Main另行实际5 PASS，F只读独立接受，见 `FINAL-S3-CURRENT-VERIFICATION.md` | 单列真实服务证明，不将full Root的5 ignored改成PASS、不把两轮执行混为同一命令。 |
| R16 proper-CA whole11 | Mainsafe evidence实际pass/exit0、11 checks；Native97e1/GoC252，fixture source root5ac850d；Gauss独立核前置binding | whole11不代表UI通过：后续UI独立FAIL/HOLD见 `UI-R16-INDEPENDENT.md`；不代表性能PASS。 |
| UI stage reporter / private v5 | `UI-STAGE-REPORTER-INDEPENDENT.md`：离线14PASS；`UI-V5-INDEPENDENT.md`：Gauss affected10独立PASS | v5 offline接受不代替actual browser/API证明，不追认旧失败实际cause。 |
| 现有UI实际失败 | `SDK-UI-ISOLATED-INDEPENDENT.md`：R15 wrapper/capture exit1、wire/model未证明，combined HOLD | 保留旧失败/潜在unknown不操作；新的实际JSON/NDJSON、refs、CloseACK、storage/served-lineage证据待James/Gauss。 |
| R16唯一actual UI | `UI-R16-INDEPENDENT.md`：Gauss独立FAIL/HOLD；captureSchema、owned SIGTERM、diag/wire0、无oracle | 静态确定捕获器错拒GET catalog合法空response ID，是捕获器兼容缺陷而非生产BUG；不追认本次实际cause。 |
| R17新fixture / actual UI | proper-CA whole11通过；`UI-R17-INDEPENDENT.md`接受实际功能：helper/capture0、四查询、CloseACK、refs、storage0、八wire关联 | ownChrome joined0但dangling SingletonLock残留，supervisor末尾assert退出1；cleanup HOLD，不称整体exit0。 |
| R17当前SDK3 | `FINAL-SDK-CURRENT-VERIFICATION.md`：Go i64/integrity、TS bigint、Rust as_i64四个实际known-success case独立接受 | Native97e1/GoC252与18项选定输入一致；不是全SDK suite、Unknown恢复、完整删除syscall或性能证明。 |
| Linux Local subset | `../experiment-v1-stress/LINUX-CURRENT-INDEPENDENT.md`接受base9e image：UID10001/properCA/whole11/actual container0 OOMfalse/clean marker | 不包含后续observer；4109 regular限定context非completeRepoClosed，非LinuxS3；service逐退出码未由空logs单独证明。作者B，Gauss独立。 |
| 最终Linux交付 | 旧startup exit2/mode错误及修复后接受证据均保留 | 旧image未含Stage1 observer；latest闭包可另gate。C本批仅test变化不因算法修复要求新生产image；不得称现image latest Ready。Main授权，B/Gauss。 |
| Metrics/checkpoint观察 | `../experiment-v1-stress/16_verification.md`实际标题/内容为R14：24 GET200/0 metrics403、checkpoint ACK1、unknown0 | 文件编号16不是R16；只接受该run功能/报告map，不证明每项仪表精度或native checkpoint算法修复。Main/E执行，F核验。 |
| 压力/性能 | 已有架构/SQL子场景与上述R14功能证据；原失败和external builder confound保留 | performance NOT PASS；未完成/STOP后的phase不是通过，不以新功能smoke替代最后授权压力及独立对抗验收。E独占runner，F/Main。 |

## 3. R16 可核证据与源版本约束

1. 仅获准读取 `/private/tmp/peri-stress-r16-ui-stage-i0ar8hek/main-launch.json` 与 `whole-smoke.json`；未读connection/launch/keys文件。
2. whole SHA实读匹配 `dc5f3e8d186c3c640a7bf11f0ff54f7ae9da1652573c7eed56a1b84a17a47704`；`external_s3_fixture` / `certificate_verified` / 11 checks PASS。
3. Native97e1的完整artifact身份见Stage1报告；GoC252及fixture root5ac850d是R16绑定。C新同步批为test-only，生产算法不变，不虚称另有新生产artifact。
4. Linux image与SDK成功使用各自报告冻结闭包；不得沿用该hash给未来版本背书。本文没有重新测试或制作任何artifact。

## 4. 最终裁定所缺的闭环

1. **当前native测试门禁接受**：F full Root413 unique PASS/0 FAIL/5 ignored；Main另跑S3 opt-in5 PASS、F只读接受。本批仍不修生产completion；产品lifecycle completion架构边界与historical cause not proven保留。
2. **UI功能接受 / cleanup HOLD**：R16首失败保留；R17实际UI与SDK3限定功能已独立接受。supervisor退出1/锁残留仍保留，C开发仅自建profile的守护清理、F独立验证后才实际删除。
3. **部署版本边界不变**：已接受Linux仅其冻结Local subset；latest image闭包由Main另定gate。本批test-only同步不是生产算法修复，不据此要求重建Native97e1或生产image。
4. **NOT PASS：性能与最终全项目结论**；待授权完整压力/对抗证据及以上HOLD解除。无缺失证据补零、忽略失败或未知请求盲Close/重试。
