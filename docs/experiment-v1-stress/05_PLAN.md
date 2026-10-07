# Round 5 预声明：相同artifact、两arm共同受控idle-awake措施

## PLAN ONLY与启动门禁

此文件不授权实际执行。不load/provision/build/start服务，不生成或调用credential，不修改旧raw/fixture。Round4首FAIL与OS suspend证据保持原样；Round5为明确重新预声明的实验，不重发历史SQL/business IDs，不覆盖失败，不自动retry。

Main负责commit本轮报告/计划、冻结matching HEAD与artifact/source manifest、确认C/F全部门禁结束及本任务builders/tests/prep停止、fresh startup/secret、新gate和具体runner授权。新gate为main-confirmed matching-HEAD operator attestation（普通private JSON），不是密码学签名证明。E不得从本文推导provision/runner/signal权限。

两arm均须main新启动完整private TLS/S3 whole-project fixture、proper CA HTTPS、两个worker、**各自unique fresh S3 bucket**并保存初始化PASS证据。旧r4及以前fixture/bucket/DB/cache不复用、不访问或清理。服务启动前须冻结新HEAD；未经明确授权或不符冻结manifest的代码/binary变化阻止启动。用户 `CLAUDE.md` dirty真实记录并排除于不可变runtime代码，不自行修改。

## 共同artifact与对照边界

| Artifact | 两arm共同固定版本 |
| --- | --- |
| Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA256 | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| New drain harness | `/tmp/peri-stress-phase-drain-bin` |
| Harness SHA256 | `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8` |
| Control native | `/tmp/peri-stress-native-baseline-bin` |
| Control native SHA256 | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Candidate native | 保持Round4预定BUSY+View整体artifact版本；精确path/SHA由main/F新gate与provenance确认，不重build、不从source推算 |

Candidate是BUSY+ownership View及artifact内全部变化的整体版本，**不是isolated View**。只有sat/point构成paired比较；不能把差异归因于单一View/BUSY/锁实现。Control仍不安排mixed，OUT OF SCOPE而非unsupported SKIP，避免把历史control mixed失败当自动重试。

固定rows1000、seed20261007、request timeout10s、单进程sampled RSS512MiB、generated256MiB、artifact64MiB、trusted执行预算/quota不变。每arm同sat→point顺序保留其服务/cache/history；candidate随后mixed/架构/SQL，不先插入lifecycle改变共同测量历史。

## 新operational前提：main-owned idle sleep assertion

两arm采取**相同**awake措施，每个fresh launcher由main拥有并核对exact PID/start identity后启动自己的assertion。仅作为计划模板，本文不调用：

```sh
caffeinate -i -t 3600 -w "$LAUNCHER_PID"
```

- Main记录assertion owned PID、PPID、启动host-wall timestamp、完整start argv、对应exact launcher PID/start identity、程序路径与实际退出/终止状态；launcher生命周期不靠仅PID数字识别，防止PID复用误绑定。
- `-i`只用于阻止idle system sleep；不修改用户pmset/keychain、不关闭TLS、不改变SQL deadline/lease/fence，不提供forced sleep、lid close、显式sleep或所有电源事件都被阻止的保证。
- 每arm启动后及每runner开始前，main必须保留只读 `pmset -g assertions` 的safe subset与host-wall timestamp，确认实际 `PreventUserIdleSystemSleep` assertion由exact owned caffeinate PID持有；同时核对其PID/start identity与本arm exact launcher PID/start identity绑定有效。只保留所需assertion类型、状态、owned PID与相关身份，不复制无关用户进程详情。
- Assertion缺失、类型/owner不匹配、PID/start identity或launcher绑定失效、窗口到期，均STOP；两arm要求完全相同。PID alive、启动argv或参数3600本身不是OS assertion实际生效证明。不自行重启过期/消失assertion、不改OS配置或用户进程；main记录真实生命周期。
- 计划25–40分钟，3600s为本轮操作预算；若因startup/等待/协调接近assertion窗口或assertion丢失，停止并报告，不在无管理idle条件下继续，也不凭本计划自动延长/重新启动。
- 不采用无人管理的5分钟等待后直接load。外部编译quiet guard仍仅限有界等待，等待期间awake前提也必须满足；不kill用户process。
- Main正常结束自己的fixture后记录caffeinate实际退出；若需正常结束owned assertion，由main操作。E不signal launcher/assertion，不修改旧服务。

## 精确串行矩阵

所有label均新 `r5` 前缀、每runner独立fresh project/DB/crypto runprefix/business IDs；一次safe provision、一次runner，不重放旧request。Label必须经main确认未消费；失败后不换label绕过STOP。

| 顺序 | 新label | Cases | C | Warmup / smoke / measure | Rate | 用途 |
| --- | --- | --- | --- | --- | --- | --- |
| Control1 | `r5csat` | saturation_admission | 1,2,4,8 | 2s / 10s / 30s | 200 | 完整measure候选 |
| Control2 | `r5cpoint` | point_read | 1,4 | 2s / 10s / 30s | 50 | 完整measure候选 |
| Candidate1 | `r5nsat` | saturation_admission | 1,2,4,8 | 2s / 10s / 30s | 200 | paired sat |
| Candidate2 | `r5npoint` | point_read | 1,4 | 2s / 10s / 30s | 50 | paired point |
| Candidate3 | `r5nmixed` | mixed_oltp | 1,4 | 2s / 10s / 30s | 50 | 非paired mixed |
| Candidate4 | `r5narch` | architecture6 | 1,2 | 2s / 10s / 10s | 50 | functional gate-only |
| Candidate5 | `r5nsql` | sql | 1,2 | 1s / 2s / 5s | 25 | functional gate-only |

先control，结束后main正常停own fixture，再main新启动candidate并另行明确授权；E不自行启动第二arm。任何unexpectedFAIL/unknown/非COMPLETE停止**整个剩余序列**。未开始项保持NOT EXECUTED，不为填表自动追加run。

Architecture6精确cases：

```text
session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate
```

Noisy C1因需两个session显式SKIP；graceful migration destination already owner显式SKIP，不假报bug或PASS。缺必需DB/management key/topology导致的其它SKIP不是授权豁免，完整coverage未达成时STOP。

## Common SQL13与共享ledger

`-cases sql`支持：point_read、index_range、pagination、mixed_oltp、hot_keys、bulk_tx、sort_aggregate_join、index_plan、rollback、constraint_error_reuse、large_values、sustained_wal、checkpoint_reopen。

`snapshot_branch_pitr_under_load` **explicit SKIP**：fault schedules/retention oracle未实现，不计入13项PASS。不得声明snapshot/PITR under load coverage。架构10s和SQL5s均明确gate-only，不作稳态性能结论。

组合一次seed、同fixture共享累积ACK ledger。Write改变balance，read oracle用固定字段；constraint独立表执行BEGIN/update/duplicatePK→typedterminalflags→同opaque session显式ROLLBACK→余额与正常query/write验证，正常ACK进入ledger。Checkpoint须实际Management执行与reopen后的持久oracle，不假证明空cache。

预计成功完成应有1preflight+24smoke后+13case后=38次fixture oracle PASS，另含每operation/constraint-step检查。12timed cases×2C×8s=192s，constraint一遍有界证明，不冒充throughput。最多4800offers，保守ledger上界约4805（bulk×8及mixed全部写假设）。既有wrapper/raw预计10–20MiB但非保证；64MiBartifact guard不改，触发即STOP，不删证据腾空间继续。

## Drain、unknown与安全合同不变

- 意外known non-admission SQLerror或ACK row mismatch停止offers/dequeue/logical admission，phase保留FAIL。
- 真实unknown自身poison、phase FAIL/UNRESOLVED，stopOffers；该session不retry、不自动ROLLBACK、不reopen、不盲close，不能假定lostCOMMIT未提交。
- 其他已经逻辑admitted的workflow在各自原有限RPC deadline与parent safety caps内自然drain；实际ACK/negative/unknown逐一保留，不延长deadline，不恢复新调度。
- Parent cancel、resource/artifact安全问题仍hardcancel；cancel/EOF/timeout/lostreply的不确定性不得降级。
- ACK ledger只收实际ACK；phaseFAIL不因其它ACK转PASS，不把nativebatch error认作完整batch rollback。

Typed quota/admission负终态可作为saturation预期终态但仍计error，clientdrop不等于serverquota。只有constraint场景的预声明SQLnegative在flags、显式rollback与oracle全部正确时可作为该步骤通过；其它sql_error仍STOP。

## OS事件与wall/monotonic异常审计

每fresh fixture由main保存覆盖startup→测量→shutdown的安全OS Sleep/Wake subset，保留host-wall timezone/source/path/hash。仅需Sleep/IdleSleep/DarkWake/FullWake相关事件；不复制用户wake请求详细日程、credential、设备私密信息或整个无关系统日志。

每run返回后立即执行只读wrapper/main证据门禁：对该run、每phase、每RPC对照raw observed wall start/finish与原elapsed_ns，并核对覆盖该run的安全OS事件及实际awake assertion证据，记录非负wall差、差值与采样缺口。必须完成并通过此审计，才允许下一label provision、下一runner或下一arm；不能延后到最终report/F复验时才审核而先继续序列。最终report/F再独立复核这些已完成审计。Wall不作为throughput分母、不取代Go monotonic，不做未经验证的校时/date因果推断。若wall倒退，明确clock anomaly，不构造负latency。

预声明timing筛查：window wall倒退、elapsed无效，或wall与monotonic差绝对值超过1s，均标timing anomaly并停止；这是操作筛查阈值，不是精确clock同步保证。相关窗口任何确认suspend，即使差值未超阈值也STOP。未超阈值或无OS日志不等于证明无微小暂停。

Main awake assertion与safe OS subset并不能替代真实证据。观测到suspend/timing anomaly、assertion失效、unexpectedFAIL、unknown、runner/case/phase非COMPLETE、oracle不PASS或资源风险：停止剩余序列、保留首次结果，不重跑。正在执行时由既有bounded drain/parent安全caps收口；E不额外发SQL、close、kill或修改deadline补救。

## 资源与背景guard

仅一个actualrunner持有host/output locks；每run前核对HEAD、dirty白名单、binary SHA、privatepermissions、lock absent、servicePID/start identity、owned awake前提和hostcapacity。生成/原始artifact/RSS上限保持不变。

Readonly ps采样脱敏PID/comm basename/CPU/RSS。外部cargo/rustc总CPU<=400%且RSS稳定时记录背景后可执行；高于阈值最多再等60s一轮并汇报，不无限等、不kill用户进程。既有稳定规则记录编译RSS<=4GiB、相邻样本增长<=512MiB，只是操作guard，不是硬内存保证。自己的任何builders/tests仍绝对禁止。

运行中外部编译/OS背景活跃或采样空档真实保存，不宣称zero interference/最大容量。两arm背景不同限制paired置信度，必要追加quiet对照只能由main另授权，不能自动重复。

## 可复用CLI模板：不是执行脚本授权

Main提供 `FIXTURE`、`OUTPUT`、`GATE`、exact launcher/assertion provenance与具体矩阵项。Private0600 connection保存凭据，不通过CLI/echo/stdout/docs传key。以下仅模板，本文不生成credential、不调用命令：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir "$FIXTURE" --output "$FIXTURE/stress-$LABEL.json" --label "$LABEL"

/tmp/peri-stress-phase-drain-bin \
  -connection "$FIXTURE/stress-$LABEL.json" \
  -output "$OUTPUT" -joint-pass "$GATE" \
  -cases "$CASES" -rows 1000 -seed 20261007 \
  -concurrency "$C" -warmup "$WARMUP" -smoke "$SMOKE" -measure "$MEASURE" \
  -offered-rate "$RATE" -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

复用已有串行wrapper的freeze/provenance/background/stdout/exit/raw/completion审计结构，但只写新private/tmp证据，不运行中改repo。每次provision成功、run实际exit0/COMPLETE、必需oracle全PASS且awake/时钟审计无异常，才可进入**已明确授权**的下一项；不做无条件batch、不重试mutation、不复用旧label/config/request ID。

## 时长与报告

| 项目 | Timed windows下限 |
| --- | --- |
| Control sat + point | 168s +84s =252s |
| Candidate sat + point + mixed | 168s +84s +84s =336s |
| Candidate architecture6 | 154s，另含lifecycle/setup/oracle |
| Candidate SQL13 | 192s，另含constraint/checkpoint/setup/oracle |
| 全部7runner合计 | **934s，约15.6分钟** |

实际预留25–40分钟，另加main两armstartup/shutdown与gate协调，不保证全部完成。异常即提前停止；禁止用无限等待、未管理idle或自动补跑换取完整矩阵。

每run保留source/manifest/dirty、全service与owned assertion PID/startargv/start identity/exit、artifact完整SHA、armvariant、bucket初始化、精确CLI、safeOS事件、wall/mono审计、实际背景/RSS、stdout/exit、rawJSON/JSONL、firsterror与drain终态。Metrics记录实际scope/map、missing为null/unavailable，不造0。

最终report区分measurement/gate-only、COMPLETE/SKIP/NOTEXECUTED、oracle真实边界和sleep/timing无效片段。**持久oracle全部PASS、完整30smeasure、awake/资源/冻结审计通过是性能讨论前提**，不是仅exit0或ACK数达标。Mixed写历史增长、两arm背景差异与native整体变化均限制归因。

Main新commit/gate/relaunch及具体授权前，E持续STOP；本计划不宣称Round5已经运行或取得任何PASS。
