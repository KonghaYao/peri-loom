# Round 5：paired 描述性样本完成，candidate mixed FAIL，整矩阵 PARTIAL

## 作者结论与实际覆盖

五个runner实际执行：control sat/point、candidate sat/point均exit0 / COMPLETE；candidate mixed exit1 / FAIL / sql_error后立即停止整个剩余序列。Architecture6与commonSQL13均未provision、未执行；snapshot-under-load未取得coverage，原预声明explicit SKIP不是PASS。

两arm共同sat/point取得12个完整30s measure、各自完整持久oracle及post-run时钟/OS/awake审计PASS，可以报告**fixed single-pair、单seed、单次顺序实验的描述性性能样本**。没有重复样本、随机arm顺序或统计显著性证明，不能声称整体优化PASS、最大容量、isolated View收益或完整压力矩阵PASS。

Candidate mixed C1完整30s measure已完成，但缺少其后持久oracle；C4 smoke发生typed SQLnegative。三个已在途peer自然ACK、0unknown是本轮实际drain终态证据，**不是最终durability证明**。原始native error body未保存，根因仍unknown；不能从binary名称、HTTP flags或既有修复推断BUSY/lease/View等具体原因。

本报告仅只读重算既有raw与private审计文件，并写本文件；没有新SQL/provision/retry/signals/build或下一轮预声明。Round1–4报告/raw保持冻结，不倒填或重分类。

## Source、授权与artifact身份

执行HEAD：`3dad8b668b5a60b5582a7639ecffebabcd0b314b`。预声明 `05_PLAN.md` SHA256 `e26efad821d724dcb04773de150d863a02849d4123db857e6dce4e66f47ea433`；main确认计划及F静态审核纳入commit `3dad8b6`。执行前后root仅用户 `CLAUDE.md` dirty，未被修改；它真实进入provenance但排除于不可变runtime代码。

五个raw source的status SHA256均为 `b4efa7568a2f2c5b463d62276d2aeb1538a80ffaa1c3fe5260600043d2f9a742`，tracked diff SHA256均为 `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848`。Harness自身untracked来源字段仍声明其限制；本轮额外wrapper provenance记录实际binary/source/dirty身份，不把用户dirty隐藏成clean。

Gate：`/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round5.json`，SHA256 `646e96ccb031ed484cafbba39af15525352cb699c2e94b258a960c9aca85ef65`。这是private0600普通JSON、main matching-HEAD operator attestation，**不是密码学签名证明**。C/F gates结束、builders停止、fixture READY与分arm授权由main确认，本报告不重新执行或替代那些gate。

| 身份 | 固定路径 / SHA256 |
| --- | --- |
| 两arm Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| 两arm new drain harness | `/tmp/peri-stress-phase-drain-bin` |
| Harness SHA | `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8` |
| Control preserved native | `/tmp/peri-stress-native-baseline-bin` |
| Control native SHA | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Candidate native | `/tmp/peri-stress-native-busy-bin` |
| Candidate native SHA | `868962e6896fa166c1429aa149812955e9740e690118609ebff20d2f0ccc89b4` |

Candidate是**BUSY + ownership View及冻结artifact内全部变化的整体版本**，不是isolated优化。Control为有意preserved old native，不声称由当前HEAD重建。共同Gateway/harness、trusted预算/quota和协议未为本次失败更改；没有SQL/HTTP自动retry。

## Fresh fixture与共同operational前提

| Arm | Private whole-project fixture | Launcher / authority / platform / workers | Main-owned caffeinate |
| --- | --- | --- | --- |
| Control | `/private/tmp/peri-stress-r5-control-MLx6698Y` | 53783 / 53819 / 53822 / 53826,53827 | PID58764，`/usr/bin/caffeinate -i -t 3600 -w 53783` |
| Candidate | `/private/tmp/peri-stress-r5-candidate-bZi2fUAJ` | 9721 / 9764 / 9766 / 9768,9770 | PID17150，`/usr/bin/caffeinate -i -t 3600 -w 9721` |

两arm独立fresh unique S3 bucket、初始化PASS、proper CA HTTPS `127.0.0.1:19446`、两个worker；未复用旧DB/cache/bucket/label。服务目录0700、connection与awake provenance0600，不公开credential。

每runner前独立核对actual OS `PreventUserIdleSystemSleep`确由对应owned caffeinate PID持有，同时核对UID、PID/start identity、exact argv与exact launcher绑定。不是仅PID alive/命令参数的推断。每run后再次核对，并保存实际safe assertion subset；未改用户pmset/keychain、SQL deadline、lease/fence，不声称阻止forced sleep。

Control launcher start identity为host observed `Wed Oct 7 11:55:40 2026`，caffeinate为11:56:21；candidate launcher为12:09:39，caffeinate为12:10:52。Worker-b candidate实际start为12:09:40；完整PID/start/executable SHA均保存在逐run provenance，而非假设所有worker同秒启动。

Bucket初始化SHA：control `835ea3725245ed9474d421e2243adb5aa4f8c6c53692d6446b0c84362cc76e6e`；candidate `7051824f06642ca8d7806df5b1e2dac48a108c009dc80826cd4e1e615ed6a56d`。二者来自各arm逐run provenance的实际bucket初始化文件hash，不是provenance文件自身hash。

Host capacity实际18 logical CPUs、51,539,607,552bytes（48GiB）。两arm同单进程sampled RSS512MiB、generated256MiB、artifact64MiB、request timeout10s；未临时调高quota或降低oracle要求。

## 实际串行命令与runner状态

每label一次safe provision、一次runner，rows1000、seed20261007；唯一runner持有host/output locks。共同精确CLI结构如下，实际private路径与argv分别保存在每run provenance，不传credential于argv：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir "$FIXTURE" --output "$FIXTURE/stress-$LABEL.json" --label "$LABEL"

/tmp/peri-stress-phase-drain-bin \
  -connection "$FIXTURE/stress-$LABEL.json" \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round5.json \
  -cases "$CASES" -rows 1000 -seed 20261007 \
  -concurrency "$C" -warmup 2s -smoke 10s -measure 30s \
  -offered-rate "$RATE" -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

| 顺序 / Label | Cases / C / Rate | Actual runner exit / status | Elapsed（monotonic） | 持久oracle PASS |
| --- | --- | --- | --- | --- |
| 1 `r5csat` | saturation_admission / 1,2,4,8 / 200 | 0 / COMPLETE | 180.267239584s | 1preflight +5 |
| 2 `r5cpoint` | point_read / 1,4 / 50 | 0 / COMPLETE | 90.216349291s | 1preflight +3 |
| 3 `r5nsat` | saturation_admission / 1,2,4,8 / 200 | 0 / COMPLETE | 176.069872208s | 1preflight +5 |
| 4 `r5npoint` | point_read / 1,4 / 50 | 0 / COMPLETE | 90.140393875s | 1preflight +3 |
| 5 `r5nmixed` | mixed_oltp / 1,4 / 50 | 1 / FAIL/sql_error | 54.112280250s | 1preflight +1，仅到C1post-smoke |

五次provision均actualexit0。Control完成并停止后，main报告正常SIGTERM own launcher53783、whole PTY25166与caffeinate PTY82860 actualexit0、all6 owned PID gone及Authority exact clean；随后才新启动candidate。E未实施这些signals。

Candidate mixed FAIL后全部E runner停止、两把锁释放；`r5narch`/`r5nsql` connection与label reservation均不存在。Main随后确认正常SIGTERM own launcher9721，whole PTY9913与caffeinate PTY63686 actualexit0、all6 owned PID gone、Authority clean。这是main交接的实际停机事实，不是E实施signal或额外服务观测。

## 从raw重新计数

作者逐行解析JSONL，按phase observed窗口匹配operation records，重算attempt/ACK/error与ACK mutations；重新排序 `http_workflow_ns`，nearest-rank p50/p95/p99/max与所有phase raw quantiles一致。每run业务ID唯一、所有非空Gateway RPC request ID唯一；metrics的空request ID不误判为SQL重放。重算是只读，不重发请求。

| Label | Offered | Attempted / completed | ACK | Typed errors | Client drops | Queue expired |
| --- | --- | --- | --- | --- | --- | --- |
| r5csat | 33600 | 6526 /6526 | 5562 | 964 admission | 27034 | 40 |
| r5cpoint | 4200 | 3115 /3115 | 3115 | 0 | 1082 | 3 |
| r5nsat | 33600 | 7130 /7130 | 6327 | 803 admission | 26431 | 39 |
| r5npoint | 4200 | 3303 /3303 | 3303 | 0 | 891 | 6 |
| r5nmixed | 2437 | 383 /383 | 382 | 1 sql_error | 2043 | 11 |
| 合计 | 78037 | 20457 /20457 | 18689 | 1768 | 57481 | 99 |

所有已执行phase unknown/cleanup_unknown/correctness_failures/timeouts均0。41个phase中40 COMPLETE、1FAIL；总计22次实际持久oracle PASS，但它们属于各自fixture/边界，不能相加冒充failed mixed最终oracle。Client drops为客户端未进入logical dispatch的offer，不是serverquota；typed admission是真实server负终态。

## Paired full-measure描述性样本

仅取完整30s **measure**，不混入warmup/smoke/初始化。每个sat measure offered6000、point measure1500。Achieved为ACK数/raw phase elapsed（包含drain），configured rate不是实际ACK吞吐；未发出的客户端drop仍在offered模型中保留。

成功latency为client HTTP workflow elapsed，非native VM独立计时、非offered-to-response排队总延迟；单位ms。成功quantiles只含ACK；all-workflow含实际尝试的negative，**不含未dispatch的client drops**。

| Workload / Arm / C | ACK | Elapsed s | ACK/s | Server admission | Client drop / expired | 成功p50 /p95 /p99 /max ms |
| --- | --- | --- | --- | --- | --- | --- |
| sat control C1 | 739 | 30.024467 | 24.613 | 0 | 5260 /1 | 38.925 /54.270 /67.059 /148.717 |
| sat candidate C1 | 1061 | 30.006728 | 35.359 | 0 | 4938 /1 | 26.950 /41.835 /53.696 /99.945 |
| sat control C2 | 1281 | 30.044270 | 42.637 | 0 | 4717 /2 | 41.031 /77.018 /115.390 /195.899 |
| sat candidate C2 | 1450 | 30.031029 | 48.283 | 0 | 4549 /1 | 39.109 /60.093 /73.821 /167.978 |
| sat control C4 | 1244 | 30.103302 | 41.324 | 0 | 4752 /4 | 79.002 /170.641 /227.010 /388.107 |
| sat candidate C4 | 1403 | 30.103188 | 46.606 | 0 | 4593 /4 | 80.284 /118.293 /159.908 /190.863 |
| sat control C8 | 691 | 30.137386 | 22.928 | 706 | 4597 /6 | 156.844 /219.370 /280.146 /1643.288 |
| sat candidate C8 | 549 | 30.147909 | 18.210 | 548 | 4897 /6 | 188.983 /322.882 /453.131 /2404.401 |
| point control C1 | 757 | 30.010127 | 25.225 | 0 | 742 /1 | 38.061 /53.980 /64.793 /112.522 |
| point candidate C1 | 1002 | 30.038243 | 33.357 | 0 | 498 /0 | 28.020 /46.631 /56.054 /102.977 |
| point control C4 | 1447 | 30.013058 | 48.212 | 0 | 53 /0 | 46.744 /108.144 /126.723 /163.843 |
| point candidate C4 | 1333 | 30.074678 | 44.323 | 0 | 164 /3 | 79.841 /128.272 /158.799 /236.375 |

C8 all-workflow分母不同于成功样本：

| Sat arm C8 | All count | All p50 /p95 /p99 /max ms | ACK count |
| --- | --- | --- | --- |
| Control | 1397 | 155.053 /215.981 /262.007 /1644.477 | 691 |
| Candidate | 1097 | 188.905 /327.648 /479.625 /2404.401 | 549 |

其它paired measure无negative，所以all与ACK quantiles相同。描述上candidate在sat C1/C2/C4、point C1的ACK/s较高，而sat C8、point C4较低；并非单调“越并发越快”或所有指标改善。固定quota和client scheduling/drop共同影响这些样本，不能从offered200认定服务可承载200成功ops/s。

两arm仅一个完整fresh service/bucket，按control→candidate顺序、同sat→point历史；没有arm随机化、重复run或quiet matched-control。OS后台、S3/network、cache/controller/lease历史均可能影响结果；下面的真实编译背景不同进一步限制因果置信度。只能描述这次固定singlepair，不能归因于孤立native优化或宣称最大容量。

## Mixed失败、真实drain与账本边界

| C / Phase | 状态 | Offered | Attempt / ACK | ACK mutations |
| --- | --- | --- | --- | --- |
| C1 warmup | COMPLETE | 100 | 31 /31 | 6 |
| C1 smoke | COMPLETE | 500 | 92 /92 | 33 |
| C1 measure | COMPLETE，30.007979250s | 1500 | 216 /216 | 64 |
| C4 warmup | COMPLETE | 100 | 15 /15 | 5 |
| C4 smoke | FAIL，4.811033083s | 237 | 29 /28 | 9 |
| C4 measure | NOT EXECUTED | — | — | — |

逐条ACK record重算：117个write ACK（每个一条mutation）+265个read ACK=382 ACK。Mutation entries=6+33+64+5+9=**117**；加4条preflight已ACK write，累积ACK期望模型ledger为 **121**，不是121条已持久验证行。

实际preflight oracle验证ledger4；C1post-smoke验证4+6+33=**43**、account sum99,999,998、账户hash与ledger hash一致。随后ACK的64+5+9=78条没有最终readback。C1 measure虽完整，case在C4返回FAIL前不会执行case后oracle，因此**无C1 post-measure、无C4 post-smoke、无最终oracle**。不能把121当作已提交/最终durable ledger实际行数，也不能断言failed mutation未发生。

C1 measure216 transport ACK的原始ACK/s7.198、成功p50/p95/p99/max26.075/463.724/557.086/594.234ms仅为传输样本记录，**不具备最终durability-qualified mixed性能结论**；写history增长也不是与其它run等价的writecount比较。

首negative为 `POST /v1/sql/execute`，request ID `stress_32f0ba1ea724d157_000000002821`，business ID `stress_32f0ba1ea724d157_000000002795`，slot1，HTTP400/173bytes，typed `sql_error`、`execution_completed=true`、`may_have_committed=true`、unknown=false、timed_out=false。原body/native message未保存，具体根因unknown。Flags不承诺完整batch rollback、无committed prefix或自动恢复；没有为诊断重发SQL或改HTTP分类。

Host-wall observed首negative结束 `2026-10-07T04:20:20.819489Z`。其它三个请求均在其前已启动，随后自然ACK：

| Slot | Request ID后缀 | Started → ACK（同日UTC，host observed） |
| --- | --- | --- |
| 0 | `000000002847` | 04:20:20.279589 →04:20:20.858533 |
| 3 | `000000002852` | 04:20:20.334203 →04:20:20.878668 |
| 2 | `000000002850` | 04:20:20.307198 →04:20:20.893550 |

这与new-drain stopOffers、保留原有限deadline让已admitted peer自然终结的合同相符；首FAIL保持FAIL，0unknown并未用retry/implicit rollback制造。它证明本次实际peer终态保留，不证明所有lostCOMMIT路径或最终durability均安全。

## 每run立即时钟 / OS / actual-awake审计

每run返回后、下一label provision前完成所有raw run/phase/RPC/workflow window对照、safeOS事件筛选、actual assertion重新核对；任何wall-vs-mono>1s、confirmedSleep或awake failure原规则STOP，未降低阈值。该检查不是等最终F报告才执行。

| Label | Run / phase / RPC / workflow windows | 最大abs wall−mono s | 实际awake samples | SafeOS Sleep/Wake事件 | Postaudit |
| --- | --- | --- | --- | --- | --- |
| r5csat | 1 /12 /6852 /6526 | 0.000085416 | 18 | 0 | PASS |
| r5cpoint | 1 /6 /3281 /3115 | 0.000042709 | 9 | 0 | PASS |
| r5nsat | 1 /12 /7453 /7130 | 0.000313208 | 18 | 0 | PASS |
| r5npoint | 1 /6 /3469 /3303 | 0.000053583 | 9 | 0 | PASS |
| r5nmixed | 1 /5 /486 /383 | 0.000030250 | 6 | 0 | PASS |

SafeOS subset来自只读 `pmset -g log`，按本run observed起止±2s过滤Sleep/DarkWake/Wake，保留timezone/source/window而不复制无关wake日程；结果空数组是真实筛选结果，不伪造整机历史“从未睡眠”。Assertion采样均actual owner/start绑定PASS，pre/post也PASS；不声称排除所有forced sleep或采样间暂停。

时间是host-wall observed，elapsed为原Go monotonic；wall用于异常筛查而非吞吐分母，不推断外部校时日期。与Round4不同，本轮没有观测到confirmed suspend或>1s时间分离；这是本轮audit事实，不用于改写Round4。

## 实际背景、资源和局限

| Label | Background samples | Sampled cargo/rustc累计CPU峰值（ps %） | Resource samples | 最大单服务RSS bytes |
| --- | --- | --- | --- | --- |
| r5csat | 20 | 238.9 | 174 | 42,205,184 |
| r5cpoint | 11 | 27.4 | 88 | 43,302,912 |
| r5nsat | 20 | 271.4 | 171 | 39,124,992 |
| r5npoint | 11 | 96.4 | 88 | 42,237,952 |
| r5nmixed | 8 | 0 | 47 | 42,762,240 |

这些是实际脱敏PID/comm/CPU/RSS采样；compiler峰值是同一样本内所有cargo/rustc的CPU求和，不是单个rustc峰值。Candidate sat完整background峰值271.4%出现在host observed `2026-10-07T04:15:57.203524+00:00`，位于实际runner窗口内；此前交接228.8%为运行中较早样本观察，不是完整文件最终峰值，本报告以独立重算为准，原raw不改。CPU为OS ps窗口/累计语义，不是瞬时全host occupancy，也不是均值负载。外部他repo编译在测量中出现，按已批准<=400%guard保留背景；未kill用户进程。混合背景峰值0也不代表无其它OS/app/S3干扰。本任务没有竞争build/tests/Docker操作；不能声称纯净环境、零干扰或满机极限性能。

Actual RSS不是cache payload计数；各sampled服务均远低于512MiB，未记录resource abort，但不保证采样间无峰值。Raw management/native metrics map与measurement scopes保留实际全局/进程观测，不作run专属增量归因，missing不造0。

五run raw JSON+JSONL总46,797,254bytes；generated request bytes charged合计4,413,554。逐runartifact guard未触发；这不是对wrapper附属证据无限空间的承诺。安全异常/SQL错误仍优先STOP，不通过删除旧artifact继续跑。

## 未覆盖与停止状态

| 项目 | 结论 |
| --- | --- |
| Control sat /point | COMPLETE，paired描述性样本可报告 |
| Candidate sat /point | COMPLETE，paired描述性样本可报告 |
| Candidate mixed | FAIL，缺最终oracle；不是完整mixed性能PASS |
| `r5narch` architecture6 | NOT PROVISIONED /NOT EXECUTED |
| `r5nsql` commonSQL13 | NOT PROVISIONED /NOT EXECUTED |
| snapshot-under-load | 预声明explicit SKIP，不计coverage/PASS |
| Faultprobe /UI /whole-smoke | 未在本授权下执行，不自动补跑 |
| 整个Round5矩阵 | **PARTIAL，含真实FAIL** |

所有E runner停止，无自动retry/replay、新label绕过、额外cleanup mutation或信号。Main拥有fixture/caffeinate正常shutdown；任何后续safe diagnostic experiment需其新的明确授权，不是本报告隐含权限。

## 冻结raw与审计索引

Base：`/private/tmp/peri-stress-results-h3wtrdm2`。每个下列run目录的两个raw文件都保留原样：

| Run ID | `run.json` SHA256 | `operations.jsonl` SHA256 |
| --- | --- | --- |
| `a66d3da894f7b624` | `2a41ea09f5211f9f0a49b123343e92ecdb69d599c942a6088edd37542964acb5` | `7bc65c85d55a1d4e52cf6bec7507640c394aa112681959f8933f55bb65a71f92` |
| `d70a2475ab0b0c7e` | `75b52b32a69fc9ce24ac871c2ed45e3266c52bbfc63c420e01308cef84aa033e` | `32348e8530cb8cb7aaacc60e8b95fd276df27d09bd1201bab5655bbc9cbd3695` |
| `3ae63e17d9c8d1aa` | `613ab6f2b1d73322b42adc1f8aaeb972eed9e4189fbdfca8ce84343f64a76840` | `791a1161769a6f031c33d39060e3edc9c836e5d382eb2dcc8cf1c6c9dc031f44` |
| `9a56c88b23e870b2` | `865d3ac0341334d1e942c6716ab58dc09163b9cfc15b98f44afcb1d3f7752410` | `9bb35e5e871695f1d194f716c7331dae10af87c696afd67290b22100872e1a4a` |
| `32f0ba1ea724d157` | `6e7669476b8675a77a5ff37289c4ec5276217cc9613a5774df72c9ca7ab749cc` | `4e68ac8383e5e332d52339fb7c7a3f1bfb75f8351bd90baee28881f68d248e66` |

逐run前缀为 `round5-control-r5csat`、`round5-control-r5cpoint`、`round5-candidate-r5nsat`、`round5-candidate-r5npoint`、`round5-candidate-r5nmixed`。均保存 `.provenance.json`、`.background.jsonl`、`.awake-before-run.json`、`.awake-samples.jsonl`、`.awake-after.json`、`.timing-windows.json`、`.safe-os-events.json`、`.post-run-audit.json`、provision/runner stdout与actualexit、`.outcome.json`；仅四个成功run有 `.completion.json`，mixed为 `.wrapper-stop.json`。

| Label | Post-run audit SHA256 | SafeOS subset SHA256 |
| --- | --- | --- |
| r5csat | `5a64619460a4a9d7bf24c3a268bcaf181d5e6333b975cb0a0f5f026c9b35be83` | `75c71f80b82a2ee6333d6b9825547e7eb3b3083da7e37253c09e36e5f0edc347` |
| r5cpoint | `fb525d8f51c051d86beadc2c2125287bc71d1ec2fc82ef7a906a448bab4ba985` | `0f8bcd7e3da1d74172163e90cc4b9bf685e0052878e97a9ee91d45281f7f5bd4` |
| r5nsat | `96b7cfd51db2dd4abe27e2aec4d7e7564734b83546c4b612dff7e773f97825a2` | `ee422f6a25592279fd0b15e4c4bf6fb7681c0c166e4df6da4b42feabab751c55` |
| r5npoint | `0f28ef5f27e046976b956d27b13cf38672c0d4ba3af78c488a62f9d6984ba422` | `184fce33bd87cf293a41fcd6c16c65303f0b2d0945c5ee4b9a0a4a5a95cc2ba6` |
| r5nmixed | `03af611c593a2365a1af497bdb2f0098f2c2c0b3dd9d1b6c0b25240a6c38c87c` | `d0a079aff08c4b7c1ecd0ec50305dfce769ac8fa1f9019aae41b18f12428e6c2` |

F独立 `05_verification.md` 应从原raw复算计数、quantiles、ACK-only ledger、firsterror/peerdrain、actualawake与time/OS审计及未覆盖项；作者报告不替F宣布验收。最终作者结论保持 **paired描述性样本有界完成 / mixed FAIL / wholematrix PARTIAL**。
