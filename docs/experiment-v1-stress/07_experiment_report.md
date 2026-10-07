# Round 7：busy4s候选 mixed FAIL，architecture/SQL未执行

## 结论与范围

唯一实际runner `r7mixed`，run ID `a22db275b5536ddf`，actualexit **1**，raw状态 **FAIL / sql_error**，elapsed60.469346375s。C1 warmup/smoke/measure及C4 warmup COMPLETE；C4 smoke出现typed SQLnegative，整个授权序列立即STOP。

首failed新Execute request精确唯一关联当前worker-b的safe `native_sql_terminal_error`事件，**`cause_class=busy`**。这是本次新R7请求的分类，不是对历史R3/R5的追溯解释，也不证明具体锁持有者、唯一架构根因或一般化性能因果链。诊断确认原因不把FAIL改PASS。

3个已在途peer在首negative之后自然ACK，unknown/cleanup_unknown均0；它们是实际drain终态证据，不是最终durability。ACK模型ledger114=110 mutations+4 preflight，**仅45条实际持久验证**，无post-measure/final oracle。

`r7arch`、`r7sql`未provision、未执行；architecture6/commonSQL13目标未完成，wholematrix **PARTIAL / 含真实FAIL**。没有新paired control-sat/point，本artifact不能借用R5历史singlepair称作已测性能PASS。

## 授权、source与artifact

Main明确授权新fixture的一次串行序列，要求每run完整oracle与time/awake审计通过才可进入下一项。A/F/James owned tests/builds已由main确认停止，native317unique及main实际S3/Go gates由main确认；作者不在测量期重跑gate。

执行HEAD：`a26b6acb802f3fa2e53f9cb90ce10b76c2d4ac10`。冻结 `07_PLAN.md` SHA `8f5a03c8af46bcffc09962bc3404bdae8a31c7d176560e1d3d7a7afb99f244f4`；执行前后root仅用户 `CLAUDE.md` dirty，真实记录并排除于不可变runtime代码，未修改。运行结束才写本报告，不在load中改变git freeze。

Gate：`/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round7.json`，SHA `4dc2646a948c90c160574ff13677fb9445903acb08636fa41a0379254fd983d1`。Private0600普通JSON、main matching-HEAD operator attestation，**不是密码学签名证明**。

| Artifact | 实际路径 / SHA256 |
| --- | --- |
| 新busy4s候选，mode0500 | `/tmp/peri-stress-native-busy4-bin` |
| Native SHA | `237625635a15b5d2ca0044ed2538cf1d67968f40cf73541c1170acd4310196cb` |
| 不变Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| 不变new-drain harness | `/tmp/peri-stress-phase-drain-bin` |
| Harness SHA | `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8` |

本候选busy等待4s、connection init once；SQL duration Budget仍5s、RPC deadline10s，其它quota/执行预算与HTTPflags/classifier/no-retry不变。没有运行时调参、增加重试或把native error当完整batch rollback。

## Fresh fixture、actual awake与一次命令

Fixture：`/private/tmp/peri-stress-r7-candidate-SluMVQ54`，新whole-project、proper CA HTTPS `127.0.0.1:19446`、新unique S3 bucket bootstrap PASS、两个worker。Bucket初始化SHA `43ebf5e56f32761a9fc8ad5a9dae3e9487f41015e0fb9014f57a6032b0e3f327`。未复用旧DB/cache/label或重放旧SQL/business IDs；目录0700、config/awake/log0600，无credential泄漏。

Launcher85184，authority85223、platform85224、worker-a85227、worker-b85228。Main-owned caffeinate88055，argv `/usr/bin/caffeinate -i -t 3600 -w 85184`。Pre-run独立验证actual OS `PreventUserIdleSystemSleep`确由该owned PID持有，并核对UID/PPID/start identity/exact argv与launcher绑定，不只靠PID alive。Main持有fixture PTY67569、awake PTY94232。

唯一runner PID96942，唯一safe provision actualexit0，精确CLI如下：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-r7-candidate-SluMVQ54 \
  --output /private/tmp/peri-stress-r7-candidate-SluMVQ54/stress-r7mixed.json \
  --label r7mixed

/tmp/peri-stress-phase-drain-bin \
  -connection /private/tmp/peri-stress-r7-candidate-SluMVQ54/stress-r7mixed.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round7.json \
  -cases mixed_oltp -rows 1000 -seed 20261007 -concurrency 1,4 \
  -warmup 2s -smoke 10s -measure 30s -offered-rate 50 -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

## Raw重算、计数与持久oracle

作者只读逐行重算operation/ACK mutations、按phase observed窗口核对attempt/ACK，并独立重算所有phase nearest-rank p50/p95/p99/max，与raw一致；338个业务operation IDs唯一，没有请求重放。

| C /Phase | 状态 | Elapsed s | Offered | Attempt /ACK | Client drop /expired | ACK mutations |
| --- | --- | --- | --- | --- | --- | --- |
| C1 warmup | COMPLETE | 2.023841458 | 100 | 30 /30 | 69 /1 | 7 |
| C1 smoke | COMPLETE | 10.245166625 | 500 | 95 /95 | 404 /1 | 34 |
| C1 measure | COMPLETE | 30.241514125 | 1500 | 184 /184 | 1315 /1 | 58 |
| C4 warmup | COMPLETE | 4.655066125 | 100 | 9 /9 | 87 /4 | 4 |
| C4 smoke | FAIL /sql_error | 8.707794500 | 328 | 20 /19 | 304 /4 | 7 |
| C4 measure | NOT EXECUTED | — | — | — | — | — |
| 合计 | PARTIAL | — | 2528 | 338 /337 | 2179 /11 | 110 |

已执行phase unknown/cleanup_unknown/timeouts/correctness_failures均0，1个typed SQLnegative。Client drop不等于serverquota；failed record `correct=false`不是ACK row mismatch。C4 warmup elapsed大于2s requested window含实际bounded drain，不把它假报为时钟异常或2s完成。

Preflight oracle实际PASS ledger4、account sum100,000,004；C1post-smoke实际PASS ledger4+7+34=**45**、account sum99,999,981，账户hash与ledger hash均与expected一致。随后ACK mutations58+4+7=69条没有最终readback。

ACK模型 **114=4+110**，不是实际查询得到114条已持久行数，也不能据此断言failed execute无committed prefix。Case在C4提前FAIL，故无C1 post-measure、C4 post-smoke或最终oracle。完整C1 measure只表明该阶段transport ACK与operation检查完成，不具备最终durability-qualified mixed性能结论。

## 首失败、安全诊断与drain

首failed request ID `stress_a22db275b5536ddf_000000002711`，`POST /v1/sql/execute`，HTTP400 /173bytes，typed `sql_error`，`execution_completed=true`、`may_have_committed=true`、unknown=false、timed_out=false。RPC elapsed4.073926167s只是实际观察；**原因来自精确事件关联，不是由接近4s时长猜测**。

Worker-b唯一完整匹配事件：

```json
{"event":"native_sql_terminal_error","action":"execute","request_id":"stress_a22db275b5536ddf_000000002711","cause_class":"busy","execution_completed":true}
```

仅审计allowlist字段，不输出SQL/native message/session/namespace/secret；typed `busy`不等于BusySnapshot或具体锁/ownership根因。Safe diag记录与HTTP request ID/action一致，首匹配数量1；没有因匹配成功改变raw FAIL分类。

三个peer request后缀2835、2906、2909在首negative之后自然ACK；已admitted workflows原有限deadline drain、stopOffers/dequeue、不恢复新调度。Unknown0是此运行的实际终态，不证明所有lost COMMIT路径安全或最终durability。未执行retry、implicit rollback、reopen、盲close或追加诊断SQL。

停后只读当前own worker logs，nonblocking/nofollow FD+fstat owned regular0600+1MiB bounded read，仅提取allowlist事件与hash：

| 当前secure日志快照 | Bytes /events | SHA256 |
| --- | --- | --- |
| Fixture `logs/worker-a.log` | 0 /0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Fixture `logs/worker-b.log` | 157 /1 | `1c2549da66d33420c1e70fb0d01c731bfee864a364b1eb94608666d60d1e6c4b` |

诊断counter每实例最多64slots、slot在writer前消费；write失败无global drop counter，日志1行不证明内部counter=1或global0drops。此首request有唯一完整匹配事件；缺日志/cap/已知writer drop/unknown class/不唯一匹配必须NOT_DETERMINED。以上SHA是当前关联时快照，不编造shutdown后身份。

## Timing /OS /awake及资源背景

Run host-wall observed start `2026-10-07T06:22:01.560169Z`、finish `06:23:02.029148Z`，wall60.468979s；原monotonic60.469346375s。时间是host观察，不推断外部校时日期；wall只用于异常审计，不替代吞吐elapsed。

立即post-run审计 **790windows=1run+5phase+446RPC+338workflow**，max abs wall−mono0.000367375s、异常0；safeOS Sleep/DarkWake/Wake subset0，7次实际awake采样与pre/post owner绑定PASS。没有确认suspend或>1s时钟分离；不声称杜绝forced sleep或全部采样间暂停。

52个actual资源样本；sampled RSS最大bytes：authority43,696,128、platform22,937,600、worker-a14,041,088、worker-b35,487,744，未记录512MiB guard触发。RSS是真实进程RSS，不是cache payload，也不保证采样间峰值。

Background9样本，全部cargo/rustc同样本CPU求和峰值97.5%，真实外部编译/应用背景保留，未kill用户进程；不声称零干扰、最大容量或孤立Busy4优化效果。Generated request bytes charged194,779，actual metrics/measurement scopes保留原观测，missing不造0。

## 停止、未执行项与后续权限

所有E runner已停止、host/output locks释放；`r7arch`及`r7sql` config与label reservation均不存在，architecture6/commonSQL13均NOT PROVISIONED /NOT EXECUTED。Snapshot-under-load原预声明explicit SKIP不计coverage/PASS；noisy/migration预声明SKIP尚未实际发生，不倒填结果。

E没有执行额外arch/SQL/faultprobe/UI/whole-smoke、新label绕过、旧ID重试或signal。Main交接实际正常shutdown COMPLETE：核验launcher85184的UID501、start identity `Wed Oct 7 14:18:30 2026`与exact fixture argv绑定后SIGTERM，whole PTY67569 actualexit0、caffeinate PTY94232 actualexit0；main只读ps确认85184/85223/85224/85227/85228/88055全部gone，Authority lock exact `clean\n`（hex `636c65616e0a`）。这些是main executor attribution，不是E实施signal或追加OS/API观测。任何新诊断/修复/实验都需另行明确授权，本报告不隐含下一轮权限。

只可引用R5 native868962…历史singlepair及其背景局限；它不是本busy4s artifact的sat/point测量。本轮说明**该冻结候选在这一fresh mixed试验仍出现Busy负终态**，不能外推为所有配置无效或完整项目性能结论。

## 冻结证据索引

Base：`/private/tmp/peri-stress-results-h3wtrdm2`。

| 相对路径 | SHA256 |
| --- | --- |
| `run_a22db275b5536ddf/run.json` | `47f9876731d61f6c14dfbe0dad8aab133f38394011631b859fda5ba00e8d9471` |
| `run_a22db275b5536ddf/operations.jsonl` | `3095ad06b175ad127aa426ac564001755006e172499b30ddc474e12846ffe3f5` |
| `round7-candidate-r7mixed.provenance.json` | `d692e02870f9a4b29c244a2521b5f0dbeba0985a3c7a8f2b66c0aba01de60705` |
| `round7-candidate-r7mixed.post-run-audit.json` | `5f17ad797212e5fb5f00b6d5e7a111a025498f206ae2f52482368fec7346ef3c` |
| `round7-candidate-r7mixed.safe-os-events.json` | `0692e93fceed8854337a52140ec053d4ab30c019ec3a5ff9318bf55f3d0976df` |
| `round7-candidate-r7mixed.diagnostic-association.json` | `f6028d070172b5770f2a37fa91d390627d283b00420807725fbbcb7a22d711e3` |

同前缀保留background/awake pre/samples/post、timing全window审计、精确argv、provisionstdout/actualexit0、runnerstdout/actualexit1、outcome及wrapper-stop；无成功completion文件。作者仅本报告写repo，旧raw/报告不覆盖。

F独立07验证负责raw重算、诊断精确关联、writer边界、clock/actualawake、ACK/oracle边界与未消费labels复核；作者不替F宣布验收。最终结论：**mixed FAIL / 新请求Busy已匹配 / wholematrix PARTIAL / 无新paired性能PASS**。
