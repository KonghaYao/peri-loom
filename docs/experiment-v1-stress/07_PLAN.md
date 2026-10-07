# Round 7 预声明：latest busy4s整体候选，common write +architecture gates

## PLAN ONLY /全部STOP

本文不授权load/provision/start/build/SQL/signals。Main先正常停止自己的R6 fixture/assertion并记录actualexit，负责新source commit/freeze、新artifact A/F完整gates、startup/secret、新matching-HEAD operator attestation（private JSON，非密码学签名）及明确runner授权；此前E持续STOP。

NEW latest native候选path/完整SHA/差异manifest待A/F/main冻结。Busy等待配置为**4s、connection init once**；SQL duration Budget仍5s、request timeout仍10s，其它执行预算/quota/HTTPflags/classifier/no-retry不改。仅预声明，不保证4s能消除负终态或实现全PASS，不在运行中改设置。

## 唯一fresh整体fixture与共同前提

- ONE fresh完整private TLS/S3 fixture、两个worker、proper CA HTTPS、新unique S3 bucket初始化PASS；不复用R6及以前DB/cache/bucket/labels，不重放历史SQL/business IDs。
- Gateway `/tmp/peri-stress-platform-correlation-bin` SHA `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d`；drain `/tmp/peri-stress-phase-drain-bin` SHA `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`，均保持不变。
- Main-owned `caffeinate -i -t 3600 -w "$LAUNCHER_PID"`；保存owned PID/UID/PPID/start identity/exact argv、exact launcher绑定及actual退出。
- 每runner前保存actual `pmset -g assertions` safe subset，确认 `PreventUserIdleSystemSleep`由exact owned PID持有，PID/start/launcher绑定有效；缺失/不匹配/窗口到期STOP，PID alive/argv不是生效证明，不改用户pmset/keychain/deadline/lease，不保证阻止forced sleep。
- Rows1000、seed20261007、单进程sampled RSS512MiB、generated256MiB、artifact64MiB、private目录0700/config0600；全host唯一runner，所有本任务builders/tests/prep停止，不传credential于argv/log/docs。
- Source HEAD、dirty用户CLAUDE排除项、完整artifact/source manifest真实记录；未经授权/不符freeze的代码或binary变化STOP。

## 唯一授权候选的串行序列

每label新project/DB/crypto runprefix/business IDs，一次safe provision、一次runner。同一fresh service/bucket按以下顺序保留历史；必须当前runner及全部持久oracle/时钟awake审计PASS才能进入下一**已明确授权**项。

| 顺序 /Label | Case | C | W /S /M | Rate | 用途 |
| --- | --- | --- | --- | --- | --- |
| 1 `r7mixed` | mixed_oltp | 1,4 | 2s /10s /30s | 50 | common write与完整persistent oracles |
| 2 `r7arch` | architecture6 | 1,2 | 2s /10s /10s | 50 | functional gate-only |
| 3 `r7sql` | sql，13 actionable | 1,2 | 1s /2s /5s | 25 | functional gate-only |

Architecture6精确cases：`session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate`。这里只是短architecture gate中的saturation，不安排新paired control-sat/point或完整sat sweep。

预声明SKIP仅限：noisy_tenant C1需至少两个independent sessions；graceful_migrate target已拥有fixture（reason `migration_target_already_owns_fixture`）；SQL `snapshot_branch_pitr_under_load`因fault schedules/retention oracle未实现。显式SKIP均**不计PASS/coverage**，其它缺配置/管理权限/DB导致SKIP不默许继续。

SQL13：point_read、index_range、pagination、mixed_oltp、hot_keys、bulk_tx、sort_aggregate_join、index_plan、rollback、constraint_error_reuse、large_values、sustained_wal、checkpoint_reopen。一次seed、共享累积ACK ledger、独立逐case oracle；constraint预期negative必须typed flags、同session显式ROLLBACK、余额/正常write/query均验证，不是任意SQL错误豁免。

## 模板：非实际执行权限

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir "$FIXTURE" --output "$FIXTURE/stress-$LABEL.json" --label "$LABEL"

/tmp/peri-stress-phase-drain-bin \
  -connection "$FIXTURE/stress-$LABEL.json" -output "$OUTPUT" -joint-pass "$GATE" \
  -cases "$CASES" -rows 1000 -seed 20261007 -concurrency "$C" \
  -warmup "$W" -smoke "$S" -measure "$M" -offered-rate "$RATE" \
  -request-timeout 10s -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

Main下发paths/新gate及具体范围后才能执行，不是无条件shell batch。Timed下限84+154+192=430s（约7.2min），另含startup/provision/lifecycle/constraint/checkpoint/oracle/drain审计，预留15–25min非完成保证；不自动补跑。

## 首失败、bounded drain与持久oracle

意外SQLnegative、unknown、runner/case/phase非COMPLETE、持久oracle失败、时间/awake/source/resource风险均立即STOP整个剩余序列，不换label绕过、retry/replay、追加诊断SQL或cleanup mutation。上面的精确SKIP及constraint声明步骤是唯一功能覆盖例外；typed admission可作为saturation正常负终态但真实计error，client drop不等于serverquota。

首known non-admission negative仍FAIL+stopOffers/dequeue，已经逻辑admitted peer在原有限deadline与parent caps自然drain；真实unknown自身poison、FAIL/UNRESOLVED，不ROLLBACK/reopen/盲close。Parent/resource/artifact安全仍hardcancel，不延长deadline或把cancel unknown降级。

ACK-onlyledger与实际持久readback分开，失败请求不承诺无committed prefix。完整mixed必须所有smoke/case后持久oracle实际PASS，不能用C1完整measure或peer ACK补缺最终oracle；短arch/SQL均gate-only，无稳态性能结论。

## 立即audit与诊断证据

每runner前actual awake、全PID/start/bin SHA/privatepermission、lock absent、预算/source freeze、hostcapacity与readonly背景CPU/RSS检查；既有<=400%外部编译求和/稳定RSS、有界quiet guard保持，不kill用户进程，不无限等待或unmanaged idle。

每run返回后立即审计所有run/phase/RPC wall-vs-monotonic、safe OS Sleep/DarkWake/Wake subset、实际awake owner绑定/生命周期、全部必要oracles，再决定下一label provision。Wall倒退、abs wall−mono>1s、confirmed suspend或awake失效均STOP，不延后到最终报告才审核。

若产生negative，只读当前own worker安全allowlisted diagnostic class与精确matching request ID；缺日志/64cap/已知writer drop/unknown或不唯一匹配均NOT_DETERMINED，不猜Busy、不声称global0drop。新事件只解释新请求，不追溯R3/R5，也不使FAIL变PASS。结束后无额外SQL/cleanup/signals，main拥有正常shutdown。

## 报告边界

保留private/tmp精确CLI、HEAD/dirty/manifest、所有PID/start/bin SHA、bucket初始化、actual awake、安全OS/clock审计、背景/RSS、secure diagnostic、stdout/actualexit、raw与SHA；missing metrics为null/unavailable，不造0。

只引用R5 native `868962e6896fa166c1429aa149812955e9740e690118609ebff20d2f0ccc89b4`的历史singlepair事实与其背景局限；**它不是新busy4s artifact的测量**，不把历史sat/point当新版本性能PASS。新artifact全体变化非isolated参数因果证明。

主要目标是common write及architecture/SQL有界功能完成，不因预声明或找到原因就宣称wholeproject/full pressure PASS。作者/F后续区分COMPLETE/FAIL/SKIP/NOT EXECUTED与实际oracle边界；main新gate/明确授权前保持STOP。
