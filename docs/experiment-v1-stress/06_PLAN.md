# Round 6：单次新诊断实验预声明

## PLAN ONLY / STOP

本计划不授权load、provision、build、startup、SQL、signal或credential生成。Main负责startup/secret、source/artifact freeze、新matching-HEAD operator attestation（普通private JSON，非密码学签名）与明确runner授权；授权前E持续全部STOP。

Native诊断artifact必须经A/F完整gate，完整path/SHA与新HEAD待main确认；不由E构建、选择未验artifact或从source猜binary身份。既有Round3/4/5原始失败及分类不变，本实验不是历史请求重放或其根因追溯证明。

## 唯一新fixture与共同条件

- ONE fresh private whole-project fixture、独立unique fresh S3 bucket及初始化PASS，proper CA HTTPS、两个worker；不复用旧DB/cache/bucket或访问旧fixture。
- Gateway保持 `/tmp/peri-stress-platform-correlation-bin`，SHA `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d`。
- Drain harness保持 `/tmp/peri-stress-phase-drain-bin`，SHA `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`。
- NEW native只增加已审计安全diagnostic证据能力；**default busy仍1s**，原quota/trusted执行预算、HTTP flags/classifier/no-retry不变，实际artifact所有差异由A/F/main记录，不宣称isolated性能优化。
- Rows1000、seed20261007、request timeout10s、sampled单进程RSS512MiB、generated256MiB、artifact64MiB；只一个actualrunner，无builders/tests/Docker竞争负载。
- Main拥有 `caffeinate -i -t 3600 -w "$LAUNCHER_PID"`；保留owned PID/UID/PPID/start identity/exact argv及exact launcher绑定、实际退出状态。
- 每arm/runner前main/E只读actual `pmset -g assertions` safe subset，确认 `PreventUserIdleSystemSleep`由exact owned caffeinate PID持有；仅PID alive/argv不证明生效，缺失/不匹配/到期STOP，不改用户pmset/keychain/deadline/lease，也不保证阻止forced sleep。

## 单一runner矩阵

| Label | Case | C | Warmup / smoke / measure | Rate | 用途 |
| --- | --- | --- | --- | --- | --- | --- |
| `r6diag` | mixed_oltp | 1,4 | 2s /10s /30s | 50 | 新请求原因诊断，非paired performance |

一次safe provision、一次runner，未消费label、新project/DB/crypto runprefix/business IDs；不重放R5请求、不换label绕过首FAIL。仅这一项，**没有后续architecture/SQL/faultprobe/UI/whole-smoke**。

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir "$FIXTURE" --output "$FIXTURE/stress-r6diag.json" --label r6diag

/tmp/peri-stress-phase-drain-bin \
  -connection "$FIXTURE/stress-r6diag.json" -output "$OUTPUT" -joint-pass "$GATE" \
  -cases mixed_oltp -rows 1000 -seed 20261007 -concurrency 1,4 \
  -warmup 2s -smoke 10s -measure 30s -offered-rate 50 -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

模板不是执行权限；main提供fresh paths/新gate后才能调用，不在argv/echo/stdout/docs中传凭据。Timed下限84s，另含startup/provision/oracle/drain/审计，无完成保证，不为凑满时间补跑。

## 安全diagnostic证据与不可确定性

Main保存新增secure worker stderr diagnostic，private owned regular日志0600；A/F冻结格式、writer边界、64cap/drop可观测性及request-ID关联合同。仅允许已审计diagnostic class与matching request ID等必要安全元数据；禁止SQL、native message、session、namespace、secret内容或无关用户日志。

只在**本次新请求**出现terminal error时，将raw request ID与实际worker diagnostic的request ID精确关联，记录实际class/source/filehash与匹配质量。日志来源、请求身份或终态不一致不得猜测。不因只有HTTP `sql_error`、文件名busy或历史修复就称BUSY。

日志缺失、64cap证据丢失/触发、dropped writer、unknown diagnostic class或无法唯一关联时，原因必须 **NOT_DETERMINED**；不伪造class、零drop或从缺失日志推断“没有native错误”。无negative则报告本次未观测到待诊断事件，不追加SQL制造失败。

诊断class只支持新请求的原因证据；即使本次确定，也**不追溯解释R3/R5**未保存的body或证明其唯一因果链。诊断结果与runner PASS/FAIL分开记录，发现原因不使SQL FAIL变PASS。

## 停止、drain与审计

首意外known negative仍FAIL并stopOffers/dequeue；已逻辑admitted peer在原有限deadline自然drain，实际ACK/negative/unknown逐条保留。真实unknown自身poison、FAIL/UNRESOLVED并STOP，不retry、implicit rollback、reopen或盲close；parent/resource/artifact安全仍原hardcancel。

每run结束立即审计所有run/phase/RPC wall-vs-monotonic、实际safe OS Sleep/Wake subset与actual awake assertion、source/artifact freeze、资源和持久oracle。Wall倒退、abs wall−mono>1s、confirmed suspend、awake失效、unexpectedFAIL/unknown/nonCOMPLETE或资源风险均停止，不自动cleanup/后续case或重跑。

Raw expected ACK ledger与实际oracle读取分开：持久oracle只报告实际成功边界，缺post-measure/final oracle不倒填PASS；peer自然ACK不等于最终durability。结束后禁止追加diagnostic SQL、rollback/cleanup mutation或signal；fixture/assertion正常退出仅main拥有。

Private/tmp保存精确argv、HEAD/dirty白名单/manifest、所有PID/start identity/bin SHA、bucket初始化、owned awake与safeOS/time audit、worker安全diagnostic、stdout/actualexit、rawJSON/JSONL及文件hash。背景CPU/RSS实际脱敏采样，既有<=400%外部编译/稳定RSS与有界quiet guard不变，不能kill用户进程或宣称零干扰。

作者与F后续只读复核诊断匹配、writer完整性、首终态、ACK-onlyledger及oracle边界；本文件不预先宣布诊断已确定、实验已运行或性能PASS。Main新gate/明确runner授权前持续STOP。
