# R8：writer-admission，mixed first 预声明

Date: 2026-10-07 Asia/Shanghai. Author/runner: current agent; independent verifier: F.

## 授权与状态
- Main明确GO仅r8mixed；arch/common未授权，不提前provision。原schema HOLD已由main修复，仅runtime gate语法迁移；保留原记录，不称prior load failure。
- Root accepted `09c478be663fff8c90da5bcc0dc697b378d17183`；preflight status仅用户CLAUDE.md dirty，不修改该文件。计划/报告是执行前新增docs，不改业务源/README。
- 新限定Parent acceptance来源 `docs/v1/WRITER-ADMISSION-VERIFICATION.md`，SHA256 `74f7f4afe100b09ad6a188e7118997cf468fd20f045d693a4559002b19b65e74`；不是最终mission/Linux/SDK/UI/压力PASS。
- Fixture `/private/tmp/peri-stress-r8-writer-l3qe2qth`，OUTPUT `/private/tmp/peri-stress-r8-results-sb3fz4hf`；main已创建唯一新桶、启动wholefixture，不重启/重建桶/取rawkeys/Docker/bucketdelete。

## Provenance 与既有 main gate
- Native `/tmp/peri-stress-native-writer-admission-bin` SHA256 `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`。
- Go `/tmp/peri-stress-platform-writer-admission-bin` SHA256 `edf09e768c5845918ec007f8dbf699cd4e4f930eacf0af1d11e77f28c977b14c`。
- Harness `/tmp/peri-stress-phase-drain-bin` SHA256 `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`；冻结bounded-drain版本，无本轮build。
- Main whole-smoke11checks PASS/exit0；本次核 `whole-smoke.json` SHA256 `9aa0eee8edec5fe7c5cbd67ae91146af4337fa177d4070bf6ef5bd3fe4ad1f8f`，实际字段pass=true/backend=external_s3_fixture/tls=certificate_verified。
- ready=true、whole-smoke exit0、wrapper uppercaseREADY假阴性留档与main-launch修正为main attribution，不把原wrappertimeout洗掉；本runner未重跑smoke。
- Gate路径 `$OUTPUT/joint-pass-round8.json`，原SHA256 `a6e2c65576c08cc2317dea15fb55247e86db6deee438a1df99e997f0874d2bff`；原main扩展attestation保留，不由runner删字段覆盖。
- Main已在同路径签发exact六字段gate，SHA256 `40d844b4558d11c4d2c3008f7e73629a6d5dd2a514b998462706697ba819fac9`；侧证 `$OUTPUT/joint-pass-round8-provenance.json` 保留原scope/provenance。本runner已核sameCorefields、acceptedartifacts、smoke完全匹配，main明确继续GO。

## 修复后 preflight（未完成前继续HOLD）
- 重新核source/bin/acceptedmanifest、private目录0700/文件0600、无global/outputlock/labelreservation，actualCA/profile/port与所有ownPID/UID/start/argv/binSHA；docs-only改动纳入before/after冻结。
- Own launcher50542/UID501/start Wed Oct 7 23:08:03 2026；exact fixture argv，不允许runner终止wholelauncher。main topology/privatechildren需身份完整核对，不能只凭PID。
- Main/A/B/C/F无builderAPI负载为交接；只读externalbuilders与背景资源，存在builder或guard不足HOLD给参数，不kill；无己方heavy prep/build/其它场景并发。
- Runner仅启动/停止自己的caffeinate/压力CLI。own `caffeinate -i -t 3600 -w "$LAUNCHER_PID"` 保存PID/UID/start/argv与绑定，并实际确认PreventUserIdleSystemSleep断言；仅alive不足。
- 每runner前/周期/后保存safeOS/awake、wall+monotonic窗口和sampledresources；无sleep、无限等待或用户pmset/keychain修改。preflight连续三次安全sample，active compiler即HOLD，既有idle esbuild留confound；己方5个fixturePID的sampleRSS须各<=512MiB、背景编译CPU求和<=400%，host至少4GiBavailable disk。周期sample由childwait有界timeout驱动。
- Public C4新regime=general3/owner1；Worker默认128/control16/sql112=general110/owner1/cleanup1，必须actualprofile核对；非旧general4 paired因果提升。

## 唯一 mixed 命令模板（gate修复并放行后）
```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-r8-writer-l3qe2qth \
  --output /private/tmp/peri-stress-r8-writer-l3qe2qth/stress-r8mixed.json --label r8mixed
/tmp/peri-stress-phase-drain-bin \
  -connection /private/tmp/peri-stress-r8-writer-l3qe2qth/stress-r8mixed.json \
  -output /private/tmp/peri-stress-r8-results-sb3fz4hf -joint-pass /private/tmp/peri-stress-r8-results-sb3fz4hf/joint-pass-round8.json \
  -cases mixed_oltp -concurrency 1,4 -warmup 2s -smoke 10s -measure 30s \
  -offered-rate 50 -rows 1000 -seed 20261007 -request-timeout 10s \
  -max-process-rss-bytes 536870912 -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```
- 每label一次provision/runner；新业务IDs/DB/project，不重放smoke/R7请求。RSS512MiB仅samplethreshold，generated256MiB为charged请求字节非S3量，artifact64MiB按runner范围；缺指标null不猜0。

## 首失败与下一轮边界
- 首unexpectednegative/unknown/resource/clock/oracle或非COMPLETE立即stopOffers/dequeue，自然drain已logicallyadmitted peers在原deadlines/parentcaps内；不cancel制造unknown、SQL/HTTP retry/replay、换label绕过或cleanupmutation。
- Unknown自身poison/FAIL或UNRESOLVED，不ROLLBACK/reopen/盲Close；必要parent/resource/artifacthardcancel保留真实unknown，不延deadline/降级。
- Wall倒退、abs(wall−mono)>1s、confirmed suspend/awake失效STOP；ACK-onlyledger非durability，须所有完整readbacks/postsmoke/finaloracle实际PASS。
- 完成后先汇报rawSHA/count/result/profileGateSourcebeforeafter与actualexit；F独立verify/main批准后才能下一matrix。不自己签verification；服务exit0不修复runnerFAIL。
- 本轮仅mixed；未来顺序arch6→common13，须另行GO，当前均NOT PROVISIONED/NOT EXECUTED。
- 不终止wholelauncher50542，不动用户rootapp55987/RustFS/container/bucket。仅核验own压力CLI/caffeinate身份后清理它们，保留fixture可检查。
- R7 run a22db275b5536ddf mixedFAIL60.469s/natural337ACK+1typedBusy WorkerB4.073s、arch/commonNOTRUN保持，不借历史结果宣称R8PASS。
