# Round 6：新请求安全诊断匹配 Busy，runner FAIL，覆盖 PARTIAL

## 结论与不可外推范围

唯一新实验 `r6diag` 实际exit **1**，raw状态 **FAIL / sql_error**，run ID `7711a82b016b254e`，elapsed48.653588708s。C1 warmup/smoke/measure COMPLETE；C4 warmup发生2个typed SQLnegative后整个实验停止，C4 smoke/measure未执行。

首失败Execute request与当前worker-b的**唯一完整allowlisted事件**精确匹配，`cause_class=busy`。这是这个**新R6请求**的native安全分类证据，不是R3/R5未保存body的追溯解释，也不证明具体锁竞争参与者、唯一架构根因或修复已经成功。匹配到原因不把FAIL改PASS。

两个已在途peer在首negative后自然ACK，unknown/cleanup_unknown均0。实际drain终态保留不等于最终durability：ACK模型ledger为101（97 mutations+4 preflight），**仅39条实际持久验证**，没有post-measure/final oracle，不发表完整mixed性能PASS。

所有run/phase/RPC/workflow时间、实际awake与safe OS审计PASS；无确认Sleep，未触发已记录的resource guard。没有重试/旧请求重放、追加diagnostic SQL、cleanup mutation、signal或后续architecture/SQL/faultprobe。

## Source、gate与固定artifact

执行HEAD：`2e462776c9248ab4281c34bb16639fd061bbc5a5`；预声明 `06_PLAN.md` SHA256 `cb16a36903c01d4bab6c7682f055d9ef7e4cd64f9dad7cd0615b5f76ee33dbb2`。Main确认A/F完整gate与James安全静态接受、所有本任务builders/prep停止，另下发唯一runner授权。本报告不重跑那些gates。

执行前后仅用户 `CLAUDE.md` dirty，未修改；status SHA `b4efa7568a2f2c5b463d62276d2aeb1538a80ffaa1c3fe5260600043d2f9a742`、tracked diff SHA `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848`。Dirty真实记录，用户文件排除于不可变runtime代码，不伪装clean。

Gate：`/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round6.json`，SHA `2db40988b7340224b2876ccd4ce8c53c603da0ca56a865661af1ff14dfda7bc5`；private0600普通JSON、main matching-HEAD operator attestation，非密码学签名证明。

| Artifact | 路径 / SHA256 |
| --- | --- |
| 新native安全诊断artifact | `/tmp/peri-stress-native-diagnostics-bin` |
| Native SHA | `63779a5c020787201ca5639430294060b870b1d162fc6cca9ac5f40e64c3a2f3` |
| 不变Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| 不变new-drain harness | `/tmp/peri-stress-phase-drain-bin` |
| Harness SHA | `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8` |

本artifact busy constructor仍1s、SQL duration budget仍5s、请求deadline10s；quota/预算与Gateway terminal flags/classifier/no-retry不变，不因诊断结果调整本次运行参数。

## Fresh fixture、awake与实际命令

Fixture：`/private/tmp/peri-stress-r6-diagnostic-6hzY7sw3`，全新完整TLS/S3、proper CA HTTPS19446、独立unique S3 bucket bootstrap PASS、两个worker。Bucket初始化SHA `449c0c962dfde5742af39f1af7d655a4fc029c73195c11726de4dc7cb08af4ec`。目录0700、private connection/awake/log0600，不公开secret。

Launcher36118，authority36154、platform36155、worker-a36159、worker-b36160；launcher host-observed start为 `Wed Oct 7 13:40:31 2026`，四service为13:40:32。逐run provenance保存每PID/start/executable完整SHA。

Main-owned caffeinate PID40764，argv `/usr/bin/caffeinate -i -t 3600 -w 36118`，main持有PTY53959。Runner前独立检查actual `PreventUserIdleSystemSleep` owned PID、UID/PPID/start identity、exact argv与launcher绑定，运行中5次采样及post-run重新核验PASS。不是只靠PID alive/argv推断生效；不改用户电源设置、deadline或lease，不保证阻止forced sleep。

一次safe provision actualexit0，唯一runner PID73950；精确参数如下：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-r6-diagnostic-6hzY7sw3 \
  --output /private/tmp/peri-stress-r6-diagnostic-6hzY7sw3/stress-r6diag.json \
  --label r6diag

/tmp/peri-stress-phase-drain-bin \
  -connection /private/tmp/peri-stress-r6-diagnostic-6hzY7sw3/stress-r6diag.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round6.json \
  -cases mixed_oltp -rows 1000 -seed 20261007 -concurrency 1,4 \
  -warmup 2s -smoke 10s -measure 30s -offered-rate 50 -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

## 原始计数、ACK账本与oracle边界

作者只读逐行重算operation及ACK mutations，342个业务operation IDs唯一；不是从HTTP累计数字推测write数。

| Phase / C | 状态 | Elapsed s | Offered | Attempt /ACK | Client drop /expired | ACK mutations |
| --- | --- | --- | --- | --- | --- | --- |
| Warmup C1 | COMPLETE | 2.047132375 | 100 | 27 /27 | 72 /1 | 7 |
| Smoke C1 | COMPLETE | 10.022231792 | 500 | 108 /108 | 391 /1 | 28 |
| Measure C1 | COMPLETE | 30.320146417 | 1500 | 196 /196 | 1303 /1 | 59 |
| Warmup C4 | FAIL /sql_error | 1.692995042 | 81 | 11 /9 | 66 /4 | 3 |
| 合计 | PARTIAL | — | 2181 | 342 /340 | 1832 /7 | 97 |

两个error均typed SQLnegative；所有phase unknown、cleanup_unknown、timeouts、correctness_failures均0。Client drop不是serverquota错误。Failed operation的 `correct=false`不等于已ACK row mismatch。

Preflight实际oracle PASS ledger4、account sum100,000,004。C1post-smoke实际oracle PASS ledger4+7+28=**39**、account sum99,999,995，账户hash/ledger hash均与expected一致。随后ACK mutations59+3=62条没有最终readback。

模型账本 **101=4+97** 仅为累积ACK期望；没有实际查询101行，不承诺两个failed执行无committed prefix。C1完整measure不能替代其后持久oracle：case在C4提前FAIL，故无C1 post-measure、无case末/final oracle。没有以新SQL或恢复mutation补齐这些缺失。

## 首negative关联与实际peer drain

首failed request：`stress_7711a82b016b254e_000000002516`，`POST /v1/sql/execute`，HTTP400 /173bytes，`sql_error`、`execution_completed=true`、`may_have_committed=true`、unknown=false、timed_out=false。RPC elapsed1.043768417s；第二failed Execute request后缀2518 elapsed1.047800208s。不能只由这些时长猜原因，首原因来自下面实际事件。

Worker-b唯一匹配首request的五字段安全JSON：

```json
{"event":"native_sql_terminal_error","action":"execute","request_id":"stress_7711a82b016b254e_000000002516","cause_class":"busy","execution_completed":true}
```

事件只含审计allowlist，不含SQL/native message/session/namespace/secret。诊断源码 `prototypes/v1/src/worker_rpc/diagnostics.rs` 的typed native `LimboError::Busy`映射为 `busy`；本次安全class不依赖SQL正则或user错误文本。它不是BusySnapshot，也不包含锁持有者等进一步因果证据。

新drain合同停止offers/dequeue，首FAIL不被其它ACK覆盖；两peer request后缀2545、2543在首negative后自然ACK，0unknown。两SQLnegative均保留，没有retry/implicit rollback/reopen/盲close。Flags不保证完整batch rollback；peer ACK也不等于最终durability。

## 诊断counter、writer边界与缺失证据

源码 `MAX_DIAGNOSTICS=64`，每个 `Diagnostics`实例通过atomic fetch-update为record预留slot，最多64个记录slot；这不是全cluster计数或时间窗口保证。Slot在序列化/写出前消费，`write_all`失败被忽略、不会补偿slot；没有全局writer-drop counter，不能从日志2行推断内部counter精确等于2或global drops=0。

Writer输出有限allowlisted字段并写stderr；64slot限制不是硬实时写延迟保证，也不是无限日志完整性保证。缺记录/达到cap/已知writer drop/unknown class/不唯一关联都应NOT_DETERMINED，不能由空日志猜无错误。本次首request有唯一完整匹配事件，可确定该新请求class；不据此宣称其它潜在丢失事件不存在。

停后只读当前own两worker log，使用nonblocking/nofollow FD、fstat regular/owner0600及1MiB bounded read，仅读取/输出allowlist JSON与文件hash，不泄露其它日志内容：

| 当前secure日志快照 | Bytes | Allowlisted events | SHA256 |
| --- | --- | --- | --- |
| Fixture `logs/worker-a.log` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Fixture `logs/worker-b.log` | 314 | 2 | `364a5b15161bc1f0accd0b4da68f33866f06f51a638b32eb30028a71b34bc4bf` |

两文件均secure regular0600，首request精确匹配数量1，action execute与raw一致，invalid allowlisted records0。Observed event count低于64，不是内部剩余slot或全局zero-drop证据。这些SHA是关联时当前日志快照，不假称尚未交接的shutdown后文件身份。

## Timing / OS /awake与实际资源

Run host-wall observed start `2026-10-07T05:45:15.053638Z`、finish `05:46:03.707092Z`，wall48.653454s；原monotonic48.653588708s。时间只是host观察，不推断外部校时日期；wall只用于异常审计，不替换elapsed分母。

Post-run立即审计 **780windows=1run+4phase+433RPC+342workflow**，最大abs wall−mono0.000134708s、异常0；safeOS Sleep/DarkWake/Wake subset0，5次actual awake采样及pre/post owner绑定PASS。无>1s时间分离或确认suspend；采样/过滤仍不证明所有forced sleep/采样间暂停都不可能。

43个actual resource samples，RSS最大bytes：authority46,006,272、platform20,905,984、worker-a12,959,744、worker-b33,341,440，未记录512MiB RSS guard触发。Actual RSS不是cache payload；采样间峰值未知。Background7样本、所有cargo/rustc同样本CPU求和峰值 **293.2%**，真实外部编译干扰保留，不声称零干扰、纯净性能或最大容量。Generated request bytes charged192,661。

Raw actual metrics/measurement scopes保留全局/进程观测，缺失不造0；此单诊断FAIL不从累计metrics推出稳定吞吐或普遍改进。

## 停止事实与冻结证据

唯一runner已actualexit1，E全部STOP、host/output locks释放；无C4 smoke/measure，无后续arch/SQL/faultprobe/UI，也不自动预声明执行下一run。Main实际停机事实经F交接：正常停止own launcher36118，whole PTY29125 actualexit0、caffeinate PTY53959 actualexit0，全部6个owned PID gone、exact Authority clean。这是main executor attribution，不是E实施signal或追加OS/API观测。

Base：`/private/tmp/peri-stress-results-h3wtrdm2`。

| 相对证据路径 | SHA256 |
| --- | --- |
| `run_7711a82b016b254e/run.json` | `8cf8e2c546eabbe2b1ae7d3d84c00b6146868b7d0b913faccfdd94f448e2e12f` |
| `run_7711a82b016b254e/operations.jsonl` | `971f824fddaa7d3e9417d960af9329bd7ab34799d8036a49ca2dbc8d2b6b2967` |
| `round6-diagnostic-r6diag.provenance.json` | `aea627458c9ae12b31eb9cdf56e87aac979febdde17f543af1b1bd23f28df06a` |
| `round6-diagnostic-r6diag.post-run-audit.json` | `aedd28e3c89f189d2848af8a1662fd128db860decbf07d963d88c8bd71528bc3` |
| `round6-diagnostic-r6diag.safe-os-events.json` | `0d30c49b5037c99be0aa410723a7b5be8526a2964778f31ede01b66d8c30ce55` |
| `round6-diagnostic-r6diag.diagnostic-association.json` | `54ab9f8516cfaea6a5e1e66ba29e7f25f7e2a9f85a0bdaaea1c0e4e87352a015` |

同前缀保存background/awake samples及pre/post实际assertion、timing全window审计、精确argv、provision stdout/exit0、runner stdout/exit1、outcome及wrapper-stop；没有成功completion文件。F独立06验证负责原raw重算与安全关联复核，作者不提前替F宣布PASS。

最终作者结论：**R6新请求Busy分类已匹配 / 实验FAIL / durability与矩阵PARTIAL / 不追溯解释R3/R5**。
