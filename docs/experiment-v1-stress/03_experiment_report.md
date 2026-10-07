# 第 3 轮 baseline：两个 runner 完成，mixed 中止 — PARTIAL / UNRESOLVED

## 判定与停止状态

本报告仅覆盖正式授权的 **baseline arm**：新 correlation Gateway、correlation harness 与保留的旧 native Release。实际严格串行执行 `basats` → `bapoint` → `bamixed`，每项 fresh project/DB label、一次 provision、一次 runner。

- `basats`：exit 0、raw COMPLETE；C1/C2/C4/C8 全部 W2/S10/M30 完成，C8 的实际 typed admission rejection 正确记录，没有降级 response-ID 校验。
- `bapoint`：exit 0、raw COMPLETE；C1/C4 全部 W2/S10/M30 完成。
- `bamixed`：exit 1、raw UNRESOLVED，reason `session_cleanup_not_proven`；C1 W2/S10/M30 完成，C4 warmup 首个 typed SQL error 导致停载，两个在途请求取消后变为 unknown。没有 C4 smoke/measure 或失败后最终 oracle。

**Baseline 整体为 PARTIAL / UNRESOLVED，不是完整测量矩阵 PASS。** 已停止全部后续。新 native arm、额外 architecture gate 和 common SQL functional matrix 均 NOT EXECUTED；没有性能对照 PASS、最大容量或部署就绪结论。

先前首轮 `session_moved`、第二轮 429 unknown、本轮 mixed 首失败，以及所有原始目录均保留。没有 SQL/HTTP retry、重新消费 label、手工 rollback/close 不确定 session、cache 删除或故障信号。

## 冻结身份与 deliberate control variant

| 项目 | 本轮身份 |
| --- | --- |
| Source HEAD / main joint gate | `792a7e1b7659bccde15f97b8e7ca9a887da7d0ee` |
| Gate | `/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-correlation-view.json` |
| Harness | `/tmp/peri-stress-correlation` |
| Harness SHA256 | `c186fd905c34f9d68d2f899e162fdae2bc0539e1a290fffa0cf64341b0e54886` |
| Go platform | `/tmp/peri-stress-platform-correlation-bin` |
| Go SHA256 | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| Preserved native control | `/tmp/peri-stress-native-baseline-bin` |
| Native SHA256 | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Fresh full service fixture | `/private/tmp/peri-stress-pairbase-8m6qyLNX` |
| Output | `/private/tmp/peri-stress-results-h3wtrdm2` |

旧 native 是主代理明确授权的 control variant，**有意不是当前 ownership-view native**，不能称为新 source HEAD 的 native 重构建产物。相同新 Gateway/harness 与后续新 native 的对照尚未执行。

主代理确认 proper CA、HTTPS19446、两个 healthy workers 和独立 fresh S3 bucket。Bucket 初始记录为 fixture 内 `bucket-initialization.json`，每 run provenance 保留该文件 SHA，不复制凭据或 bucket 私有配置。

实际 PID：launcher 64647、authority 65343、platform 65403、worker-a 65414、worker-b 65415。每 run 前只读核对 service executable 路径、完整 SHA 和 start identity；native authority/workers 使用上述保留 binary。host capacity 为 48 GiB、18 logical CPUs；config/gate `0600`，fixture/output `0700`，启动前串行 locks absent。没有自己的 build/test 与测量竞争。

### Dirty provenance 与两次环境前提暂停

最初 tracked dirty 仅为用户 `CLAUDE.md`，保留不修改。三个 run 的 tracked diff SHA 均为 `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848`；dirty=true 如实保留。

| Run | Raw status SHA |
| --- | --- |
| basats | `b4efa7568a2f2c5b463d62276d2aeb1538a80ffaa1c3fe5260600043d2f9a742` |
| bapoint / bamixed | `56dcc00afe36a68a10e4ca319bb74a8f1afba961bde9579f25d62d3ee400b658` |

首次外壳环境检查因外部编译 activity 延迟，没有 provision、SQL 或 label 消费；随后只读 quiet sample 达到条件才首次启动 basats。原始记录保留于 `round3-baseline-quiet-wait.jsonl`，不把环境检查恢复称作 SQL retry。

basats 完成后，外壳“仅 CLAUDE dirty”检查发现新增 untracked `docs/experiment-v1-stress/FINAL-DEPLOYMENT-PLAN.md`，暂停在 bapoint provision 之前。保留 `round3-baseline-sequence-stop.json`：外壳 exit 1，不是 basats runner 失败，也不是 SQL 失败。bapoint/bamixed 当时均未消费。

主代理明确批准仅将该计划文档加入白名单，固定 SHA `0c452c5ccc8b4a863b9a11f73ceff341bcf108eb23bd059cf577228f98bf996a`。后两 run provenance 记录路径、SHA、实际 dirty status 与只读计划用途。HEAD、runtime source、工具 classifier/response-ID/unknown poison/no retry 均未改变；原暂停记录没有覆盖。

## 精确已执行配置

共同参数：rows 1000、seed `20261007`、W2/S10/M30、request timeout 默认10s、response cap默认8 MiB；单进程 sampled RSS guard `536870912` bytes、generated request budget默认256 MiB、artifact budget默认64 MiB。

| Label | Case | Concurrency | Offered workflows/s |
| --- | --- | --- | --- |
| basats | saturation_admission | 1,2,4,8 | 200 |
| bapoint | point_read | 1,4 | 50 |
| bamixed | mixed_oltp | 1,4 | 50 |

以下命令模板是本轮实际 argv 的展开方式，**不是重跑授权**。按上表依次使用 label/case/C/rate，每条 provision mutation 仅尝试一次：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-pairbase-8m6qyLNX \
  --output /private/tmp/peri-stress-pairbase-8m6qyLNX/stress-$LABEL.json \
  --label "$LABEL"

/tmp/peri-stress-correlation \
  -connection /private/tmp/peri-stress-pairbase-8m6qyLNX/stress-$LABEL.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-correlation-view.json \
  -cases "$CASE" -rows 1000 -seed 20261007 -concurrency "$C" \
  -warmup 2s -smoke 10s -measure 30s -offered-rate "$RATE" \
  -max-process-rss-bytes 536870912
```

各 run provenance 保留完整 argv（包括实际 Python executable）、PID、binary SHA、source/gate 与环境前提。三个 provision 均 exit 0。项目/DB独立，但同 arm 的服务进程与 cache 历史按相同顺序增长；不能称每项均重启完整服务或空 cache。

## 实际观察窗口

| Label / raw run | Host wall-clock observed start / finish | Monotonic run elapsed |
| --- | --- | --- |
| basats / `run_5e446dcda7d2a559` | `2026-10-07T02:26:38.656989Z` / `2026-10-07T02:29:36.988633Z` | 178.330452958s |
| bapoint / `run_00480a7c7bfcb0b7` | `2026-10-07T02:35:12.20499Z` / `2026-10-07T02:36:42.839466Z` | 90.633870375s |
| bamixed / `run_8b7e29f6f8aeafb2` | `2026-10-07T02:36:50.263914Z` / `2026-10-07T02:37:38.248361Z` | 47.984126250s |

墙钟时间戳仅为 raw observed labels，不推断日历正确性，不用墙钟差计算性能。Run elapsed 包含 setup/oracle/多个阶段；下面 throughput 分母为各 measure 自身 monotonic elapsed，包括 drain。

## 完整 30s 片段：描述性观测，不是 paired comparison

| Run / C | Offered | Attempted | ACK | Typed admission | Client drop / expired | Elapsed ns | ACK/s incl drain |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sat C1 | 6000 | 778 | 778 | 0 | 5221 / 1 | `30004268833` | 25.9296 |
| sat C2 | 6000 | 1509 | 1509 | 0 | 4489 / 2 | `30021139250` | 50.2646 |
| sat C4 | 6000 | 1490 | 1490 | 0 | 4507 / 3 | `30071534292` | 49.5485 |
| sat C8 | 6000 | 1295 | 643 | 652 | 4699 / 6 | `30113501833` | 21.3525 |
| point C1 | 1500 | 763 | 763 | 0 | 737 / 0 | `30033051292` | 25.4053 |
| point C4 | 1500 | 1470 | 1470 | 0 | 30 / 0 | `30013799083` | 48.9775 |
| mixed C1 | 1500 | 154 | 154 | 0 | 1345 / 1 | `30477353125` | 5.0529 |

只有以上 measure 是完整30s；W2/S10 不作正式30s测量。Mixed C1 measure 有44个 ACK write mutations、110个 ACK read workflows，但没有其后的全量 oracle，不能把该片段称为已独立证明所有写入恢复正确的 performance PASS。

| ACKed workflow latency，ms | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| sat C1 | 37.012875 | 52.257334 | 61.868208 | 112.765625 |
| sat C2 | 38.056291 | 58.061834 | 71.071625 | 140.032250 |
| sat C4 | 73.956625 | 127.505041 | 185.082125 | 210.930375 |
| sat C8 | 156.824417 | 316.003709 | 591.328209 | 2415.415959 |
| point C1 | 37.975959 | 52.679041 | 61.988166 | 100.042625 |
| point C4 | 40.777708 | 98.551875 | 120.005833 | 206.158333 |
| mixed C1 | 47.105667 | 685.059042 | 721.158500 | 799.119750 |

这些是 HTTP-workflow latency，不是 native SQL-only，也不包含完整 client queue/offered-to-response 窗口。Sat C8 成功643样本与全部1295样本不同：全部 p50/p95/p99/max 为 `155.953291/313.863875/620.954708/2415.415959` ms，包含652个 admission errors。其余已完成 measure 没有 failure，成功/全部分布一致。不用更快的拒绝混入“成功 query latency”。

## 正确性、分母与覆盖

| Run | 全阶段 offered / attempted / ACK | Client drop / expired | Oracle | 状态 |
| --- | --- | --- | --- | --- |
| basats | 33600 / 7107 / 6190 | 26454 / 39 | 1 preflight +5后置 PASS | COMPLETE |
| bapoint | 4200 / 3124 / 3124 | 1073 / 3 | 1 preflight +3后置 PASS | COMPLETE |
| bamixed | 2107 / 287 / 284 | 1816 / 4 | 1 preflight +1 C1 smoke后 PASS | UNRESOLVED |

Sat attempted=6190ACK+917 typed admissions，均发生在 C8：W/S/M 分别49/216/652。这是实际 Gateway admission，而不是 client drop；拒绝不计成功。Sat/point unknown、cleanup unknown、timeouts、confirmed correctness failures均0。

Mixed C1 warmup/smoke/measure ACK分别30/97/154，分别6/23/44条 write mutations；C4 warmup的3个ACK为reads。合计73条已ACK业务mutation，另有4条 preflight。唯一后置 oracle发生在 C1 smoke后：ledger33、余额100000006、account/ledger hashes一致。由于后续 C4 abort，没有 C1 measure后的最终账本校验，不补造ledger77的实际查询或PASS。

Sat/point oracle均验证1000accounts、ledger4、余额100000004、expected/actual account和ledger hashes一致。成功读 operation提供rows hash；写execute ACK不能替代后置 durable ledger oracle。

| 未完成项目 | 状态 |
| --- | --- |
| Mixed C4 warmup | 部分执行，UNRESOLVED |
| Mixed C4 smoke/measure及最终oracle | NOT EXECUTED |
| Ownership-view新native arm全部项目 | NOT EXECUTED / 未获本次授权 |
| 新arm architecture6、SQL functional组合 | NOT EXECUTED / 未获本次授权 |
| Faultprobe / cold-cache reset / kill | NOT EXECUTED |

## Mixed 首失败与取消链

首个失败是C4 warmup的 `POST /v1/sql/execute` request `stress_8b7e29f6f8aeafb2_000000002439`，business ID `stress_8b7e29f6f8aeafb2_000000002438`：HTTP400、173 response bytes、RPC elapsed50543125ns。Decoded `sql_error` 为已知terminal：`execution_completed=true`、`may_have_committed=true`、unknown=false，**不是429 correlation gap**。

阶段在该非-admission SQL错误上停止。已在途的 query `...002441` 和 execute `...002435` 随取消结束为 `request_failed` unknown；不是重试。两个poisoned session没有再发SQL/ROLLBACK/close，cleanup unknown=2，最终reason为`session_cleanup_not_proven`。

C4 warmup原始计数为7offered、6attempted、3ACK、1sql_error、2unknown、1queue-expired；elapsed131328541ns。Confirmed correctness failures和timeouts为0；这不证明abort后所有写入都完整，也不保证已知SQL错误前没有committed prefix。

原始body/native错误message未记录，不能断言BUSY、锁竞争、lease phase、owner变化或某个native exception就是根因。需要主代理只读server诊断及另行授权回归；本报告不通过重发SQL或显式rollback探测未知session。

## 资源与背景干扰限制

每run前仅只读 `ps` CPU/RSS/comm、sysctl capacity；comm保留basename用于背景样本，不记录credentials或完整argv。运行中每10s采样背景。Sat/point/mixed分别18/11/6个背景样本，均未观察到compiler activity，compiler aggregate CPU为0；**离散样本不证明期间没有短时编译或其它干扰**，其它用户UI/服务CPU记录于原始top-CPU样本。

主代理将最初50% compiler阈值修订为<=400%可在RSS稳定且如实记录时执行。后两run仅改外壳guard：最多一个60s wait block、不kill用户进程；记录两样本间compiler RSS增长<=512MiB、总compiler RSS<=4GiB作为明确操作性preflight标准，不宣称RAM硬cap或强稳定性证明。没有自己的build/test。

Native/Gateway RSS来自真实PID样本，不把cache payload accounting当RSS；本轮stop reason非resource abort。Sat采样RSS peak bytes：authority39075840、platform22904832、worker-a14172160、worker-b33849344；mixed为38174720、22315008、32047104、32145408，均低于512MiB guard。Point资源样本保留于raw，本文不捏造未列出的峰值。

Sat/point/mixed生成请求记账分别1443658/696904/178694bytes；两raw文件总量分别17346083/5339133/1582138bytes，均低于预算。请求字节不是S3实际存储量。实际management/native maps保留，不以进程global累计量直接解释当前case成本；缺失metrics仍null/unavailable。

两arm背景、写入数量、cache/history若不同，必须限制对照置信度并按需要另行授权quiet对照。当前没有新arm结果，不能声称零干扰、最大容量、native view收益或性能退化因果。

## 证据保留与报告状态

Output base：`/private/tmp/peri-stress-results-h3wtrdm2`。每label保留`round3-baseline-<label>.provenance.json`、`.background.jsonl`、`.provision.stdout.log`、`.provision.exit-code`、`.stdout.log`、`.exit-code`；完成的sat/point另有`.completion.json`。Mixed exit-code为1，无伪造completion。

| Run | run.json SHA256 | operations.jsonl SHA256 |
| --- | --- | --- |
| `run_5e446dcda7d2a559` | `7e0489e9973c42a426b81a480e03aecf35db5575908515e66f588acc1e659154` | `50f916af477a681251fb0443ee02412f564b8b2b7aa285d6993337a38615cc62` |
| `run_00480a7c7bfcb0b7` | `47c71436df8d0620e0627e800f206754ef4f10cc0dba1e75918eb28786b50210` | `781b83ad233686beb6adf2289d967fdd6d6d70704481b904678d9437ac756e53` |
| `run_8b7e29f6f8aeafb2` | `f765334cc656508cd3b0cfcd0a6227c6b4bedfd86433b69567de79d6f61e1ba9` | `1290f996079a0e06eb9d76e24b47d328afe05720952d9a99eba42e74ad63a770` |

作者报告为baseline partial事实记录；F的Round3独立分析尚待主代理统一明确通知，不把作者计数核对写成F独立PASS。旧Round1/2失败和报告均保持原样。

Baseline actual runner已停止。仅写本报告、读取本地产物；不启动新arm、额外functional、SQL重试、API探测、服务重启、信号或清理。后续必须由主代理重新明确授权。
