# Round 4 预声明：保留 native control 与 BUSY + View candidate

## 状态：PLAN ONLY，未授权执行

本文件是预声明计划，不是实验结果或启动授权。仅写计划；不执行 load、provision、build、fixture API、SQL、信号或 credential 生成。Round 1–3 的首失败、报告、raw 及原分类保持不变。

实际开始必须同时满足：C 侧 probe 开发及正常 tests 已结束；F 的全部独立 gates 已结束并汇总；本任务所有 builders/tests 停止；主代理完成 startup、secret 注入、source/artifact freeze、commit 和 new main-confirmed matching gate；主代理另外明确下发 arm、fixture、gate、binary 路径及 runner 授权。该 gate 是 main matching-HEAD operator attestation（普通 private JSON），不是密码学签名证明。E 不代替主代理确认 gate、不自行启动服务或生成凭据。

已存在的旧 `c186…` correlation harness 不包含新的 phase drain 合同，**不能用于本轮**。新 drain harness 的完整 SHA、路径、构建 flags 与 source HEAD 待主代理冻结；两 arm 必须使用同一新 harness。

## 研究问题与不可归因范围

1. 在相同 Gateway、drain harness、数据规模、负载配置、顺序和 durability/fencing 条件下，control 与 candidate 的 saturation/point-read 观测是否不同？
2. Candidate 能否完成 bounded mixed、架构 lifecycle 和普通 SQL 功能覆盖，并通过持久性 oracle？
3. 新 drain 合同能否在失败时保留已 admitted 请求的真实终态，而不制造额外 unknown、不掩盖首失败？

Candidate 是 **BUSY + ownership View 及该冻结 native artifact 内全部变化的整体版本**，不是 isolated View patch。即使 paired 结果不同，也不能把差异单独归因于 View、BUSY 或某一个锁/缓存实现。

Control 不安排 mixed：这是预声明 OUT OF SCOPE / NOT SCHEDULED，不是 unsupported SKIP，也不是将 Round 3 失败 mixed 自动重试。Candidate mixed 是新 arm、新 fixture、新 label、新业务 ID 的新实验，不重放历史失败请求。仅 saturation/point-read 构成 paired 对照。

## 两 arm 的共同条件

| 项目 | 共同固定要求 |
| --- | --- |
| Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA256 | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| Harness | 同一新 drain harness；完整 SHA 与 source manifest 待新 gate 固定 |
| TLS | proper CA，HTTPS 验证保持启用，无 bypass |
| Service topology | 每 arm 全新 private whole-project fixture，两个 worker；Gateway/controller、authority、workers 完整路径 |
| Storage | 每 arm 独立 fresh unique S3 bucket；保留 bucket 初始化证据，不改旧桶/container |
| Rows / synthetic seed | 1000 / `20261007` |
| Trusted execution / quota 配置 | main 冻结并记录；共同项相同，不能临时调高 quota 隐藏 admission |
| RSS / generated / artifact guard | 单进程 sampled RSS 512 MiB / generated requests 256 MiB / artifacts 64 MiB |
| Request timeout | 10s；不延长已 admitted 请求的原有限 deadline |
| Serialization | 全 host 单一 actual runner，不并行 competing load |

Control native 固定 `/tmp/peri-stress-native-baseline-bin`，SHA256 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`；有意为 preserved old native，不声称由新 HEAD 构建。Candidate native 全体服务进程的完整 artifact SHA、可执行路径与差异清单由主代理/F 固定，不从源码推算二进制身份。

每 run 使用未消费的新 label、project/DB、crypto run prefix 和 business operation IDs；一次 safe provision、一次 runner。Fresh label 不代表每次重启整个服务：同一 arm 的服务/cache/history按下表顺序保留，两 arm 共同的 sat→point 顺序相同。不能通过重用旧 label、重发 old request ID 或复制旧 DB/cache 达到所谓 fresh。

Source HEAD、git dirty 状态、用户 `CLAUDE.md` 排除项与任何另获批准的文档白名单均如实进入 provenance。不可自行放宽 runtime source freeze；未经明确授权/不符合冻结 manifest 的新代码/文件或 binary 差异阻止启动。

## 预声明顺序矩阵

先完成 control arm，主代理正常结束其 fixture 并签发 candidate startup/执行授权后，再进入 candidate；E 不自行 signal launcher。

| Arm / 顺序 | 建议新 label | Cases | C | Warmup / smoke / measure | Rate | 判定用途 |
| --- | --- | --- | --- | --- | --- | --- |
| Control 1 | `r4csat` | saturation_admission | 1,2,4,8 | 2s / 10s / 30s | 200 | 四个完整30s片段 |
| Control 2 | `r4cpoint` | point_read | 1,4 | 2s / 10s / 30s | 50 | 两个完整30s片段 |
| Candidate 1 | `r4nsat` | saturation_admission | 1,2,4,8 | 2s / 10s / 30s | 200 | 对照 control sat |
| Candidate 2 | `r4npoint` | point_read | 1,4 | 2s / 10s / 30s | 50 | 对照 control point |
| Candidate 3 | `r4nmixed` | mixed_oltp | 1,4 | 2s / 10s / 30s | 50 | Candidate-only correctness + observations |
| Candidate 4 | `r4narch` | 下列 architecture6 | 1,2 | 2s / 10s / 10s | 50 | functional gate-only |
| Candidate 5 | `r4nsql` | sql | 1,2 | 1s / 2s / 5s | 25 | common functional gate-only |

Label 为待主代理批准的建议，不是额外 provision 权限。任意早期 STOP 后，整个未开始序列保持 NOT EXECUTED，不能换 label 自动绕过失败。

Architecture6 的精确选择：

```text
session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate
```

Architecture 后置于 candidate 的共同 sat/point 及 mixed 测量之后，避免预先 lifecycle/多 DB 活动改变 paired sat/point 的服务历史。不能把本轮计划称为“candidate 所有架构 gate 均先于测量”；启动前依赖主代理的新联合 gate。

Noisy C1 必须明确 SKIP（需至少两个 session）；migration target already owner 时明确 SKIP，不假造 FAIL/PASS。若因 private management key、DB或目标配置缺失导致必需功能 SKIP，完整覆盖目标未达成，应停止追加测量并报告原因，而不是调整 secret 或盲目重发 management mutation。

## Common SQL 13 项与共享 oracle

`-cases sql` 按冻结 harness 的声明顺序执行：

1. point_read
2. index_range
3. pagination
4. mixed_oltp
5. hot_keys
6. bulk_tx
7. sort_aggregate_join
8. index_plan
9. rollback
10. constraint_error_reuse
11. large_values
12. sustained_wal
13. checkpoint_reopen（需实际 Management key）

另一个被 `sql` 选中的 `snapshot_branch_pitr_under_load` 明确 **SKIP：fault schedules / retention oracle 未实现**，不计入13项 PASS，不能把 SKIP称为snapshot/branch/PITR coverage。

组合 runner 一次 seed、同 fixture/共享累积 ACK ledger。写 case 只变更 balance，读 oracle 使用固定 id/grp/score/payload；constraint场景使用独立表验证 typed terminal错误→同session显式ROLLBACK→余额与正常write/query，再把正常ACK纳入主ledger。Checkpoint 后复用同一累积oracle，不证明空cache重启。

若所有13项完成，应有1 preflight、24 timed smoke后、13 case后，共38次fixture oracle PASS，另有每operation/constraint-step oracle。12个timed cases×2C×8s=192s；constraint是一遍有界证明，不冒充C1/C2 throughput。最多4800 offered workflows，保守累计ledger上界约4805条（含bulk×8、mixed全部写的保守假设、4 preflight及constraint正常write）。

所有短阶段明确 **functional gate-only**，不把5s measure字段或全部ACK用于稳态性能结论。不为不支持项增加新RPC、不编造缺失 metrics。

## 新 drain 合同：main 明确批准的语义变化

此合同适用于两个 arm，不仅限 known SQL error；F的独立文档必须明确记录，不能继续声称“unknown必hardcancel peers”。

- 首个意外 known non-admission SQL错误或oracle mismatch：停止新offers、dequeue与逻辑admission；该phase保留FAIL，即使其他在途请求随后ACK。
- 真实unknown自身仍unknown、poison；整个phase保持FAIL/UNRESOLVED并停止新工作。该session不retry、不ROLLBACK、不reopen、不盲close，不把lost COMMIT认作未提交。
- **其他已经逻辑admitted的workflows** 可在原RPC有限deadline与parent safety caps内自然drain，按实际ACK/typederror/unknown记账；不是恢复调度，也不是新批次。已有rollback workflow的必要后续oracle只属于原已admitted workflow，不新增业务offer。
- Parent cancel、resource或artifact安全问题仍hard cancel。真实timeout、EOF、取消、reply丢失仍保留其不确定性；不能把cancel造成的unknown降级为安全negative。
- 不延长deadline、不自动retry、rollback或reopen；ACK ledger只收实际ACK，错误/未知mutation不计入ACK。所有终态与第一错误顺序保留。

该变更改善证据质量，**不修复或隐藏SQL失败**。Round3 首个typed SQL错误和随后两个unknown仍按旧真实运行证据保留，不事后重分类或用新unit行为倒填旧raw。

Drain可能让失败后的已admitted请求成功完成，因此 attempted/completed/ACK/unknown和phase elapsed/drain时长可与旧harness不同。仅本轮相同new-drain harness的两arm可直接比较；不得与旧c186结果做无条件等价吞吐比较。

## Oracle、停止规则与可报告性能范围

任何unexpected SQL error、unknown、整体runner非COMPLETE、未预声明的case/phase非COMPLETE、已确认correctness mismatch、artifact/RSS/resource风险或runtimefreeze变化：停止当前与全部后续序列，保留firstfailure。上述noisy C1、同owner migration与snapshot-under-load的预声明非测量SKIP只作为显式覆盖例外，绝不计PASS；其它SKIP不能自动放行。先完成必要有界drain或parent硬取消，不追加诊断SQL、cleanup mutation、signal或cache删除。只读server诊断由主代理另行安排。

`constraint_error_reuse` 中预期且flags正确的terminal SQL错误是该场景已声明步骤，必须显式rollback与oracle均成功才可PASS；其它SQL错误不得借用此例外。Typed admission rejection可作为saturation测例正常错误终态，仍记录为error而非成功；client drop不等于server quota。

只有run/case COMPLETE、smoke与case后持久oracle均PASS，且对应measure完整30s，才能报告performance observation；若某片段完整但缺少后置oracle，只报告ACK/latency原始事实，不给持久正确performance PASS。账本、余额、rows hash及已ACK mutation完整性优先于更高QPS。

成功与全部latency必须分开，分别列count、p50/p95/p99/max；另列offered、attempted、completed、ACK、typedadmission、unknown、timeouts、clientdrop、queueexpiry和drain elapsed。HTTP-workflow、queue/end-to-end、native/control metrics范围不能混写。

Mixed实际read/write/ledger增长是时变因素。保持同seed不保证achieved writecount等价；需列各阶段write ACK/mutation数量，不能用不等价writes或不同native变更集合推导isolated性能收益。Control没有mixed结果，不做mixed paired ratio。

## 背景、资源与artifact预算

每run前只读sysctl capacity和脱敏`ps` PID/CPU/RSS/comm，运行中保留背景采样；RSS是OS process RSS，不是segment/cache payload。采样CPU为OS lifetime/window口径，不是精确phase CPU；离散样本不证明zero interference或瞬时RAM硬cap。

自己的builders/tests必须全部结束。外部用户compiler不kill：开始前quiet等待最多60s；若总compilerCPU<=400%且采样RSS稳定，可记录背景后执行，不宣称无干扰。若>400%或内存风险持续，等待一轮并汇报，不无限等待或擅自启动。明确400%=约四logicalCPU的ps口径，不当作全host400%。RSS稳定的操作性判据和sample间隔在外壳provenance预先固定，不把它称为物理内存安全证明。

测量中出现外部compile时保留样本与局限；两arm背景不匹配会降低置信度。是否追加quiet短对照须主代理另授权，不自行延展矩阵。Sampled fixture进程RSS>512MiB、artifact64MiB或generated256MiB风险仍安全STOP，不提高限额来完成结果。

SQL组合artifact约10–20MiB为基于既有raw单条大小的估算，非保证；真实metrics/metadata增长可能改变预算，64MiB guard保留。原始actual management/native maps保留scope，进程/global累计值不当作case独占成本；缺失指标为null/unavailable，绝不填fabricated0。

## 可复用的执行模板（只声明，不运行）

由main供给`FIXTURE`、`OUTPUT`、`GATE`、`DRAIN_HARNESS`和本次精确label/case/C/timing/rate。凭据只在main的private0600config；不能通过CLI、echo、argv、stdout或本文传递key。

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir "$FIXTURE" \
  --output "$FIXTURE/stress-$LABEL.json" \
  --label "$LABEL"

"$DRAIN_HARNESS" \
  -connection "$FIXTURE/stress-$LABEL.json" \
  -output "$OUTPUT" -joint-pass "$GATE" \
  -cases "$CASES" -rows 1000 -seed 20261007 \
  -concurrency "$C" -warmup "$WARMUP" -smoke "$SMOKE" -measure "$MEASURE" \
  -offered-rate "$RATE" -request-timeout 10s \
  -max-process-rss-bytes 536870912 \
  -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

复用Round3外壳的preflight/provenance、逐run独立stdout/exit、背景sampler与completion验证结构，不复用旧label/config/raw或旧nativecandidate假设。模板不是无条件shell batch：只有provision确定成功、run exit0且raw/phase/oracle满足预声明条件才可进入下一项；任一失败不重试mutation或runner。两arm与附加gate的实际授权可分次，不由模板推导权限。

## 时间预算（非结果）

| 阶段 | 计划timed windows |
| --- | --- |
| Control sat + point | 168s +84s =252s |
| Candidate sat + point + mixed | 168s +84s +84s =336s |
| Candidate architecture6 | timed合计154s，另有lifecycle/setup/oracle |
| Candidate common SQL | timed合计192s，另有constraint/checkpoint/setup/oracle |
| 合计下限 | 934s，约15.6分钟，不含startup/provision/drain/oracle |

共7个fresh-label runner，而非逐SQLcase16次seed。按此前初始化、校验与lifecycle耗时，执行预留约25–40分钟，加main负责的两armstartup/停机协调；异常、bounded deadline drain或背景等待会延长或提前终止。无完成保证，停止后不得为凑矩阵自动追加测量。

## 产物、验证与交接

每run保留精确argv、sourceHEAD/dirty/manifest、所有servicePID/start identity/executable完整SHA、armvariant、bucket初始化证据、configredacted身份、hostcapacity/background、stdout、exit、rawJSON/JSONL、firsterror及drain终态。不要覆盖旧round目录；hash冻结包括本轮新raw与作者报告。

作者后续Round4报告区分control/candidate、measurement/gate-only、COMPLETE/SKIP/NOTEXECUTED和缺oracle片段。F独立验证计数、唯一requestID/no replay、latency分母、ACK-onlyledger、真实unknown保留、drain合同和持久oracle；作者PASS不替代独立PASS。

本计划就绪不代表candidate部署PASS、两arm性能比较PASS或实际执行已发生。主代理负责startup/secret/commit/gate；E保持停载，等待新的具体授权。
