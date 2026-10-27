# Issue #15 性能优化候选交付与验证

本轮已实现并验证两项代码优化，但**没有证明写入尾延迟已稳定改善，不能关闭 #15 或宣称性能验收 PASS**。读控制两对 p50 下降 5.74%–10.37%；H4 p50 下降幅度为 0.16%–12.92%；H6 第一对略好、第二对 p99 回退 82.46%。原定 4 倍跃升也未达成。

## 源码与行为

- 基线：`6b5f24ff0371fb3ea60176ce1f5f307531b63f81`，是当前 main/v1.1.16 加 launcher 修复。代码实现提交：`c3906365eb2acc1e9222d2547636f8e17baa48f3`；最终文档提交见 `MANIFEST.json`。最终文档变动没有修改二进制源码。
- Turso 固定 `24a3a4609683b73935888f8bc30b47727d100aa8`，未改引擎或 gitlink。
- **Cursor 完成状态随首屏返回**：Rust 在同一个 connection/lane 锁内、原执行与结算成功后读取原生 autocommit，未完成页不给出快照。Go 只用于同一 stream/batch 的紧邻条件，跨请求、插入任何步骤、失败或多页结果均失效。H4 内部 RPC **8→6（减少25%）**；H6 **10→8（减少20%）**。旧 Worker 缺字段时仍探测状态；非法字段保持 Unknown，不重放 SQL。
- **v1 快照锁范围缩小**：原 snapshot→witness Sync→truncate 顺序、256 mutation/8MiB 阈值、同步返回及磁盘格式不变。文件 I/O 不持共享 registry 锁，认证与已确认状态读取可继续；写入、registry Update、checkpoint 和 Close 仍正确等待。失败传播与恢复有故障注入测试。该改动有一次有界 map 复制的分配成本；单连接消融未证明一致收益，真实收益范围是已验证的读锁阻塞隔离。
- 未提高 quota、修改业务表结构、启用 pack/completion 默认格式、重放 SQL、跳过持久化屏障或清除 dirty marker。
- 升级要求：Go 先于 Rust，或配对更新。新 Go 能读旧回复；旧 Go 的严格校验不接受新 Rust 新字段。此处仅优化 JSON v3 cursor；不宣称所有 Hrana 协议都有同样收益。

## 测量与全部不利结果

下面保留独立审核原文。主测关闭 observer；消融开启 observer，二者分账。名义50ms调度不是严格的每条raw时间戳间隔，也不是开放式容量压测。

### 主 ABBA 独立审核

结论：本轮**不支持“已稳定解决写尾部”**。H4 的两对改善幅度不一致；H6 第一对略好，第二对 p99 回退 82.46%。H1 读控制两对都改善，但样本内仍有时间漂移。所有慢样本保留，不合并两轮的分位数。

基线为 current main `6b5f24ff0371fb3ea60176ce1f5f307531b63f81`；候选为 `c3906365eb2acc1e9222d2547636f8e17baa48f3`。四个二进制先复制冻结并核对 SHA256，使用新构建 Rust，不与 v1.1.15 历史数据混比。

同一 Linux x86_64 环境、真实 SDK `@tursodatabase/serverless@1.4.1`、TLS 校验开启。pack/completion/writer-admission/observer 全 OFF；每场独立新卷、512 次写读老化、50 次 warmup、C1 串行目标 50ms 起始节奏。实际 quota 全部为 500 req/s、并发 4、会话 16，未调整。H4/H6 每臂 1000 timed；H1 另一个 ABBA，每臂 300 timed。

| 场景 | 臂 | p50 ms | p95 ms | p99 ms | max ms | >100ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| H4 | A1 | 34.27 | 62.01 | 90.09 | 248.91 | 7/1000 (0.7%) |
| H4 | B1 | 29.84 | 50.76 | 78.19 | 196.06 | 5/1000 (0.5%) |
| H4 | B2 | 30.55 | 50.08 | 78.12 | 148.67 | 3/1000 (0.3%) |
| H4 | A2 | 30.59 | 52.46 | 81.07 | 146.99 | 3/1000 (0.3%) |
| H6 | A1 | 38.25 | 67.67 | 102.32 | 305.63 | 12/1000 (1.2%) |
| H6 | B1 | 36.89 | 61.51 | 90.04 | 155.09 | 6/1000 (0.6%) |
| H6 | B2 | 41.31 | 84.53 | 158.86 | 257.30 | 33/1000 (3.3%) |
| H6 | A2 | 38.03 | 58.17 | 87.06 | 250.27 | 5/1000 (0.5%) |
| H1 | A1 | 8.96 | 21.90 | 27.65 | 33.58 | 0/300 (0.0%) |
| H1 | B1 | 8.03 | 13.22 | 18.23 | 28.20 | 0/300 (0.0%) |
| H1 | B2 | 9.33 | 17.45 | 24.51 | 33.32 | 0/300 (0.0%) |
| H1 | A2 | 9.90 | 27.16 | 42.69 | 47.41 | 0/300 (0.0%) |

变化为候选/基线 − 1，负数表示更快。每对独立计算：

| 场景 | 配对 | p50 变化 | p99 变化 |
| --- | --- | ---: | ---: |
| H4 | A1 → B1 | -12.92% | -13.21% |
| H4 | A2 → B2 | -0.16% | -3.63% |
| H6 | A1 → B1 | -3.56% | -12.00% |
| H6 | A2 → B2 | +8.61% | +82.46% |
| H1 | A1 → B1 | -10.37% | -34.09% |
| H1 | A2 → B2 | -5.74% | -42.59% |

H4 的基线 p50 从 A1 34.27ms 变为 A2 30.59ms，所以不能只报第一对 -12.92%。H6/B2 的 33 次 >100ms 中，17 次在 timed 250–499、11 次在 750–999，尾部有聚集；与 A2 的 5 次相比存在明确不利结果，原因还未从这批无 trace 数据证实。H1/A2 最后 50 次 p50 20.05ms，而前 250 次为 9.40ms，读控制也显示时间变化。

独立核对结果：12/12 服务 clean joined exit 0；9200/9200 timed 操作成功；包含 prelude/warmup 共 15944 个操作、34780 个实际 HTTP 请求全部成功，其中 timed HTTP 21200。精确核对 14544 个 benchmark 写 ACK 身份/键值摘要（含老化和 warmup，其中 timed 8000）、4232 个真实 close ACK（其中 timed 4000）。setup DDL/DML 的 HTTP 也计入总请求，未冒充 benchmark write_effect。零 Unknown、零 429、零操作重叠。配额前后一致，raw 文件哈希、SDK/client 哈希、冻结二进制哈希及 stop proof 哈希均匹配。

计时审计限制：scheduler 的 50ms 边界在 UUID/操作对象创建前，而 raw `issued_ms` 在其后；未单独记录 scheduler 边界，不能宣称每个 raw 间隔严格 ≥50ms。15932 个相邻间隔中 5 个 <49ms（0.031%），无 <45ms；最小 45.216ms 在 A1/H6 的 prelude，写场景 timed 最小 48.540ms，H1 timed 最小 47.904ms。原始数据与首次严格间隔审计失败均保留，最终审核状态明确为 `RAW_EVIDENCE_AUDIT_PASS_WITH_PACING_TIMESTAMP_LIMITATION`。没有为通过审核重跑产品样本。

首个 campaign 在启动任何服务/SQL 前因构建用 Turso 符号链接令 git status 失败；父 agent 修复 checkout 后，从新目录完整 A1 开始。失败目录 `issue15-abba-default-off` 留存，0 SQL，不是弃掉差样本。

这不是容量测试，也不是 M4/OrbStack 原旧卷复现；512 次人工老化不等同用户历史数据。AF_UNIX 不可用，只省略 launcher 可选维护 socket，不能覆盖维护 IPC。此性能 campaign 不提供 restart/power-loss recovery 证明。只有两个 A/B 配对，不给显著性或正式性能 gate PASS。

完整证据分别在 `issue15-abba-default-off-verified/` 和 `issue15-read-control-verified/`；每个目录包含原始 operations/requests、SHA256SUMS、各场 summary、profile/quota/stop 证据、冻结 helpers/二进制与 `independent-audit.json`。初次审计版本也留在 helpers。正式计时与早先 `issue15-baseline-trace300` 分账。


### Issue 15 journal 消融诊断

结论：本次隔离**未稳定复现 journal 改动带来的大回退，也未证明一致收益**。不能覆盖主 ABBA 中 H6/B2 的不利结果，也不能据此宣称 journal 改动无风险或写尾部已经解决。

A 为 cursor-only `7a0234a31d3afeec7e731347ad19c467b057663e`，B 为完整候选 `c3906365eb2acc1e9222d2547636f8e17baa48f3`；双方使用同一个 Rust candidate 二进制，SHA256 `fc40f85b965075cdd216900616768efd50a841d0c04280d99f3b30ffdf482e2a`，Rust 源码 diff 为空。Go 差异限定在 journal/state 优化。真实 SDK1.4.1，每场 prelude512 + warmup50 + timed600、名义50ms调度、C1；pack/completion/writer-admission OFF，双方 Gateway全采样+Rust slow/group trace ON。只比较这四臂，与主无trace ABBA分账。

| 臂 | 变体 | p50 ms | p95 ms | p99 ms | max ms | >100ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| A1 | cursor-only | 36.57 | 55.72 | 123.26 | 405.21 | 9/600 (1.5%) |
| B1 | full | 37.27 | 62.06 | 97.91 | 174.36 | 6/600 (1.0%) |
| B2 | full | 38.03 | 61.96 | 89.62 | 124.20 | 3/600 (0.5%) |
| A2 | cursor-only | 37.13 | 56.66 | 86.36 | 240.61 | 3/600 (0.5%) |

| 配对 full/cursor-only − 1 | p50变化 | p99变化 |
| --- | ---: | ---: |
| A1 → B1 | +1.92% | -20.56% |
| A2 → B2 | +2.42% | +3.78% |

直接按同一操作聚合的互斥 Gateway span 均值（ms；不是分位数相减，native wait 包含 Authority 等嵌套工作）：

| 臂 | native | worker | intent | terminal | audit |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1 | 20.247 | 2.500 | 2.751 | 3.398 | 4.302 |
| B1 | 20.928 | 2.517 | 2.861 | 2.747 | 4.617 |
| B2 | 21.224 | 2.552 | 2.966 | 2.864 | 4.562 |
| A2 | 20.264 | 2.528 | 2.966 | 2.795 | 4.441 |

逐请求观察：A1 最慢405.21ms（sample160）包含 terminal366.62ms；A2 最慢240.61ms（sample27）包含 intent102.23/native74.44/terminal20.54/audit29.72ms。去掉 journal 改动后仍出现这些长尾。Full B2最慢124.20ms（sample544）分散在 native44.43/worker14.55/terminal18.72/audit27.43ms；Full B1最慢174.36ms（sample404）的 write HTTP164.11ms，内部Gateway请求合计76.30ms，约87.81ms在这一trace边界之外，不能未经额外证据归因到某个存储阶段。

核对：2400/2400 timed 成功，含 prelude/warmup4648个操作，11924个实际HTTP全部成功（timed7200）。核对4648写ACK（timed2400）、2608close ACK（timed2400）；0Unknown/429/操作重叠，4/4 clean joined stop，实际quota500req/s、并发4、会话16全程不变，二进制与原始哈希匹配。四个Gateway trace各8291条，footer complete=true、dropped0、没有span截断，全部HTTP可关联。Rust slow sink是阈值采样且complete=false，仅能支持已捕获事件，不能称为完整原生阶段账本。

保留与主测相同的计时限制：50ms scheduler边界未独立记录，issued_ms在UUID/对象创建后；这组最小issued间隔49.360ms，无<49ms，零重叠，不声称每个raw间隔严格≥50ms。没有修改测量helper，没有重试SQL、删除样本或追赶到达。

仍为Linux本地诊断，使用TCP-only launcher且省略可选AF_UNIX维护socket；非M4/OrbStack旧卷、非容量测试、非正式性能或恢复gate。原始文件、各臂attribution-summary、independent-audit与冻结helpers都在本目录。


## 对慢点的判断边界

四轮 H6 均有约65样本周期慢点，符合既有 receipt roll 的周期线索，但缺少逐事件关联，不能直接当作根因。主测 B2 的33个 >100ms 样本中，21个集中在两个连续窗口，write/read/close 都抬高；四轮最终 operation 数量、state/snapshot/journal 大小一致。

按照实际 journal revision 和每操作6条 mutation 推导的23个 compaction 样本，没有一个命中B2的33个慢样本；这是源码与计数推导，不是运行时事件证明。Cursor-only消融仍有405ms样本，其中terminal阶段约366.6ms。它说明去掉新增锁改动并未消除长尾，但不能据此排除间接GC、磁盘或其它共享资源因素。

独立审查还核对了默认legacy读取、WAL成功后回读、session常驻实例复用、CAS失败冷验证、receipt page缓存以及`available_parallelism()`开销。没有足够证据支持新的存储路径改写，本轮未改这些模块。完整推导和原始微测见两份 storage review 与 derived analysis。

## 功能、恢复与源码检查

| 验证 | 本轮结果 | 限制 |
| --- | --- | --- |
| Go全量 | 565顶层 + 740子测试通过，4个测试包通过，0失败 | 使用新Go源码+新构建的基线Rust，验证旧可选字段兼容；原5项skip保留 |
| Go环境排除 | `TestOwnedStopProofDefaultCLINativeResponseLoss` 未执行 | 环境禁止AF_UNIX，不能称该项通过 |
| Journal定向race | 56顶层，含子测试110通过；新增4项再重复3轮 | 故障/并发/恢复，不是性能认证 |
| Cursor定向race | 通过；含true/false、旧回复、多页、跨batch、非法回复Unknown | 详细行为也由上述全量Go覆盖 |
| Rust statement_worker | 24通过，0失败 | 本轮未重新运行所有Rust集成目标 |
| 原P0脚本 | 42个Rust测试 + 15个固定Turso SQL用例通过 | 子进程结果不重复计数；64MiB预算不变、快照更新关闭 |
| 官方SDK原矩阵 | 52/52执行通过，2个原E1 known-gap skip | 维护socket因AF_UNIX限制省略，不是原样完整发布launcher认证 |
| 原生SDK1.4.1错误/分页 | constraint与prepare错误、无SQL重放、601行有序结果、close ACK通过 | pack/completion/writer-admission均ON，verified TLS |
| 正常重启 | H4/H6分别175条ACK，合计350条身份/值/载荷摘要复核零差异 | graceful joined stop/restart，保留Worker cache；不是断电/冷缓存验证 |
| TS SDK | build与30个测试通过 | 功能测试 |
| Rust格式/Clippy | fmt与all-targets Clippy通过 | 冻结Turso已有依赖warning不修改 |

兼容矩阵、错误测试与恢复测试使用最终候选成对二进制。恢复压测与构建可并行，其耗时**不**作为正式性能结果；正式ABBA与消融窗口无编译、测试或额外负载。

六个subagent分别承担request/cursor、journal实现、journal独立时序审查、storage热点、WAL/快照独立审查、benchmark与raw审核。没有仅凭子代理建议就跳过验证：实际补齐了checkpoint/Close门控、旧字段兼容、多页/事务和失败回归，并保留了性能反例。

## 没有证明的范围与下一轮观测

- #15 的目标环境为Apple M4/OrbStack与用户原历史卷；本轮是Linux x86_64、独立卷人工老化，不能替代该环境。
- 两对ABBA不能提供稳定性或置信区间认证。H6长尾未解决，不能宣称p99<50ms、>100ms≤0.1%、4倍速度或容量门禁通过。
- 仍缺逐样本关联的Authority内部`root_publish_retries`、冷/增量WAL验证次数、Go GC/heap与系统IO证据。下一轮应在目标卷固定配置采这些阶段；不能把未覆盖时段自动算成磁盘或宿主噪声。
- E1写cursor abort已知缺口、维护IPC、真实断电/冷缓存、S3与HA认证均不在本轮完成范围。

包内有完整主测/消融raw及摘要、失败的0SQL预检记录、首轮严格pacing审计失败与最终带限制的审核记录。真实运行卷、API key、TLS私钥和重复冻结的基线二进制未随包复制；二进制身份、stop proof哈希和校验值保留，最终候选二进制在`bin/`。初轮diagnostic使用上一轮Rust（src相同，仅版本元数据不同），正式ABBA使用本轮新构建基线，二者不混算。

`changes.patch` 是最终候选；`evidence/cursor-only-ablation.patch` 仅用来复现消融，不是另一个已认证发布版本。源码保持默认行为可审查，未推送、发布或关闭GitHub issue。
