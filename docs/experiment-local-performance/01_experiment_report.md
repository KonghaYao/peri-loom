# 实验 01：固定本地真实 SQL 工作量的 release 前后配对

日期：2026-10-17。执行者：主线程；本文由 workload/报告 agent 按已归档原始数据整理，没有自行执行构建、测试或压测。完整数据 [results.json](evidence/experiment-01/results.json) 已包含 `finished_unix`，四个正式样本全部完成。

**本轮未达到 1.5×。** 两对总体 speedup 为 **1.026755× / 0.885801×**，配对倍数中位数 **0.956278×**；`history_commits` 的配对倍数中位数 **0.894976×**，同时未通过固定阶段不回退超过 10% 的门槛。runner 状态为 `NOT_YET_1_5X`。不删除第二对较慢候选样本，不以局部复制/编码收益替代该结果。

## 假设、版本与构建身份

本轮检验：accepted WAL 状态消费、单次 canonical WAL record 编码、dirty segment Arc/COW 与单次 publish 去重，连同冻结完整候选内已有调整，能否让同一实际本地 SQL 读写、历史、checkpoint 和冷重开工作量至少快 1.5×。独立代码审查发现的 lost-ACK 采用后来视图漏 pin 问题已在候选中修复；普通成功 publication 路径不新增该补 pin 事务，见 [代码审查](candidate-code-review.md)。本轮比较整个快照，不能将结果归因于某一项单独改动。

候选相对 1198b18 共有 20 个新增/变更路径、无删除，包括已经提交的 IO、maintenance 和 integration test 调整；不是只有上述未提交的三类优化。归档 tracked patch 只描述其快照 HEAD 上的工作树差异，不能代替与 A 的完整差异；应结合 source archive/manifest、HEAD 至基线的提交差异一起核对。

| 身份 | 冻结值 / 证据 |
| --- | --- |
| A 基线 | `1198b18611601f7417f7b9ae4bbb3c370b41ff03`，已包含本轮前提交的缓冲/parser/pread 修复 |
| B 完整源码 ID | `a3edf74410c9d2a4a32b09c05ab9b04a4365b4b4e0e7b6cf05aca2a9d68f44bb` |
| B 快照 HEAD | `042476a9abc4beb0ad35be330202dd99cd47be7f` 加归档时工作树；723 个文件，不能只按 HEAD 名称复现 |
| B tar SHA-256 | `1754f94e93e4dcca31ccc74f8e0f71b43258686c0a60142eab43939d7a31518f` |
| B tracked patch SHA-256 | `67be846bbf402862213c7e1cf6ce15c1f95aa6870114b89a7da2bf2414409a40` |
| 固定 Turso | `24a3a4609683b73935888f8bc30b47727d100aa8` |
| 两臂相同 harness SHA-256 | `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef` |
| A 可执行文件 SHA-256 | `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3` |
| B 可执行文件 SHA-256 | `ce3744e8b9a5d5f7159f1cfe151d2c06bfe8b9dd86dedb4e8e0bac580a4e9d28` |

来源详见 [identity](identity.json)、[candidate source](evidence/candidate-source.json)、[每文件 manifest](evidence/candidate-source-manifest.json)、[tracked patch](evidence/candidate-tracked.patch)、[A binary](evidence/baseline-binary.json)、[B binary](evidence/candidate-binary.json)、[实际 harness](evidence/local_performance.rs)。完整源码 tar 与保存二进制的绝对位置在这些 metadata 中。

两臂均在同一临时 lab 中顺序构建相同 integration test，保存不同 executable 后再测量。Cargo 使用 `--release --locked --test local_performance --no-run`；[A 构建日志](evidence/baseline-build.jsonl)与[B 构建日志](evidence/candidate-build.jsonl)的该 target 均记录 `opt_level=3`、`debug_assertions=false`、`fresh=false`。不能从日志中其他 build dependency 的 opt_level=0 推断 benchmark 是 debug 构建。

## 固定工作量与计时口径

遵循 [00_plan](00_plan.md)，没有根据首对结果变更工作量。真实路径为 LocalObjectStore → OwnershipAuthority → fenced branch store → RemoteDatabase → Turso，保留正常校验、fencing、CAS 和持久化；无模拟 WAL、MemoryObjectStore 或跳过 Sync 的计时替代。

- 初始 512 行，每行完整 payload 为 512 B，总 payload 262,144 B，三列为 id/version/payload；单独记录建库 fixture，不计入请求总量。
- 32 次预热后，256 次点查/16 行范围查交替，共返回 2,176 行，逐行比对全部内容。
- 32 次早期单行提交、512 次真实历史增长提交、32 次长历史提交；历史增长完整计入请求总量。
- 8 个事务，各更新 32 行，每个事务后执行实际 native Full checkpoint。被测 SQL 合计 584 次提交、832 次有效行更新，另有初始 CREATE/seed。
- WAL 历史后及 checkpoint 后各一次独立子进程、空 cache、只读冷重开；中间父进程另以空 cache 重开 writer。三次重开均执行真实 Authority boot、旧 binding 拒绝及 epoch floor+1 新 grant。四次 boot/grant 最终 epoch=4。

`total_request_ns` 是九个固定 phase 的单调 wall ns **之和**，不是进程从启动到退出的 wall。计时包含 SQL 执行/结果物化、真实提交、全部历史增长、checkpoint 事务与 checkpoint、三个重开阶段的实际 bootstrap/grant/open；排除 SQL/model 构造、预热、验证用查询与 oracle 比对、进程 spawn/退出、drop/close、oracle.json 传递与临时目录清理。fixture 单独记录。

所有阶段末与两次冷进程都按模型核对完整有序行，实际 SQL 数据来自对象存储，oracle.json 仅提供期待值及前一个 lease 身份。所有正式样本工作量非计时字段完全相同，两次内容摘要全部一致：

- WAL 后：`42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59`
- checkpoint 后：`aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`

四个样本中，每条 `total_request_ns` 均精确等于全部九个 `wall_ns` 之和。冷子进程失败或任何 oracle 不符均不能产生成功 summary。静态方法和限制见 [harness-review](harness-review.md)；它是作者复核，不作为本轮独立验证。

## 全部正式样本

顺序固定 A1 → B1 → B2 → A2。下表单位 **ms**；倍数统一为同对 `A/B`，越大越快。表列的中位数是“两对倍数的中位数”，不是先求各 arm 时间中位数再相除。

| phase | A1 ms | B1 ms | A1/B1 | A2 ms | B2 ms | A2/B2 | 配对倍数中位数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot_reads | 31.284 | 31.857 | 0.982006 | 32.351 | 32.005 | 1.010837 | 0.996421 |
| small_commits | 200.832 | 202.699 | 0.990792 | 246.947 | 197.705 | 1.249069 | 1.119931 |
| history_commits | 7,221.682 | 6,991.530 | 1.032919 | 7,209.846 | 9,523.828 | 0.757032 | 0.894976 |
| long_history_commits | 632.854 | 679.113 | 0.931883 | 644.931 | 625.564 | 1.030959 | 0.981421 |
| cold_wal_reopen | 4,952.883 | 4,804.724 | 1.030836 | 4,935.929 | 5,511.690 | 0.895538 | 0.963187 |
| writer_reopen | 4,961.943 | 4,921.371 | 1.008244 | 4,824.498 | 5,104.173 | 0.945207 | 0.976725 |
| checkpoint_updates | 701.179 | 691.054 | 1.014652 | 715.716 | 749.811 | 0.954529 | 0.984591 |
| checkpoints | 901.951 | 865.132 | 1.042559 | 991.464 | 972.228 | 1.019786 | 1.031172 |
| cold_checkpoint_reopen | 5,023.441 | 4,798.807 | 1.046810 | 5,089.942 | 5,157.903 | 0.986824 | 1.016817 |
| **total_request** | **24,628.049** | **23,986.286** | **1.026755** | **24,691.626** | **27,874.906** | **0.885801** | **0.956278** |

初始 fixture 分别为 A1 172.288 ms、B1 181.186 ms、B2 199.094 ms、A2 204.322 ms，不参与上述总体倍数。runner 从四进程开始到完成的采集 wall 为 104.526 s，包括编排开销，未用于性能门槛。

原始日志全部保留：

| 样本 | stdout | stderr / 完整 ns 记录 |
| --- | --- | --- |
| A1 | [round-01-baseline](evidence/experiment-01/round-01-baseline.stdout.log) | [round-01-baseline](evidence/experiment-01/round-01-baseline.stderr.log) |
| B1 | [round-01-candidate](evidence/experiment-01/round-01-candidate.stdout.log) | [round-01-candidate](evidence/experiment-01/round-01-candidate.stderr.log) |
| B2 | [round-02-candidate](evidence/experiment-01/round-02-candidate.stdout.log) | [round-02-candidate](evidence/experiment-01/round-02-candidate.stderr.log) |
| A2 | [round-02-baseline](evidence/experiment-01/round-02-baseline.stdout.log) | [round-02-baseline](evidence/experiment-01/round-02-baseline.stderr.log) |

## 正确性门禁：77 项已执行通过

主线程在候选 release 上串行执行以下针对性门禁，所有退出码为 0；计数直接读取实际日志，不把空匹配计作测试。索引见 [correctness.json](evidence/correctness.json)。

| 门禁 | 实际通过 | 原始日志 |
| --- | ---: | --- |
| `--lib storage::` | 8 | [correctness-0](evidence/correctness-0.log) |
| `--lib authority::branch::tests` | 6 | [correctness-1](evidence/correctness-1.log) |
| `--test remote_storage` | 17 | [correctness-3](evidence/correctness-3.log) |
| `--test recovery` | 14 | [correctness-4](evidence/correctness-4.log) |
| `--test p4_runtime` | 30 | [correctness-5](evidence/correctness-5.log) |
| `--lib wal::receipt_tests` | 2 | [correctness-receipt](evidence/correctness-receipt.log) |
| **合计** | **77** | **0 failed** |

首次 `--lib wal::tests` 实际为 **0 passed / 295 filtered out**，日志 [correctness-2](evidence/correctness-2.log) 保留；随后补跑上表 `wal::receipt_tests` 的 2 项。77 不包含这个零匹配，也不代表完整 SQL 兼容或完整 SDK/平台测试矩阵通过。四个正式性能进程自身均为 1 passed；其内部冷 oracle 属于该工作量，未重复算入 77。

## 预先排除的错误产物

首次 baseline smoke 因旧 harness 的非法重开失败，不是性能样本；修复后 smoke v2 通过，亦未加入正式配对。旧 harness 文件已命名为 `failed-v1-*`。

第一次所谓 candidate 曾因 tar 的 mtime=0 触发 Cargo stale artifact，binary SHA 与 A 完全相同；[invalid-candidate-mtime-binary](evidence/invalid-candidate-mtime-binary.json) 保留。主线程强制更新时间并重编，正式 B hash 已不同。runner 测量前要求 A/B binary hash 不同、当前 harness hash 与 identity 相同；每样本还验证总和及工作量/oracle。来源错误在正式样本前发现，不允许将错误产物当作候选结果或掩盖该事件。

## 解释、限制与下一轮

门槛在测前固定：至少两对中每对总体均 ≥1.5×，总体配对中位数 ≥1.5×，所有固定阶段的配对中位 speedup ≥`1/1.10≈0.909091`。本轮两个总体都不达 1.5×，history 阶段又低于 0.909091，故不能宣称目标完成。热读接近 1×、小提交中位 1.12×、checkpoint 中位 1.03×均不改变总体失败。

宿主为 `macOS-26.5.1-arm64-arm-64bit`。一分负载在采集开始为 5.572，A1 后 4.678、B1 后 3.679、B2 后 10.098、A2 后 7.996；第二候选的历史阶段明显较慢与负载升高同时出现。AB/BA 反转顺序有助于发现漂移，但四个样本无法把主机噪声和代码影响分开。保留较慢样本并按预设门槛失败，不能因此证明候选必然让历史写变慢，也不能归一化或删去它后宣称有效提升。

实际冷启动/恢复在各样本仍占大量时间，优化提交中的局部复制和重复编码没有证明足以支配整个工作量。下一轮继续相同 workload、相同 A 基线，先细分冷启动共同路径；[cold-start-audit](cold-start-audit.md) 给出可验证候选，包括 boot 后重复 deep verify、历史 index 前缀查询、重复 epoch-floor 解析等。该审计是源码假设，不能预报其收益；新候选必须另冻源码、补 correctness、再运行完整配对，不改变门槛。

外推仅限这组单线程本地真实 SQL 请求。应用进程和 cache 冷态不等于 OS/设备缓存冷态；graceful close 的冷 oracle 不代替断电/提交后立即崩溃耐久性实验；未覆盖 HTTP/Gateway、网络/S3、并发公平性或 p99。本报告等待另一 agent 的 [01_verification.md](01_verification.md)，不把作者自审写成独立确认。
