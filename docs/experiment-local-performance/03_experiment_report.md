# 实验 03：固定本地 SQL 工作量达到 1.5× 门槛

日期：2026-10-17。执行与整理：workload agent。按主线程明确 GO，仅执行冻结 release 二进制，未构建或修改产品代码。[完整结果](evidence/experiment-03/results.json) 已包含四个正式样本及 `finished_unix`。

**预设门槛通过：两对总体分别 2.370838× / 2.421294×，配对倍数中位数 2.396066×，全部固定阶段均未触发中位延迟回退超过 10%。** runner 退出 **0 / PASS**。该结论限于固定 `local-sql-v1` 请求集合；主要收益集中在综合冷重开，不能表述为所有热读、提交、checkpoint 均提高 2.4×。

## 假设与快照范围

candidate02 的完整结果为 1.456163×，仍未达标。candidate03 在其基础上增加单次固定 root 的 Walker WAL chunk positive cache，复用已经成功验证的历史 index 共享前缀注册结果；每个 index 的独立结构与摘要校验保留。cache 为 Walker 局部、4096 项 FIFO，miss/淘汰仍完整校验，失败不缓存，不跨 root/请求。浅/深 WAL 扫描标记分离，防止浅校验误当已完成深扫。设计和边界见 [walker-chunks-candidate](walker-chunks-candidate.md)。

独立比较 candidate02/03 的 source manifest，恰好三处不同：`authority/semantic.rs`、新增 `authority/semantic_wal_chunks_tests.rs`，以及外部已提交的 `maintenance/integration_tests.rs` 测试调整。harness 完全相同。相对原 A，candidate03 仍包含前两轮完整候选与先前已提交 IO 等变更；本轮总体倍数不能拆成某一项独立优化的因果贡献。后续工作区维护/platform 变更未混入本轮保存的 binary。

| 身份 | 冻结值 |
| --- | --- |
| A commit | `1198b18611601f7417f7b9ae4bbb3c370b41ff03` |
| B source ID | `e4620e51fa8e987a644aaab867cf4fe19099d662d4e0de2c7fe56b066ea3e3c2` |
| B snapshot HEAD | `dc8e6e870b87b4abff0a23fe8a1193167cefa435` 加归档工作树；725 文件 |
| B source tar SHA-256 | `cc2ca441a024155ac6b2ef7ac12170f3607a443ddb2658007a32246496ca87ab` |
| B tracked patch SHA-256 | `ac11d47e00c6b9dc4ff4e32fe14a4e2d35a71e51f229d8d00ccfbec8e34db1b6` |
| A binary SHA-256 | `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3` |
| B binary SHA-256 | `f03bc6bd2903dd665ae259fc271421815625335a99cc5d6d966dc73f95967e8c` |
| 相同 harness SHA-256 | `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef` |
| 固定 Turso commit | `24a3a4609683b73935888f8bc30b47727d100aa8` |
| runner SHA-256 | `244f59c9ed7dbde6fe4f75b7fb999953677aade7175638ee2386a06feee2fc98` |

来源：[identity](identity.json)、[A binary metadata](evidence/baseline-binary.json)、[B source metadata](evidence/candidate-03/source.json)、[B manifest](evidence/candidate-03/source-manifest.json)、[B source tar](evidence/candidate-03/source.tar.gz)、[B tracked patch](evidence/candidate-03/tracked.patch)、[B binary metadata](evidence/candidate-03/binary.json)、[B build stdout](evidence/candidate-03/build.stdout.log)、[B build stderr](evidence/candidate-03/build.stderr.log)。正式样本未使用旧失败 harness 或 mtime stale candidate，处理事实保留在前两轮报告。

## 固定方法、调度与 oracle

主线程确认所有构建/门禁结束后发出 GO。正式启动前 `ps` 无 cargo/rustc/rustdoc 或相关 benchmark，[相关进程摘要](evidence/experiment-03.pre-run-processes.txt) 留存；实际读取两臂文件并核对 metadata SHA。执行期间没有调度其他构建或压测。保存的 binary 来源不依赖测试时工作区源码，未搭建提前预热 fixture。

[完整命令与身份](evidence/experiment-03.command.json) 保存 runner argv、hash、preflight、两臂实际 SHA 和退出码；[runner stdout](evidence/experiment-03.runner.stdout.log) 保存顺序/结果，[runner stderr](evidence/experiment-03.runner.stderr.log) 为空。固定 `--rounds 2`，串行顺序 **A1→B1→B2→A2**，每臂各自从独立对象存储建立相同夹具。

沿用原 [00_plan](00_plan.md) 与相同 [harness](evidence/local_performance.rs)：真实 LocalObjectStore → OwnershipAuthority → fenced branch store → RemoteDatabase → Turso。保留原 schema/协议/Full sync/fencing 和 SQL 错误直接返回，不引入 SQL 自动重试。工作量没有为 candidate03 改简：

- 初始 512 行，每行 512 B 完整 payload，总 payload 262,144 B。
- 32 预热后 256 次点查/16 行范围查，共返回 2,176 行，计时内物化，计时外逐行核对。
- 32 早期、512 历史增长、32 长历史单行提交，全部历史增长计时；另 8 个各更新 32 行的事务与 8 次真实 native Full checkpoint。
- 共 584 次被测 SQL 提交、832 次有效行更新；WAL 后和 checkpoint 后各一个独立子进程/空 cache 只读冷重开，中间另以空 cache 重开 writer。
- 四次实际 Authority boot/grant，旧 binding 拒绝、持久 epoch floor 递增，最终 epoch=4；所有阶段和两次冷进程核对完整有序 id/version/TEXT payload。

四个样本全部非计时工作量字段一致；冷摘要与各父阶段一致，全部样本的摘要为：

- WAL 后：`42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59`
- checkpoint 后：`aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`

九个单调 `Instant` 请求 phase 的 ns 精确相加得到 `total_request_ns`。它包括实际 SQL/结果物化、耐久提交、全部历史增长、checkpoint 事务与 checkpoint、重开的实际 store/Authority bootstrap/grant/open 和冷读查询；排除建库 fixture、预热、SQL/model 构造、验证 SELECT/oracle、进程 spawn/退出、drop/close、oracle.json 传递及清理。**该总量不是完整进程 wall。** runner 采集 wall 为 71.489 s，未用于性能门槛。

## 全部正式结果

单位 **ms**；倍数统一为同对 A/B。中位数按两对倍数求得，没有选最好阶段、删除样本或把各 arm 时间中位数相除替代预设公式。

| phase | A1 ms | B1 ms | A1/B1 | A2 ms | B2 ms | A2/B2 | 配对倍数中位数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot_reads | 31.583 | 32.340 | 0.976585 | 31.952 | 32.460 | 0.984349 | 0.980467 |
| small_commits | 263.267 | 174.509 | 1.508615 | 189.524 | 187.306 | 1.011842 | 1.260229 |
| history_commits | 7,114.453 | 6,854.787 | 1.037881 | 6,801.516 | 6,801.588 | 0.999989 | 1.018935 |
| long_history_commits | 650.806 | 601.875 | 1.081297 | 620.110 | 593.149 | 1.045454 | 1.063375 |
| cold_wal_reopen | 4,719.794 | 288.740 | 16.346199 | 4,959.481 | 291.212 | 17.030455 | 16.688327 |
| writer_reopen | 4,751.760 | 277.454 | 17.126323 | 5,231.689 | 281.107 | 18.611055 | 17.868689 |
| checkpoint_updates | 709.131 | 706.213 | 1.004131 | 690.514 | 683.745 | 1.009900 | 1.007016 |
| checkpoints | 872.982 | 868.166 | 1.005547 | 848.661 | 873.224 | 0.971871 | 0.988709 |
| cold_checkpoint_reopen | 4,848.088 | 302.832 | 16.009161 | 4,980.229 | 314.332 | 15.843842 | 15.926501 |
| **total_request** | **23,961.864** | **10,106.916** | **2.370838** | **24,353.677** | **10,058.123** | **2.421294** | **2.396066** |

fixture 单独为 A1 170.664 ms、B1 165.927 ms、B2 176.152 ms、A2 189.685 ms，不计入总量。四份 stderr 的唯一 raw `LOCAL_PERF` JSON 与 results 对应对象逐项一致；每样本总量精确等于九阶段之和。四个 stdout 均为 **1 passed / 0 failed**。

| 样本 | stdout | stderr / 完整 ns |
| --- | --- | --- |
| A1 | [round-01-baseline](evidence/experiment-03/round-01-baseline.stdout.log) | [round-01-baseline](evidence/experiment-03/round-01-baseline.stderr.log) |
| B1 | [round-01-candidate](evidence/experiment-03/round-01-candidate.stdout.log) | [round-01-candidate](evidence/experiment-03/round-01-candidate.stderr.log) |
| B2 | [round-02-candidate](evidence/experiment-03/round-02-candidate.stdout.log) | [round-02-candidate](evidence/experiment-03/round-02-candidate.stderr.log) |
| A2 | [round-02-baseline](evidence/experiment-03/round-02-baseline.stdout.log) | [round-02-baseline](evidence/experiment-03/round-02-baseline.stderr.log) |

## 本轮正确性门禁：63 项通过

主线程在 GO 前针对冻结 candidate03 的 release 门禁，实际日志计数如下。本 agent 读取证据，没有自行重跑或把先前轮次测试重复算入本轮。

| 门禁 | 实际通过 | 日志 |
| --- | ---: | --- |
| 新 Walker 共享前缀、拒绝和容量回归 | 4 | [walker stdout](evidence/candidate-03/correctness-walker.stdout.log)、[stderr](evidence/candidate-03/correctness-walker.stderr.log) |
| boot admission | 4 | [boot stdout](evidence/candidate-03/correctness-boot.stdout.log)、[stderr](evidence/candidate-03/correctness-boot.stderr.log) |
| branch | 6 | [branch](evidence/candidate-03/correctness-branch.log) |
| recovery | 14 | [recovery stdout](evidence/candidate-03/correctness-recovery.stdout.log)、[stderr](evidence/candidate-03/correctness-recovery.stderr.log) |
| p4_authority | 35 | [authority stdout](evidence/candidate-03/correctness-authority.stdout.log)、[stderr](evidence/candidate-03/correctness-authority.stderr.log) |
| **本轮合计** | **63** | **0 failed** |

Walker 新测试覆盖共享前缀只重复查询一次且 GC reachable 完整、新 root 的缺失/未注册/tombstoned/错误 hash 连续拒绝、浅后深扫仍发现缺失 chunk、4096 项 FIFO 淘汰后重新验证且 reachable 不丢失。cache 的两容器 hash 文本有界约 512 KiB，另有节点/String 开销，不能称总内存预算恰好为 512 KiB。

这些是针对性功能证据；不是完整 SQL/SDK/平台矩阵，也不因性能 PASS 就替代独立代码审查或故障/耐久性测试。正常 close 后冷读仍不证明断电或提交后立即崩溃耐久性。

## 门槛判断与归因边界

预先要求：至少两对中每对总体 ≥1.5×，总体配对中位数 ≥1.5×，各固定 phase 配对中位数 ≥`1/1.10≈0.909091`。本轮两对 2.370838/2.421294 均通过，总体中位 2.396066 通过；最低 phase 中位为 hot_reads 0.980467，仍高于阶段门槛。因此 **固定本地工作量的性能验收 PASS**。

节省主要位于三个综合 reopen：A1/B1 合计 **14.320/0.869 s**，A2/B2 **15.171/0.887 s**。它们各自的配对中位倍数约 **15.93–17.87×**，包括多种初始化、恢复、授权和查询工作；没有函数分段计时，不能把整个倍数写成 `verify_deep` 或某一个函数快了 16–18×。boot admission 与 Walker 机制有静态/针对性回归支撑，完整配对则证明该组合快照在此 workload 的用户可观察效果。

热读 0.980×、历史增长 1.019×、checkpoint 0.989×接近 1×；小提交 1.260×也有两对差异，不能据总体 2.396×推断稳定单条热 SQL 提交都快 2.4×。这组 workload 固定包含长历史和三次重开，重开权重是总体提升的关键条件，并非普通持续热会话的普遍吞吐模型。

宿主为 `macOS-26.5.1-arm64-arm-64bit`，一分负载从开始 1.989 到 A1/B1/B2/A2 后 2.750/2.855/3.220/3.356。只有两对，无法给稳健的置信区间或 p99；宿主 OS 与设备缓存未清除，只有应用进程与 cache 冷态。无 HTTP/Gateway、S3 网络、并发公平性或容量外推。等待 [03_verification.md](03_verification.md) 独立核对和最终对抗审查；本报告不充当它们。
