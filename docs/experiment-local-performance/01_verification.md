# 实验 01 独立验证

日期：2026-10-17。验证者未执行本轮实验、构建或 benchmark；依据原始文件独立重算摘要、工作量、计时总和和配对汇总，并阅读冻结 harness 与 runner。输入为 [实验报告](01_experiment_report.md)、[原始结果](evidence/experiment-01/results.json) 及报告链接的全部四组 stdout/stderr、构建与正确性日志。

## 总体评估

**PASS：实验报告“本轮未达到 1.5×”的结论可信。性能验收为 NOT_YET_1_5X。** 本次没有充分证据认定候选提高或降低了真实稳定性能，也没有测量出某一个冷启动函数的耗时贡献。

| 维度 | 评估 | 独立验证结果 |
| --- | --- | --- |
| 数据完整性 | PASS | 四个正式样本全部保留，原始记录与 results 完全一致；九阶段计时总和准确，所有非计时字段/oracle 一致 |
| 方法正确性 | PARTIAL | 同 harness、release、固定 A 基线、独立夹具、串行 AB/BA、真实持久路径符合规划；只有两对且主机负载漂移，不能分离噪声与代码影响 |
| 结论合理性 | PASS | 两对与中位数都远未达到 1.5×；history 阶段也未过预设回退门槛；报告未删慢样本、未将局部机制收益当整体收益 |
| 报告质量 | PASS | 身份、全部阶段、77 项实际测试、失败产物、计时范围与外推限制齐全，事实与源码假设有区分 |

## 原始数据、工作量与结果重算

逐一读取四份 stderr 中唯一的 `LOCAL_PERF` JSON，均与 results 对应 `result` 完全一致；四份 stdout 均为 `1 passed / 0 failed`。独立执行 runner 的纯计算函数，`summarize(samples)` 与已保存 summary 完全相等，`workload(result)` 四臂完全相等。此操作没有启动测试二进制。

| 样本 | 请求总量 s | 同对 A/B |
| --- | ---: | ---: |
| A1 | 24.628049459 | 1.026755429 |
| B1 | 23.986286081 | |
| B2 | 27.874906416 | 0.885801225 |
| A2 | 24.691626252 | |

实际执行顺序为 A1 → B1 → B2 → A2。配对倍数中位数重算为 **0.956278327**。`history_commits` 两对为 **1.032918755 / 0.757032367**，中位数 **0.894975561**，小于固定阶段门槛 `1/1.10 ≈ 0.909091`。九阶段总和逐条精确等于 `total_request_ns`，无挑阶段替代总量。

冻结 harness 的有效业务量为 512 行 × 512 B payload、256 次查询返回 2,176 行，32+512+32 次单行提交，以及 8×32 行事务和 8 次实际 checkpoint；合计 584 次被测 SQL 提交、832 次有效行更新。所有 UPDATE 都改变 version/payload；步长 73 与 512 互质，同一历史阶段遍历全部行，阶段末完整 oracle 不会仅凭最终少数行掩盖中间遗漏。query/execute/checkpoint 失败直接返回错误，没有 SQL 自动重试。

oracle 逐行检查三列、id、version、TEXT 类型及完整 512 B payload，冷进程从实际 `objects` 加载 SQL 数据；oracle.json 只提供期望结果与旧 lease。四样本 WAL 摘要均为 `42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59`，最终摘要均为 `aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`；冷子进程摘要与父阶段相符，旧 binding 被拒绝，最终 epoch 均为 4。

## 版本、源码和实际二进制身份

独立读取保存文件重新计算 SHA-256，而非只比较 JSON 标签：

- A 实际二进制：`a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`；B 实际二进制：`ce3744e8b9a5d5f7159f1cfe151d2c06bfe8b9dd86dedb4e8e0bac580a4e9d28`。均与 binary metadata 和正式 results 相同，且两臂不同。
- `git archive 1198b18611601f7417f7b9ae4bbb3c370b41ff03 prototypes/v1 rust-toolchain.toml` 的原始 tar 字节 SHA 为 `38f86727c29cddb293d4998c2154419e97ef4526f55fd39015f6f41cbef22750`，与 identity 一致。
- B 的 `candidate-source.tar.gz` SHA 为 `1754f94e93e4dcca31ccc74f8e0f71b43258686c0a60142eab43939d7a31518f`；读取 tar 内 723 个 manifest 文件，逐文件摘要全部相同。冻结 lab 的相同 723 文件也全部相同。
- manifest 的精确字节 SHA，以及 `sha256((json.dumps(manifest,sort_keys=True,indent=2)+'\n').encode())`，均为 B source ID `a3edf74410c9d2a4a32b09c05ab9b04a4365b4b4e0e7b6cf05aca2a9d68f44bb`。
- tracked patch 文件 SHA 为 `67be846bbf402862213c7e1cf6ce15c1f95aa6870114b89a7da2bf2414409a40`，与 source metadata 一致。tracked patch 是相对快照 HEAD 的工作区补丁，单独使用它不能重建全部 A→B 差异。
- 当前 harness、归档 `evidence/local_performance.rs`、冻结 lab harness 三者均为 `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef`。Turso submodule 为固定 `24a3a4609683b73935888f8bc30b47727d100aa8`。

独立比较 A git archive 与 B tar，共 **20 个新增/变化路径、无删除**；包含 accepted WAL、编码/COW/pin 修复、IO 锁外发布，以及 maintenance 的测试模块等。实验比较的是整个快照，不能拆出某一优化的因果贡献。

构建 JSON 的 integration target 均为 `opt_level=3 / debug_assertions=false / fresh=false`；最终 B 的产品 lib 也是 `fresh=false`。A 此次日志中的产品 lib 为 `fresh=true`，因为它复用了此前 baseline 产物；不能宣称这份 A 日志重新编译了该 lib。现存 tar/manifest/build/binary 记录在来源上相互一致，但 source 标签来自执行者环境变量，本身没有把代码内容嵌入二进制，也不能以“binary hash 不同”单独证明编译来源。本轮审查没有重建二进制。

已排除的 stale candidate 与 A 的二进制摘要完全相同，未出现在四份正式样本中。旧 harness 首次失败 smoke 和修正后 smoke 也都未进入配对结果；失败记录得以保留。当前主工作区的 `migration.rs` 在本轮之后已经改变，但冻结 lab 与 B manifest 在核验时一致；这个后续改动不属于实验 01。

## 正确性门禁

独立读实际日志中的测试 summary，而非只采信退出 0：

| 日志 | 实际通过 |
| --- | ---: |
| correctness-0 / storage | 8 |
| correctness-1 / branch | 6 |
| correctness-3 / remote_storage | 17 |
| correctness-4 / recovery | 14 |
| correctness-5 / p4_runtime | 30 |
| correctness-receipt | 2 |
| 合计 | **77，0 failed** |

`correctness-2 / wal::tests` 是 0 passed / 295 filtered out，报告已如实排除，并补有 2 个 receipt 测试。branch 日志包含本审查提出的 lost-ACK 后采用视图跨 GC 回归。77 项是本轮针对性证据，不是完整 SQL/SDK/平台兼容性门禁；graceful close 后冷读也不构成掉电耐久性实验。

## 计时边界、解释与下一轮约束

读源码确认：九 phase 计时来自单调 `Instant`，SQL 结果在返回前物化，提交及 checkpoint 在调用成功返回后停表。总量包含全部 512 次 history commits。建库/seed、SQL 与模型构造、预热、验证 SELECT/oracle 比对、spawn/退出、drop/close 和临时目录清理在请求计时外。冷子进程计时从读完 oracle 后开始，涵盖真实 store/Authority bootstrap、term/grant/open 和第一次完整 SELECT；writer reopen 计时没有显式完整 SELECT，但 open 本身仍有数据库打开工作。因此这里是固定请求时间之和，不能叫全进程端到端时间。

三项 reopen 在 A1/B1/B2/A2 的总量占比分别为 **60.66% / 60.56% / 56.59% / 60.14%**。它们是综合 phase，**不能把这个占比或约 5 s 直接归到 `verify_deep`、boot admission、epoch-floor 扫描或 WAL replay 的任一函数**。源码可指出 boot 后重复深校验这一待验证切口，未提供分段计时就不能声称该切口已经解释、消除或预测了多少秒。

B2 后 1 分钟 load 为 10.098，明显高于 B1 后 3.679；这是采样边界的主机状态，并非每个 phase 的负载测量。第二对 history 异常可以由噪声、代码或二者共同造成，目前不能确认因果。两对足以按既定门槛拒绝本轮通过，不能可靠估计稳定收益或为候选永久判定回退。

后续若做 boot admission 候选，应继续相同完整 workload、原 A 基线和全部门槛，另冻源码/二进制；同时验证 newly observed root 仍 deep verify、CAS 失败与 Unknown 不 admit 错候选。只有新的完整配对结果才能说明这个源码切口是否提高整体性能。不能据本轮 cold 占比预先宣布它可达 1.5×，也不能删除历史阶段或慢样本改善验收数字。
