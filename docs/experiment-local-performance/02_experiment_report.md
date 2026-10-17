# 实验 02：boot 自有 root admission 复用后的完整 SQL 配对

日期：2026-10-17。执行者与整理者：workload agent。取得主线程明确 GO、确认构建/门禁结束后，仅执行保存的 release 二进制；没有构建或修改产品代码。四个样本全部完成，[results.json](evidence/experiment-02/results.json) 已有 `finished_unix`。

**仍未达到完整 1.5× 门槛。** 第一对总体 **1.507917×**，第二对 **1.404408×**，配对倍数中位数 **1.456163×**；无固定阶段的配对中位回退超过 10%。第二对与总体中位数未达到 1.5×，runner 正确返回 **2 / NOT_YET_1_5X**。该退出码是性能门槛未通过，不是 SQL/恢复测试失败。

## 假设与冻结身份

实验 01 的总体尚无改善，实际综合 reopen phase 占请求总量的大部分。源码审计指出 boot 成功发布自有 root 后未登记 admission，首次 state snapshot 会再完整 deep verify。candidate02 相对 candidate01 仅叠加 [boot admission 候选](boot-admission-candidate.md)：对确定持久成功且基于已验证 root 构造的自有 boot candidate，复用同一次编码所得 digest；CAS 拒绝、Unknown 与外来 root 保持原校验。新增四个对应回归。

本轮检验该完整候选能否在相同原 A 基线、相同工作量下达到总体 1.5×。A 仍是 `1198b18`，所以倍数衡量的是整个 candidate02 快照相对 A，不是 boot 改动相对 candidate01 的单项因果收益。candidate03 在工作区并行开发源码，未编译/执行于本轮；执行内容由以下保存二进制固定，不由当前工作区决定。

| 身份 | 冻结值 |
| --- | --- |
| A commit | `1198b18611601f7417f7b9ae4bbb3c370b41ff03` |
| B source ID | `8e2e66a92cfc980e8ca367a852d84b7cad77c4ceabc0a061d0f495a0d9dc5e45` |
| B snapshot HEAD | `042476a9abc4beb0ad35be330202dd99cd47be7f` 加工作树；724 文件完整快照 |
| B source tar SHA-256 | `230fc3cd335e3f224f576151d31c9e7212db645bb87435cfe96a5b1e25a3c8f7` |
| B tracked patch SHA-256 | `88afae20d2370e44213b79338562875e9b53ab4a1fee3bf2e74ea005b05afc5b` |
| A binary SHA-256 | `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3` |
| B binary SHA-256 | `e068ce8467c8c4de20382d437c8544775df59d3ef659fd8797cdcac2af7c1da1` |
| 两臂相同 harness SHA-256 | `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef` |
| 固定 Turso commit | `24a3a4609683b73935888f8bc30b47727d100aa8` |
| runner SHA-256 | `244f59c9ed7dbde6fe4f75b7fb999953677aade7175638ee2386a06feee2fc98` |

来源与复现输入：[A binary metadata](evidence/baseline-binary.json)、[B source metadata](evidence/candidate-02/source.json)、[B manifest](evidence/candidate-02/source-manifest.json)、[B source tar](evidence/candidate-02/source.tar.gz)、[B tracked patch](evidence/candidate-02/tracked.patch)、[B binary metadata](evidence/candidate-02/binary.json)、[B build stdout](evidence/candidate-02/build.stdout.log)、[B build stderr](evidence/candidate-02/build.stderr.log)。A 沿用实验 01 同一冻结 binary，来源链和先前无效产物处理见 [01_verification](01_verification.md)。

## 调度、命令与计时范围

第一次 preflight 发现一个尚未自然退出的 cargo（PID 19194），因此**没有启动任何计时**。待其退出，再次取进程快照，确认无 cargo/rustc/rustdoc 或相关 benchmark，读取实际 A/B 文件并核对 metadata SHA 后开始正式测试；没有停止任何现有进程。阻挡记录与干净快照均保留：[blocked 摘要](evidence/experiment-02.preflight-blocked-processes.txt)、[pre-run 摘要](evidence/experiment-02.pre-run-processes.txt)。原始清单另以相应 `.full.txt` 名称保存，仅 PID/PPID/COMM，无进程参数或凭据。

[完整命令与执行身份](evidence/experiment-02.command.json) 保存了 runner argv、hash、两臂 hash、preflight 与退出码；[runner stdout](evidence/experiment-02.runner.stdout.log) 保存开始/完成顺序和最终 summary，[runner stderr](evidence/experiment-02.runner.stderr.log) 为空。固定 `--rounds 2`、顺序 **A1→B1→B2→A2**，每个进程独立临时对象存储/夹具，串行执行，未与其他构建/性能实验重叠。

沿用原 [00_plan](00_plan.md) 的 `local-sql-v1`，没有缩减场景：512 行×512 B payload；32 预热后 256 次查询，共 2,176 行；32 早期+512 历史+32 晚期单行提交；8 个各更新 32 行的事务，每个后接真实 native Full checkpoint。合计 584 次被测 SQL 提交、832 次有效行更新。两次独立子进程/空 cache 只读冷重开，中间父进程另以空 cache 重开 writer，四次真实 Authority boot/grant 最终 epoch=4，旧 binding 必须拒绝。

九阶段 `wall_ns` 的精确和为 `total_request_ns`。这是固定请求 wall 的总和，**不是全进程 wall**：包含实际 SQL/结果物化、耐久提交、512 次历史增长、checkpoint、重开的 store/Authority boot、grant/open 与冷读查询；排除建库 fixture、预热、SQL/model 构造、验证 SELECT/oracle、spawn/退出、drop/close、oracle.json 传递与目录清理。此次 runner 采集 wall 为 85.123 s，仅作为执行记录，不参与门槛。

## 全部四个正式样本

单位 **ms**，倍数均为同对 A/B，越大越快；最后一列是两对倍数的中位数。所有原始样本均保留，没有用跨轮最快 A/B、首对或单一 phase 替代整体判断。

| phase | A1 ms | B1 ms | A1/B1 | A2 ms | B2 ms | A2/B2 | 配对倍数中位数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot_reads | 32.073 | 33.974 | 0.944051 | 31.599 | 31.783 | 0.994199 | 0.969125 |
| small_commits | 211.849 | 193.522 | 1.094699 | 258.310 | 188.343 | 1.371491 | 1.233095 |
| history_commits | 7,148.859 | 6,770.492 | 1.055885 | 6,730.947 | 6,932.949 | 0.970864 | 1.013374 |
| long_history_commits | 671.946 | 593.848 | 1.131510 | 656.274 | 599.077 | 1.095474 | 1.113492 |
| cold_wal_reopen | 5,079.653 | 2,480.388 | 2.047927 | 4,810.559 | 2,476.834 | 1.942221 | 1.995074 |
| writer_reopen | 5,032.859 | 2,399.656 | 2.097325 | 4,774.208 | 2,548.499 | 1.873342 | 1.985333 |
| checkpoint_updates | 742.340 | 703.271 | 1.055553 | 705.294 | 680.932 | 1.035778 | 1.045666 |
| checkpoints | 944.756 | 883.447 | 1.069397 | 901.974 | 867.468 | 1.039778 | 1.054588 |
| cold_checkpoint_reopen | 5,203.412 | 2,565.485 | 2.028237 | 4,797.006 | 2,525.470 | 1.899451 | 1.963844 |
| **total_request** | **25,067.746** | **16,624.084** | **1.507917** | **23,666.171** | **16,851.354** | **1.404408** | **1.456163** |

fixture 单独为 A1 171.563 ms、B1 167.475 ms、B2 180.932 ms、A2 178.172 ms。四个 raw `LOCAL_PERF` 对象与 results 中对应对象逐项一致；每条 `total_request_ns` 均严格等于九阶段总和。

| 样本 | stdout | stderr / 完整 ns |
| --- | --- | --- |
| A1 | [round-01-baseline](evidence/experiment-02/round-01-baseline.stdout.log) | [round-01-baseline](evidence/experiment-02/round-01-baseline.stderr.log) |
| B1 | [round-01-candidate](evidence/experiment-02/round-01-candidate.stdout.log) | [round-01-candidate](evidence/experiment-02/round-01-candidate.stderr.log) |
| B2 | [round-02-candidate](evidence/experiment-02/round-02-candidate.stdout.log) | [round-02-candidate](evidence/experiment-02/round-02-candidate.stderr.log) |
| A2 | [round-02-baseline](evidence/experiment-02/round-02-baseline.stdout.log) | [round-02-baseline](evidence/experiment-02/round-02-baseline.stderr.log) |

## 内容、恢复与正确性证据

四个正式进程 stdout 均为 **1 passed / 0 failed**。各阶段验证完整有序 id/version/TEXT payload；所有样本的非计时工作量字段相同，两个独立冷进程的摘要与父进程一致，`prior_binding_rejected=true`、cold epochs=2/4、writer epoch=3、final epoch=4 均保持。

- WAL 后摘要：`42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59`
- checkpoint 后摘要：`aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`

主线程在 GO 前针对 candidate02 的 release 门禁如下，全部通过；本 agent 只读取日志，没有重跑：

| 门禁 | 实际通过 | 日志 |
| --- | ---: | --- |
| 新 boot admission 回归 | 4 | [boot stdout](evidence/candidate-02/correctness-boot.stdout.log)、[boot stderr](evidence/candidate-02/correctness-boot.stderr.log) |
| namespace 初始化 | 5 | [namespace](evidence/candidate-02/correctness-namespace.log) |
| ownership | 8 | [ownership](evidence/candidate-02/correctness-ownership.log) |
| p4_authority | 35 | [p4_authority](evidence/candidate-02/correctness-p4-authority.log) |
| **本轮合计** | **52** | **0 failed** |

四项新测试覆盖首 state 复用且保留 epoch floor、外来 root 仍 deep verify/坏历史拒绝、CAS 失败候选不提前 admit、Unknown 在落盘前后均不 admit。实验 01 的 77 项是其前一快照的证据，不能虚加成 candidate02 重跑 129 项，也不代表完整 SQL/SDK 平台门禁全绿。graceful close 后的冷 oracle 仍不是掉电或提交后立即崩溃耐久性证明。

## 判断与下一轮约束

原门槛要求至少两对中**每对**总体 ≥1.5×，配对中位数也 ≥1.5×，且所有固定 phase 的配对中位数 ≥`1/1.10≈0.909091`。本轮 phase 条件全部通过，但第二对及中位数不通过；不四舍五入 1.456 为 1.5，不只采用第一对的 1.508 宣布目标完成。

可报告的测量事实是：三项综合 reopen 的配对中位倍数约 **1.964–1.995×**；其总时间 A1/B1 为 **15.316/7.446 s**，A2/B2 为 **14.382/7.551 s**。这些 phase 涵盖 bootstrap、深校验、epoch floor、grant、session/native open 与部分首次 SELECT，不能把全部节省直接称作某一个 deep verify 函数的时间，亦不能据其接近 2× 推断整个 SQL 工作量达到 1.5×。

机器为 `macOS-26.5.1-arm64-arm-64bit`；采集开始一分负载 5.089，各臂结束 A1 4.443、B1 4.621、B2 5.112、A2 4.583。只有边界 load、只有两对，尚不足以给稳定收益置信区间。A2 比 A1 快，历史阶段 B2 也未占优，都按原样保留。不得用跨轮结果挑选更有利基线。

后续继续原 A、相同 harness/工作量与全部门槛，另冻 candidate03 来源和可执行文件，完成其独立 correctness 后再配对；当前工作区的新开发不属于本轮已测代码。测试只覆盖单线程本地真实请求，宿主 OS/设备缓存未清除，无 HTTP/Gateway、S3 网络、并发公平性或 p99 外推。等待另一 agent 的 [02_verification.md](02_verification.md) 独立核对。
