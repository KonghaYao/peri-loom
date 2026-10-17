# 实验 03 独立验证

日期：2026-10-17。验证者独立读取 [完整 results](evidence/experiment-03/results.json)、四份 stdout/stderr、执行命令/进程快照、candidate03 源码 tar/manifest/patch、保存二进制和构建/正确性日志。未运行构建、测试或 benchmark。核验开始时原始结果已含 `finished_unix`，四个正式样本全部完成。

随后已核对落盘的 [03_experiment_report.md](03_experiment_report.md)：全部阶段数字、身份、63 项实际计数、门槛判断和综合 reopen 的归因边界与原始证据一致，没有发现报告扩大结论范围。

## 评估

**PASS：本轮固定本地真实 SQL 工作量通过预设性能门槛。** 两对均超过 1.5×，中位数超过 1.5×，所有固定 phase 的配对中位数均通过回退门槛。该判断限定于原计划的完整工作量和请求计时口径，不能泛化为所有 SQL/部署均快至少 1.5×。

| 维度 | 结果 | 独立证据 |
| --- | --- | --- |
| 数据完整性 | PASS | 四样本完整且全部保留；raw JSON 与 results 逐项相等，九 phase 精确总和无误 |
| 方法正确性 | PASS，限固定工作量 | 原 A 基线、同冻结 release harness、串行 AB/BA、每臂独立夹具；工作量与实验 02 完全一致 |
| 性能门槛 | PASS | 两对 2.370838× / 2.421294×，中位数 2.396066×；无 phase 中位回退超过 10% |
| 内容与针对性正确性 | PASS，范围有限 | 四正式进程各 1 passed，完整 oracle/冷恢复一致；候选针对日志 63 passed / 0 failed |

## 原始记录与独立重算

读取每份 stderr 中唯一的 `LOCAL_PERF` JSON，四份分别与 results 的 result 完全一致；stdout 均为 `1 passed / 0 failed`。每条九个 phase 的 `wall_ns` 精确相加等于 `total_request_ns`。对保存结果重新调用 runner 的纯计算汇总函数，完整 summary 与原始 summary 相同，没有启动任何测试进程。

| 顺序 | 样本 | 请求总量 ns | 同对 A/B |
| --- | --- | ---: | ---: |
| 1 | A1 | 23,961,864,125 | 2.3708384061473575 |
| 2 | B1 | 10,106,915,791 | |
| 3 | B2 | 10,058,122,584 | 2.4212944774346568 |
| 4 | A2 | 24,353,676,666 | |

配对倍数中位数为 **2.396066441791007**，两对分别通过 ≥1.5；最小 phase 配对中位数为 hot_reads **0.980466836**，仍高于 `1/1.10≈0.909091`。history_commits 中位 **1.018935201**，checkpoints 中位 **0.988709324**；没有靠删除历史增长阶段或用局部改善替代完整总量。command 记录的 runner 退出码 0 与门槛判断一致。

四样本非计时字段全部相同，且与实验 02 的整个 `workload(result)` 完全相同：512 行×512 B payload、256 query/2,176 返回行、32+512+32 单行提交、8×32 行事务和 8 checkpoint、两次独立冷只读进程及中间 writer reopen。两个完整内容摘要分别保持 `42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59` 和 `aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`，冷子进程与父 oracle 一致，旧 binding 拒绝和 epochs 保持相同。没有调整工作量或 oracle 以取得门槛通过。

## 冻结身份和来源

- 实际读取 A binary 字节重算为 `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`，B 为 `f03bc6bd2903dd665ae259fc271421815625335a99cc5d6d966dc73f95967e8c`，均与正式 results、command 和各自 binary metadata 相同。
- B source-manifest 字节 SHA 为 `e4620e51fa8e987a644aaab867cf4fe19099d662d4e0de2c7fe56b066ea3e3c2`，等于 source ID。tar SHA 为 `cc2ca441a024155ac6b2ef7ac12170f3607a443ddb2658007a32246496ca87ab`，725 个 manifest 文件的 tar 内字节摘要全部正确；tracked patch SHA 也匹配。
- B harness 为固定 `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef`，runner 为 `244f59c9ed7dbde6fe4f75b7fb999953677aade7175638ee2386a06feee2fc98`。未改测试内容或 runner 门槛。
- candidate03 相对 candidate02 的 manifest 仅 semantic.rs、新 semantic_wal_chunks_tests.rs 和外部 `#[cfg(test)]` maintenance/integration_tests.rs 变化；独立静态检查见 [candidate-code-review.md](candidate-code-review.md)。当前 HEAD 或主工作区内容不能代替上述归档复现。
- B 的产品 lib/integration build JSON 均为 `fresh=false / opt_level=3 / debug_assertions=false`。A 沿用同一 `1198b18` 保存 binary；其先前源码归档核对与当时 lib fresh=true 的证据边界见 [01_verification.md](01_verification.md)。如实验结束后另补 A 强制重编同摘要证据，应单独归档，不能作为新的正式性能样本混入。

### 实验结束后的 A 强制重编补证

已独立核对 [baseline-rebuild/verification.json](evidence/baseline-rebuild/verification.json)、source manifest、baseline.tar 与 build.jsonl。baseline.tar 与本审查重新执行 `git archive 1198b18 prototypes/v1 rust-toolchain.toml` 的字节完全相等。最终 source manifest 为 **719 项：tar 的全部 718 个文件，加原冻结 harness 一项**；逐项核对 tar 全部文件均在 manifest 中且字节摘要相同，没有缺项，唯一新增项为同摘要 harness。build JSON 的 `peri_loom_v1` 和 `local_performance` 均为 `fresh=false / opt_level=3 / debug_assertions=false`，build-finished success。

最终对抗审查发现初版 manifest 只列 V1 范围的 718 项，漏列 tar 内的 `rust-toolchain.toml`；本报告此前“仅新增 harness”的描述因此不严谨，现已纠正。主线程从 tar 补齐 toolchain 项后，本审查重新核验完整集合与全部字节摘要，最终 manifest SHA 为 `331a0ab5908f3f5d46b9ff0478ce431476eaad4b1172c1efee5e47bad8ac06a6`，与 verification metadata 一致。这是来源证据补齐，没有重跑测试或新增计时样本。

直接读取保存的 rebuilt binary 重算 SHA，精确为 `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`，与三轮正式使用的 A 完全相同。这补齐了原 A 日志中 lib 复用所留下的来源疑问；该重编只作身份复核，没有产生或混入正式计时样本。lab 随后可恢复候选源码，因此复现应以已保存的 tar/manifest/binary 为准。

环境变量 source ID 本身不证明编译内容；这里核验的是源归档、构建记录、保存文件 hash 和实际运行记录的完整对应关系。本审查者没有为复验而重建。

## 针对性测试和执行范围

独立读取 candidate03 实际日志：walker **4**、boot **4**、recovery **14**、p4_authority **35**、branch **6**，合计 **63 passed / 0 failed**；63 已包含 branch 6 项。不能把实验 01/02 的其他快照测试数累计成 candidate03 重跑总数。

pre-run 快照明确没有匹配的 cargo/rustc/rustdoc/相关性能进程；command 与四样本顺序均为 A1→B1→B2→A2。采集前一分负载 1.989，各样本后为 2.750、2.855、3.220、3.356。边界 load 不能排除所有宿主噪声；两对没有提供尾延迟、稳定置信区间或并发公平性证据，但其完整测量均超过事前门槛。

## 解释与外推边界

总量仍是九 phase 请求 wall 之和，包含全部 history、真实持久提交、checkpoint 和综合 reopen，排除 fixture/预热、SQL 与模型构造、验证 SELECT/oracle、spawn/退出、drop/close 等。冷阶段使用新应用进程/空应用 cache，未清宿主 OS 或设备缓存，graceful close 后恢复也不是断电耐久性实验。

三项综合 reopen 在 A1/B1 合计 **14.319643 / 0.869025 s**，A2/B2 为 **15.171400 / 0.886651 s**；对应各 phase 的配对中位倍数为 **16.688327 / 17.868689 / 15.926501**。这是包含 Authority bootstrap、deep verify、floor/grant/session/native open 和部分首次 SELECT 的综合结果，不能把全部时间称作单次深校验函数耗时，也不能推断热读或历史提交提高相同倍数。

本轮与原 A 比较的是完整 candidate03 快照，不能单凭跨轮结果把全部改善归因于 Walker cache。源码/注册查询次数回归可以支持机制解释，单项因果收益仍需要相应对照。通过结论限定为既定的单线程、本地 LocalObjectStore 真实 SQL 请求组合；不包含 HTTP/Gateway、外部 S3、网络、并发公平性、p99 或完整 SQL/SDK 兼容矩阵。
