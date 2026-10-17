# 实验 02 独立验证

日期：2026-10-17。独立读取 [02_experiment_report.md](02_experiment_report.md)、[完整 results](evidence/experiment-02/results.json)、四份实际 stdout/stderr、candidate02 源码归档/manifest/build/binary 与正确性日志。验证者未执行构建、测试或 benchmark。

## 评估

**PASS：报告的“本轮仍未达到完整 1.5×”结论可信；性能验收为 NOT_YET_1_5X。** 没有发现阻碍继续原工作量实验的具体证据问题。

| 维度 | 结果 | 证据 |
| --- | --- | --- |
| 数据完整性 | PASS | 四样本完整，`finished_unix` 已写，原始 stderr JSON 逐项等于 results；stdout 各 1 passed / 0 failed |
| 方法正确性 | PASS，限固定本地工作量 | 串行 AB/BA、同 release harness、原 A 基线、独立对象存储夹具；完整历史/checkpoint/冷重开都在固定计时集合中 |
| 结论合理性 | PASS | 第二对和中位数不足 1.5，不能只采用第一对；三项 reopen 改善只按综合 phase 描述 |
| 报告质量 | PASS | 四样本全部呈现，来源和 52 项实际正确性日志明确，未把实验 01 的 77 项重复计入本轮 |

只有两对且未隔离宿主 OS/设备缓存，仍不能据此提供稳定收益置信区间或泛化至其他部署。

## 独立重算

读取四份原始 stderr 中的唯一 `LOCAL_PERF`，均与 results 完全一致；每条九 phase 的正整数 `wall_ns` 之和精确等于 `total_request_ns`。对保存样本重新运行 runner 的**纯汇总函数**，完整 summary 与 results 相等，未启动测试二进制。

| 配对 | A 总量 ms | B 总量 ms | A/B |
| --- | ---: | ---: | ---: |
| 1 | 25,067.746 | 16,624.084 | 1.5079173985684569 |
| 2 | 23,666.171 | 16,851.354 | 1.4044076884773442 |
| 配对倍数中位数 | | | **1.4561625435229004** |

实际顺序 A1→B1→B2→A2。phase 中位数全部大于 `1/1.10`，无预设超过 10% 回退，但第二对和总中位数未达到 1.5，runner 状态及退出码 2 均正确。

四样本所有非计时工作量/oracle 字段完全相等，固定 512×512 B、256 query/2,176 返回行、32+512+32 单行提交、8×32 行事务与 8 checkpoint、两次独立冷只读进程及中间 writer reopen。WAL 和最终完整内容摘要分别为 `42cb049af25fcda5a22e56a63754c17414dffbc128ebde0fa90ba605d60b4f59`、`aa4afe61f2e066e8da7a9bb7d712e72dc89d1bfc24c4e377292445a7061ba549`，均与实验 01 固定模型一致；epochs 与旧 binding 拒绝标志没有改变。

## 源码和二进制

独立重算实际文件 SHA-256：

- A binary 为 `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`，确为实验 01 同一保存文件；其来源核对及 A lib 构建复用边界见 [01_verification.md](01_verification.md)。
- B binary 为 `e068ce8467c8c4de20382d437c8544775df59d3ef659fd8797cdcac2af7c1da1`，与 candidate02 binary metadata、command、正式 results 一致。
- source-manifest 精确字节 SHA 为 `8e2e66a92cfc980e8ca367a852d84b7cad77c4ceabc0a061d0f495a0d9dc5e45`，等于 B source ID。tar SHA 为 `230fc3cd335e3f224f576151d31c9e7212db645bb87435cfe96a5b1e25a3c8f7`；tar 中全部 724 个 manifest 文件逐文件摘要正确。
- tracked patch 摘要正确，归档 candidate01→candidate02 manifest 只改变 `authority/migration.rs` 并新增 `authority/boot_admission_tests.rs`，没有混入 candidate03。
- frozen harness 仍为 `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef`；candidate02 build JSON 的产品 lib 与 `local_performance` 均 `fresh=false / opt_level=3 / debug_assertions=false`。

源码标签本身依然由环境变量传入，不等于二进制内的代码证明；本轮归档、重新编译记录、保存 binary hash 与执行结果在来源上相互一致。没有为验证而重建。

## 正确性与调度

独立读取日志 summary：boot 4、namespace 5、ownership 8、p4_authority 35，合计 **52 passed / 0 failed**，无零匹配虚计。新 boot 回归覆盖已审查的真实 state 复用、epoch floor、外来 root 深校验、CAS 未确认候选、Unknown 落盘前后。candidate02 静态安全分析见 [candidate-code-review.md](candidate-code-review.md)。这些不是完整 SQL/SDK/平台门禁，也不与前一快照的 77 项合并声称本轮重跑。

command metadata 的二进制身份、source ID、runner hash 与原始采集相符，记录 `--rounds 2`、退出码 2。preflight 首次遇到 cargo 后未开始计时，pre-run 保存快照明确无匹配 cargo/rustc/rustdoc/性能测试进程。这是边界快照证据，不是持续全机资源监控；报告未据此排除一切宿主负载噪声。

## 计时和归因边界

沿用实验 01 同一 harness，计时仍是固定请求 wall 之和，包括真实 SQL 物化/持久提交、全部 history、事务与 checkpoint、综合 reopen；fixture/预热、SQL 与模型构造、验证用 SELECT/oracle、进程 spawn/退出和 drop/close 等仍不计入。工作量或门槛均未缩减。

三个 reopen phase 的配对中位倍数 **1.995074 / 1.985333 / 1.963844** 与报告一致。这些 phase 同时覆盖 bootstrap、deep verify、floor/grant/session/native open，以及冷只读阶段的首次 SELECT；没有函数分段计时，不能把全部节省称为一次 `verify_deep` 的时间。源码上只叠加 boot admission 可以支持进一步研究该机制，但相对原 A 的本轮倍数仍衡量完整候选，不是 candidate02 相对 candidate01 的严格单项 A/B。

下一轮可继续同一完整 workload 与原门槛。首对 1.508 不能替代第二对和中位数，不能跨轮挑最快基线，也不能依据这三项接近 2× 推断总体已经达标。
