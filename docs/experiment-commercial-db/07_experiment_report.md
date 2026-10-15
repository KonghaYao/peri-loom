# 第 7 轮：独占 S3 后端有限正确性验证（2026-10-14 至 2026-10-15）

## 先决条件与方法

在专属 Docker Compose project 下启动本地 RustFS `rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`，仅绑定回环 S3 端口，限制 0.5 CPU/512 MiB。客户端镜像 digest 固定，建桶成功。每项执行前核对容器 Compose project label 与运行状态；测试使用 `PERI_S3_TEST=1`、`PERI_S3_TEST_RETAIN=1`、私有测试凭据及已有隔离 Rust debug 构建目录，**单项串行运行** `cargo test --locked --test minio <exact-name> -- --ignored --exact --test-threads=1`。测试程序每项用新随机桶，不触碰已有共享 RustFS 或用户数据库。五个测试桶均按保全策略保留；原始日志与标识在 `trial-ground/private/`，不得自动删桶、删卷或重试 Unknown。

## 实际数据：正确性用例（不是吞吐 benchmark）

| 用例 | 本轮结果 | 私有原始输出 |
| --- | --- | --- |
| `minio_network_object_store_conformance` | 1/1 PASS | `trial-ground/private/s3-conformance-2026-10-14.log` |
| `p2_s3_authority_refs_pitr_and_gc_with_empty_worker_cache` | 1/1 PASS | `trial-ground/private/s3-p2-recovery-2026-10-14.log` |
| `p3::network_worker_multi_db_unload_and_fresh_cache_recovery` | 1/1 PASS | `trial-ground/private/s3-p3-multidb-2026-10-15.log` |
| `p4::network_authority_fences_two_remote_workers_and_preserves_restored_snapshot` | 1/1 PASS | `trial-ground/private/s3-p4-fencing-2026-10-15.log` |
| `minio_network_fresh_cache_sql_recovery` | 1/1 PASS | `trial-ground/private/s3-fresh-cache-2026-10-15.log` |

合计 **5 次单独运行、5 次 PASS、0 次 FAIL**；这是本机 RustFS 的有限 S3/SQL 正确性覆盖，不是生产 S3 或完整平台网关的演练。全部 5 份日志含本项唯一的保留桶标识。随后只读桶清单私有记录 `trial-ground/private/s3-retention-audit-2026-10-15.jsonl` 在 **2026-10-15 00:09 CST** 观察到 **6 个桶**：5 个测试桶与 1 个独立试炼场桶；通过与日志标识去重比对，5 个测试桶当时均可列出，且无意外桶。这证明检查时的可见性，不证明每个对象完整或永久保留。

同一轮只读资源快照 `trial-ground/private/s3-posttest-resource-2026-10-15.txt` 记录于 **2026-10-14 16:09:40 UTC**：独立 RustFS 容器约 **184.9 MiB / 512 MiB 上限**、**0.05% CPU**。这是五项测试结束后的**单点**状态，**不是**测试期间峰值、平均值或压测资源指标。测试程序报告的秒数是整项测试耗时，不是查询或事务时延。

## 缺口与停止界限

Rust 全套日志中另外 4 个 `p4_completion` 真 S3/故障相关 ignored 用例尚未执行；这些用例当前复用会在失败时 `kill` **本用例子进程**且自动删除临时状态目录的夹具，未给出满足首败/dirty marker 保全的失败契约，故不在本试炼场擅自启用。全部已运行测试使用 debug 二进制/库，未冻结与当前脏工作树的一致性，未启动独占 Authority + 两 Worker + Controller + Gateway + Key Broker 的 release TLS 全栈。当前 `trialctl.py launch` 仍无 Main 准备声明且拒绝脏源码。S01/S04 还未在新网关/新 session 独立核对；不得用本轮有限通过签发 P0–P6 联合 PASS。

2026-10-15 后续改进（不回写上段当时事实）：`p4_completion` 真实 S3 夹具现要求提供私有 0700 证据目录，进入网络后端初始化前就固定该次 0700 临时目录并保留；本用例的子进程 stderr 会分别保留为 0600 文件。默认本地测试新一轮 `trial-ground/private/p4-completion-retention-full-2026-10-15.log` 报 15 passed、0 failed、4 ignored（其中新增 2 条保全防护测试）；**这不代表四条实际 S3 测试通过**。独立静态审阅指出代码本身尚不核实目标 endpoint/桶归属，也不创建/删除桶；四条 ignored 仍**未运行**。若未来运行，须先独立核对独占 RustFS 当前项目标签及端口，并给每项提供预建唯一桶；故障后绝不自动清桶或覆盖原始结果。新增保全措施不等同于完整故障安全/持久性 oracle 或联合门禁。

本轮文档冻结后才实施的四项独立 P4 S3 预检及首次执行器权限失败详见 `08_experiment_report.md`；以上“仍未运行”指当时的历史状态，不是最新状态。

**ACK/s、p50/p95/p99/max、Unknown、错误率、租户公平性、恢复时间、S3 请求放大、数据库 CPU/RSS 峰值：NOT MEASURED。** 不得将本轮 5/5 正确性用例升级成 SpaceX 认可或商用数据库性能证明。首先完成未测的安全故障契约与全栈独立 oracle，再在预冻结 SLO/预算后开始真实 S06/S08 阶梯。
