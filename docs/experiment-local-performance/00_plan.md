# 本地真实读写性能验收规划

日期：2026-10-17。当前目标：在不削弱耐久性、fencing、恢复或 SQL 行为的前提下，让固定本地真实 SQL 工作量至少快 1.5×。本文制定门槛，不报告已经获得的收益。

## 冻结与责任

- 唯一 A 基线为 `1198b18611601f7417f7b9ae4bbb3c370b41ff03`，已包含先前缓冲、parser 和 File pread 修复；不得改用更早版本放大收益。
- B 是最终候选的完整源码快照，包含现有未提交 WAL 修改时须归档 patch 和 hash。Turso 固定 `24a3a4609683b73935888f8bc30b47727d100aa8`；版本和 lab 见 [identity.json](identity.json)。
- 两臂使用字节相同的 [local_performance.rs](../../prototypes/v1/tests/local_performance.rs)，相同 Cargo.lock、release profile、机器和临时文件系统。分别归档源码、harness、可执行文件 SHA-256 和构建命令。
- 主线程统一调度构建与计时，所有性能实验串行，禁止和其他 cargo/压测重叠。现有服务不停止、不读取凭据。

## 固定工作量 local-sql-v1

测试走 `LocalObjectStore → OwnershipAuthority → fenced branch store → RemoteDatabase → Turso` 的实际公开接口，使用生产对象序列化、校验和、锁、CAS 与耐久发布。无 MemoryObjectStore、禁用 Sync、test-only 优化开关或手写 WAL 微操作。

| 阶段 | 固定工作量 | 计时与 oracle |
| --- | --- | --- |
| 初始夹具 | CREATE TABLE；单事务插入 512 行，每行 512 B payload | 单独 fixture_ns，不进入请求总量；完整有序逐行 oracle |
| 热读 | 32 预热后 256 次真实 SELECT，点查和 16 行范围查交替，共 2,176 行 | 全 query/materialization wall；逐行核对 id、version、全部 payload |
| 小提交 | 32 次独立单行 UPDATE，每次有效改变内容 | 每次真实提交完成；阶段后完整 oracle |
| 历史增长 | 512 次独立单行 UPDATE，持续保留真实操作及 WAL 历史 | **计入请求总量**，不可把增长的成本藏在预载中；完整 oracle |
| 长历史提交 | 上述历史之后再 32 次独立 UPDATE | 和早期提交分别报告；完整 oracle |
| WAL 后冷重开 | drop 全部原 SQL 句柄，新进程、新 LocalObjectStore/Authority、空 cache，只读打开 | 打开到完整 SELECT 物化；逐行比对与父进程 digest 相同 |
| Writer 恢复 | 冷子进程退出后父进程以新 Authority、空 cache、更新 epoch 的 writer lease 继续 | 独立 writer_reopen 计时，计入总量 |
| 检查点更新 | 8 个事务，每事务更新 32 行不同内容 | 事务提交单独计时；总计 256 次实际行更新 |
| 检查点 | 每个上述事务后立即实际 native Full checkpoint，共 8 次 | 单独计时，成功返回；全部完成后完整 oracle |
| 检查点后冷重开 | 再次新进程、空 cache 读取完整 512 行 | 同上，最终完整 oracle/digest 相同 |

SQL 字符串构造、预热和 oracle 比对不进入对应请求计时；完整历史增长、checkpoint 前事务与两次冷重开都进入 `total_request_ns`。此外保留 `fixture_ns`，不得把该请求总量说成从建库到清理的整个进程 wall。schema、行数、payload 字节、更新数量、每阶段请求数、返回行数和完整内容 digest 全部输出，任何失败都不能生成成功 summary。

冷重开是应用进程/cache 冷态，宿主 OS page cache 和设备缓存未清除。每次新 Authority 均先通过 `state` 正常加载/深校验持久 namespace 并 boot 新 term，验证旧 binding 被拒绝，再以持久 epoch floor+1 重新 challenge/grant；冷子进程获取新只读 lease，父进程继续 checkpoint 时另获取新 writer lease，绝不跨进程复用失效 lease。初始 grant 和 SQL open 使用同一新 control；全流程共有 4 次 Authority boot / 4 次 grant，最终 epoch=4。冷计时包含其真实 boot/grant 耐久成本。固定时钟只隔离 lease 到期的计时噪声；仍走实际 grant/binding/fencing 校验。该测试覆盖本地 Authority 和 SQL 数据路径，不包含 HTTP/Gateway、认证、审计队列、外部 S3 RTT、网络或并发公平性。

首次 baseline 冒烟因 harness 把 grant 的 control 丢弃后直接用新 control 打开旧 binding，返回 `Fenced unknown namespace`；完整日志保留在 `evidence/baseline-smoke.*`，该失败不是性能样本。上述 bootstrap/重新授权流程修复 harness 后，两臂必须使用同一更新源码重新冻结/编译，旧二进制不参与比较。重启行为依据现有 `restart_term_revokes_old_binding_but_keeps_epoch_floor` 回归契约，未修改产品实现或放宽验证。

## 成功门槛与两轮独立验证

1. 相同 release harness 在 A/B 各建立独立真实夹具，至少两对测试。第 1 轮 A→B，第 2 轮 B→A；后续轮次交替顺序，保留全部成功及失败日志。不得挑轮次、换简单场景、减少内容或只报告最好一条。
2. 所有非计时字段和所有 oracle 必须两臂完全一致；两次独立冷重开均须成功。至少两轮结果都有 `A.total_request_ns / B.total_request_ns ≥ 1.5`，配对倍数中位数同样 ≥1.5；任何固定阶段的配对中位数不得回退超过 10%。未达门槛继续定位、修改、重跑两轮，不降低门槛。
3. 单个 WAL scan/append 的数十倍、减少复制/读对象计数、debug 测试速度都只可用于机制定位；不能代替此测试的 1.5×。若仅某阶段达到，结论必须仅限该阶段且整体目标仍未完成。
4. 第 1 轮实验后由未执行该轮的 agent 审核身份、源码差异、原始输出、工作量和 oracle；第 2 轮同样独立审核。最终另由独立审查挑战 fixture 偏差、顺序/主机负载、耐久性、故障与恢复兼容，输出 `adversarial_review.md`。
5. 原有关键 correctness 门禁必须通过，尤其历史提交、WAL 非法追加、错误后的状态、CAS 失败不污染已提交状态，以及本次 IO/存储变更触及的 checkpoint/恢复测试；性能 PASS 不能覆盖 correctness 失败。

报告按技能工作流输出 `01_experiment_report.md`、`01_verification.md`、`02_experiment_report.md`、`02_verification.md`、`conclusion.md`、`adversarial_review.md`，注明当前提交与未提交补丁范围。

## 可复用运行方式

先由主线程把同一 harness 拷入冻结 lab，顺序构建两臂，并在替换源码/重建前把每个 integration 可执行文件保存为独立路径：

```sh
cargo test --release --locked --manifest-path <lab>/prototypes/v1/Cargo.toml --test local_performance --no-run
python3 docs/experiment-local-performance/paired_runner.py \
  --baseline-bin <saved-A-test-binary> --candidate-bin <saved-B-test-binary> \
  --baseline-source 1198b18611601f7417f7b9ae4bbb3c370b41ff03 \
  --candidate-source <frozen-source-and-patch-sha256> \
  --rounds 2 --output <new-evidence-directory>
```

[paired_runner.py](paired_runner.py) 不构建、停止或清理服务，只串行执行已保存二进制；输出每臂 stdout/stderr、机器/负载信息、二进制/harness hash、每阶段原始 ns 与配对汇总。输出目录必须不存在，防止覆盖旧数据；退出 0 为完整门槛 PASS，2 为未达到 1.5×或有阶段回退，其他错误立即停止并保留已有日志。来源标签由执行者提供，独立验证必须核对构建归档，不能把标签当作二进制来源的自动证明。
