# 第 10 轮本机测试报告：提交修复后补充覆盖（2026-10-15）

## 判定与版本

**本机有限正确性复测 PASS；正式商用压力/联合门禁 NOT PASS。** 生产路径和测试修复已提交为 `5ae6dc1`，历史实验/场景资料 `7cf2f86`；全套离线测试在源码未改动时执行，随后提交本轮预注册 `02664de`。本轮真正 S3 用例运行时根仓库 HEAD 为 `02664de`，工作树干净；`trial-ground` 是被根仓库忽略的独立仓库，其私有证据没有提交。离线原始日志及摘要在 `trial-ground/private/local-offline-tests-20261015/`（0700/0600）。这些提交并不等于产物供应链证明或 Main 联合门禁签署。

## 已完成的本机验证

| 类型 | 结果 | 限制 |
| --- | --- | --- |
| Rust v1 全套 `cargo test --locked -- --test-threads=2`，离线构建 | 47 条测试二进制摘要均成功；合计 531 次 `passed` 计数，13 次 `ignored` 计数 | 包括不同层级汇总，**不是 531 个独立测试**；忽略的网络用例不在其中。 |
| 平台 Go 全包 `go test -p 2 ./...`，禁网络依赖 | PASS，主包 151.208 s | 是测试耗时，不是网关 SQL 时延。 |
| Controller Go 全包 | PASS，主包 31.540 s | 不是独占多 Worker 真实服务验收。 |
| Stress + faultprobe Go 全包、Stress `go test -race -p 2 ./...` | PASS（race 主包 2.560 s） | **只跑工具的单测，没有运行 stress workload。** |
| Go 竞态补测 | 平台 race 全包 PASS（主包 346.023 s）；faultprobe race 全包 PASS；Controller race **首次 FAIL**，修复测试等待后定向 20 次 PASS、全包 PASS（主包 37.016 s） | 首败保留；修复的是 SIGKILL 测试时序保护，**不代表**发现或排除了生产竞态。 |
| Stress Python、计划/证据脚本、试炼场 | Stress Python 73/73；实验文档 Python 12/12；试炼场 Python 14/14；provision 离线自检 19/19 + 凭据自检 1/1 | 均不是生产性能指标。 |
| 已提交源码上的独立真 S3 冷缓存 SQL | `minio_network_fresh_cache_sql_recovery` **1/1** 成功，测试墙钟 **1.41 s**；无包装器首次失败 | 单项 ignored 用例显式执行，非联合门禁；时间不是操作延迟。 |

## 冷缓存用例原始证据与失败

事前范围/停止条件见 `10_plan.md`；唯一运行目录为 `trial-ground/private/local-s3-smoke/run-*/`，内有 0600 意图、原始 `test.log`、S3 只读前后桶清单、对象清单和 `runner-result.json`。用例在自己的随机桶建表、checkpoint、提交两行后删除本地缓存，新缓存读回 count=2；再次 checkpoint、删除本地缓存、新缓存读回 count=2，`PRAGMA integrity_check` 为 `ok`。S3 客户端可重试 2 次，无法给出每请求的实际重试数；没有额外 SQL 层重试或并发。

读前 **11** 桶、读后 **12** 桶，仅新增本次桶，旧桶未消失；该桶在只读清单有 **12** 条对象条目。检查前后自有容器的 Compose project/image、回环端口、卷、凭据、512 MiB/0.5 CPU 配额匹配；这些是采样身份而非全程证明。只读 `mc ls` 不能证明对象字节、独立持久 oracle 或运行中资源峰值。本轮测试/包装器未见失败；**第 9 轮首次包装器退出 1 的原始证据仍保留**，不得因本轮绿而改写。

## 竞态测试首败与修复

在本轮追加的 Controller `go test -race -p 2 ./...` 中，`TestStoreRecoveryActualSIGKILLReleasesLockAndPreservesAllocatedFloor/intent` **首次 FAIL**：子进程 `syscall.Kill(self, SIGKILL)` 返回后，马上执行 `test.Fatal("SIGKILL did not terminate writer")`，父进程看到退出码 1 而不是 SIGKILL。随后未修改源码的定向重复 5 次为 PASS，说明故障具有时序性，不能用绿覆盖首败。修订 `prototypes/v1/controller/store_recovery_test.go`：子进程发信号后等待其实际终止；父进程为自有 helper 设置 10 秒截止，超时明确失败，仍严格要求终止信号为 SIGKILL。修订后定向 race 连续 **20/20** 次 PASS，Controller race 全包 PASS。首败、前后定向及全包日志存入 `trial-ground/private/local-offline-tests-20261015/`，`race-addendum.json` 记录哈希。**这是测试夹具的误报/时序修复，不是数据库生产 bug 或断电恢复证明。**

## 下一阶段

这台机器还可继续运行**有界、隔离、预注册**的单项正确性/小样本实验；`challenger` 的 S01/S03/S04 等全栈场景和正式 stress harness 仍要求同一冻结版本的 Main 联合 P0–P6 PASS、独立完整服务夹具及安全门禁，不可因为本机获得 1/1 就直接开启。当前没有业务 SLO、TLS Gateway 下 ACK/s/p95/p99、租户间公平或持续性资源序列；专用机器留给之后的可比容量、稳定性与商业验收。上述正式指标均 **NOT MEASURED**。
