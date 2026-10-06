# v1 实施与验收记录

## 工作方式

每阶段拆成有独立写范围的开发任务，由多个 subagent 实现；主控集成后，由独立
subagent 按该阶段文档验收。修复后复验，通过才提交。P0–P6 完成后执行联合大验收。
提交已获持续授权；不自动推送。原有平台不作为新 segmented 数据面的权威状态。

全部阶段及联合验收通过后，启动整套可用项目，执行架构优先压测、常见数据库
场景压测和实测驱动的性能修复。压测工具与修复继续分模块多 subagent 开发；
同机负载实验串行执行，每轮有独立 verifier，最终另做对抗验证。

## 阶段状态

| 阶段 | 状态 | 开发拆分 | 独立验收 / 提交 |
| --- | --- | --- | --- |
| P0 | 完成 | manifest/storage、fork IO、SQL/CLI | 36 测试；Turso 1a548d14a；父仓库 eef60fb |
| P1 | 完成 | ObjectStore/S3、remote segments/cache、DurableWal/SQL | b8912a5；独立 PASS；63 顶层测试 + 2 RustFS live；缓存锁 100 轮复验 |
| P2 | 完成 | durable root authority、native commit history、refs/PITR、reachability GC | 独立 PASS；121 常规 + 3 RustFS live；设计见 P2-DESIGN.md |
| P3 | 待实施 | multi-DB lifecycle、资源/idle eviction、migration/cold-open | 待验收 |
| P4 | 待实施 | ownership state machine、durable epoch authority、coarse RPC | 待验收 |
| P5 | 待实施 | Go registry/placement、持久 epoch/lease、reconciliation/failover | 待验收 |
| P6 | 待实施 | Go gateway/management/security、SDK/admin、metrics/deployment | 待验收 |
| 联合 | 待实施 | 跨阶段场景、fault/stale writer、API/部署端到端 | 待验收 |
| 压测 / 调优 | 待联合验收后执行 | 架构/fault harness、SQL workloads、measurement、性能修复 | 规划见 ../experiment-v1-stress/00_plan.md |

## 验收原则

- 不把空接口、mock 返回成功、不可运行部署文件当作功能完成。
- 区分本机集成证据、真实远程 backend 证据和未执行的环境依赖测试。
- Rust 保持 Pager/WAL/recovery/实际 ownership；Go 只持有 desired state 与全局策略。
- 每阶段记录命令、测试数、失败修复、限制和可复现步骤。
