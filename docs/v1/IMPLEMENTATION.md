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
| P2 | 完成 | durable root authority、native commit history、refs/PITR、reachability GC | 2bf3c1e；独立 PASS；121 常规 + 3 RustFS live；设计见 P2-DESIGN.md |
| P3 | 完成 | multi-DB lifecycle、资源/idle eviction、migration/cold-open | bd2bfed；独立 PASS；165 常规 + 4 RustFS live；真实 SIGKILL/cache 删除与 lazy migration |
| P4 | 完成 | ownership state machine、durable epoch authority、coarse RPC | 5119187；独立 PASS；254 常规 + 5 RustFS live；原始冷恢复回归已修复并重新冻结验收；见 P4-VERIFICATION.md |
| P5 | 完成 | Go registry/placement、持久 epoch/lease、reconciliation/failover | b96d700；独立 PASS；62 顶层测试 + 1 helper skip；全包 race / vet / fmt / build 通过；8 个真实进程故障场景；见 P5-VERIFICATION.md |
| P6 | 完成 | Go gateway/management/security、原生页加密/KMS、SDK/admin、metrics/deployment | 独立 ACCEPTED；Rust 289 常规 + 5 真实 S3；平台 Go 80 PASS / 2 helper skip、Controller 63 PASS / 1 helper skip，各全包 normal/race；真实加密 S3/TLS、三 SDK、浏览器与 Linux 容器启动 / joined clean；见 P6-VERIFICATION.md |
| 联合 | 完成 | 跨阶段回归、真实 S3 冷恢复、API/SDK/UI/部署源证据复核 | 独立功能 PASS；本轮 native 325 PASS / 5 ignored；真实 SIGKILL、删除 owner cache、epoch 2→3、恢复已提交数据并拒绝旧 session；保留首次观察竞态失败及故障会话非 clean 退出；见 JOINT-VERIFICATION.md |
| 压测 / 调优 | 执行中 | 架构/fault harness、SQL workloads、measurement、性能修复 | 联合验收已通过；独立 RELEASE / S3 / TLS 全项目已启动，尚无性能结论；规划见 ../experiment-v1-stress/00_plan.md |

## 验收原则

- 不把空接口、mock 返回成功、不可运行部署文件当作功能完成。
- 区分本机集成证据、真实远程 backend 证据和未执行的环境依赖测试。
- Rust 保持 Pager/WAL/recovery/实际 ownership；Go 只持有 desired state 与全局策略。
- 每阶段记录命令、测试数、失败修复、限制和可复现步骤。
