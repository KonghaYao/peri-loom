# F03：operation 持久区与平台注册表

状态：独立持久区和恢复路径已实现；追加日志、group commit 和 T03 实测仍未完成。

生产 `stateStore` 保留原有 `StateStore` 接口。`updateOperationRecords` 调用私有 `updateOperations`，仅复制有上限的 operation records、验证既有不变量并同步提交 `operations/state.json`。提交成功后才更新内存视图并允许 SQL 派发或确认。提交不明时 poison 状态存储并返回 `ErrStateUncertain`。测试用 StateStore 保持原有 `Update` 适配。

注册表 `state.json` 持有 `operation_storage_version` 和单调 revision。版本 0 启动时，在服务接收请求前建立 sidecar，将注册表升级为版本 1，再以新注册表 revision 做 sidecar checkpoint。版本 1 要求 sidecar 已存在且通过独立 checksum、revision floor 和私有路径校验；丢失、回退或校验失败均拒绝启动。启动时合并两份状态并完整验证，随后恢复未完成的 operation。`Snapshot` 在同一锁内返回合成视图。

目录更新先将当前合成视图提交到更高 revision 的注册表，再将 sidecar 的 `base_revision` 推进到该 revision。若两次提交间崩溃，重启时较新的注册表包含完整 operation 视图，安全前滚 sidecar；若 sidecar 的 base revision 与注册表相同，则 sidecar 包含之后的 operation-only 提交，作为最新 operation 视图。注册表更新后的 sidecar 提交失败会 poison 服务。单个持久区内部的中断写入仍由其 pending intent 恢复。这个顺序允许数据库删除、operator decision 等目录与 operation 同时变化的更新安全恢复，无须额外的跨文件 intent。

operation-only 提交不写 `state.json`，因此单次编码与写盘量不再随组织、项目、密钥和数据库注册表扩张。但 sidecar 当前仍是每次完整写入有界 operation 集合，登记、结算和审计各自同步；追加日志、索引、组提交屏障及 T03 负载测量是 F03 的剩余工作。

验收必须持续覆盖：登记持久化先于 SQL dispatch、结算 ACK、重启隔离、Unknown、去重、256 条已知终态保留、4096 条未决上限、密钥撤销即时可见、sidecar 缺失 fail closed，以及注册表与 sidecar revision 交错恢复。任何组提交都必须让每个请求等待覆盖自己的持久化屏障。
