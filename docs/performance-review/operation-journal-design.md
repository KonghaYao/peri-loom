# F03：operation 持久状态与平台注册表拆分设计

状态：实施设计，尚未实现。现有 `StateStore` 接口供管理、Gateway、启动恢复共用；外部调用方式应保持不变。热路径目前通过 `updateOperationRecords` 调用 `State.Update`，两次 SQL 操作状态变化都完整克隆、校验并持久化 `state.json`。单纯缓存或延后写回会破坏“登记持久化后才派发 SQL”和未知结果隔离。

## 模块与接口

外部模块仍为 `StateStore`。生产 `stateStore` 实现内部增加独立的 `operationState` 持久区，包含 operation records、registration sequence、evictions 和自身 revision。仅在 `updateOperationRecords` 内使用私有 `updateOperations(change)` seam；测试用 StateStore 继续走原有 `Update` 适配。调用方不需要了解落盘位置或重放过程。

`updateOperations` 持有状态锁，复制最多 `operationCapacity + operationKnownRetention` 条 records，执行既有 `change` 与 `compactKnownOperations`，调用 `validateOperations(candidate, previous)`，然后提交 operation 持久区；只有提交明确成功后才更新内存视图、释放锁并允许调用方派发或确认 SQL。目录、项目、API key、数据库与加密 key descriptor 不参与这次编码或写盘。提交结果不明时 poison 该存储并返回 `ErrStateUncertain`，不得重放 SQL。

`Snapshot` 在同一状态锁下合成 catalog 与 operation 视图。已有管理与恢复代码仍读合成的 State。`State.Update` 若只改 catalog，提交 catalog；若同时改 catalog 和 operation records（数据库删除、operator decision、资源退役等），必须作为联合事务处理，不能分别成功返回。后续可将调用点改为更窄的方法，但接口改变应集中在此模块。

## 磁盘迁移与崩溃一致性

1. 在旧 `state.json` 中新增持久 `operation_storage_version` 标记。旧版本无标记时，先从已验证的旧 State 构造 operation 持久区；在其完整落盘并校验后，以联合事务把 catalog 标为版本 1 并移除内嵌 operations。服务在迁移完成前不能开始接收 SQL。
2. 版本 1 启动时，operation 持久区缺失、回退、校验失败或与 catalog 不一致都必须拒绝启动；不得从空目录重建。持久区有独立 revision、checksum 和反回滚 floor。迁移中断后重启只能完成或安全拒绝，不能回退到旧内嵌 records 接受重复 request ID。
3. 跨 catalog 与 operation 的更改先写入并同步一个包含两份 after image 与预期 revisions 的联合 intent，再分别提交两个持久区，最后删除 intent。启动在暴露 State 前先恢复未完成联合 intent，并复核两个 floor。单个持久区自己的 pending 恢复必须与联合恢复顺序明确，不能先把一份新状态与另一份旧状态作为有效合成状态提供给调用方。
4. operation-only 提交不写 `state.json`；`state.json` 的字节与 checksum 在连续 SQL 请求之间应保持不变。租户目录扩张不应增加单次 operation 提交的编码和写入字节。状态提交和 audit 仍各自同步；减少 Sync 次数需要后续有覆盖每个请求的 group commit 屏障，不能以异步确认代替。

## 不变量与验收

- 登记 ACK 必须先于 SQL dispatch 的持久化屏障；结算 ACK 必须覆盖对应结果。`in_flight` 在重启后变成隔离，Unknown 和待清理 open 记录不得丢失。
- 同 request ID 的去重、最多 256 条已知终态保留、4096 条未决上限、registration sequence 和 evictions 的单调性保持；已驱逐记录不存在不代表可重试。
- API key 撤销后的首个请求仍拒绝；operation 文件故障不能让 catalog 状态被误判为可用，也不能清除已确认的撤销。
- 在每个联合事务写入阶段故障注入并重启；验证旧格式迁移、注册后崩溃、执行后未结算、结算后崩溃、删除库和资源退役。缺失或回滚任一持久区应 fail closed。
- 固定 SQL 与热库数，扩大非热点 catalog 到 100/1k/10k 条；实测 operation 提交编码量、逻辑写字节、alloc/op、Sync/s、状态锁和审计锁等待。只有热 SQL 不再重写完整 catalog，且上述安全门禁通过，才可将 F03 的写侧标为完成。

当前实现中 `openDurable` 严格拒绝未知目录项，新增持久区与联合 intent 需同步更新其白名单及私有路径校验。`State.Update` 和 `validateState` 仍以单份 State 为中心；实施时必须先把上述崩溃测试固定，逐步迁移，不能只在 Gateway 层绕过它们。
