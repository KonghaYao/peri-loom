# F01/F02：Authority 在线状态改造设计记录

状态：部分实现。泛用回执使用实际 schema v4（v3 在兼容测试中保留为拒绝版本）；完整目录、历史、ownership 命令回执和 WAL index 仍未迁移。此文记录最终迁移约束，不能当作 F01/F02 完成或性能验收结果。

## 当前路径及问题

`Authority::snapshot` 每次读取 `namespace/authority/root`，解码完整 `CatalogRoot`，再由 `validate_root` 遍历全部分支历史和对象依赖。`Authority::transact` 克隆完整 root，追加回执并再次完整校验、编码、CAS。`BranchRef.history`、`CatalogRoot.receipts`、ownership 命令回执、对象注册表、pins、uploads 和 tombstones 都在同一 root。每次 WAL append 还产生完整累计 index；`Walker` 校验每个历史点引用的 index 并保留原始字节。热路径成本因此依赖累计历史，而不是本次变更。

现有 `transact(FnMut(&mut CatalogRoot))` 把整个 root 结构暴露给多个调用者。它是 v3 持久化迁移的主要 seam：只把底层 JSON 拆成多个文件、继续对调用者交付完整可变 root，无法消除全量读取和克隆。

## 目标 interface

对 `BranchStore`、ownership、GC 和恢复能力保留现有行为；内部以按操作读取的 Authority 模块代替公开可变 root 的事务闭包。每次调用需明确本次读集、写集、预期 revision/epoch 与结果回执。模块负责持久化顺序、CAS 冲突重试、未知结果读回和依赖校验，调用者不需要知道对象页、回执页或 WAL index 页的布局。

在线 root 只保存固定大小的身份、修订号、fencing 状态与版本化目录指针。历史提交、对象身份、回执、pins 和 GC 状态进入不可变分段索引；在线索引每页有严格字节上限，通过内容摘要及 root 指针验证。一次提交只加载、验证和重写受影响的页及依赖路径。所有页先持久化，最后由被 fencing 约束的 root CAS 发布；CAS 失败的未引用页按未知上传意图规则处理，不能直接删除。

WAL index 也必须改为分段、持久树或等价有界结构：只迁移 root 而仍把每次完整累计 index 写入历史，会继续产生 F01 的二次读取。追加时验证上一受信根和新增记录、回执摘要、LSN 连续性及提交位置；冷启动或后台审计完整图。缓存只能保存已验证不可变页并有字节/条目上限；缓存命中不能跳过当前 root 和 ownership fencing 检查。

## 不可改变的语义

1. 已 ACK 的 root/WAL 发布必须能从持久状态读回；丢失 ACK 不得被当成未执行。回执可以归档，但必须有完成水位或证明重放边界，且在其之前保留可验证的请求身份与结果。
2. 旧 term/epoch 在任何 final publish 前都要由存储权威拒绝。普通 TTL、进程内锁或页面缓存不能替代最终 CAS/fencing。
3. 任一分段页缺失、摘要不符、版本不支持或依赖无法证实时 fail closed；不得初始化新 root 覆盖旧状态。
4. 活跃 reader、恢复点、未知上传意图及旧 handle 的 pins 必须保持可达。GC 只能基于同一已发布视图判定不可达，并按有界批次推进。
5. 从 v1/v2 迁移必须可中断重试：先写不可变页并核对摘要，再通过单次 root CAS 切换版本；崩溃后要么仍读旧 root，要么完整读新 root。旧版镜像兼容性和回滚策略需单独验证。

## 实施顺序与验证

1. 冻结旧版格式与故障语义的回归：丢失 root/WAL ACK、CAS 冲突、旧 epoch、损坏页、pin/GC、恢复点和跨版本 reopen。
2. 加入有上限的不可变页存储与校验，先承载历史和回执；单次查找需按页读取，不重新构造全量 root。泛用事务回执已经采用 128 条近期窗口与 16 KiB 分页索引，旧回执在 boot 时批量建树；仍需消除归档批次在前台事务中的延迟尖峰，并迁移 ownership 命令回执、对象注册表、pins、uploads、tombstones。
3. 引入分段 WAL index 与增量验证；清理热路径对 `CatalogRoot` 和完整 `WalMetadata` 的依赖。启动/后台完整审计应有可观察进度和资源预算。
4. 跑 T05 的 100/1k/10k/100k 递增历史、F02 的 30 天续租和 1/8/32 分支竞争。记录每请求 GET/PUT 数与字节、root 大小、CAS 重试、CPU、分配、峰值内存和 p95/p99。达到停止阈值时留存现场，不以未完成大规模负载声称通过。

目前 F01 未完成，F02 仅泛用回执局部完成；`implementation-status.md` 记录可验证的实际范围。Rust 全套本轮仍在 `runtime_resources::compacted_cold_open_is_lazy_and_old_immutable_recovery_remains_valid` 因默认 5 秒查询预算失败，不能称完整回归通过。
