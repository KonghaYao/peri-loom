# 同次 Walker 的 WAL 历史前缀去重候选

日期：2026-10-17。独立 candidate03，只修改 `authority/semantic.rs`，新增 `authority/semantic_wal_chunks_tests.rs`。没有修改 candidate02 的 migration 文件，没有叠加 request scope、跨请求缓存或协议变更。尚未构建、运行测试或压测，主线程统一调度。

## 重复工作与实现

同一次 deep verify 或 GC mark 的 Walker 借用一个固定不可变 CatalogRoot。不同历史 WAL index 通常包含相同 chunk 前缀，原 `wal_chunks` 对每个 index 的每条 entry 都再次 logical registry lookup、比对 entry hash、physical registration 与 tombstone 检查；分页 registry 下重复查询还要读、校验和解析相同 page tree。

本次新增独立的 4096 项 FIFO positive cache，key 是已经完整验证成功的 64 字符 chunk logical hash。cache miss 仍依次执行 valid_hash、`wal/chunks/{hash}` lookup、entry.hash 比对、`direct`（注册身份与 tombstone 检查、GC reachable 收集）；全部成功后才缓存。命中复用同一 root 的相同 logical mapping 与注册验证结果。失败不缓存，超容量逐出最早项，再次访问仍执行完整校验。

cache 只属于该 Walker，新 Walker、新 root、新请求都为空。它与原 128 项 metadata 验证表独立；缓存上限不会随历史 entry 数增长。两份 key 容器的纯 hash 文本上限约 512 KiB，另有 String/树/队列节点开销，不能视为总内存上限正好 512 KiB。

WAL metadata 的浅检查 `wal_index` 仍使用原 `w` 标记；完整 chunk sweep 改用独立 `W` 标记，避免同一 Walker 先做浅 metadata 校验后误认为 chunks 已完成深扫。每个不同 index 的 JSON 结构、entries 顺序和 digest 字段仍独立解析/校验；本候选只省共享 chunk 的重复 registry 解析与查询，不复用整个历史 index 的解析。

## 安全依据

固定 root 的 logical registry pointer 和 inline/changes 内容不在 Walker 存活期修改。缓存项在成功查到 logical mapping、确认 logical digest 对应、通过该 root 的 physical registration 和墓碑检查后才加入，因此后续相同 logical hash 指向同一已验证注册身份。底层 immutable pages 仍以内容 hash 校验；没有把一次 root 的注册结果带到另一个 root。

GC 模式下第一次 `direct` 已将 physical blob 加入该 Walker 的 reachable 集合；cache hit 不必再次插入同一集合。FIFO 淘汰只移除验证复用项，不移除 reachable 项，所以以后重新验证或 metadata 验证表淘汰都不会丢失已发现的可达对象。缺失、未注册、tombstoned、错误 logical hash 与非法 hash 仍拒绝。

## 四项回归

测试 fixture 使用真实 MemoryObjectStore 和已分页的 objects/blobs registry，计数 objects registry 的实际 root-page GET，而非新增优化计数器；chunk 和 index 物理 body 也实际写入 fixture store。

1. 两个 recovery history index 分别含前两个与前三个 chunks，同一 Walker 依次扫描，只发生三次 logical registry root-page GET。同时校验 Walker reachable 与真实 `Authority::reachable` 均包含两个 index、三个 chunk 的完整集合。
2. 原 root 成功校验 chunk 后，在分别缺失 logical key、未注册 physical blob、存在 tombstone、错误 logical hash 的新 root 创建新 Walker；每项连续两次都必须失败且不错误增加 reachable。非法 hash 也连续拒绝。
3. 合法浅 `wal_index` 后再对同一 index 做完整 sweep，缺失 chunk 仍必须拒绝，覆盖浅/深 metadata 标记隔离。
4. 扫描 4096 项后复读首项不再 GET；插入第 4097 项并复读已逐出的首项会新增两次真实 GET；验证缓存保持 4096 项，reachable 保留全部 4097 项。

相关文件已 rustfmt，`git diff --check` 通过；未执行上述测试，不能提前声称通过。已通知 candidate_review 独立静态复核。

## 性能口径与限制

这项按同次 Walker 触及的不同 chunk 数减少共享前缀重复查询；若 history 很短、没有共享、或工作集超过 FIFO 容量，收益会更小。仍保留 full registry 枚举、每个 index JSON 解析、每次 metadata 直接注册校验、boot floor 解析、WAL replay、root GET 与持久化等待。

没有时间收益或 1.5× 结论。正式比较需单独冻结 candidate03，对同条件 SQL workload 的总量与各 reopen 阶段配对；candidate02 的 admission 收益不能归到本项。真实服务 RPC 的外层 read cache 与本地 fixture 不同，也需要分开解释。
