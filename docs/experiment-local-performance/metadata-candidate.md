# WAL 元数据重复编码候选

日期：2026-10-17。负责范围：`prototypes/v1/src/wal.rs`，以及经主线程协调的 `authority/branch.rs` publication 入口、`authority/fenced_store.rs` 与 `authority/branch_tests.rs` 调用适配。本轮只做源码检查与编辑；未运行构建、测试或压测，执行由主线程串行安排。

## 路径证据与改动

`RemoteWal::append_using` 的合法新增路径先通过 `index` GET 完整 `wal/index`、反序列化并校验 entries 顺序、末 LSN、replay baseline、receipts 和 commit；随后创建 `WalRecord`，此前对同一不可变 record 连续执行两次 `serde_json::to_vec` 与 BLAKE3：一次产生 `entries[].hash`，一次产生 `receipt_digests[append_id]`。这两次输入、编码器和字段序列完全相同。`append_local` 的 `chunk` 与 Authority 的 `BranchStore::publish_wal_append` 还各自在对应分支再次编码和哈希 record。

已引入内部 `EncodedWalRecord`：私有构造器只接受 owned `WalRecord`，生成一次 canonical JSON 与 BLAKE3，record/bytes/hash 字段均私有，跨模块仅可只读借用。`append_using` 的 callback 仍为三个参数，第一个参数改为该编码结果；元数据、本地 chunk PUT 与 Authority publication 都复用同一编码结果。这样每个合法新增 attempt 的 record JSON 编码和 chunk identity 哈希由三次变为一次，确定减少两次完整 JSON 分配、编码和 BLAKE3 遍历；Authority 的 physical object checksum、安全校验及 metadata 编码仍保留。该节省随本次 batch 大小增长，与历史 entries/receipts 大小无直接关系。每个 CAS retry 仍根据该轮 LSN 重建 record 和摘要。

本次没有更改任何计数器。完整 index GET、JSON 解析/校验/重编码及全量 metadata 写入仍然发生；不能由局部工作量减少推断完整提交已有 1.5× 收益。

## 语义边界

- record 序列化类型与持久 JSON 字段没有变化；entry 与 receipt 绑定的完整 record 内容相同，epoch、LSN、timestamp、position 和 payload 都继续参与摘要。
- 回执命中仍走 `verified_receipt`，检查摘要、读取 chunk、验证原始 checksum、解析并重编码校验内容，最后重新检查 writer；没有减少任何验证。
- chunk 写入和 index CAS、fencing/session 检查、错误回读与 retry 条件没有变化；SQL 重试规则未变。
- 未引入缓存，因此不存在跨 session、namespace、etag 或 epoch 的缓存失效边界。
- Authority 仍先校验 record/metadata identity，再持有原 publication lock、持久化 group、head CAS、回读和 candidate 状态消费流程；没有改动 92ef3df 的已验证状态消费与故障失效逻辑。truncate 生成 baseline 时也经过同一私有构造器，仍只编码一次。

## 统一验证建议

1. 主线程串行运行 Rust 格式与构建门禁，并运行既有 `wal::receipt_tests`；其覆盖正常/legacy 回执、publication response lost、payload/position/epoch/timestamp 冲突、stale epoch 和 chunk 缺失。
2. 同一测试场景检查新增 record 对应的 `entries[].hash`、`receipt_digests` 及持久 chunk 的 canonical JSON BLAKE3 一致性；落盘格式应逐字节保持一致。
3. 在冻结 release 构建和相同后端上测完整提交 A/B，分别记录 payload 大小、index 历史长度、吞吐和延迟。若 batch 很小或持久化等待占主导，本候选可能没有可辨识的端到端收益。

## 丢 ACK 后接纳后来 WAL 的 pin 修复

独立复核发现 92ef3df 的既有安全缺口：publication 丢失 ACK 后可解析到后来者的 root，原实现从该 root 接纳 HEAD/WAL，但 `added` 只包含自己发布的 chunk/index。后来者选中点及其依赖未进入原 session 的 durable pin；mutable `get` 又直接命中 view，绕过补 pin。后来者 markerless WAL 不进 recovery history，其 session 释放且 branch 引用退出后，GC 可删除原 session 仍在使用的 index。

修复在原交易返回的 HEAD 不同于原 view，或 WAL 不同于本次 index 时执行一次额外 pin 交易。在交易 closure 的当前 root 中完成 branch incarnation 绑定、选取当前 HEAD/WAL、解析其依赖、扩充 durable pin；refresh 只使用该 closure 捕获的已 pin 选中点。即使补 pin 交易本身丢 ACK 后返回更晚 root，也不接纳未 pin 的新点。普通提交路径不增加交易。本次 publication 自己已 pin 的两个对象不会再次加入 durable pin，transaction retry 的补 pin 候选每轮重算。缓存保留条件改为最终已 pin 选中 WAL 等于本次 index，原状态消费与失败丢弃规则继续生效。

新增 `a_later_wal_adopted_after_a_lost_ack_is_pinned_through_gc`：真实 FaultStore 丢 ACK hook 发布无 commit marker 的 lsn3 并释放后来 session；断言原 session 已 pin lsn3 index、history 没有该 index；暂时移除 branch WAL 引用后运行真实 `GarbageCollector::collect(Duration::ZERO)`，检查原 session 仍能读取 index 和全部三条 WAL record；重新挂回该保留点，通过原 session 继续提交 lsn4 并冷验证 commit。既有 lost-ACK 后继续 append 回归仍保留。测试已写入，尚未运行。

## 静态检查与限制

已通过 `rg` 核对所有 `append_using` 和 `publish_wal_append` 调用点：生产路径为本地与 fenced Authority，测试路径为 branch append helper 与回执 publication-error closure，均已适配（后三参数占位 closure 无需改动）。相关文件的 `git diff --check` 通过。未对其他 agent 所有的 storage、IO 或 recovery 文件做编辑。

`WalAppendRequest::record` 仍克隆 batch；index 的全量解析/校验/重写仍随历史增长。这些都不是本补丁已解决的问题。编码复用由既有内容绑定与错误路径回归覆盖，pin 缺口另有上述真实 GC 回归，主线程统一安排执行。
