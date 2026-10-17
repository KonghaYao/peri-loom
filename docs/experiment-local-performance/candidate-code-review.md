# 本地性能候选独立代码正确性审查

日期：2026-10-17。基线：`1198b18`；审查对象为本次工作区的 `storage.rs` / `storage_cow_tests.rs`、`EncodedWalRecord` 及其 BranchStore / FencedStore 调用，以及已提交 `92ef3df` 的 accepted WAL 所有权移动与丢 ACK 处理。已读 `CLAUDE.md`。这是静态代码审查；按任务约束没有构建、执行测试或 benchmark，没有审查性能 harness 的计时，不构成 1.5× 性能验收。

## 原 P1：丢 ACK 采用更晚 HEAD/WAL，却没有为采用的视图持久化 pin（修复已静态复核）

状态：实现 agent 已在工作区修复，独立复核未发现剩余具体安全缺陷；新增真实 GC 回归尚待主线程统一执行。以下保留原问题的触发条件和证据，修复评估见本节末尾。

位置：`prototypes/v1/src/authority/branch.rs`，审查时第 414–431 行，`publish_wal_append` 的 `extend_pin` 与成功后的 `refresh`。

`transact_with_bodies` 可能在本次 publication 已落盘但 ACK 丢失时，通过 durable operation receipt 确认成功并返回**更晚**的 root（`authority.rs` 的 `transact_inner` 错误分支）。本次 mutation 加入 session pin 的只有本次 chunk 和本次 index。成功后的 `Self::selected(&published.root, ...)` 却采用更晚的 HEAD/WAL；这些较新引用及其依赖不在本 session 的 pin 中。注释所说“本次新增 pin 也在 later head 中”成立，但不能推出 later head 的引用也被本 session pin。

`BranchStore::get("wal/index")` / `get("HEAD")` 命中 `view.wal/head` 时直接读取 entry，不走缺失 immutable key 的 `accessible → extend_pin` 路径。其他 session 销毁、较新对象退出 branch/history/snapshot 后，GC 能删除本 session 仍保存的视图。影响是已保留的 adapter/native handle 读到缺失物理对象，违反既有快照与 GC 保留契约；同类问题也适用于更晚 HEAD 的 manifest/segment 依赖。

### 最小可触发重现

复用 `authority/branch_tests.rs` 的 `FaultStore`、`fixture`、`append` 与现有 lost-ACK 用例：

1. session A append 一个有效的 4-frame committed WAL（LSN 1）。
2. A append 第 5 个 committed frame（LSN 2）；令 `root_fault = 1`，使本次 root 持久化后返回错误。
3. 在 `after_root` hook 内创建 session B，append `WalBatch::default()`、`position=None`、新 append ID（LSN 3），随后 drop B。此 markerless index 不进入 recovery history。
4. A 的 ACK 回读确认其 LSN 2 已提交，采用 LSN 3 的 `view.wal`。此时可直接断言：`view.wal.blob` **不在** `root.pins[A.pin]`，无需等待 GC 即可定位不变量破坏。
5. 通过 authority transaction 将 `main.wal = None`，保持 A、其 pin 和 LSN 1/2 history；运行 `GarbageCollector::new(authority, Duration::ZERO).collect()`。
6. A 再次 `get(namespace.key("wal/index"))`：应仍能读其 LSN 3 视图，当前实现会读取已回收的物理 index 而失败。

这里使用 markerless append 避免等待 history 窗口淘汰；history 仍保留 A 已提交的 WAL，因此无需篡改历史或删除 pin。

### 现有覆盖与缺口

- `a_lost_acknowledgement_that_resolves_to_a_later_wal_does_not_keep_the_image` 覆盖丢 ACK 后采用较新 WAL、丢弃 accepted image 及继续 append，但后续 append 发生在 GC 前，会掩盖漏 pin。
- `tests/authority.rs::old_pinned_view_survives_writes_until_final_adapter_arc_drop` 覆盖通常发布的旧视图存活，未覆盖 lost-ACK 返回更晚 root。

### 最小安全修复方向

采用新视图前，必须在 authority transaction 的同一个候选 root 内选择同 incarnation 的 HEAD/WAL，并对 `referenced(authority, root, head, wal)` 返回的**这对引用及依赖**执行 `extend_pin`。只在 pin publication 确认后 refresh 到闭包捕获的这一已 pin 的 point。该补 pin transaction 自身发生 lost ACK、返回更晚 root 时，也必须使用闭包所选择且已 pin 的 point，不能再直接采用返回 root 的更新引用。

仅恢复旧的“采用本次 publication 的 candidate selected”虽可保住 pin，但下一次 append 仍从旧 session WAL 生成 expected ETag，遇到后来者 WAL 后会返回冲突，不能满足现有 lost-ACK 后继续 append 用例。不要为规避问题 pin 全部 `root.objects`；应遵循现有 HEAD/WAL 快照依赖边界。

问题已立即发给主线程，并按主线程要求把复现与修复方向直接发给负责 `branch.rs` 的实现 agent；本审查者未改实现文件。

### 修复的独立复核

已复核工作区 `publish_wal_append` 的补 pin 事务及新增 `a_later_wal_adopted_after_a_lost_ack_is_pinned_through_gc`。结论：原 P1 已在代码上修复，未发现新的具体安全漏洞；没有执行构建或测试。

- **选中视图与 pin 一致：** 普通成功路径仅在返回 HEAD 与旧 view 相同、WAL 正好为本次 index 时直接 refresh。此时旧 view 的 HEAD 及依赖已被 pin，本次 chunk/index 由首次交易 pin。其他情况在第二次 transaction 的同一候选 root 内执行 `bound → selected → referenced → extend_pin`，再使用闭包捕获的 `pinned_selected` refresh。只选择 HEAD/WAL 的依赖，没有枚举并 pin 全部对象目录。
- **第二次交易也丢 ACK：** 该交易通过 operation receipt 确认后，即使返回 root 已由第三个 publication 推进，调用方也忽略该返回 root 的 HEAD/WAL。采用的是此交易闭包选中并持久 pin 的 point。因此新增第三个 point 不会递归造成漏 pin。若交易没有被确认为成功，`?` 使 refresh 不执行，旧 view 仍然有旧 durable pin。
- **第二次交易发生 CAS retry：** 每次闭包重新检查 incarnation/pin，重新选择 root 引用、读取依赖，并覆盖 `pinned_entries` 和 `pinned_selected`；没有累计未成功候选的选择。`transact_inner` 只有本次 candidate 成功或其 operation receipt 已被确认才返回成功，因而最后捕获的 point 与落盘 pin 对应。闭包、prepare 或最终交易失败时不会 refresh 到失败候选。
- **首次新增 pin 与补 pin 去重：** `publication_blobs` 只排除首次已成功 publication 的 chunk/index，虽然它们尚未加入 in-memory view，但 durable pin 已经存在。其余依赖仍经 `referenced` 校验注册身份并 pin。`added.extend(pinned_entries)` 将全部新 immutable 依赖加入 view；HEAD/WAL 单独由 captured selected 更新。
- **accepted image 仍绑定准确身份：** 只有最终 captured selected 的 WAL 等于本次 index 才回填本次 validated image；若补 pin 交易选中更晚 WAL，原 image 丢弃，下一次追加冷重放，不会把原 image 冒充后来者 WAL。

新增回归使用 markerless LSN 3，断言它在 A 的 session pin 中且不在 recovery history，移除 branch WAL 引用并真正执行零窗口 GC，随后读取 LSN 3 metadata 和三个 records，再恢复引用并通过原 session 追加到 LSN 4、用新 session cold validate commit。静态检查未发现 fixture 或断言使原漏洞被其他持有者掩盖。该回归本身没有注入补 pin 交易的第二次丢 ACK / CAS retry；这些分支以上述代码路径推理为证据，不能表述为动态覆盖已通过。

## 其余候选的审查结论与证据范围

未发现以下改动新引入的具体可触发正确性缺陷；此结论限于静态代码路径检查，不代表测试已执行。

- **dirty Arc / COW：** `resolve_segment` 只克隆 Arc；partial write 通过 `Arc::make_mut`，full write 替换 Arc，truncate 对局部 Arc COW 后插回。旧 dirty 快照和已发布 cache Arc 不会被原地修改。write 先在持 storage lock 时完成全部可失败的 partial base load，再修改任何 dirty 段；现有跨段 dirty 前缀失败测试与此结构相符。size/revision 更新仍在整次修改完成后，read 的 revision 重试保留。
- **单次 publish 重复段去重：** 每个 hash 的首次操作仍执行 `put_immutable`，重复 hash 必须完整 bytes 相同；表仅为本次栈变量，任何错误/返回都会销毁，后续 publish 重新检查 store。跳过相同逻辑 key 的第二次 GET 不跳过首次 pin/注册，manifest 每个段索引仍写入，最终 HEAD fenced CAS 保留。没有跨发布的对象存在性缓存。
- **EncodedWalRecord：** 私有不可变 record、bytes、hash 由一次原有 `serde_json::to_vec(WalRecord)` 得到；entry hash、receipt digest、chunk key/body 共用同一结果。无 serde schema、字段顺序或落盘格式改动；retry 每次根据新 LSN 构造新 encoded record，receipt 回读仍校验原请求内容与 chunk。
- **accepted WAL 消费：** `view.validated_wal.take()` 后只有确认匹配本次 index 才缓存结果；validation error、事务错误和 CAS retry 不把已原地扩展的 image 放回。retry 第 2 次起 cold replay；匹配旧 index 的 accepted state 才允许增量验证。prefix rewrite/truncate 仍沿 `WalFrameScan` 失效规则，`apply_batch` 仍为字节修改的唯一实现。除上述 pin 问题外未发现恢复镜像污染、重复应用候选或持久格式不兼容。
- **fencing / 错误类别：** 编码复用没有移走 FencedStore 的 role/namespace/epoch 检查、service gate、binding 检查或 BranchStore candidate publication 的 ownership/epoch 校验；`ChunkUploadFailure` 包装位置与解包语义不变。首次 immutable 上传错误回读、root receipt 确认及 WAL receipt 内容确认路径仍保留。

已阅读的相关现有覆盖包括：`storage_cow_tests.rs` 的 5 个快照/缓存/发布失败测试，`remote_storage.rs` 的跨段失败与 truncate 失败，`authority/branch_tests.rs` 的成功缓存、head 失败、CAS race、伪造 commit 和 lost ACK，用于 receipt 内容绑定与旧 chunk 缺失的 `wal.rs::receipt_tests`。没有把这些用例的存在表述为本轮执行通过。

## 后续候选 02：boot admission 最小修复的独立静态复核

本节针对实验 01 冻结之后的 `authority/migration.rs` 与新增 `authority/boot_admission_tests.rs`，不属于实验 01 的源码或 77 项测试结果。未执行构建、测试或性能测量。

结论：**未发现具体新增正确性漏洞。** 修改只给自己成功发布的 boot candidate 登记已有 admission 机制，不改变外来 root 的验证规则，也不新增跨 root 的依赖校验缓存。

1. **继承合法性：** 每次 boot 仍先 `self.snapshot()`；已承认 root 执行现有 head/reference 校验，外来 root 执行 `verify_deep`。candidate clone 该 root，修改 ownership term/revocation/epoch floor、schema version 和 receipt，branch refs/history/snapshot/pin/blob 身份继承不变。原有 receipt、head 校验、registry publish/seal 与持久 group 均保留；legacy registry 的表示变更由同一 Registry::publish 完成，不是跳过依赖验证。
2. **精确身份：** `encode_with_digest` 同次产生将要写入 group 的 bytes 和 head checksum；`remember` 使用该 checksum 和 candidate incarnation，没有用旧 ETag、重新编码的近似内容或自己声明的 metadata 作为证明。
3. **成功与失败出口：** 只有 `put_group` 返回 Ok 且取得 root 的最终 ETag 后直接 remember candidate。写入前失败、Unknown before/after persistence、空 ETag 返回都不经过该出口。CAS 失败后原有 `self.snapshot()` 仍会独立校验实际 observed root；若其通过 deep verify 后被 remember，这是现有合法 admission，不能误写为“CAS 路径绝不 admit 任何 root”。未被观察并验证的失败 candidate 不会仅因尝试过写入就进入 admission。
4. **CAS 与并发推进：** 每次重试从新的 snapshot clone 并重新编码。先前失败 candidate 的 digest 不被登记；实际成功 head 在 ACK 后被并发较新 head 覆盖时，remember 旧 digest 至多让下一次 snapshot miss 后重做 deep，不会使较新 head 获得错误 admission。
5. **外来新 root：** `snapshot` 的 digest 比对与 `from_loaded` 的无条件 deep verify 均未修改。boot 自有 candidate 的 admit 不会为更新 revision、篡改 history 或另一个 checksum 自动授信。

新增四个回归静态检查：真实 `OwnershipAuthority::state` 的 history-only WAL index GET 计数区分一次 deep 与一次 epoch-floor read，并核对 floor=7；外来合法新 root 要重读 history，外来坏 history 要拒绝且不 admit；CAS fault 在 retry 前断言所有尝试 candidate 未 admit，成功后断言最终身份；Unknown 在持久化前/后两种情况下都断言 candidate 不 admit、旧 root 记录保留。未发现 fixture 使这些断言失效的具体问题。这些测试与源码结构不能预先给出任何耗时改善倍数，后续仍须用新冻结版本执行完整配对。

## 后续候选 03：同 root 的有界 WAL chunk 校验复用

独立静态复核 `authority/semantic.rs` 与新增 `authority/semantic_wal_chunks_tests.rs`，并核对冻结来源和已执行日志。**未发现具体新增正确性缺陷，可进入独立性能测量。** 本审查者没有构建、运行测试或 benchmark；以下测试通过数来自读取主线程保存的实际日志，不是静态推断。

### 身份与范围

candidate03 source ID 为 `e4620e51fa8e987a644aaab867cf4fe19099d662d4e0de2c7fe56b066ea3e3c2`，独立重算 source-manifest 精确字节 SHA 相同。tar SHA 为 `cc2ca441a024155ac6b2ef7ac12170f3607a443ddb2658007a32246496ca87ab`，725 文件逐项与 manifest 一致；本次阅读的 semantic 实现和新增测试也与 tar 完全一致。实际保存 binary SHA 为 `f03bc6bd2903dd665ae259fc271421815625335a99cc5d6d966dc73f95967e8c`，与 binary metadata 相同；lib 和 integration target 的构建记录均为 `fresh=false / opt_level=3 / debug_assertions=false`。harness 仍是原冻结 `99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef`。

独立比较 candidate02→03 manifest 只有三项变化：`authority/semantic.rs`、新增 `authority/semantic_wal_chunks_tests.rs`、外部推进的 `maintenance/integration_tests.rs`。最后一项由 `maintenance.rs` 的 `#[cfg(test)] mod integration_tests` 引入，属于维护回归测试扩展，不改变 integration benchmark 链接的产品 lib。没有只凭已推进的 HEAD 判断实验范围。

### 安全不变量

- **同一 root 的映射身份：** Walker 持有固定的 `&CatalogRoot`，验证期间没有切换或修改该 root。完整 logical key `wal/chunks/{hash}` 在这个 root 中映射到同一 ObjectEntry，故按完整 hash 字符串记录成功检查可复用该映射、blobs 注册和 tombstone 结果。缓存仅属于一个 Walker；新 root、新 GC walk 或新验证创建新空缓存，不共享到其他 candidate/epoch/session。
- **先检查，后缓存：** miss 顺序仍为 `valid_hash → logical lookup → entry.hash == hash → direct/registered`；registered 比较完整物理 ObjectEntry 身份并拒绝 tombstone。只有全成功才插入正向缓存，missing、错误 digest、未注册或 tombstoned entry 都不能污染缓存。该优化没有缓存错误或跳过首次校验。
- **GC 首次覆盖：** `direct` 在 `collect_reachable=true` 时先把物理 blob 加入 reachable，随后才缓存。hit 跳过重复 direct 不会漏标，因为同一 Walker 的 reachable 只增不减；collect_reachable 也不会在此 Walker 中切换。chunk FIFO 淘汰不会删除 reachable，重验仅重复集合插入。metadata 的全扫成功标记同样在所有子依赖遍历完成后才记录。
- **有界 FIFO：** BTreeSet 仅 miss 才插入，FIFO 也仅首次当前 miss 入队；hit 不产生重复队列项。超过 4096 时同时从两容器删除最老项，任意时点缓存成功条目最多 4096；淘汰后必须重新 lookup/direct。只缓存完整 64 字符 digest，不缓存任意长非法输入。即使 history 超过此工作集导致命中率降低，校验语义不变。
- **浅扫与深扫隔离：** 原 `wal_index` 保持 metadata identity kind `'w'`；完整 `wal_chunks` 改用独立 `'W'`，因此一次只解析 index 结构的浅扫不能伪装为完成 chunk 注册校验。深扫失败不会记录 `'W'`。完整 sweep 仍独立校验每个不同 index 的 JSON/layout，只有共享 chunk 依赖的逻辑映射与注册检查复用。
- **格式与 fencing：** 未改 WAL/index/authority 的持久格式、receipt、root admission、fencing gate、CAS 或 ownership 校验。缓存属于依赖图校验，不代替 recovery 对 WAL record/commit 字节的重放校验。

### 测试证据与限制

四个新测试静态构造与实际日志一致：真实 paged objects registry root-page GET 计数证明两个 history index 的共享前缀只解析一次，同时 `Authority::reachable` 集合包含两个 index 和三个 chunk；新 root 的 missing/unregistered/tombstoned/wrong-hash 以及非法 digest 重复失败；先浅 index 校验再深扫仍发现缺 chunk；4097 个 chunk 触发 FIFO 淘汰，重验产生实际 page GET 而 reachable 保持完整。

fixture 的 chunk bytes 是用于注册关系的任意 immutable body，不是可重放的真实 WalRecord；这符合 Walker 本身只校验依赖注册关系的职责，不能把四项测试表述为 SQL/WAL record 重放正确性。配套实际日志为 walker 4、boot 4、recovery 14、p4_authority 35、branch 6，**合计 63 passed / 0 failed，包含 branch 6 项**。这些检查支持该切口进入完整固定工作量比较，不预先证明有任何性能倍数收益。
