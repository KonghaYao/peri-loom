# Storage 实质优化候选

日期：2026-10-17。此文件记录实现与验证范围；release 时间与真实 SQL 验收以主实验报告为准。

## 真实路径与改动

`RemoteStorage.write_at` 的非整段写原先调用 `load → resolve_segment`，dirty 段先 clone 一份 128 KiB `Vec`，随后 `as_ref().clone()` 再 clone 一份；读 dirty 段也在 `resolve_segment` clone 整个段。native checkpoint 已合并连续页，因此不能假定每个 SQL 页都支付这些成本；重复 dirty 部分写与 dirty 读确实经过此路径。

1. dirty 段改为 `Arc<Vec<u8>>`。读取保留不可变 Arc；部分写通过 `Arc::make_mut` 仅在缓存或正在读取的快照共享 buffer 时复制，后续独占 dirty buffer 的部分写修改对应字节。写入先完成所有可能失败的基段加载，再修改 dirty 状态，保留跨段失败的原子性；整段覆盖直接复制输入，避免先清零整段。truncate 保留读快照，尾部清零规则相同。
2. 单次 `publish` 以 hash 与完整 bytes 记录已经成功执行 `put_immutable` 的对象。相同 hash 的段必须完整 bytes 相等，才省去重复 immutable GET/PUT 检查。首次 GET 的完整比较、Absent PUT 的冲突回读、manifest 校验、manifest immutable 发布和最后 fenced HEAD CAS 都保留。此表不跨 publish，不在失败重试间保存。

只改 `src/storage.rs`，新增 `src/storage_cow_tests.rs` 与诊断 `tests/storage_local_bench.rs`；未改持久格式、IO 协议、Authority、WAL、recovery 或 SegmentCache 实现。

## Pins 与 fencing 边界

`authority/branch.rs` 的 `get` 首次发现对象时调用 `extend_pin`，将对象放入 `view.objects`，并将 blob 放入 `view.pinned`；后续同一 key 的 GET 直接复用 entry，再读取相同 blob。`put` 在事务中同样调用 `accessible → extend_pin`，`refresh` 保留此前的 objects/pinned。`extend_pin` 本身按 blob 去重。因此单次 publish 中跳过相同 key 的第二次 GET，不省略新对象所需的首次 pin 或注册；每个 manifest 段索引仍被写入。最终 HEAD CAS 与 BranchStore 的 bound/epoch 校验仍是发布 authority。

独立 MemoryObjectStore 测试能证明调用计数与失败语义，不能单独证明真实 Authority 的 pin 生命周期；应结合现有 Authority/branch/GC 契约测试。此优化不做全局对象存在性缓存，否则可能跨 pin 生命周期误判。

## 针对性正确性验证

新增 5 个单元测试：

- dirty 读快照跨部分写与整段覆盖保持原内容；
- dirty 读快照跨 truncate+extend 保持原内容，新读取尾部为零；
- 部分写不污染已经发布的 RAM cache 内容；
- 8 个相同段在单次 publish 只 GET/PUT 一次，下次 publish 再 GET，远端损坏必须拒绝；
- 首次 GET 与最终 HEAD 发布失败均保留 dirty image/原 LSN，重试重新 GET 并安全完成。

现有 `tests/remote_storage.rs` 覆盖跨段写失败且 dirty 前缀不变、truncate 失败不改变状态、dirty/cache 隔离、HEAD 冲突、immutable 上传失败、并发读取与 revision 重试。

统一调度执行命令：

```sh
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --lib storage::cow_tests
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --test remote_storage
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --release --test storage_local_bench -- --ignored --nocapture
```

本子任务按主线程安排未运行 cargo 构建、测试或 benchmark。已完成对所改 Rust 文件的 rustfmt，`git diff --check` 通过。实际测试结果须由主线程统一执行并填入主报告。

## 性能结论限制与风险

ignored benchmark 是真实 `RemoteStorage.write_at/read_at` API，100,000 次 4 KiB dirty 页操作、128 KiB 段、MemoryObjectStore，无人为 sleep。最后实际 publish 并核对 LSN/dirty，但持久发布不在诊断计时内。它只能定位 CPU/复制成本，不能充当持久提交或公开 SQL 1.5× 验收；SQL 仍需同条件完整请求 A/B。

publish 去重只利于同一轮多个段最终内容相同的负载。不同内容的段仍保留原 GET/PUT，并额外支付小型 map 登记成本；不会降低真实不同新内容稀疏写的 128 KiB 格式放大。相同新内容原本是 8 GET、1 PUT，现在是 1 GET、1 PUT；已有相同内容从 8 GET、0 PUT 降为 1 GET、0 PUT。这些计数须以测试执行结果确认，不宣称真实 SQL 有该去重率。

仍有读快照或 published cache 引用时会触发一次 COW；快照生命周期内的旧 buffer 计量不在 dirty 当前字节数中，这与原先读时的临时 Vec copy 一样存在瞬时额外内存。原存储锁仍覆盖写与 publish；未引入新的并发发布或调整 revision/read retry 边界。
