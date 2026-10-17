# 本地 SQL 冷启动只读审计

日期：2026-10-17。范围：主线程报告的真实 LocalObjectStore SQL smoke；只读源码分析，未修改冻结候选，未构建或压测。本报告中的时间由主线程提供，审计没有独立重测。

## 现象与结论边界

基线总量约 25.17 s，其中 `cold_wal_reopen` 5.10 s、`writer_reopen` 4.71 s、`cold_checkpoint_reopen` 5.06 s，三项合计 14.87 s。冷 WAL、checkpoint 后冷读、同进程重新打开 writer 都约 5 s，说明应优先细分其共同经过的 Authority 初始化路径；这还不足以证明某段源码贡献了这 5 s。

源码没有发现这些直接本地打开路径上的固定 5 s sleep。RPC connect/keepalive 等 5 s 配置不在该 fixture 路径中。`cold_child` 的计时覆盖 `open` 加首次 SELECT，`writer_reopen` 计时仅覆盖 `open`，而 writer 的 `RemoteDatabase::open_configured` 自己又会读 schema 并发布 open header，两个计时口径也应保留区别。

## 实际调用链

`tests/local_performance.rs::open` 直接创建 LocalObjectStore 和新 OwnershipAuthority，调用 `state`，验证旧 lease 被撤销，再 challenge/grant/open_session，最后 RemoteDatabase::open。这里没有 RPC server 外层的 request-read scope。

首次 `state` 经 namespace cell 初始化：

1. `namespace_init.rs::initialize` 读 root，进入 `Authority::from_loaded`。
2. `authority.rs:215` 对载入 root 无条件执行完整 `verify_deep`，随后记住该 root 的完整字节 digest。
3. `migration.rs::boot_ownership` 调 `snapshot`、增加 term、撤销旧 lease、扫描 WAL epoch floor，持久发布 boot root。
4. 初始化返回 Service 后，`ownership.rs::Service::state` 再调 `authority.snapshot`。
5. challenge、grant、open_session 继续检查绑定；branch_store 会收集 HEAD/WAL 依赖并写入 session durable pin。
6. RemoteIO::open 打开 manifest，读 WAL metadata，然后完整 `wal.for_each_record(namespace, 0, ...)` 重建 WalImage；最后原生 SQL 引擎打开和首次读。

## 最直接的重复工作：boot 后再次 deep verify

`from_loaded` 已 deep verify 并 remember 原 root R。boot 开始的 `snapshot` 可以据此走已承认 root 路径；但 `migration.rs:99` 的 boot group 成功分支返回新 root R+1 时，没有为它调用 `remember`。接下来的 `Service::state` 首次 `snapshot(R+1)` 与 admitted 表中的 R digest 不匹配，因此 `authority.rs:274` 再次完整 `verify_deep`。

这是完整本地初始化中可静态证明的重复全量校验。它会发生于全新子进程，也会发生于本 fixture 的同进程 writer reopen：`from_loaded` 本身无条件 deep verify，boot 又产生新 digest。尚未测量两次扫描分别占多少时间。

候选切口：在 boot 的自有 publication 已充分验证且确定持久成功后，按普通 `transact_inner` 的思路登记该自有 candidate 的 admission digest。必须证明 boot 候选只做已经受验证的 ownership/schema/receipt/registry 表示变更，所有继承引用仍来自此前完整验证的 root，且 digest 必须来自实际编码的精确字节；不能仅凭 envelope 自洽或 etag 将外来 root 标记可信。CAS 失败、异常和 Unknown 路径继续 fail closed，legacy migration 必须单独覆盖。此建议还未实施。

## deep verify 的历史前缀与分页查询成本

`semantic.rs::verify_deep` 枚举 blobs、tombstones、uploads、objects，然后扫描 current branch refs、保留的 recovery history、snapshots 及所有 session pins。每个 `registered` 调用都执行 `root.blobs.get` 与 tombstone membership。

`Walker::wal_chunks` 对每个不同的 WAL index 遍历全部 entries；最近 32 个 history index 虽然共享几乎全部 chunk 前缀，仍各自进行 logical lookup、digest 字段比对和 physical registration 校验。Walker 的 128 项 `verified` 只记录 manifest/HEAD/WAL metadata identity，没有记录已验证的 chunk registration identity。因此单分支长历史约是 current+32 个 index 各扫一次当前规模的 prefix，两个 deep verify 都会支付。相同 index identity 在 current/history 重复出现会被去重，不能说每一条 history 都必然重复读取。

超过 inline budget 后，`Registry::get` 会构造 PagedIndex，顺着 page tree 查找；`PagedIndex::read` 每次校验 page digest 并解析 Page。即便 page bytes 在 request cache 命中，解析与查找仍发生。LocalObjectStore GET 还包括 store lock、文件读取、JSON envelope/base64 解析和 checksum 校验，因此大量 metadata 查询不是单纯内存 BTreeMap 查询。

候选切口：在单次 deep verify 的固定 root 内共享有界的 registration/lookup 验证结果，或利用同次已经完整枚举的 registry 作为只读查询表，消除同一物理实例/逻辑 key 的反复 page traversal。必须保留每个历史 index 的独立结构、hash 对应关系与 root 归属检查；不能按 chunk hash 全局跳过不同 root 的注册校验。全 registry 物化已有数量级内存代价，不能无预算复制多份大表。

## 本地初始化没有外层 metadata read scope

`request_reads::Scope::enter` 目前用于普通 Authority transaction 和 SQL lane，RPC handler 也有自己的 scope；直接 `OwnershipAuthority::state` 初始化、`from_loaded` 和 `boot_ownership` 本身不创建 scope。因此本 fixture 的两个 deep verify 和 boot floor 查询没有该层 metadata/page byte 复用。

候选切口：为 namespace 初始化或 deep verify 建立有界、同线程、单次操作 scope，保留每次 root GET 与 fencing/guard 检查。现有上限仅 256 KiB/128 项，体积较大的 WAL index 可能被拒绝，cache 满后也不逐出旧项；即使增加 scope，也不能假定覆盖完整深扫。RPC 请求本来已有外层 scope，故此候选对真实远程 API 的收益可能不同。

## boot epoch floor 的重复完整 index 解码

`migration.rs:39` 对每个 branch 的 current WAL、history WAL、全部 snapshot WAL 逐一执行 `read_blob` 和 `semantic::index`，只为取 epoch 最大值。current 与最后一条 history 可指向同一 index；snapshots 循环还位于每个 branch 内。`read_blob` 绕过 metadata request cache，`semantic::index` 又先解析 JSON Value、校验未知字段，再 `from_value` 为 WalMetadata 并验证整个 index。

候选切口：同一次 boot 按完整 ObjectEntry identity 对 floor 解析去重，或在第一次 deep verify 时以明确返回值带出已验证 WAL epoch 的有界汇总。必须保持所有当前/历史/snapshot WAL 对 epoch floor 的原有贡献，不能只信 ownership 中已有 epoch 或只读 current WAL；异常 index 仍须拒绝 boot。不能为了省解析改成仅抽取 `epoch` 而省掉结构/摘要校验。

## WAL replay 与 checkpoint 的反例

RemoteIO::open 当前总是从 lsn0 replay；checkpoint 后的 WAL 仍有 replay baseline，因此具体 chunk 数量和 bytes 需要另记。这个阶段可能有真实成本，但 checkpoint 后重开仍约 5 s，只能提示共同初始化扫描值得量化，不能证明 replay 免费。每个 WAL chunk 的 BranchStore GET 也经过 binding/root 校验与注册解析；prefetch 线程的 thread-local read cache 不继承主线程 scope。

## 下一轮最小测量与安全回归

冻结候选的正式配对完成后，再单独细分 `state` 初始化、challenge、grant、open_session、RemoteDatabase/RemoteIO 打开及首次 SELECT。初始化进一步分 `from_loaded/verify_deep`、boot snapshot、floor 扫描、boot durable group、首次 state snapshot。每阶段记录实际 registry/index 数、page GET 数、page bytes、WAL records/bytes 与物理读取耗时，区分两次 deep verify，避免把整个 reopen 时间归到某一次扫描。

优先验证第一个候选是否消除一次 deep verify；仍保留冷启动缺失/坏 page、未注册历史引用、损坏 index、旧 lease 撤销、epoch floor、boot CAS 竞争和 boot Unknown 回归。request scope 和固定 root 内去重可以随后独立 A/B，避免与 boot admission 修改混测。

从主线程这轮时间作纯算术，若其余阶段完全不变，要把 25.17 s 降到 1.5× 所需的 16.78 s，需要三项 reopen 合计至少减少 8.39 s（约其 56.4%）。这是目标分解，不是以上候选已能达到的收益预测。
