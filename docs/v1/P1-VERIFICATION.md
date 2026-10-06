# P1 Diskless — 独立验收

## 最终裁定

**PASS — P1 Diskless 独立验收通过，限定于本报告列明的已测 backend 与单 writer 原型边界。**

冻结源码后的最终验收时间：2026-10-07 00:46:03 CST。全量 **63 passed / 0 failed / 2 ignored**；另有真实 **RustFS S3-compatible 2 passed / 0 failed**，合计 **65 项不同测试通过**。真实服务测试使用既有 RustFS `127.0.0.1:9000`，不是 MinIO；MinIO / AWS S3 / R2 未获真实服务验收。

fmt、strict clippy 及测试前后源码快照检查全部通过。曾发现的 cache lock blocker 有根因修复、确定性对照及独立复验，不是忽略失败或只重跑到绿。此签发不包含 P2，也不代表 P4 完整 fencing 已实现。

验收日期：2026-10-07（Asia/Shanghai）。需求：`docs/v1/02-p1-diskless.md`。
工作目录：`prototypes/v1`。验证工具链：rustc / cargo 1.98.1。

本报告基于独立源码审阅、实际执行测试及验证者新增反例，不以开发 agent 的自述代替结果。
验证者只写 `prototypes/v1/tests/p1_verify.rs` 与本报告，没有修改开发源码，没有 commit、push 或派发 agent。

## 验证方法与当前结果

在 `prototypes/v1` 执行：

```sh
cargo test --locked
cargo test --locked --test p1_verify
cargo fmt --manifest-path Cargo.toml -- --check
cargo clippy --locked --all-targets --no-deps -- -D warnings
```

- 冻结后的最终全量：63 passed，0 failed，2 ignored（按顶层测试计，不重复计入子进程运行的 helper），exit 0。
- 验证者独立测试：10 passed，0 failed，已包含在上述 63 中。
- 真实 RustFS 另外 2 项 live 测试实际执行通过，4.67 秒；不是默认 ignored 的空通过，也不是 HTTP mock。
- fmt：exit 0，只检查 v1，不格式化或改写 Turso/P0。
- strict clippy：exit 0，`--all-targets --no-deps -- -D warnings`。Turso path dependency 自身仍有 9 个 warnings；不把 no-deps 通过写成整个依赖树无 warning。
- 早期质量门发现 5 处开发测试 `let_underscore_future` 与 2 处 MinIO signing dead code，已即时通知主控；修复后由验证者实际复跑通过，strict clippy 命令原样保留。真实服务测试对直接 include 的共享 signing / HTTP helper 模块使用局部 `#[allow(dead_code)]`，这里如实披露，不宣称不存在局部 lint allowance。
- 最终全量及质量门前后 `src/`、`tests/`、`Cargo.toml`、`Cargo.lock` 的 SHA256 清单一致，没有把测试中途变化的代码签为已测快照。清单自身 SHA256：`7125723d09f76318c0aa619e7711f09f8552ec69a7323ef97ed5f7b560c75ebc`。
- 普通 `cargo test` 的 ignored MinIO 测试不能计为真实服务通过。早期测试曾用环境判断返回成功，该早期计数也不作为 MinIO 证据。

最终原始日志：`/tmp/p1-verify-final.log`、`/tmp/p1-verify-independent-final.log`、`/tmp/p1-verify-fmt-final.log`、`/tmp/p1-verify-clippy-final.log`、`/tmp/peri-p1-rustfs-live.log`。代码快照清单：`/tmp/p1-verify-sources-before.sha256`、`/tmp/p1-verify-sources-after.sha256`。

| 顶层测试目标 | passed | ignored |
| --- | ---: | ---: |
| lib unit tests（含 2 个确定性锁对照） | 4 | 0 |
| main unit tests | 0 | 0 |
| diskless_acceptance | 1 | 0 |
| object_store | 20 | 0 |
| remote_storage | 13 | 0 |
| remote_database | 12 | 0 |
| p1_verify（独立新增） | 10 | 0 |
| minio（普通全量：signing helper + 2 个 setup 安全性测试） | 3 | 2 |
| doc tests | 0 | 0 |
| 默认全量合计 | 63 | 2 |
| RustFS live 补验（执行上述 ignored 的 2 项） | 2 | 0 |

重复 stress 的运行次数不计为新的不同测试数。

## Blocker 与修复验收

1. 验证者实际全量发现 `cache_lock_covers_retained_native_file_handles` 失败：`remote cache directory already locked` / `operation would block`。focused 单测通过，但独立完整 remote_database 20 轮中第 9 轮再次失败，已即时通知主控；失败证据保留在 `/tmp/p1-verify-lock-failed-full.log` 与 `/tmp/p1-verify-lock-round-9.log`。
2. 独立审阅 `src/io/lock.rs`：原本只 close owner descriptor，不能确保持锁的 duplicated / fork-inherited descriptor 一并消失；修复为 `DirectoryLock` RAII 在最后 `Shared` owner 销毁时显式 unlock。没有添加重试来掩盖生命周期错误。
3. 最终全量实际执行两个确定性 unit：close-only + 存活 duplicate 时仍锁住；explicit unlock + 存活 duplicate 时可以新开，同时关掉旧 duplicate 不得解开 replacement 的锁。两项均通过。
4. 修复后验证者独立执行默认并行 remote_database **20/20 轮全部通过**，每轮 12 个 parent tests，日志 `/tmp/p1-verify-lock-fixed-round-{1..20}.log`。此外逐一读取核对主控提供的 `/tmp/p1-lock-fixed-round-{1..100}.log`，**100/100** 均含 parent `12 passed / 0 failed`，没有缺文件或失败日志；核对摘要 `/tmp/p1-verify-zeno-stress-audit.log`。该 100 轮由开发侧运行，不冒充验证者自己运行。
5. 验证者新增 direct RemoteIO 回归：64 次并发子进程创建与 256 轮锁重开；保留 native File 时即使 IO wrapper 已 drop 仍拒绝新 owner，最后 File drop 后立即可新开。它不依赖 Turso SQL connection 的生命周期。最终全量实际通过。
6. 修复并冻结后重新执行完整 `cargo test --locked`、fmt、strict clippy，全部通过；不以此前偶然绿的结果替代该最终复跑。

## 真实 RustFS 证据

- **服务身份 / endpoint**：主控提供并使用用户既有 RustFS S3-compatible 服务 `127.0.0.1:9000`。验证者没有重启、替换容器或另行修改既有数据。
- **执行来源**：live 测试由主控运行；验证者独立读取 `/tmp/peri-p1-rustfs-live.log` 并审阅对应 fixture / assertion，而非仅接收开发 agent 的 PASS 自述。原始日志实际列出 2 个 live 测试、2 passed、0 failed、3 filtered out、4.67 秒。
- **等价复现命令**：安全注入 `PERI_S3_TEST_ENDPOINT`、`PERI_S3_TEST_ACCESS_KEY`、`PERI_S3_TEST_SECRET_KEY` 后，在 `prototypes/v1` 执行 `PERI_S3_TEST=1 cargo test --locked --test minio -- --ignored --nocapture --test-threads=1`。原始日志没有打印完整调用命令或环境；以上是复现方式，不冒充从日志读出的原始 argv。根据主控说明，凭据仅注入 silent PTY 的环境，不写入日志或仓库；报告不记录 secret。
- **对象协议通过**：SigV4 网络请求、16-way conditional PUT CAS 恰好一个 winner、stale conditional delete 拒绝、1005-key 分页、payload/checksum mismatch 拒绝、tenant prefix 隔离。
- **SQL recovery 通过**：远端 checkpoint 后提交 2 行 WAL tail；删除整个 cache，fresh instance query=2；checkpoint 后再次删除 cache，第三个 fresh instance query=2 且 `PRAGMA integrity_check=ok`；没有本地 `database` / `database-wal` 文件。
- **fixture 安全性**：随机唯一临时 bucket，先 HEAD 确认 absent；已有 bucket 则拒绝创建和清理。只清理该临时 bucket 中测试 prefix，随后删除该 bucket。默认全量也实际通过 existing-bucket refusal 与 destination-host/path signing 两个 setup 回归。
- **不外推**：文件名 / case 名沿用 `minio`，但上述实际服务是 **RustFS，不是 MinIO**。这两项是 fresh-instance cache-disposal recovery，不是真实服务上的 SIGKILL / fresh-process crash test；后者单独由 LocalObjectStore 进程验收证明。

## 需求与能力矩阵

| 需求 / 能力 | 独立证据 | 已测 backend / 范围 | 当前结论 |
| --- | --- | --- | --- |
| ObjectStore GET/PUT、conditional write、checksum、namespace | object_store contract、Local corruption/symlink、跨进程 CAS、S3 协议测试、RustFS live | Memory、Local；S3 HTTP mock；真实 RustFS | 已列 backend 通过；mock 不等于真实服务 |
| retry、timeout、签名和秘钥脱敏 | S3 protocol tests；审阅 signing/transport/S3 adapter | S3 HTTP mock | 协议及故障注入范围内通过 |
| HEAD、manifest、segments 远端权威 | storage COW、HEAD CAS、失败上传、manifest/segment integrity；源码审阅 | Memory、Local | 通过 |
| lazy segment fetch，不预取整个 DB | open 后 segment_loads=0；按访问取 segment；进程验收 inspect | Memory、Local | 通过，WAL 重建代价另见限制 |
| RAM/SSD cache eviction、容量、restart reuse | remote_storage cache tests；验证者缓存目录破坏测试 | 本机 RAM / filesystem cache | 通过，限制针对 segment cache，不是进程总 RSS |
| DurableWal append/read/truncate、batch、LSN、replay baseline | WAL/SQL tests；独立重复 compact、稀疏字节 replay | Memory、Local | 通过 |
| commit 等待 durability confirmation | native WAL sync → RemoteWal append → chunk/index 远端 ACK；SQL ACK failure 与丢 ACK readback；RustFS live SQL tail | Memory、Local、RustFS；ACK 注入使用测试 store | SQL/WAL 链路通过；不宣称 mock 覆盖真实 SQL 服务 |
| 冷恢复 committed tail、rollback 与坏 WAL fail-closed | remote_database tests；独立坏 chunk 拒绝 cold open；RustFS cache-disposal SQL recovery | Memory、Local、RustFS（live 正常恢复路径） | 通过；坏 WAL / rollback 注入不是 live 服务实验 |
| SIGKILL、删除整个 worker cache、新进程 query | diskless_acceptance：400 checkpoint rows + 40 acknowledged tail rows，确认 signal=9，fresh process query=440，integrity_check=ok | LocalObjectStore（独立权威对象目录） | 通过，不冒充 MinIO crash test |
| 无本地权威 DB/WAL | RemoteIO 虚拟 main DB/WAL；State.bytes 纯 RAM；独立目录文件白名单与 cold SQL | Memory、Local | 通过，cache 中 lock/cache 非权威 |
| 真实 RustFS S3-compatible | 主控运行的原始 live 日志 + 验证者独立源码 / assertion 核对 | RustFS，127.0.0.1:9000 | 2 项 live 通过 |
| 真实 MinIO | 未运行；不能把 RustFS 或 HTTP mock 写成 MinIO | 未测 | 不计 PASS |
| AWS S3 / Cloudflare R2 | 共享 S3-compatible adapter 静态实现，无账户实测 | 未测 | 不宣称认证、部署或行为兼容验收通过 |

## 独立反例

`prototypes/v1/tests/p1_verify.rs` 不依赖开发报告，共 10 项：

1. `forged_public_namespace_cannot_touch_remote_state`：绕过 `Namespace::new` 直接构造非法公开字段，WAL/storage/database 入口全部拒绝，远端未创建对象。
2. `disappeared_or_obstructed_ssd_cache_is_not_authoritative`：已填充 SSD cache 后删除其目录，替换成普通文件；连续读取依然从远端取得正确字节，不把 cache 写失败当作权威存储失败。
3. `simultaneous_head_publication_has_exactly_one_authoritative_winner`：两个 reader/writer 基于同一 HEAD 真并发发布，恰好一方成功，loser dirty buffer 保留，fresh reader 只看到 winner。
4. `repeated_wal_compaction_preserves_sparse_bytes_and_monotonic_lsns`：两次 truncate/baseline 后新实例 replay 保持 header/稀疏零填充/截断字节；旧 history 请求拒绝，LSN 不回退。
5. `lost_chunk_and_index_acks_resolve_by_remote_readback_without_duplicate_lsn`：分别注入 chunk 与 index 已落远端但 ACK 丢失；remote readback 确认后不重复提交。
6. `unacknowledged_index_failure_leaves_only_unreachable_orphan_chunk`：chunk 成功、index 失败，append 报错，orphan chunk 不被 recovery 读到；后续成功提交从 LSN=1 开始。
7. `cold_sql_recovery_keeps_tenant_namespaces_separate_without_local_db_or_wal`：同名 DB、不同 tenant，删除各自 cache 后只恢复各自 SQL 数据；worker 根目录只有 segment cache 与 lock。
8. `corrupted_committed_wal_chunk_rejects_fresh_sql_open`：直接破坏已索引 chunk，fresh SQL open fail-closed，不能静默丢 committed rows。
9. `sql_sync_override_cannot_acknowledge_a_non_durable_commit`：尝试 `PRAGMA synchronous=OFF` 后提交；允许明确拒绝，但若 SQL 成功则必须推进远端 LSN，删除 cache 后行必须恢复。当前源码每条语句强制 Full，该成功路径已实测。
10. `released_io_lock_is_not_retained_by_concurrent_process_creation`：direct RemoteIO + native File 生命周期与并发子进程创建，验证最后 Shared owner 的显式锁释放，且没有提前放掉 retained File 的锁。

## 静态审阅结论

- `src/storage.rs`：content-addressed BLAKE3 segment/manifest，immutable PUT，HEAD CAS 最后发布；读 manifest/segment 校验；cache insert 失败不阻止已校验的远端读取；open 不拉 segment。
- `src/cache.rs`：RAM/SSD segment cache 的 byte limit、LRU、磁盘复用；SSD 内容重新校验，坏 cache 可被抛弃。
- `src/wal.rs`：immutable batch chunk，CAS index，append receipts 处理 ACK 歧义，单调 LSN；truncate 用 baseline 保持 byte-operation WAL 可以从零重建。
- `src/io.rs`、`src/io/files.rs` 与 `src/io/lock.rs`：main DB/WAL 为虚拟文件，WAL 字节与待发送 batch 仅在 RAM；sync completion 在远端 append 完成后返回；reset 必须已有成功 DB publication；cache directory lock 覆盖 native handle 生命周期并在最后 Shared owner drop 时显式 unlock。
- `src/database.rs`：单连接 Mutex，执行语句/query/checkpoint 前强制 Full、关闭自动 WAL reset/checkpoint；native checkpoint position 纳入 manifest。
- 进程恢复测试的 LocalObjectStore 权威对象目录与 worker cache 分开；删的是整个 worker 目录，不是单个 DB 文件，也不是留下一份可恢复的本地 WAL。

## 不得外推的限制

1. **P4 fencing 尚不完整**：WAL epoch stale check / fence 与 HEAD CAS 不等于完整的租约、调度所有权或分布式 single-writer fencing；不能宣称多个独立 SQL worker 可安全同时写同一 namespace。
2. **bounded cache ≠ bounded worker RSS**：dirty segments、WAL bytes、batch、manifest metadata、append receipts/index 不受 segment cache byte cap 的全局约束。没有做生产规模内存/性能承诺。
3. **cold WAL 仍重建 byte-stream**：open 从可读 baseline/历史重建 RAM WAL，不是仅凭 manifest.applied_lsn 跳过所有旧记录；历史增长与 replay 成本需要后续 compaction/GC 策略。
4. **truncate 不等于物理 GC**：baseline 保留恢复所需字节，旧 immutable chunks 与 receipts 可以继续占空间；失败 publication 产生的 orphan object 也没有独立 GC 验收。
5. **cache 不是可用性零依赖**：运行时 segment cache 写失败已测可绕过；启动仍需要可创建的 cache/lock 目录。测试不承诺完全不可用的本地 filesystem 上仍能启动。
6. **ACK 歧义**：remote readback 能确认的持久化可成功返回；readback 本身不可用时可以返回错误并要求 recovery，不能承诺网络分区下精确判断提交状态。
7. **namespace 是 key 隔离，不是身份认证**：没有实现租户授权/IAM policy 验收。
8. **真实服务范围必须标明**：RustFS live 只证明本次既有 RustFS 服务上的测试路径通过；协议 mock、Memory recovery、RustFS 结果均不能外推 AWS S3、R2、真实 MinIO 或所有故障场景。SIGKILL / fresh-process 证据来自 LocalObjectStore。
9. **P2 不在此次签发内**：BLAKE3、COW、generation/parent history 是基础，不证明 branch/snapshot/clone/PITR/GC 已实现；P2 必须另验 metadata-only refs、immutable pins、WAL retention 与 delayed GC 安全边界。
