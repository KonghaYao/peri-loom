# 2026-10-16 WAL 增量验证设计（commit 路径增长项）

基线：`997abbdeeb5cbd6b827677a3e39046acc6acf86e`（origin/main，v1.1.4）。
worktree：`perf/wal-incremental-20261016-33a7654d-68f0-4277-9d72-f530ce0e4ba5`。
范围：Rust `prototypes/v1/src/recovery.rs`、`src/io.rs`、`src/io/files.rs`、针对性测试与配对基准。
不改 Go、不改 E01 skill、不改持久格式、不改数据库表结构、不改 fencing/审计/Unknown 语义。

## 1. 待消除的增长项（文件与行号为 997abbd）

| 位置 | 现状 | 复杂度 | 本轮 |
| --- | --- | --- | --- |
| `io.rs:294` `sync_wal` | 每次提交对整幅 Worker WAL 镜像调用 `committed_position`，从首帧重算滚动 checksum | 每次扫描 O(镜像字节) | 已消除 |
| `recovery.rs:243` `validate_record` | `apply_batch` 后 `committed_position(bytes)` 再次全扫镜像 | 每次扫描 O(镜像字节) | 已消除 |
| `recovery.rs:132` `validate_index_append` | `previous.bytes.clone()` 复制整幅已重放镜像 | 每次提交 O(镜像字节) 内存拷贝 | 保留，见第 4 节 |
| `recovery.rs:111-131` | 新索引与旧索引 entries/receipts/receipt_digests 全量比对，且每次全量解码索引 JSON | 每次提交 O(条目数) | 保留：属持久格式问题，需审批 |

**口径（2026-10-16 修正）：** 上表「已消除」只指**两个站点各自的整幅 checksum 重算**从 O(镜像)/次
降为 O(新增帧)/次，是按扫描调用计的增长项，不是提交总量的复杂度。提交路径上仍有一次整幅镜像
memcpy（第 3 行，未消除），durable 读写没有减少（测量两臂 `copied_bytes`/`gets`/`groups` 相同），
提交级配对测量**未区分**两臂（见[配对测量](./2026-10-16-wal-incremental-scan-measurement.md)第 4 节）。
因此不得把本轮结论表述为「提交变快」或「消除了累计 O(N²) 的提交成本」。

计时口径：`io.rs:311` 的 `wal_durability` 从 `append_commit` 开始，**不覆盖**第 294 行的扫描，
所以该成本此前既不计时也不计入任何观测。本轮把扫描/复制字节显式计数。

## 2. 设计

### 2.1 状态所有权：镜像与其扫描状态不可分离

新增 `WalImage { bytes: Vec<u8>, scan: WalFrameScan }`（`recovery.rs`），所有写点必须经由它
（`io.rs`、`io/files.rs`、`recovery.rs` 三处调用点已全部迁移）：

- `apply(&WalBatch)`：先按每个操作的起点通知扫描（起点 < 已检查长度 ⇒ 整体重置），再调用既有
  `wal::apply_batch` 落字节；字节语义仍由 `apply_batch` 单一实现。
- `resize(size)`：`try_reserve` + `resize`；缩短到已检查长度以下 ⇒ 重置。
- `clear()`：镜像与其扫描状态一起清空。
- `committed_position()`：增量结果；失效后的下一次调用从 header 重新推导（等价全扫）。

### 2.2 增量与等价契约

`WalFrameScan::position` 与参考实现 `committed_position` 必须逐点等价（用同一份字节对照测试）：

1. `len < 32` → `None`；magic/format/page_size/header checksum 非法 → `Err`。
2. header 与已确认前缀不同（重写/restart）⇒ 丢弃前缀，从 header 重算。
3. `(len-32) % frame_size != 0`（尾部半帧）⇒ `None`，即使前缀中已有 commit 帧。
4. 任一帧 page 号 0、salt 不符或 checksum 不符 ⇒ `None` 且置 `stopped`：其后追加不改变结论，
   直到有写入落在已检查区间内。
5. **每一帧都重写位置**：只有最后一帧带非零 db_size 才返回位置。若只在提交帧赋值、非提交帧保留
   旧值，就会与全扫分歧（本轮测试抓到的真实缺陷，已修正）。

失效规则：任何写起点或 truncate 目标小于“已检查字节数”即整体重置；镜像缩短到已检查长度以下也重置；
header 改变视为 restart。已检查长度永远停在帧边界，因此落在其后的写入不会影响前缀。

### 2.3 完整验证 fallback

- 非纯追加的 batch 由 `apply` 的通知机制自动置失效，下一次 `committed_position()` 从 header 重算，
  即全量重新验证；无需额外 API（原先设计的显式 `rescan_position` 因此删除）。
- `validate_index_state` 仍是冷路径唯一全文重放，并顺带构建扫描状态供后续追加复用。
- Worker `open()` 回放后仍做一次结果扫描（扫描状态为空 ⇒ 等价全扫）。

### 2.4 成功发布点与 CAS/恢复边界（本轮未改）

`ValidatedWal` 仍由 `validate_index_append` 返回**新值**，`branch.rs:428` 只在发布成功后写入
`view.validated_wal`；CAS 冲突、发布失败或重放时旧缓存状态原样保留，候选状态被丢弃。
本轮保持这条不可变/事务边界不变，因此没有污染风险。

### 2.5 观测与开销复核

`recovery::recovery_observations()` / `reset_recovery_observations()`：
`scanned_bytes`、`copied_bytes`、`scan_calls`、`scan_nanos`。

- **线程局部范围**：与 `request_reads` 同风格。Worker 的 `sync_wal` 与 Authority 的
  `validate_index_append` 都在发起提交的线程上同步执行，因此该线程读到的计数覆盖两个站点；
  其它线程（如指标聚合器）读到 0，本模块不提供跨线程聚合，需要时由 metrics 层负责。
- **统计开销**：每次 scan 调用一次 `Instant::now()` 对、一次 TLS 读取和 4 次加法；不按字节累加，
  扫描本身仍是每帧一次。调用点即提交点，不是每帧热点。
- **溢出**：所有字段 `saturating_add` 而非回绕，诊断计数回绕会报出比实际更小的成本。
  `scanned_bytes` 在 header 检查时按整 32 字节计，属保守上界。
- **变异边界**：`apply`（含截断/缩短/重写）、`resize`、`clear` 三种入口都会按上表失效；
  header 被破坏时两条路径返回同样的错误（`assert_same_outcome` 覆盖）；
  尾部半帧、零填充增长、非提交尾帧均有定向测试。
- **对照臂**：`WalImage` 上的 `#[cfg(test)] whole_image: bool`（默认 `false`）。置位后每次扫描先丢弃
  已确认前缀，即本改动替换掉的整幅扫描，供配对测量在同一二进制、同一夹具上归因；生产构建不含该
  字段与 `force_whole_image()`。**按实例**而不是进程级：调用点（`RemoteIO` 的测试入口）切的是自己
  那个镜像，因此并行执行的测试与两个测量臂互不影响——原先的进程级 static 会让不相关的测试互相
  翻转扫描模式（`recovery::` 测试多线程下概率性失败）。
  覆盖范围：Worker `sync_wal` 站点可按实例切换；Authority 侧 `validate_index_append` 的镜像是
  Authority 自己持有的实例，按实例开关到不了，因此 commit 级对照臂只覆盖 Worker 站点（测量报告第 0 节）。

## 3. 已实现范围（Stage 1）

- `recovery.rs`：`WalFrameScan`、`WalImage`、观测计数、测试用按实例对照臂；`ValidatedWal` 改持
  `WalImage`；`validate_record` 拆为 `load_record` + `apply_record`。
- `io.rs` / `io/files.rs`：`State.bytes` → `State.image`（9 处调用点），`sync_wal` 不再全扫。
- `recovery/fixture.rs`：合成 WAL 夹具（正确性测试与基准共用，保证两侧构造同一份字节）。
- `recovery/tests.rs`：13 个正确性/隔离测试，其中 3 个是本轮新增（拒绝后前缀仍可复用、覆盖写后
  必须整幅重扫且与冷重放一致、对照臂不跨镜像泄漏）。
- `recovery/bench.rs`、`authority/store_scaling_tests.rs`：两个配对基准（默认 ignored）。

未改：持久格式、WAL index 结构、fencing、审计、Unknown 语义、提交发布顺序、Go 侧。

## 4. 第二阶段（未实施，需批准）

去掉 `validate_index_append` 的整幅镜像复制（`previous.image.clone()`，仍是每次提交 O(镜像)）：
要么就地扩展 + checkpoint/restore（需跨 CAS 边界回滚，且 `WalMetadata` 仍是 O(条目) 新建），
要么共享前缀 + 自有尾部（会让 Worker `pread` 每次拼装）。两条路都要动事务边界或读取路径，
风险高于本轮，收益量级小于已消除的 checksum 链（通用估算，未实测）。

## 5. 正确性验证（已执行）

`f4bcee2` 首次验证：

```
cargo check / cargo fmt --check / cargo clippy --no-deps --lib -- -D warnings   通过
cargo test --lib                    203 passed, 0 failed, 4 ignored
cargo test --test recovery          14 passed
cargo test --test runtime_resources  5 passed
cargo test --test p4_runtime        26 passed
```

测试隔离修复后重跑（同一 worktree，`--locked`，debug）：

```
cargo fmt --check                                          通过
cargo clippy --all-targets --no-deps -- -D warnings        通过
cargo test --lib                206 passed, 0 failed, 4 ignored
cargo test --lib recovery:: -- --test-threads=8   13 passed, 0 failed, 1 ignored（连跑 5 次同结果）
cargo test --test recovery              14 passed, 0 failed
```

`recovery/tests.rs` 的 13 个测试全部以“增量结果 == 全扫结果”为断言：
append/部分尾帧/无提交帧/覆盖重写/truncate/salts 与 header 改变/checksum 损坏后 stopped 与恢复/
`clear` 与零填充增长/200 步随机混合突变逐点对照；另有两个测试证明对照臂确实做整幅工作且仍与参考一致。

本轮新增 3 个用例：

1. `a_rejected_append_leaves_the_validated_prefix_reusable`（索引级）：同一 `previous` 先被拒绝
   （候选索引伪造 commit 位置，失败发生在镜像已被扩展之后）再成功，两次扫描字节都是 header +
   新增帧，即前缀未被失败路径消耗或污染。
2. `an_overwriting_append_forces_the_next_one_to_re_derive_the_prefix`（索引级）：
   `validate_index_state → append(覆盖写，无 commit) → append(修复+追加，有 commit)` 后，第二步必须
   整幅重扫（`header + 6 帧`，而非仅新增 2 帧），且最终镜像字节与 commit 与冷重放一致。
3. `the_control_arm_does_not_leak_between_images`（隔离级）：只给一个镜像置对照臂，另一个镜像的
   单次追加仍只扫描新增帧，而对照臂镜像仍整幅重扫——进程级开关无法满足这条断言。

改坏验证（写完先故意改坏生产代码确认用例会失败，再改回）：强制每次扫描丢弃前缀 → 用例 1 与 3 失败；
写入落在已检查区间内不再失效 → 用例 2 失败；两个方向都已确认断言有效。

## 6. 配对基准（已执行，结果见测量报告）

两个基准都在**同一二进制**上交替运行两臂，唯一差异是该臂自己镜像的扫描臂（对照臂整幅重算
checksum 链），因此差值只能归因于扫描状态；夹具（镜像与追加帧）在计时外生成，每样本结束时校验
两臂结论与整幅扫描一致。对照臂是按实例的，所以两臂与并行测试互不影响；代价是 commit 级对照臂
只覆盖 Worker `sync_wal` 站点（第 2.5 节），其对照臂数值口径见测量报告第 0 节。
默认 ignored，避免与其它 owner 的测量互相污染。release 构建下已按冻结配置执行：
扫描级 2000/16000/65536 帧 × 3 轮，commit 级 1000/4000/12000 行 × 3 与 9 轮。
原始每轮值、中位与范围、计数与异常见
[配对测量](./2026-10-16-wal-incremental-scan-measurement.md)。

```
# 扫描级：固定追加帧数，累计 WAL 多档，≥3 轮交替
PERI_WAL_BENCH_TIERS=2000,16000,65536 PERI_WAL_BENCH_APPENDS=32 PERI_WAL_BENCH_ROUNDS=3 \
  cargo test --release --manifest-path prototypes/v1/Cargo.toml --locked --lib \
  -- --ignored --exact recovery::bench::wal_scan_paired_curve --nocapture --test-threads=1

# commit 级：固定事务（单行 INSERT）新增帧，累计行数多档，≥3 轮交替
PERI_COMMIT_BENCH_ROWS=1000,4000,12000 PERI_COMMIT_BENCH_COMMITS=16 PERI_COMMIT_BENCH_BATCH=200 \
  PERI_COMMIT_BENCH_ROUNDS=3 cargo test --release --manifest-path prototypes/v1/Cargo.toml \
  --locked --lib -- --ignored --exact \
  authority::store_scaling_tests::commit_incremental_scan_paired_curve --nocapture --test-threads=1
```

- 实测预算：release 构建 232 s（首次，单独报告）；扫描级测量 < 1 s，commit 级 3 轮 17 s、
  9 轮 32 s；测量总计约 1 分钟（共享主机、非独占）。
- 输出字段：`scan_us`/`scanned_bytes`/`per_append_scan_bytes`/`scan_calls`/`copied_bytes`（机制项），
  commit 级另有 `wall_us`/`per_commit_us`、store `gets/puts/groups/page_gets`（提交总量与持久写入），
  以及 `COMMIT_BENCH_VERIFY`（两臂 lsn 与帧数一致）。
- 判读规则：`scanned_bytes`/`scan_us` 的差异是本次改动的作用域；`wall_us` 只有在 commit 级才代表
  单机提交耗时，且**仍不等于平台端到端**（不含 Gateway、Go 侧、网络与并发）。
  若 `wall_us` 差异小于档位噪声，应报告“机制项改善、提交总耗时未区分”，不得反向宣称。

## 7. 未验证 / 边界

- **扫描级已实测（成立）**：1.07–35.1 MB 镜像上，单次追加的扫描由 O(镜像) 降为 O(新增帧)，
  中位 31→5506 / 10→50847 / 8→174723 µs（见配对测量报告）。
- **提交级未证明**：1000/4000/12000 行档位上，每提交总耗时两臂不可区分（方向随档位翻转、
  范围重叠）；该规模下机制项占比约 2% 或更低。E01（570k 行 / 8M 目标）规模的提交级与端到端
  仍未测，不得据此宣称突破。
- 未跑 Go 平台测试、未跑全栈、未跑 E01 基准；无端到端结论。
- 剩余已知增长项：镜像复制；WAL index 单文档 O(条目数) 的解码/序列化/上传。
