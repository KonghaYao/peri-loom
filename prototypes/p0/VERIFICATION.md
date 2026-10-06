# P0 独立验收

## 结论

**PASS — 在本机、单进程 SQL 接入和进程崩溃恢复范围内，P0 核心验收通过。**
最终独立执行 `cargo test --locked`：**36 项通过，0 项失败**
（23 个 lib 单测 + 8 个 acceptance 集成测试 + 5 个独立 final_verify 测试）。
没有剩余的已复现 P0 blocker。此结论不是生产级持久性认证，不包含电源故障证明。

- 时间：2026-10-06，Asia/Shanghai；初次报告编写于 23:38，随后按直接 Git 工作流及 submodule 迁移再次复验并更新。
- 环境：Darwin arm64；rustc 1.98.1；cargo 1.98.1。
- 需求：`../../docs/v1/01-p0-core-prototype.md`。
- 工作区：`prototypes/p0`。
- 审阅：`src/manifest.rs`、`src/storage.rs`、`src/io.rs`、`src/database.rs`，
  CLI 和全部现有验收测试；四文件 upstream Git diff、Cargo 路径依赖。
  先前独立 patch / bootstrap 的实际验收结果仅作为历史记录，已不属于当前交付。
- 验收者仅新增 `tests/final_verify.rs` 和本报告；没有修改生产源码、已有测试或其他文档，
  没有 commit、push 或派发其他 agent。

## 最终命令与结果

所有 Cargo 命令均在 P0 目录运行。这里记录的是验收者自己执行的结果，而非开发者自述。

| 命令 | 结果 | 证据 |
| --- | --- | --- |
| `cargo test --locked` | 直接 Git 工作流更新后再次 exit 0；23 + 8 + 5 = 36 PASS | `/tmp/p0-final-direct-test.log` |
| `cargo test --locked --test final_verify -- --nocapture` | exit 0；5 PASS | `/tmp/p0-final-extra-rerun.log` |
| `cargo fmt --check --package peri-loom-p0` | 直接 Git 工作流更新后再次 exit 0；无 diff | `/tmp/p0-final-direct-fmt.log` |
| `cargo clippy --all-targets --no-deps --locked` | exit 0；初次有 1 个本项目 warning，后续修复 | `/tmp/p0-final-clippy.log` |
| `cargo clippy --locked --offline --all-targets --no-deps -- -D warnings` | exit 0；本项目零 warning | `/tmp/p0-final-clippy-strict.log` |
| `git -C turso diff --check` | exit 0 | 无 whitespace error |
| `git -C turso status --short` / `git -C turso diff --stat` | 恰好 4 个 modified 源文件；29 insertions、4 deletions | 本机独立读取；文件名单见 Provenance |
| `cmp /tmp/p0-final-live.diff /tmp/p0-final-direct-git.diff` | exit 0；当前核心改动与之前通过验收的 diff 逐字相同 | 两份本机 diff 快照 |
| `git ls-files --stage prototypes/p0/turso`（父仓库） | mode `160000`，指向基础 commit | submodule 已注册；不再忽略 checkout |

fmt 使用 package 限定，只检查 P0，不格式化巨大 upstream。严格 clippy 的 `--no-deps`
只覆盖本项目 lint；依赖构建仍输出 Turso 原有 **9 个 warning**：2 个 unused import、
1 个 dead-code、4 个函数指针比较、1 个 lifetime syntax、1 个 experimental specialization。
这些不是零警告的 upstream 构建，也没有在此验收中修改 upstream 无关代码。
主控将 segment 数量计算改为 `u64::div_ceil`；独立复核它等价于此前的商加非零余数计算，
除数固定为 128 KiB，且最终 strict clippy 和完整测试均通过。

日志位于本机 `/tmp`，不是永久归档；可用记录的命令和新增测试重新产生证据。
子进程 helper 重复执行不额外计入 36 项；main 和 doctest 各有 0 项。

## 验收矩阵

| 需求/风险 | 结论 | 独立检查与实测 |
| --- | --- | --- |
| Turso 可追踪修改边界 | PASS（Git submodule） | 用户 fork 为 origin；开发分支 codex/p0-core-prototype；Git status / diff 为权威；4 文件、29 insertions、4 deletions；与先前验收 diff 相同；Parser / VM / BTree 无 diff |
| 固定 segment / offset 映射 | PASS | 128 KiB；跨段、EOF、稀疏、truncate、随机操作与平坦字节向量比较 |
| Manifest / generation | PASS | generation、logical size、page size、segment size、mapping、WAL salts/frame 均持久化；HEAD 摘要与 manifest 一致 |
| 不可变对象与 HEAD 原子发布 | PASS（进程模型） | 代码顺序检查和并发读者校验；20 次发布期间 1178 个完整 HEAD 快照，无部分 JSON、悬空 manifest/segment 或摘要错误 |
| fsync 顺序 | PASS（静态审阅） | segment 文件 sync → 安装对象和目录 sync → manifest 同样处理 → HEAD 临时文件 sync → rename → root directory sync；未做系统调用跟踪或断电注入 |
| lazy loading | PASS | 直接打开 storage 的 segment_loads=0；单段读取只增加 1；未请求的坏段不阻止打开，请求后 checksum error |
| 无完整物理 DB | PASS | SQL 多段数据库正常查询；`database` 和 `database.db` 不存在，持久化使用 HEAD、manifests、segments 和本地 WAL |
| 独占锁与 alias | PASS | 活跃进程阻止第二进程；SIGKILL 后可重新打开；同进程和 file handle 生命周期现有测试通过；独立 symlink 父目录 alias 测试证明 WAL reset 防护未被绕开 |
| WAL committed tail / torn tail | PASS | checkpoint 后 kill、提交 tail 后再 kill；HEAD 未变；额外追加 19 字节半帧后恢复 127 行、payload 总长度 260096；再 checkpoint 与 integrity_check 正常 |
| Checkpoint / WAL truncate 顺序 | PASS | pending/dirty/failed 防护；失败后不能 truncate；native TRUNCATE 和新 WAL epoch SQL 恢复实测通过 |
| AppliedWal 准确性 | PASS（所测模式） | FULL 结果与原始 WAL header salts、最后 commit frame 对照；TRUNCATE 前水位由现有 acceptance 对照；重启 epoch 后再次 FULL 对照 |
| changed segments / reuse | PASS | 修改只重建脏段，不变 mapping 复用；旧 generation 的内容仍可读取 |
| truncate 缩小和零填充 | PASS（storage 层） | 存储单测验证 shrink / extend、丢弃尾部和不复活字节；不等于已通过 SQL VACUUM |
| 新机器共享 Turso 改动 | 本地提交已完成，尚未 push | P0 Turso 提交为 1a548d14a5be41e32b05bc43cb559cfb8b44dae4，父仓库 gitlink 跟踪此提交；远端发布未授权 |

## Submodule 迁移独立复验

按用户要求将已修改的 Turso checkout 注册为 `turso` submodule，使用用户提供的
`https://github.com/KonghaYao/turso.git`，创建本地分支 `codex/p0-core-prototype`。
另一名只读验证 agent 独立确认迁移 **PASS**：

- `.gitmodules` 工作区与暂存区的 path、URL、branch 正确。
- 父仓库 gitlink 为 mode `160000`，指向 `8549c16595d2faf1bdd6ee24aee0be8bfabb3d4a`，与 child HEAD 一致。
- child Git 目录已移至父仓库 `.git/modules/turso`，当前分支正确；origin 为用户 fork，upstream 为官方仓库。
- `git diff --abbrev=7` 的 SHA-256 为 `1bc483473c50c91579187a65f965c767dcb859f8d32148904ea619f23c8bbffb`，与迁移前一致；四文件仍为 29 insertions / 4 deletions。
- 独立重跑完整测试 36 项通过，fmt check 与 strict clippy 均 exit 0。
- 未 commit 或 push；父仓库原有 `CLAUDE.md` 修改未触碰。

Git 默认缩写长度可能随对象数量改变，因此对比 diff 时显式固定 `--abbrev=7`；
该显示变化不代表源码变更。submodule 注册自动暂存 `.gitmodules` 和 gitlink，
但不会提交 child 的工作区修改，branch 配置也不会将未提交改动同步到另一台机器。

## 真正的 kill 证据

独立测试 `sigkill_lock_release_committed_tail_and_torn_frame` 并非用正常退出冒充崩溃：
等待子进程输出 `ready: true`，调用 `Child::kill()`，`wait()` 后在 Unix 上断言
`ExitStatusExt::signal() == Some(9)`。第二轮带输出运行记录：

```text
independent kill evidence: pid=66150 signal=9
independent kill evidence: pid=66155 signal=9
atomic HEAD evidence: 1178 complete snapshots
native watermark evidence:
  first salts=[3488304131, 1742663262], frame_count=124
  second salts=[1734348064, 2180168152], frame_count=1
```

第一次 kill 在 checkpoint 完成之后，第二次在 7 条新增记录的事务提交之后。
kill 后验证锁释放、manifest 原样保留、未发布 WAL tail 恢复、数据总量及完整性。
原有 acceptance 还运行了 400 行多段场景和 40 行提交 tail 场景。
这证明了完成检查点/提交后的真实进程崩溃路径，**没有**证明检查点每个内部步骤中的
随机 kill 或真实机器断电路径。

## 初轮失败与处置历史

1. 首轮 `cargo test --locked`：23 个 lib 测试中 22 PASS、1 FAIL。
   `io::tests::failed_publication_blocks_wal_reset_until_successful_retry`
   在当时的 `src/io.rs:522` 对错误 completion 的断言失败。
   已即时通知主控，日志 `/tmp/p0-final-test.log`。最终源码审阅发现路径匹配已经改为
   对父目录 canonicalize 后比较文件名；主控确认真正根因是 macOS `/var` / `/private/var`
   alias 导致直接打开 WAL 绕过 wrapper，而不是单纯编译快照交错。
   最终该现有测试通过，验收者再独立新增 symlink alias 回归测试并验证通过。

2. 首轮 `cargo test --locked --test acceptance`：8 项中 7 PASS、1 FAIL。
   原 `vacuum_and_truncate_checkpoint_publish_the_smaller_database` 因 upstream
   VACUUM 为 experimental、未启用而 parse error，日志 `/tmp/p0-final-acceptance.log`。
   主控没有扩大 P0 范围启用 experimental VACUUM，而是将该项替换为
   `truncate_checkpoint_records_the_pre_restart_wal_generation`，验证 native TRUNCATE
   的 pre-restart salts/frame。最终 8 项 PASS；**不能据此声称 SQL VACUUM 已实现或已验收**。
   Storage 层 truncate 缩小仍有独立现有单测覆盖。

3. 初次 clippy 的 `src/manifest.rs:54` `manual_is_multiple_of` warning 已由主控
   等价改写消除；首轮 lib-test must_use warning 后续也已消失。
   本次验收过程中没有遇到 E0282；未将开发过程中的其他结果当作独立验收证据。

4. **已退役工作流的历史成功证据，不是当前交付入口：**此前确实在
   `/tmp/p0-bootstrap-verify.JMOcXa` 用 `git clone --shared --no-checkout` 创建干净 checkout，
   设置正确 upstream remote 后执行 bootstrap 两次，均 exit 0；reverse check exit 0，
   重放 diff 与当时 live diff 的 `cmp` exit 0。日志 `/tmp/p0-final-bootstrap.log`。
   该验证没有覆盖空目录从公网 fetch 的分支。用户随后要求直接 clone 并修改 Turso，
   主控移除了独立 patch、bootstrap 脚本和 upstream 配置文件；验收者已确认这些文件不存在。
   此前测过的重放路径不能再作为当前新机器的运行或复现方式。

## Provenance 与限制

用户随后授予持续提交权限。P0 四文件源码已提交为
`1a548d14a5be41e32b05bc43cb559cfb8b44dae4`，父仓库 gitlink 更新到该提交。
下文关于未提交修改的描述记录独立验收当时的状态；目前已完成本地提交，仍未 push。

- `turso/` 是父仓库已注册的 Git submodule，HEAD 为
  `8549c16595d2faf1bdd6ee24aee0be8bfabb3d4a`，本地分支 `codex/p0-core-prototype`。
  `origin` 指向用户提供的 `https://github.com/KonghaYao/turso.git`，
  `upstream` 指向 `https://github.com/tursodatabase/turso.git`；没有创建新远端 fork 或发布分支。
  当前交付采用直接在该 submodule 修改的普通 Git 工作流，
  **以 `git -C turso status` 和 `git -C turso diff` 为权威改动记录**，不再维护独立 patch 或重放入口。
- Cargo 使用 `turso/core` 路径依赖；最终确认 `vendor/` 已不存在。
  Git diff 只改 `core/io/mod.rs`、`core/storage/database.rs`、`core/storage/pager.rs`、
  `core/storage/wal.rs`，增加 checkpoint 水位传播，不替换 native WAL recovery。
- 四个修改仍是嵌套 Turso 仓库中的未提交工作区改动；没有自动 commit 或 push。
  父仓库通过 gitlink 记录基础 commit，不再忽略 checkout；当前状态仍**不能仅 clone peri-loom 在新机器复现 P0 修改**。
  若要共享，需要先在 Turso fork 内正常提交、推送修改，再更新父仓库 gitlink，
  由另一机器执行 `git submodule update --init --recursive` 获取包含修改的 revision。
  当前没有这样的已共享 revision，也没有验收 push。
  这不改变现有本机 P0 的功能 PASS，但共享/重建能力不能再沿用历史 patch 重放结论。
- AppliedWal 保存 checkpoint 的 salts + backfilled frame，不从 WAL 文件长度推断。
  Recovery 仍使用 native Turso WAL 校验/回放；该 metadata 不是自定义 recovery 截断游标。
  FULL、TRUNCATE 和合成 partial watermark 已覆盖；native PASSIVE 有活跃 reader 时的
  部分 checkpoint 未单独构造验证，不把 synthetic hook 等同于该场景。
- SQL 打开可能因 Turso 的 header/schema 初始化读取必要 segment；严格“仅加载 manifest、
  零 segment 读取”的证明针对 `SegmentedStorage::open`，而不是整个 `SegmentedDatabase::open`。
  SQL 按需查询读取的 segment 数少于总数，由现有 acceptance 断言验证。
- 支持的是本机同步 PlatformIO、单个 segmented SQL owner。裸 storage mutation 和
  `synchronous=OFF` 不在安全契约内；OS lock 为 advisory，不覆盖恶意删除/替换锁文件。
  没有跨平台、网络文件系统、分布式、remote storage、HA 或异步 IO 安全证明。
- **未验证电源故障**、磁盘缓存丢失、首次 WAL 创建目录项断电持久性、fsync/rename
  的系统级故障注入、checkpoint 内部各阶段随机 kill、空间耗尽或长时间压力运行。
  当前 SIGKILL 保留内核缓存；fsync 代码审阅和 concurrent HEAD 测试不能替代断电验证。
- 本报告按最终稳定文件复验结果判断；初轮历史失败保留，最终不再存在其可复现 blocker。
