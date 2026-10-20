# P0 segmented Turso prototype

本地单进程原型的逻辑数据库由 manifest、不可变的 128 KiB segments 和本地 WAL 构成，而不是完整的物理 `.db` 文件。SQL parser、VM、BTree 使用本地 Turso Git checkout 的 `turso/core`，IO 与存储层负责虚拟数据库文件及持久化。

## 运行

在本目录执行，需要 Rust/Cargo（Rust 至少 1.98）。`turso/` 是已注册的 Git submodule，Cargo 直接编译其中的源码：

```sh
cargo build
BIN=target/debug/peri-loom-p0
ROOT=/tmp/peri-loom-p0-demo
"$BIN" "$ROOT" demo 400
"$BIN" "$ROOT" inspect
"$BIN" "$ROOT" query 'SELECT count(*) FROM p0_records'
"$BIN" "$ROOT" query 'SELECT payload FROM p0_records WHERE id = 1'
"$BIN" "$ROOT" exec "UPDATE p0_records SET payload = 'changed' WHERE id = 1"
"$BIN" "$ROOT" checkpoint
cargo test --test acceptance
```

## 发布门禁

`scripts/p0-gate.sh` 是本地发布检查与 CI 共用的唯一入口：先确认 `turso/` 停在父仓库锁定的 gitlink，再运行本目录全部测试（`acceptance`、`final_verify`、`statement_safety`）与固定 commit 的 Turso SQL 冒烟。`.github/workflows/p0-gate.yml` 在每次 push / pull request 与 tag 发布时调用它；门禁失败时不会构建或发布镜像。

在仓库根目录执行：

```sh
scripts/p0-gate.sh
```

## Turso fork 跟踪

`turso/` 是父仓库的 Git submodule，`origin` 指向 `https://github.com/KonghaYao/turso`，`upstream` 指向官方仓库。开发分支为 `codex/p0-core-prototype`。

直接在 `turso/core/` 上修改；不再维护 vendor、独立 patch 文件或重放脚本。Turso 改动以它自己的 Git 工作区为准，外围原型代码由 peri-loom 仓库管理：

```sh
git -C turso status --short
git -C turso diff
git -C turso fetch upstream
```

父仓库通过 `.gitmodules` 和 gitlink 跟踪子模块提交；该分支的本地提交发布到 Turso 远端后，其他机器才能从父仓库锁定的提交初始化。发布顺序必须是先子仓库、后父仓库；普通 `submodule update --init` 使用 gitlink，不会自动切换至 `.gitmodules` 配置的分支。

共享 revision 已提交并推送后，新机器在 peri-loom 根目录初始化：

```sh
git submodule update --init --recursive
```

## CLI

CLI 使用 `std::env`，格式为 `peri-loom-p0 <directory> <command> [args]`。SQL 必须作为一个 shell 参数传入。错误写 stderr 并以非零状态退出。成功结果为一行 JSON，随后 flush stdout。

| 命令 | 行为 |
| --- | --- |
| `exec SQL` | 执行 SQL，可以包含多个语句；输出 stats，不隐式 checkpoint |
| `query SQL` | 执行单个查询，输出裸二维 JSON 数组，例如 `[[400]]` |
| `inspect` | 只调用 `SegmentedStorage::open`，输出 `{manifest, stats}`，不打开 SQL |
| `checkpoint` | 显式 Full checkpoint，输出 manifest 和 stats |
| `demo [rows]` | 默认 400 行，每行 2048 字节 payload；提交后显式 checkpoint |
| `crash-write COUNT` | 追加并提交，不 checkpoint；输出 `ready: true` 后 park 等待 kill |
| `checkpoint-wait` | 显式 checkpoint；输出 `ready: true` 后 park 等待 kill |

`demo` 和 `crash-write` 都使用 `p0_records(id INTEGER PRIMARY KEY, payload TEXT)`，在事务中读取最大 ID 后追加。新目录的 ID 从 1 开始；`demo 400` 后 `crash-write 40` 写入 401–440，不产生重复主键。默认 demo 的 payload 总量为 800 KiB，足以跨越多个 128 KiB segment。重复 demo 不清空旧数据。

JSON scalar 映射为 null、整数、浮点数、字符串；BLOB 为字节数组。非有限浮点数按 JSON 序列化规则输出 null。查询结果全部收集到内存，不提供流式分页。

## 数据布局与惰性加载

根目录保存 HEAD、generation manifest、不可变 segment 对象及本地 WAL；具体文件命名由 `manifest`、`storage`、`io` 模块定义。HEAD 选择已发布的 generation。manifest 包含逻辑大小、page/segment 大小、segment 映射及 applied WAL position（salts 与 frame count）。checkpoint 重建受影响的 segment，未修改对象继续复用，再发布新 manifest 和 HEAD。

```text
<directory>/
  HEAD
  manifests/<generation>-<digest>.json
  segments/<immutable-object>
  database-wal
  .peri-loom.lock
```

虚拟 database path 仅供 Turso IO 路由，不应存在完整物理 `database` 或 `database.db`。`inspect` 的 `stats.segment_loads == 0` 证明 storage open 只加载 manifest。**不声称 SQL database open 零读取**：Turso 初始化可能读 header、schema 及恢复 WAL；这些访问仍按需加载 segment。`segment_loads` 是当前 storage 实例的累计加载数，`cached_segments`、`dirty_segments` 为当前计数。

## Crash recovery 验证

必须直接启动二进制并 kill 该进程，不能把 `cargo run` 的 PID 当作数据库进程。先完成 demo，然后：

```sh
rm -f /tmp/p0-crash-ready.json
"$BIN" "$ROOT" crash-write 40 > /tmp/p0-crash-ready.json &
PID=$!
until test -s /tmp/p0-crash-ready.json; do sleep 0.1; done
cat /tmp/p0-crash-ready.json
kill -9 "$PID"
wait "$PID" || true
"$BIN" "$ROOT" inspect
"$BIN" "$ROOT" query 'SELECT count(*) FROM p0_records'
"$BIN" "$ROOT" checkpoint
```

等待文件中的完整 JSON 行且确认 `ready: true` 后才 kill；自动测试应逐行读取子进程 stdout 并设置超时。readiness 表示事务已提交且 stdout 已 flush。重启以 manifest + segments + 已提交 WAL tail 恢复，计数应为 440。crash-write 及后续 query 都不应改变 manifest generation，只有显式 checkpoint 才发布新 generation。`checkpoint-wait` 使用相同 readiness 协议，可验证 checkpoint 完成后 SIGKILL 再恢复。

## 包装与边界

`SegmentedDatabase` 只有一个私有 SQL 连接，用 mutex 串行化 SQL 与 checkpoint 调用，不提供新建连接接口。默认 `SyncMode::Full`，通过 `wal_auto_actions_disable()` 禁止自动 checkpoint **及 WAL header restart**，避免 tail 测试中 salts/frame position 被隐式重置。显式 checkpoint 使用 Full 模式，不主动 truncate/restart WAL。包装 drop 使用 Turso 的连接 drop 路径，不调用 `Connection::close()`，不会隐式 checkpoint；未提交事务不具有恢复保证。

调用者必须遵守单进程、单连接约束，不通过 SQL 改写同步策略或绕过包装做 WAL maintenance。需要主动重置 WAL 时可执行 `exec 'PRAGMA wal_checkpoint(TRUNCATE)'`；exec 会完整执行并丢弃返回行，不留下 suspended statement。跨多个 API 调用的事务也应由单个调用者驱动，mutex 不代表多调用者事务隔离。活动数据库的独占进程锁由 IO 层负责，第二个 SQL opener 应失败；manifest-only inspect 不等价于 SQL opener。

这是本地 P0，不含远程存储、复制、HA、租户、分支、在线 segment GC 或生产级并发保证。SIGKILL 测试验证进程崩溃，不等价于电源故障证明；真实持久性仍依赖 IO 层正确 fsync、原子 HEAD 发布与文件系统语义。
