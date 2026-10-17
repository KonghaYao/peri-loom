# 02 执行顺序与命令身份

所有 cargo 命令均从 identity.json 的 lab/prototypes/v1 执行，环境固定 `CARGO_BUILD_JOBS=2 GOMAXPROCS=2 CARGO_TARGET_DIR=../../target-02`；不安装依赖，不改共享业务代码，不改 Turso。完整工具版本、最终 lab 源码 SHA-256 与 Cargo.lock hash 见 02-environment.json。

1. 读取 experiment-prompt.md、00_plan、identity、01_report、source-challenge；01_verification 当时不存在，后来正式完成后补读。冻结源码来自 c8990c4，Turso 为 identity 中固定版本。
2. `cargo test --manifest-path Cargo.toml --locked --offline --lib a_head_is_decoded_once_per_document -- --nocapture --test-threads=1`：退出 0，输出 02-build-initial.log。首次离线 dev 构建约 1m33s，只运行既有 H6 断言。期间在 evidence 写实验草稿，没有挂载进 lab，故初始二进制仍是原冻结 lib tests。
3. 编译前将草稿 MemoryObjectStore::new 改为 default，将负向观察窗口 300ms 改为 1s。草稿从未编译/运行；归档的是首次实际编译的最终测试字节。执行 `python3 .../02-prepare.py` 将三个测试模块追加进 lab。已存在模块时脚本不重复追加。
4. `cargo test --locked --offline --lib rw02_ -- --nocapture --test-threads=1`：退出 0，4 tests PASS；02-frozen-raw.log。编译约 6s，测试合计约 4s。这些时间只是工具输出，未作为性能结果。
5. 从共享 git 对象只读导出 `git show 1198b18 --format= -- prototypes/v1/src/io/files.rs` 到 02-main-pread.patch，并在 lab `git apply <该文件>`，退出 0。没有 checkout/合并其余 HEAD 文件。补丁只移动 pread Shared lock 并删除重复 fill，加注释；共 5 added / 2 removed lines（7 行变更）。
6. `RW02_PATCHED=1 cargo test --locked --offline --lib rw02_file_lock_and_controls -- --nocapture --test-threads=1`：退出 0，1 test PASS；02-patched-raw.log。测试源码与第4步字节完全相同，只改变业务 pread 补丁和显式环境期望。
7. 依次单独执行相同 cargo 命令 filter `a_rejected_append_leaves_the_validated_prefix_reusable`、`an_overwriting_append_forces_the_next_one_to_re_derive_the_prefix`、`a_head_is_decoded_once_per_document`：均退出 0；02-existing-controls-raw.log。此时仅 pread 业务文件有上一步小补丁；recovery/Authority 仍为冻结实现加测试，三个既有断言未改。
8. `python3 .../02-archive.py`：退出 0，归档最终完整模块、测试补丁、21条 JSON 观察、工具版本和 hash。最终二进制对应 patched pread；frozen raw 对应同测试+原 pread。日志保留原行号，不因后续文档编辑重写日志。

源码查看中有两次 `sed` 猜测路径失败（`src/deadline.rs`、`core/io/completion.rs` 不存在），改读实际 `src/session/budget.rs`、`core/io/completions.rs`，没有构建或测试失败。首次 shell glob 查找 target* 无匹配，不影响显式独立 target-02。初次读取 01_verification 尚不存在，已补读正式文件。所有实际实验 cargo 和 patch 命令成功，无丢弃失败样本。

复现入口 02-run.sh 按同样次序重新构建/运行，默认将新日志写新临时目录，不覆盖本次原始证据。01 与02实验构建不并行；02各 cargo 命令也串行。
