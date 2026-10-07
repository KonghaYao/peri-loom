# Checkpoint 私有 Native probe：作者实验结果

1. **本次隔离实验 PASS / NOT REPRODUCED**：唯一一次 `both 15` 实际exit0，plain15＋encrypted15＝30cycles；92次公共Full checkpoint均成功，`RUN_END elapsed_ms=255 result=COMPLETED`，60s watchdog未触发。这不是独立验收或旧fixture根因证明。
2. 私有目录 `/private/tmp/peri-r9-checkpoint-repro-ChmWYmFO`（0700）；核心 `src/main.rs` 97行，helper46/52行。未修改root生产、manifest或旧测试；本报告是唯一root写入。未操作19447/19448、S3、UnknownDB或用户秘密。
3. seed1000 accounts、ledger、两个索引、4×16KiB假payload，包含明确COMMIT/ROLLBACK。每cycle：default-lane checkpoint→两个独立session由Barrier共同起步读/写→idle-session checkpoint→交替显式Close/Drop→checkpoint；全程无SQL retry或用sleep安排竞争。
4. Oracle核精确ledger业务ID、1000账户余额/总额、索引读、payload长度、integrity与成功checkpoint前后query digest。首次checkpoint Err设计为立即停止后续SQL/模式并保留原error chain；此次没有Err，因此没有实际error-chain或cleanup-failure覆盖。
5. encrypted使用P6现成公共helper源：自己MemoryAuthority/临时loopback broker生成测试credential/key，再经公共key provider取得key并调用实际 `RemoteDatabase::open_with_encryption_and_limits`。数据位于另一独立MemoryObjectStore；不是Authority RPC数据fencing、真实KMS或S3验证，生成key/token未输出。
6. 授权唯一builder：Rust1.98.1，private offline生成lock；与root lock比较仅新增本probe包身份。命令 `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target cargo +1.98.1 build --release --offline --locked --manifest-path /private/tmp/peri-r9-checkpoint-repro-ChmWYmFO/Cargo.toml --features encrypted -j 2` 实际exit0，24.15s；仅selected library/probe，无workspace/NAPI构建。日志有warning，本轮不是strict-clippy gate。
7. 唯一运行命令：`TMPDIR=/private/tmp/peri-r9-checkpoint-repro-ChmWYmFO/tmp /tmp/peri-loom-p6-dev/prototypes/v1/target/release/peri-r9-checkpoint-repro both 15`。255ms为整次probe记录，不含build；不是benchmark结果。
8. 编译/运行前后 `source-before.json` / `source-after.json` 完全相同，SHA256均为 `1fba4b095900dfb02f3a14b3eae62203da2378ad759b48399dc2dbb3ba2cfc32`。523个捕获输入涵盖原五私有路径、private lock/metadata/snapshot及六个resolved local packages的保守Rust/Cargo/C源集合；不是全仓任意文件不变声明。
9. source-map SHA256：`767a0bc921da6f24c9e9a0ebf0d40b2e4a48e4acab044f9f8568662be35b1fc6`；parent HEAD `9c3248efd75c9166261b405439481451ebae9537`，Native HEAD `ad892d4f113b2e62544bef48be6cebfd6e20da52`，均前后不变。
10. probe executable SHA256：`6f55f37863a04a0371901659e5692a12b361b85b818239c8199d4936f3d7a47f`。授权hot target的root CLI SHA256前后均为 `64ec5bd6e7b143b95151c9a52934c03f42a2a5396297f4cbe91c06de33c5f85e`，未被sidecar替换。
11. 原始 `native.log` SHA256：`048c6b987e11b30c15b9b0d32cb6cc3ddc9ce96ccc539a62ff253717e9a8267d`；`build.log` SHA256：`974b6ae5f6a82cd4b41f37a4d84e4aca154e95e376d2fda9e5bacf4b49ae16d5`。实际exit/次数/源比较保存在同目录 `result.json` 与 `run-exit.txt`，无取消冒称PASS。
12. **限制**：Barrier只保证共同起步，不证明特定native reader-lock overlap；没有刻意保持读事务、pending/failure注入或实际partial backfill。无法据成功digest解释旧未知操作、默认lane历史锁状态或真实backend时序；encrypted mock clock不证明lease expiry，trusted library不代表runtime容量/跨进程保证。
13. **后续**：必须经RootProbe独立验证再采用实验结论；根因仍未证明，不改算法、不扩大压力PASS。probe/builder已结束，不重复run，未commit/push。
