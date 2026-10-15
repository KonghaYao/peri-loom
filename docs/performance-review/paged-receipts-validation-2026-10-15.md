# Authority 分页回执与历史遍历局部验证（2026-10-15）

## 被测范围

分支 `perf/authority-paged-receipts`：v1/v2 root 启动时迁到 v4，泛用回执保持最多 128 条在线近期窗口，旧回执写入最大 16 KiB 的不可变索引页；历史校验的 Walker 不再保留所有原始元数据字节，仅记住最多 128 个已验证身份。ownership 命令回执、历史点、对象目录、pins 和 tombstones 仍在完整 root；此记录不证明 F01/F02 完成。

## 本地与跨语言回归

| 命令/场景 | 实际结果 |
| --- | --- |
| `cargo test --manifest-path prototypes/v1/Cargo.toml --locked --quiet -- --test-threads=2` | 退出 0；47 条摘要合计 539 次 passed、0 failed、13 次 ignored 计数。嵌套摘要会重复计数；新增 ignored S3 迁移用例是在这次全套之后加入并单独执行。 |
| `cargo test --test p4_compat --quiet` | 5/5；100 条旧回执迁移后在线 root 小于原来一半，旧回执可查；256 次后窗口有界，归档回执可查，已发布页损坏拒绝快照和重开。 |
| `p4_completion::delayed_backend_sql_setup_preserves_original_budget` | 1/1；40 ms 后端延迟下仍在原 5 秒 SQL 预算内完成。每次事务都写页的中间实现曾使该测试超时，因此改为有界近期窗口。 |
| 平台 Go：`GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` | 平台包 PASS（170.804 秒），CLI 和 Go SDK 子包 PASS；二进制 SHA-256 `462626ba6fab1a7b4f5c27d9b57234efa68b8d96c0d55b1e1dda446ae92535a3`。 |
| Controller Go：`go test -count=1 -p 2 -timeout 10m ./...` | Controller 包 PASS（34.137 秒）。 |

第一次 Rust 全套在过期门禁测试中遇到旧格式固定写入计数断言；测试改为核对写入类别和最终 root 未发布。随后全套在默认 5 秒预算的 `runtime_resources` 大 WAL 用例重复超时；Walker 去掉累计原始字节后，该用例单项 PASS（21.07 秒），再跑全套退出 0。这是本机结果，不能据此推导 T05 的 100k 历史 p95 或内存目标。

## 隔离对象存储

使用新建 `peri-v4-s3-0d77f493fae6` 容器和专用卷，镜像 `rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`，容器仅绑定 `127.0.0.1`，限制 0.5 CPU、512 MiB、64 PID。`PERI_S3_TEST=1`、`PERI_S3_TEST_RETAIN=1`；测试各自创建随机桶，不访问现有服务。三项均使用 `--test minio <精确用例> -- --ignored --exact --test-threads=1 --nocapture`，每次恰好 1 passed、0 failed：

1. `p4::network_authority_fences_two_remote_workers_and_preserves_restored_snapshot`：远端 Worker fencing、恢复点与只读代际。
2. `p2_s3_authority_refs_pitr_and_gc_with_empty_worker_cache`：引用、PITR、GC 与空缓存恢复。
3. `p4::network_legacy_receipts_migrate_to_bounded_root`：S3 上 100 条旧回执批量迁移，在线 root 缩小，旧回执逐项可查。

首次尝试使用了无模块前缀的 exact test 名，结果为 **0 tests**；第二次因测试 access key 含连字符，被配置校验拒绝，均没有形成业务测试结果。改为正确用例名和合法的独立测试凭据后，以上三项通过。自有容器已正常停止（退出 0），桶与卷按保全设置留存；本报告不包含凭据或 SQL 探针原值。

## 仍未证明

归档边界每批 64 条的前台延迟、不可变页垃圾回收、真实云 S3/多宿主、长时间续租、分支竞争、累计 100k 历史及 T01–T12 冻结容量门槛均未验收。完整 root 与累计 WAL index 仍可能随历史增长；不能将局部单测和本地 RustFS 结果称为生产能力通过。
