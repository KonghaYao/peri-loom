# F03 operation 持久区模拟验收（2026-10-15）

被测提交：`8517894`（`perf/authority-paged-receipts`）。本轮在本机临时目录模拟注册、结算、重启、目录提交中断、旧格式迁移、持久区缺失和持久区写入失败。所有断言使用生产 `stateStore` 与真实私有文件持久化，未启动 Docker 或注入 SQL 负载。

| 能力 | 结果 | 证据 |
| --- | --- | --- |
| operation-only 提交不改写 `state.json` | 通过 | `TestOperationSidecarDoesNotRewriteRegistry` 比较提交前后原始字节，并重启检查 in-flight 变为隔离 |
| 较新目录在 sidecar checkpoint 中断后前滚 | 通过 | `TestOperationSidecarRegistryCheckpointWinsAfterCrash` 构造旧 base revision sidecar，重启后保留已结算记录 |
| sidecar 缺失或写入失败时拒绝服务 | 通过 | `TestOperationSidecarMissingFailsClosed`、`TestOperationSidecarWriteFailurePoisonsBeforeDispatch` |
| 旧版目录迁移 | 通过 | `TestOperationSidecarMigratesLegacyRegistry` 保留 operation 并二次重启 |
| 既有注册表 pending intent 与 Unknown 保护 | 通过 | `TestDurableRecovery*`；夹具先把 operation checkpoint 到目录，再注入目录写入中断 |

完整 Go 套件：`GOMAXPROCS=2 PERI_RUST_BINARY=.../target/debug/peri-loom-v1 go test -count=1 -p 1 -timeout 10m ./...`，platform 及其子包均通过；platform 运行 210.667s。新增边界与并发：`go test -count=1 -race -run '^Test(OperationSidecar|StateLockReopenAndConcurrentRevisions)' .` 通过。

**范围结论：** 上述故障路径具备本机模拟证据。此测试不证明 F03 整体性能验收通过；仍缺真实 SQL 压测、T03 的 C=1/8/32/128 时延与资源数据、追加日志及组提交屏障。之前 Docker/S3 的生产恢复模拟使用更早源码及本地镜像，不能当作本提交的容器验收结果。
