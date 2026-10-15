# F08 可处理 placement 视图验证（2026-10-15）

生产 `diskStore` 在每次成功的控制目录提交后维护一份可处理 placement 的键索引，并在重启加载后从已校验的持久目录重建。`suspended` placement 保留在持久目录、`Snapshot` 和单库 `Status` 中，但不进入每轮 `Reconcile` 的工作视图。`active`、迁移和待收敛阶段仍进入视图。视图在存储锁内一次复制所需 placement 与 Worker 心跳，返回的 Grant 和 PendingCommand 与内部状态分离。单库 `Status` 改为窄读，不再克隆全目录。

定向测试覆盖 128 个暂停库与活跃、迁移库混合目录，检查状态提交、删除、重启和恢复活跃后的索引结果，并检查返回对象不别名内部 Grant。另一个测试以拒绝完整 `Snapshot` 的探针执行含暂停库的 `Reconcile` 和 `Status`，确认生产窄读路径；相关 race 测试通过。Controller 全套 `go test -count=1 -timeout 10m ./...` 通过，主包耗时 35.469 秒。Platform 全套 `GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 通过，主包耗时 174.516 秒。

本机微基准固定为 10,000 个暂停 placement、32 个活跃 placement、1 个 Worker，`darwin/arm64` Apple M5 Pro、`GOMAXPROCS=2`，命令为 `go test -run '^$' -bench '^BenchmarkReconcileViewWithColdCatalog$' -benchmem -benchtime=1s -count=1 .`：

| 单次读取 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 完整 `Snapshot` | 28,067,263 | 12,965,741 | 120,697 |
| 可处理库视图 | 5,333 | 9,975 | 36 |

该比较只测 Controller 内存读视图，未包含网络、Authority、Worker、持久写入或轮询。控制变更仍克隆并持久化完整目录，Worker 轮询仍遍历注册 Worker；因此不能据此判定 F08 或 T01/T05 整体完成。下一步需隔离冷/热库规模，测真实 Reconcile 周期的 CPU、内存、路由 p99 与恢复历史 GET，并处理控制写入的全目录成本。
