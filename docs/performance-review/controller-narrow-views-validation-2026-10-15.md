# F08 控制目录窄读验证（2026-10-15）

生产 `diskStore` 增加同一状态锁下的单库 placement+Worker 视图、单 Worker 视图和 Worker 集合视图。Gateway SQL 路由只读取目标 placement 与 Worker；Controller 每个活跃 placement 的健康检查只读目标 Worker，Worker 选择和心跳轮询只复制 Worker 集合。测试用 `MetadataStore` 若未实现窄读能力，仍走原有 `Snapshot` 兼容路径。

`ReadRouting` 返回的 Grant 和 PendingCommand 与存储内存隔离，避免调用方修改持久状态；路由仍逐字段检查数据库、grant、epoch、role、worker 健康和 draining 标志。定向测试用拒绝全量 `Snapshot` 的代理验证 Gateway 开库和控制存储窄读；两个新增测试的 `go test -race` 通过。

Controller 全套 `go test -count=1 -timeout 10m ./...` 通过，主包耗时 44.407 秒。Platform 全套 `GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 通过，主包耗时 188.645 秒。

本项只消除了逐活跃库和逐 SQL 路由的全目录克隆。后续纯心跳写盘拆分见 [心跳验证](./controller-heartbeat-validation-2026-10-15.md)。Reconcile 每轮仍扫描完整 placements；Rust Authority 的 epoch floor 历史扫描、T01/T05 的 10k 冷库与历史规模验收也未完成。
