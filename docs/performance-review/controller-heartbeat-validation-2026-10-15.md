# F08 Worker 心跳持久化拆分验证（2026-10-15）

生产控制存储把 Worker 心跳的健康状态、负载和最后成功时间放在可重建的内存视图。`HeartbeatWorker` 与 Controller 轮询在生产存储上只更新该视图；不改 `metadata.json`、epoch floor 或 revision。Worker 身份、endpoint、draining、epoch、placement 与 pending intent 仍由原有同步持久化协议保护。控制变更需要提交时，先将当前内存视图合成候选状态，再以磁盘真实状态为 before image 写入原有恢复 intent；提交成功后内存与磁盘一致。

重启加载持久目录时，不把上次进程的健康状态当成新心跳：所有已注册 Worker 在内存视图中先标为未验证。已有 active placement 时，Platform 在报告 ready 前只探测这些 placement 所需的 Worker；后台 Reconcile 继续常规探测。过期探测和旧 endpoint 的响应不能覆盖较新的观测，即使中间发生过控制状态提交。

新增测试验证纯心跳后 `metadata.json` 原始字节和 revision 不变、当前健康状态可见、drain 控制变更仍可持久恢复、重启不能凭旧心跳选择 Worker、过期及旧 endpoint 响应不覆盖新状态，控制提交后也保留探测时间水位。显式 `HeartbeatWorker` 对回退时间仍返回错误；后台迟到探测被忽略。启动定向探测只访问指定 Worker；Platform 的密钥重封装重启 SQL 用例通过。Controller 定向测试和相关 `go test -race` 通过；Controller 全套 `go test -count=1 -timeout 10m ./...` 主包耗时 41.788 秒。最终代码上的 Platform 全套 `GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 主包耗时 165.511 秒，全部子包通过。

这只移除了纯心跳的同步目录提交。Reconcile 每轮仍扫描全部 placements；其他控制更新仍克隆并持久化完整目录。T01/T05 冻结负载规模、控制 CPU、路由 p99 和恢复历史 GET 数据尚未取得。
