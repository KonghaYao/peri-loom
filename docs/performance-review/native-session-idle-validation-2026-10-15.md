# F07 原生会话空闲清理验证（2026-10-15）

Gateway 为原生 SQL session 记录最近一次请求完成时间，默认空闲期限为 5 分钟。服务中的单个后台循环每秒检查一次；过期清理最多 16 路并发，慢会话不挡住其他项目的健康会话。每项清理尝试取得该 session 锁，使用既有持久 operation 协议向原 Worker 发出 `close_session`，等待明确的 `closed=true` 回执与 operation 结算，然后从 Gateway 注册表移除会话并归还 session 配额。使用中的 session 本轮跳过。密钥已撤销时仍可执行服务端清理；Hrana 创建的会话由内部 context 标记，只使用 Hrana 原有的 idle timer。

关闭响应不确定或 operation 结算不确定时，Gateway 保留并隔离会话及占额，停止自动重试；不能把未知关闭当作已释放。前置可确认拒绝仍可在后续调度重试。后台循环随服务 context 取消并在服务结束前 join。

定向测试验证：额度在明确 close 后才归还、活动刷新期限、密钥撤销后的清理、使用中的 session 不被并发关闭、未知 close 保留且不重放、Hrana 会话不受原生扫描器影响、慢 A 清理不阻塞另一项目 B，以及后台循环能执行清理并随取消退出。相关 `go test -race` 通过。最终代码上的 Platform 全套 `GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 通过，主包耗时 177.969 秒。

本轮尚未完成 T09 的真实 Worker 失联、open 回执丢失、长期负载及 TTL+5 秒外部测量。Worker 进程内 native session 的独立清理、空闲数据库自动卸载和同库唤醒合并也尚未实现；F07 仍为部分实现。
