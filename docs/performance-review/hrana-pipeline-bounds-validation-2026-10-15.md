# Hrana pipeline 聚合边界局部验证（2026-10-15）

F11 的可选 Hrana HTTP pipeline 已有总步骤 64 的派发前检查。本次加入单 pipeline 8 MiB 聚合 JSON 结果上界和 30 秒总执行时限。batch 每步检查新增结果大小，达到上界立即停止后续 SQL；超限响应携带 `partial_execution_possible` 和 `retryable=false`。总时限使用请求上下文取消当前内部 SQL 调用，响应另标记 `outcome_unknown=true`。新建而未返回 baton 的会话进入既有异步清理流程；已有 baton 的会话仍由原生命周期管理。

本地 Go fixture 在提高单条结果配额后，让一个三步 batch 每步返回约 5 MiB：前两步派发，第二步触发聚合上界，第三步未派发；HTTP 413 返回 `pipeline_result_limit`、可能部分执行且不可自动重试。另一测试使第二步在总时限到期时仍在执行：HTTP 504 返回 `pipeline_timeout`、可能部分执行、结果未知且不可自动重试。这两个测试的 `go test -race` 也通过。

`go test -count=1 -run '^TestHrana' .` 通过；`GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 退出 0，平台包耗时 231.625 秒，CLI 与 SDK 子包通过。Go 改动没有触及 Rust；这里的全套结果不代表 T03/T10 的 1/8/32/64 steps 容量实测。

限制：8 MiB 是当前全局单 pipeline 上界，尚未按租户输出配额收紧；没有跨请求分页或背压，也没有固定负载下的峰值内存、取消回收与跨库 writer 公平性数据。当前错误字段是 Hrana HTTP 扩展，客户端仍须遵守不可自动重试的未知/部分执行语义。
