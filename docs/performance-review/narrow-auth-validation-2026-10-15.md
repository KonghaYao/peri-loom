# Gateway 鉴权窄读局部验证（2026-10-15）

生产 `stateStore` 在打开时从已验证状态建立凭据摘要索引，只在新增 API key 的成功提交后增量加入索引；撤销沿用既有密钥记录，不缓存撤销状态。凭据验证在状态锁内按摘要取单个密钥。授权在同一锁内复制单个密钥、目标项目和数据库及其 key descriptor 的必要字段；返回的 descriptor 不含加密 envelope。操作日志和非热点目录不进入这次只读结果。

专门的测试代理在 `Snapshot()` 被调用时返回错误，以核实凭据验证、身份授权和 Gateway 授权均走窄读路径；实际 `stateStore` 上撤销提交后，下一次凭据验证与已有 principal 的授权都立即失败。既有 bootstrap、角色、数据库 token 与撤销测试也通过。该实现不改变持久化格式和密钥撤销的持久化屏障。

`GOMAXPROCS=2 PERI_RUST_BINARY=<当前 debug 二进制> go test -count=1 -p 1 -timeout 10m ./...` 退出 0，平台包耗时 263.266 秒，CLI 与 SDK 子包通过。窄读撤销和数据库 token 撤销用例的 `go test -race` 通过。

这只解决 F03 的读侧局部成本。`registerOperation` 和 `settleOperation` 仍对完整 State 做克隆、校验与持久化，正常 SQL RPC 的同步 Sync 次数、逻辑写字节和审计锁成本尚未下降。尚无固定查询负载下的 alloc/op、JSON 编解码量、状态锁等待与目录规模对比数据；不能据此称 F03 完成。
