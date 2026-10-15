# F03 Gateway 窄读验证（2026-10-15）

生产 `stateStore` 从已验证状态建立凭据摘要索引。凭据验证按摘要读取单个密钥；授权读取当前密钥、项目、数据库和去掉 envelope 的 descriptor。密钥撤销沿用持久化记录，成功提交后下一次验证与授权立即读取到撤销状态。

普通 Gateway HTTP SQL 请求现在只读取所属项目配额。全局管理员打开数据库时在同一状态锁内读取目标数据库及其项目；其余管理员请求先验证已有 session 的身份与 key，再读取该 session 的项目。相关请求不再调用完整 `State.Snapshot()`；测试用 StateStore 若不实现私有窄读接口，仍可使用原 Snapshot 路径。

`TestStateStoreNarrowAuthSeesCommittedRevocationWithoutFullSnapshot` 和 `TestGatewayRequestUsesNarrowProjectRead` 使用拒绝全量 Snapshot 的代理，分别检查鉴权、撤销和 HTTP 请求路径。`TestGatewayGlobalAdminSQLUsesDatabaseProjectAndReauthorizes` 保留跨项目管理员配额、会话及撤销语义。上述 Gateway 路径的定向 race 检查通过。

此验证仅覆盖 F03 的读侧。operation 写侧的独立持久区见 [故障模拟](./operation-sidecar-validation-2026-10-15.md)。异常与关闭路径仍可能取完整 State；审计同步写、追加 operation 日志、group commit 和 T03 固定负载对比仍待完成。
