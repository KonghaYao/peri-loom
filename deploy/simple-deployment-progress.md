# Simple 部署实施记录

本次实施已由用户明确授权；`simple-deployment-design.md` 的历史草稿状态不再阻止本次实现。
现有 distributed 契约继续适用。Simple 是独立装配，只有本地可靠持久化保证。

## 验收清单

- [ ] 本地 WAL 提交、同步失败、checkpoint、强杀恢复及丢失未同步写验证
- [ ] SQLite 元数据：身份权限、数据库、审计、幂等、Operations、Jobs、备份、Panel
- [ ] 有界进程内宿主：会话、事务、取消、超时、流式背压、关闭与恢复
- [ ] 共享 HTTP / NDJSON / Hrana v2、v3 pipeline / v3 cursor
- [ ] 本地对象存储、一致性备份、恢复、整实例导出
- [ ] 单文件启动、实例锁、版本检查、初始化凭据、持久签名密钥
- [ ] 内嵌 Web、能力信息、不支持动作明确失败、同端口健康检查
- [ ] 文档、发布包装、Simple 验收、distributed 回归

## 实施前基线

- Git 起点：`0e5fa43`；工作分支：`feat/simple-deployment`。
- `cargo test -p engine-adapter --lib --locked --offline`：61 passed。
- macOS 全 workspace 构建受既有 Worker Linux pidfd 依赖限制。
- `raft-proto` 旧构建器仅识别 protoc 3.x；缓存的 protoc 3.9 可用于它的构建。
- 本机 Turso 0.8.1 源码提供 interrupt、progress handler、step、checkpoint；
  Runtime 旧注释不能作为引擎不支持取消的依据。

## 提交与验证

实现按元数据、宿主、存储、服务装配与验收分阶段记录。下方只记录实际完成的验证；
未运行、被环境阻断或仅有代码检查的项目不能标成通过。
