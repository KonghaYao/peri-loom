# 控制台合并验收（2026-10-14）

基线：`main` 已快进合并 `feat/console-redesign`，以 `/Users/konghayao/Downloads/peri-loom-ui-redesign.md` 的 UX-01～UX-24 为检查范围。本轮按要求**不启动浏览器，不进行真实浏览器交互验收**。源码检查和协议测试不能替代真实服务、键盘或多视口演示。

## 自动检查

| 检查 | 结果 |
|---|---|
| `bun install --frozen-lockfile`、`bun run typecheck`、`bun run test`、`bun run build` | 通过；产物重建，控制台协议与路由/分页测试通过。构建提示 JS chunk 超过 500 kB。 |
| Go 平台已跟踪测试（排除主工作区预存的未跟踪测试） | 243 项通过；新增嵌入资源测试单独通过。 |
| Go Controller `go test -buildvcs=false ./...`；Go 平台 `go build -buildvcs=false ./cmd/peri-platform ./cmd/peri` | 通过。 |
| TypeScript SDK `bun run build`、`bun run test` | 30 项通过；构建使用前端依赖中的 TypeScript。 |
| `cargo fmt --manifest-path prototypes/v1/Cargo.toml -- --check` | 通过。 |
| Go 嵌入资源 | `AdminHandler` 可返回 `/`、`/console.js`、`/console.css`，首页指向两项实际静态资源且有 CSP；没有运行浏览器。 |
| CI 配置 | 改为锁定 Bun 安装与构建前端，新增 Go 嵌入资源 smoke；YAML 已解析，GitHub Actions 本身未运行。 |

全量 `go test -buildvcs=false ./...` 唯一失败项是主工作区**合并前已存在、未跟踪**的 `prototypes/v1/platform/publish_image_visibility_test.go`，其 `TestPublishImageInvalidatesAlreadyLoadedWorker` 断言未通过；未修改或加入本次提交。Rust 全量测试独立失败于 `tests/p2_verify.rs` 的 `last_reader_handle_protects_lazy_segment_and_corruption_is_not_hidden`；过滤它后又失败于 `tests/p4_verify/leases.rs` 的 `nonce_binding_exact_deadline_and_receipt_payload_conflict_are_fail_closed`，继续过滤后失败于 `tests/statement_worker.rs` 的 `statement_v2_owner_state_and_cleanup_ack`。前两项单独复测仍失败。本次合并没有修改 Rust 源码或测试；这些失败需要单独排查，不能据此宣称全量回归通过。

## UX 合同核对

“代码具备”仅指接口/布局路径可从源码和构建确认，**不是**浏览器或真实服务验收。

| 编号 | 状态 | 证据或剩余条件 |
|---|---|---|
| UX-01～04 | 代码具备，未实测 | 首页目录、创建表单、表预览、连接弹窗及无凭证地址均已实现。 |
| UX-05 | 代码具备，未实测 | 分支来源随当前分支传入，资源配额按未删除的分支计数。 |
| UX-06～07 | 部分 | 快照/时间点恢复有对象确认和 lossless 整数路径；尚无真实服务的 PITR 乱序/大整数恢复流程测试。 |
| UX-08～10 | 部分 | `describe`、`statement`、写入确认、会话阻断路径存在；现有协议测试主要覆盖旧协议模块，尚无新 React 页面事务/未知结果流程测试。 |
| UX-11～12 | 部分 | 同 namespace 操作记录会触发隔离提示，表预览防止并发选择；未执行 20 次延迟乱序切换试验。 |
| UX-13～15 | 部分 | 管理/SQL key 分离，一次性展示及缺少 key 列表的说明存在；服务端尚无 `/v1/me` 和完整 key 列表接口，角色矩阵未实测。 |
| UX-16 | 未通过 | 只读请求失败会显示错误，但没有统一的旧值保留与“更新失败/过期”状态。 |
| UX-17～19 | 未验收 | 按要求不测真实浏览器；视口、焦点/键盘、文本缩减率及点击耗时均不可标记为达标。 |
| UX-20 | 部分 | 1,000 分支分页单测通过、每页最多 100 行、SQL 首屏最多 200 行；SQL 大结果尚未接 cursor 分页。 |
| UX-21 | 代码具备，未实测 | 没有伪造 P95、历史曲线、完整审计日志和地域发现接口。 |
| UX-22 | 部分 | Kumo 2.14.0 官方样式、Button/Input/Table/Badge/Dialog/Tabs 已锁定；部分选择器仍为原生 `select`。 |
| UX-23 | 代码具备，未实测 | 删除明确是分支，目标需输入 `database/branch`，不会承诺重用名称。 |
| UX-24 | 未通过 | 类型检查、协议测试及静态嵌入通过；真实浏览器流程未按前置要求执行，全量 Go/Rust 测试存在上列失败。 |

结论：**代码合并与可自动验证的前端构建/嵌入链路通过；整份设计文档尚不能宣称统一验收通过。** 后续需先补读请求过期态、SQL cursor 和官方 Select，并在获准浏览器验收后验证 UX-07～19、UX-22～24 的真实交互；独立排查三项 Rust 失败及未跟踪 Go 测试。
