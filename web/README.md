# Peri Loom Web · DBA / Developer Panel

基于自建 TursoDB Engine 的 DB Platform 控制台（架构 §4.5 / §17.1 / §17.14）。
技术栈冻结：**TypeScript + Vite + React + Ant Design**；中文界面。

浏览器与 API 默认**同 Origin**：开发时由 Vite dev proxy 转发，生产由 nginx 反代，因此不依赖宽泛 CORS。

```text
/api/*   -> http://127.0.0.1:8080（dev proxy） / http://db-server:8080（nginx）
/data/*  -> 同上（NDJSON 流式，nginx 已关闭 proxy_buffering）
```

---

## 快速开始

```bash
npm install         # 走 web/.npmrc 配置的 npmmirror 镜像
npm run dev         # http://127.0.0.1:5173 ，/api 与 /data 代理到 127.0.0.1:8080
npm run build       # tsc --noEmit && vite build，产物在 dist/
npm run preview     # 预览构建产物
npm run typecheck   # 仅类型检查
npm run gen:api     # 由 openapi.json 生成类型参考代码（见下）
```

Docker：`web/Dockerfile` 执行 `npm install -> npm run build -> dist/ 拷入 nginx`，因此 **package.json 必须保留 build 脚本**。

---

## 目录结构

```text
web/
├── .npmrc                     # 国内镜像（勿改 registry）
├── Dockerfile                 # Vite 构建 + nginx 运行
├── nginx/default.conf         # SPA + /api /data 反代（/data 关闭 buffering）
├── index.html
├── vite.config.ts             # dev proxy：^/api/、^/data/ -> 127.0.0.1:8080（正则，避免 /databases 被误代理）
├── orval.config.ts            # gen:api 配置，input = ./openapi.json
├── tsconfig.json              # strict + noUnusedLocals/Parameters
├── scripts/gen-api.mjs        # gen:api 预检（缺契约时给出导出指引）
└── src/
    ├── main.tsx               # React Query + Router + AuthProvider
    ├── App.tsx                # 登录前后路由分叉、按偏好注入明暗主题
    ├── styles.css             # 饱和度条 / SQL 编辑器 / 结果单元格样式
    ├── api/
    │   ├── client.ts          # ★ 手写运行时 client（fetch + NDJSON 流式），不依赖生成代码
    │   ├── types.ts           # 契约类型（管理面 / 数据面 / 操作 / Worker / Token …）
    │   ├── errors.ts          # 错误码 -> 中文提示、retryable 判定
    │   └── generated/         # orval 生成物（gitignore，仅类型参考）
    ├── hooks/
    │   ├── useAuth.tsx        # Bearer Token 上下文（localStorage）
    │   ├── usePreferences.tsx # /panel/preferences/{key} 读写（乐观更新）
    │   ├── useOperationQuery.ts   # 长操作轮询（终态自动停止）
    │   └── useSubmitOperation.ts  # 202 提交 -> operation_id -> 进度弹窗
    ├── layouts/AppLayout.tsx  # 侧边导航 + 顶栏（主题切换 / 退出）
    ├── components/
    │   ├── SaturationBar.tsx  # 饱和度条：70~75% 目标区间 + 80%/90% 水位线
    │   ├── SqlEditor.tsx      # textarea + 行号，Ctrl/Cmd+Enter 执行
    │   ├── ResultTable.tsx    # 结果表格（分块渲染）
    │   ├── OperationModal.tsx # 长操作进度弹窗
    │   ├── ErrorAlert.tsx     # 错误码中文提示 + retryable 重试按钮 + request_id
    │   ├── StatusTag.tsx / JsonBlock.tsx
    │   └── database/DatabaseModals.tsx  # 创建 / 迁移 / 恢复弹窗
    ├── pages/                 # 见「页面清单」
    └── utils/                 # format / saturation / worker / database / rows 归一化
```

---

## 页面清单

| 路由 | 页面 | 说明 |
| --- | --- | --- |
| `/login` | 登录 | 账号密码（`POST /api/v1/auth/login`）；接口未实现（404/405/501）时自动降级为**粘贴 Token** 模式 |
| `/dashboard` | 概览 | DB 总数与状态分布、Worker 数量与饱和度（目标区间/水位线）、最近操作 |
| `/databases` | 数据库列表 | 分页、状态过滤、关键字筛选、创建、启动/停止/重启/迁移/快照/备份/恢复/删除（二次确认 + 进度弹窗） |
| `/databases/:dbId` | 数据库详情 | 基本信息、生命周期、路由（worker/epoch）、快照列表、慢查询 |
| `/sql` | SQL 控制台 | SQL 编辑器、执行、结果表格、影响行数/耗时/错误；**大结果集 NDJSON 流式 + 分块渲染**；显式会话模式（BEGIN/COMMIT/ROLLBACK） |
| `/workers` | Worker 管理 | 状态、CPU/内存/进程进度条、饱和度着色排序、Drain（二次确认 + 进度） |
| `/workers/:workerId` | Worker 详情 | 容量/用量、饱和度水位、运行中的数据库、Drain |
| `/operations` | 操作中心 | 操作列表与详情（operation_id 可复制、状态、进度、错误） |
| `/audit` | 审计日志 | 分页表格 + 当前页关键字筛选 + 明细展开 |
| `/settings` | 设置 | API Token 管理（明文只显示一次）、Panel 偏好、Saved SQL 管理 |
| `*` | 404 | — |

---

## 关键实现约定

### 1. 手写 runtime client（`src/api/client.ts`）

运行时不依赖任何生成代码：

- `Authorization: Bearer <token>`（登录后存 `localStorage`），401 时清 token 并广播 `peri-loom:unauthorized`，由 `AuthProvider` 跳回登录页；
- 统一错误体 `{ error: { code, message, request_id, retryable } }` -> `ApiError`，按 `code` 映射中文提示（`src/api/errors.ts`），`retryable=true` 或 5xx 时 `ErrorAlert` 显示**重试**按钮，同时展示 `request_id` 便于对照服务端日志；
- 长操作：任何返回 `202 { operation_id, state }` 的接口都用 `useSubmitOperation` + `OperationModal` 轮询 `/operations/{id}`（1s 一次，终态自动停止）。

### 2. NDJSON 流式（> 1000 行的场景）

SQL 控制台默认开启「流式」：

```text
POST /data/v1/databases/{db_id}/query
Accept: application/x-ndjson
-> {"columns":[...]} / {"rows":[...]} / {"affected_rows":..,"wal_lsn":..,"elapsed_micros":..}
```

- `fetch` + `ReadableStream` + `TextDecoder` 逐行解析，**跨 TCP 分片保留行缓冲**；
- 边收边渲染：按 120ms 节流刷新，表格**分块渲染**（默认 200 行，「继续渲染」按钮递增），避免一次性塞入 DOM；
- 客户端接收上限 50 000 行，超出即中断流并提示加 `LIMIT`（服务端不缓存完整结果集，客户端同样不无上限接收）；
- 后端未启用 NDJSON 时自动回退普通 JSON 模式并提示。

### 3. 显式会话

`POST /data/v1/databases/{db_id}/sessions` 打开会话后，语句改走 `POST /data/v1/sessions/{id}/query`，
提供 BEGIN / COMMIT / ROLLBACK 按钮；关闭或切换数据库、组件卸载时 `DELETE /sessions/{id}` 回收。

### 4. 偏好

每个 key 独立存储：`GET/PUT /api/v1/panel/preferences/{key}`（404 = 未设置）：

```text
panel.theme / panel.page_size / panel.sql_stream / panel.table_density
panel.default_database_id / panel.sql_editor_height
```

写入为乐观更新，失败回滚。

### 5. 饱和度水位

`饱和度 = max(CPU, 内存, 进程数)`（最紧张的维度决定可调度性）：

```text
< 70%  偏低（蓝）   70%~75% 目标区间（绿，条上高亮带）
75%~80% 偏高（青）   ≥ 80% 预警（橙，水位线）   ≥ 90% 危险（红，水位线）
```

---

## OpenAPI 类型生成（`npm run gen:api`）

契约由 **db-server 导出**，仓库不提交 `openapi.json`：

```bash
db-server --dump-openapi > web/openapi.json          # 离线导出
curl -s http://127.0.0.1:8080/api/v1/openapi.json > web/openapi.json   # 运行时导出
npm run gen:api                                      # orval -> src/api/generated/
```

- 生成物在 `src/api/generated/`（已 gitignore / tsconfig exclude），**仅作类型与契约参考**；
- 运行时一律使用手写的 `src/api/client.ts`；
- 缺少契约文件时 `gen:api` 会打印上述导出指引而不是抛 orval 的原始报错。

---

## 未实现 / 精简项

- **后端尚未落地**：`services/db-server/src/main.rs` 目前是占位实现（仅打印一行），因此本 Panel 只完成了契约对接与前端逻辑，未做真实端到端联调。
- 登录页的账号密码入口依赖 `POST /api/v1/auth/login`（契约未冻结），未实现时自动降级为粘贴 Token。
- 审计日志的契约只冻结了 `limit/offset`，操作者/动作等筛选在**当前页**做前端过滤（表格标题已注明）。
- 数据库列表的关键字搜索同样是当前页过滤（契约只提供 state / tenant_id 过滤）。
- Worker 详情「运行中的数据库」优先使用 `/workers/{id}` 返回的 DB 列表；后端未返回时回退为 `/databases` 首页按 Owner Worker 过滤（分页 200 条）。
- 概览的状态分布优先用「state 过滤 + total」精确计数；后端忽略该参数时退化为按前 200 条本地聚合并标注。
- SQL 编辑器为「textarea + 行号」（按需求不引入 Monaco/CodeMirror），无语法高亮与自动补全。
- 未实现：Active Query 实时列表、用户/角色管理、故障切换（Failover）操作入口——契约内暂无对应接口。
- 未做前端单测（无 Vitest/Jest 依赖）；已用脚本冒烟验证 NDJSON 解析与错误映射（见 REPORT）。
