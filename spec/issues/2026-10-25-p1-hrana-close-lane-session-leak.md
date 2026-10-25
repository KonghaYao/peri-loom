# P1：Hrana 并发 close 被 429 拒绝并永久泄漏 session seat

**状态**：已确认，待修复。
**优先级**：P1。
**类型**：错误。
**创建日期**：2026-10-25。
**来源**：GitHub issue #9（用户报告，v1.1.13 镜像）。
**确认方式**：explorer 只读确认 + 主 agent 在真实本地栈复现（基线 ffcc37b，v1.1.13）。

## 确认证据

真实本地栈（`dev.py`，`PERI_HRANA_EXPERIMENTAL=1`，默认配额）复现：

- 2 个并发 Hrana v2 pipeline，每个 `[execute select 1, close]`，连续 5 轮：
  每轮稳定 1×200 + 1×429 `quota_exceeded`，拒绝原因 `close_lane_busy=5`；
  每轮后 `sessions` +1（5 轮后 5），`active_permits=0`、`unknown_held_permits=0`。
- 8 并发、每轮 `[execute select 1, close]`：`sessions` 单调增长（5→7→9→12→…→15），
  `rejection_reasons_total={close_lane_busy:…, concurrency_limit:…}`；随后出现 `session_limit:50`。
- 泄漏后串行请求持续 429；等待 40s 不恢复（native 空闲回收跳过 Hrana session，且该 session 没有 baton，
  Hrana stream 空闲计时器也不存在）。只有重启进程能清空 `gateway.sessions`。
- 无并发、有可用 session seat 时，单请求 `batch`（10 条 `select 1`）+`close` 串行 100 次全部 200；
  一旦 seat 被前述泄漏占满，同类请求即报 429。用户报告的 45/100 成功率是同一泄漏的累积结果。

关键代码路径：

- `prototypes/v1/platform/admission.go:181-185`：`control` 通道硬编码 `state.control >= 1` 即拒绝，
  拒绝原因 `close_lane_busy`（`admission.go:39-40`）。`max_concurrent` / `max_sessions` 与该分支无关。
- `prototypes/v1/platform/gateway_sessions.go:315-326`、`gateway_cleanup.go:100-132`、
  `gateway.go:237-241`：close、清理重试、关停 drain 都走这条唯一 close lane。
- `prototypes/v1/platform/hrana_http.go:268-282`：新流（无 baton）先 `/v1/sql/open`；
  `hrana_http.go:472-475` 才注册 stream。close 失败在 `hrana_http.go:358-372` 提前返回，
  不注册 baton、不清理 session。
- `prototypes/v1/platform/gateway_native_idle.go:40,87`：native 空闲回收显式跳过 `session.hrana`；
  Hrana 建的 session 正是 `hrana:true`（`gateway_sessions.go:109`），因此无主 session 没有任何回收路径。
- 同类提前返回见 `hrana_http.go` 的 describe / get_autocommit / sequence / execute unknown /
  batch condition unknown / store_sql 超限 / baton 分配失败；`limitFailure`
  （`hrana_http.go:241-261`）已有清理，但其余路径没有。

`unknown_held_permits`：本地复现始终为 0。代码确认 pre-dispatch 429 不产生 permit
（`gateway_http.go:290-296`、`gateway_sessions.go:147-155`）；unknown 只在已派发 RPC 结果不明确时产生
（`gateway_sessions.go:319-325`、`gateway_operations.go:117-126`、`gateway_cleanup.go:119-128`）。
issue 中的 1～3 残留未能在本地稳定复现，按“已派发 close 结果不明确”归类，属于既有 unknown 证明模型，
不在本修复中自动释放。

## 根因链

1. 每个 project 只允许 1 个并发 close（`admission.go:181-185`）；并发 close 其余全部 429，且
   `retry_allowed=false`。
2. 429 发生在 `/v1/sql/close`，Hrana adapter 在注册 baton 之前直接返回失败
   （`hrana_http.go:358-372`）；该 session 既没有 baton，也没有 stream 计时器。
3. session 标了 `hrana:true`，native 空闲回收永久跳过它；于是 seat 只增不减，直到进程重启。
4. seat 耗尽后所有新 open 得到 `session_limit` 429，用户看到“串行请求也连续 429、
   只有重启容器才恢复”。

## 影响面

- 用户影响：并发 `db.close()` / 批量操作后可用性持续下降；最终只能重启网关。
- 受影响模块：Hrana v2 pipeline（`hrana_http.go`），经其转发/继承的 v3 JSON 非分页游标与
  protobuf pipeline 同样受影响；v3 cursor 的“open 回包不可解析”分支有同类窄窗口。
- 是否可规避：客户端串行化 close 只能降低触发概率，不能阻止泄漏；seat 耗尽后不可在进程内恢复。

## 修复方案

- 范围：
  1. close lane 容量改为 `max(1, Quotas.MaxConcurrent)`（复用现有可配置配额，不新增持久化字段），
     并发 close 在配额内不再被拒。
  2. Hrana v2 pipeline 统一“open 成功但未能交出 baton”的失败收口：任何提前返回都调度
     `cleanupRetiredSession`，覆盖 close 失败、unknown、store_sql 超限、baton 分配失败等全部路径；
     已注册并返回 baton 的 stream 维持现有空闲计时器语义。
  3. v3 cursor 中“open 回包可解析但 session 非法”的分支，用已解析出的 session 调度同一回收。
- 明确不做：不改 `retry_allowed` / 失败体契约（拒绝发生在 dispatch 前，平台接管回收，
  再让客户端重试会与回收竞争）；不自动释放已派发 close 的 unknown；不改 WS 语义
  （连接关闭已有统一回收）；不新增配额字段，不改表结构。
- 回归测试：
  - `admission_rejection_test.go`：close lane 在 `MaxConcurrent` 内允许，超出才 `close_lane_busy`。
  - `admission_writer_test.go`：close 独立上界按新容量断言。
  - `hrana_pipeline_cleanup_test.go`：并发 close 不再 429；close 被拒时新流 session 会被回收；
    已注册 baton 的流不被误回收。

## 验收标准

1. 默认配额下，2 个并发 `[execute select 1, close]` 均 200，`sessions` 回落到 0。
2. 把 `MaxConcurrent` 调到 1 时，第二个并发 close 仍可得到 `close_lane_busy`，但泄漏的 session
   会被后台回收，`sessions` 最终回到 0。
3. `cd prototypes/v1/platform && go test ./...`（含 `PERI_RUST_BINARY`）通过。
4. 真实栈复现脚本重复 100 轮并发 close，`sessions` 不增长，`unknown_held_permits` 不增长。

## 验证记录

- 运行时复现（修复后，本地真实栈，`PERI_HRANA_EXPERIMENTAL=1`，默认配额 `MaxConcurrent=4`）：
  - 100 轮 × 2 并发 `[execute select 1, close]`：全部 200，`rejected_requests_total=0`，
    `close_lane_busy` 为 0；结束后 `active_permits=0`、`unknown_held_permits=0`、`sessions=0`、`control_active=0`。
  - 10 轮 × 8 并发：每轮 4×200 + 4×429，拒绝原因均为 `concurrency_limit`（SQL lane 配额，
    非 close lane）；结束后 `sessions=0`、`unknown_held_permits=0`。
  - 100 次串行 `batch(10)+close`：全部 200；结束后 `sessions=0`、`control_active=0`。
- 针对性 Go 测试（主 agent 复跑）：
  `PERI_RUST_BINARY=... go test ./... -run 'TestAdmissionCloseLaneFollowsConcurrencyQuota|TestHranaConcurrentClosePipelinesStayWithinQuota|TestHranaRejectedCloseReclaimsUnattachedSession|TestHranaDeliveredBatonKeepsItsSession|TestHranaPipelineClose|TestHranaPipelineLocal|TestAdmissionRefusalReasonsPerLane|TestWriterAdmissionGeneral' -count=1`
  → `ok peri-loom/platform`，其余包 no tests to run。
- 全量 Go 测试（实现 agent 在隔离 HOME + `GOFLAGS=-buildvcs=false` 下执行）：
  `go test ./... -count=1` → platform 137.5s，`cmd/peri`、`sdk/go`、`sqlcontract` 全部 ok。
  未使用该环境规避时，linked worktree 会让嵌套 `go build` 在父目录损坏的 `.git` 上失败
  （`error obtaining VCS status`），属环境问题，与本改动无关。
- 遗留：`unknown_held_permits` 的复现未在本机触发（修复前后均为 0）；仍由既有
  `peri recover` / 隔离释放路径处理，本 PR 不改变 unknown 的证明要求与 `retry_allowed` 契约。
