# Renewal routing fix — 独立验证

## 结论

**限定修复的静态、独立 normal/race 回归 gates PASS。不是实际架构 retest PASS、性能 PASS，也不追认 Round 1。** 所有本 verifier 启动的测试/build 已结束；下一轮真实测量仍由 main 更新 HEAD/gate、确认重型任务停止后单独授权 E。

只编辑本验证文档；没有修改生产源码/测试、Rust/Turso 或用户 `CLAUDE.md`，没有启动 stress runner/provisioner，也没有访问 Round 1 实际 fixture 的 SQL/API 或发送信号。获批全平台测试会启动其独立临时 native fixture，并运行自身的 failover/EOF/fencing 测试；这不等同于执行实际压力或操作 baseline 服务。

## 冻结 scope 与静态审核

审核时 ROOT HEAD 为 `2bbd9fb563027ff97a2db204b063ac93aa5566de`，相对 Round 1 `96303c0` 的 committed 前进只有两份实验报告。B 的四文件改动仍未提交；F 不 stage/commit。

| 文件 | 行数 | SHA256 |
| --- | --- | --- |
| `prototypes/v1/platform/gateway_routing.go` | 71 | `f93aa4faf55ab99c84010ad588e13e6731e409c920fe36c8413826af4ff645b5` |
| `prototypes/v1/platform/gateway_sessions.go` | 195 | `03ccbe2d0a7109621bcf5d13ce21941eb19ad96313a9f1c115491b5db0b37631` |
| `prototypes/v1/platform/gateway_renewal_test.go` | 227 | `507dc2ec69c50c26de5d5d9738f6d2dd44533ceb9e94a8a1432a90ea9e57af16` |
| `prototypes/v1/platform/gateway_security_test.go` | 228 | `c38f81e961d5482706204dd3d728eaa88a196d1d245a8ce4a1f4e8a05727ae0e` |

B manifest `/tmp/peri-root-gateway-renewal-freeze.sha256` 与 F before/after `/tmp/peri-renewal-f-verify-{before,after}.sha256` 字节完全相同。四文件均 <500 行。

- route 仅额外允许 `renew_intent/renew_pending/renew_worker_pending`。desired active、grant 非空/TTL 非零、database/epoch/worker/lease/role 一致、root/authority/branch 身份及 healthy/non-draining worker 校验保留。Controller 正常 renew intent/receipt 保持同一 lease 身份，单独递增 renewal sequence；native 的实时 lease/TTL/fencing 校验不被 Gateway 放宽替代。
- 临时 route error 返回 predispatch `placement_unavailable`，不永久置 moved；成功取得 route 后的真正 binding/address 差异仍永久 moved，恢复旧 metadata 也不能复活此 handle。没有 SQL replay 或自动重新绑定。
- DB token route 不可用与已取得的 token incarnation mismatch 分开；root/branch scope mismatch 仍拒绝。原始授权、quota、unknown outcome、native dispatch 和 close 路径没有被替换。
- Security test 仅 FIRST `migrate_drain` 断言改为 `placement_unavailable`；该时刻仍要求 zero upstream query。后续 active owner/epoch/lease 已变化的旧 session 仍要求 `session_moved`，没有删除 fencing 断言。
- 新四 root tests 有 45 leaf subcases：6 稳定 renewal/API-key 与 DB-token；28 临时 unavailable/非 serving 状态及显式新业务操作恢复；9 真正 binding/address 变化永久 fencing；2 DB-token root/branch strictness。

## F 独立 gates 与原始日志

全部固定 `PERI_RUST_BINARY=/tmp/peri-loom-p6-dev/prototypes/v1/target/release/peri-loom-v1`，不使用 ROOT stale debug 自动选择；`PERI_P6_VERIFY_RACE_CLI=1` 同时开启测试 fixture 实际 Go CLI race instrumentation。未构建 Rust。

| 独立 gate | 实际计数/结果 |
| --- | --- |
| Focused normal / race，各 `-count=1` | 各 5 root PASS +45 subcases PASS，0 fail/skip；四 new tests 外额外覆盖原 migration security test |
| Full `go test -count=1 -timeout=150s -json ./...` | 84 root PASS +126 subcases PASS，2 root SKIP，0 fail |
| Full `go test -race -count=1 -timeout=180s -json ./...` | 同上；实际 7 native tests PASS，各有 Go CLI race-enabled 日志 |
| `go vet ./...` / 四文件 readonly `gofmt -l` | 均 exit 0；fmt 输出为空 |

两 full 的 skip 是 `TestBrokerUnsupportedContinuousClockRefusesStartup` 和单独调用的 `TestStateLockChild`，没有把 SKIP 当 PASS。计数以唯一 package/test terminal identity 核对；normal/race 重复覆盖不能累计为更多不同测试。

日志：`/tmp/peri-renewal-f-{focused,full}-{normal,race}.jsonl` 及对应 `.stderr`；状态 `/tmp/peri-renewal-f-focused-status.log`、`/tmp/peri-renewal-f-full-status.log`；vet/fmt `/tmp/peri-renewal-f-{vet,fmt}.log`；计数/hash `/tmp/peri-renewal-f-proof.log`。

7 个真实临时 native tests 覆盖 encrypted ACK、KEK rewrap/restart/authenticated tamper、constraint rollback reuse、migration/actual child-worker kill failover、scoped tokens/native readonly、bounded NDJSON/committed prefix、实际 native reply EOF unknown-held permit。它们支持相关回归边界，但没有观测 Round 1 request 420 当时 phase，也不是 30s 压测或全部场景验收。

## Native / binary / raw 不变证明

- `/tmp/peri-v1-joint-native/{before,after}.json` 的 171 native scope records 相同，aggregate `839fe1f8126b0ec5c4cbabf8b77ffdc7aee9fa1a2c2efc6427eee1b5463a33eb`；F 测试前后重新读取当前对应文件，全部匹配。
- Turso 4,115 tracked records 同样匹配，按原 snapshot 的 symlink 编码核对，aggregate `a75d2381853abcc65a3543a20150b32d32d84d3f2e1e34709344f5a8b5688782`。没有修改 submodule。
- accepted host release native SHA 保持 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`，构建来源 `/tmp/peri-p6-root-release-build.log`；不重建或降级 debug。
- B 指定新 Go artifact `/tmp/peri-stress-platform-renewal-bin` 重算 SHA 为 `8636dd84d2157e89ad506697efa912269e1ce598a7f8a871152b8bc8c493ac8f`，与作者 freeze 匹配。main 仍需在最终 commit/gate 后记录下一轮实际启动身份，不能把 build-time dirty VCS HEAD 当 final commit。
- Round 1 `run.json` / `operations.jsonl` 仍分别为 `cbcd587be9d597cabc9327a1c929d2535f3177bede0ba17272d3a558f2265f36` / `6ee95fd6f9a86282d97b6997733f620cce34eeb8166b314d1f96b8d682bd03f3`；首失败证据未覆盖。

## 失败历史与实验限制

作者初次自动选择 ROOT stale debug 的 `/tmp/peri-root-platform-renewal-normal.log` 保留 7 native create409 `operation_incomplete` 失败及旧 migration assertion 失败。不能删掉或标为当时成功。后续显式 accepted release 的 `/tmp/peri-root-platform-renewal-{normal,race}-current-rust.log` 中 7 native cases 全通过，但旧 assertion 仍 FAIL。获批只更新 FIRST assertion 后，作者 final logs 与 F 独立 gates 才全通过；本报告区分 artifact selection、合同更新和最终结果，不将它们混成零失败历史。

单元回归证明 baseline 的 renewal-phase误标 moved 机制可被修复并保持 fencing，**不证明 Round 1 的那个请求恰好处于 renewal phase**。下一轮只有获授权 E 能执行实际 retest；gate-only 结果不能升格为 performance PASS，未知结果仍必须保留并停止，不做 SQL retry。
