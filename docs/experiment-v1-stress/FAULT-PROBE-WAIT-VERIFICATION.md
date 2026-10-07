# Faultprobe：post-SIGKILL 观察竞态独立核验

## 结论与执行边界

1. **独立 MOCK GATES + STATIC REVIEW PASS。** 本轮独立运行冻结 ROOT faultprobe 模块的 normal、race、vet 与格式检查，并审查退出观察、身份验证、缓存删除和审计路径，没有未解决的本次 blocker。
2. 本结论不是实际故障、真实 PID 消失或部署恢复 PASS。所有 signal 调用均由 synthetic Host 计数；HTTP 测试只连接隔离的 `httptest` server。未运行 LocalHost、OS signal、部署 Gateway/Authority API、Docker、压力、Cargo 或真实缓存删除。
3. 本轮未修改生产代码或作者测试，只新增本报告；不采作者 normal/race 结果作为自己的执行，也不授权启动故障或压力。

## 冻结输入与独立来源

1. C 的冻结清单 `/tmp/peri-host-wait-root-freeze.json` SHA-256 为 `70ca23f737331665269441a672526b3e5d8f0bd2398273587b548b9e5e357268`；本轮直接核对四文件字节一致：

   | 文件（位于 `prototypes/v1/stress/faultprobe/`） | 行数 | SHA-256 |
   | --- | ---: | --- |
   | `host.go` | 303 | `3fc9c46d49e6d727cdfdbcd09f33e548a9b48acdf3e22e203cc5d4ccbc62340c` |
   | `host_wait_test.go` | 382 | `83008e6bb2b5140e3c9957100c82b2b572c6120d17989afb770e0c20f95832ce` |
   | `safety_test.go` | 245 | `3f0117ff79e1fd9575e8ccff135a2f50fe22ba6b0f74f3b2bf13b7e4287b7d9d` |
   | `README.md` | 166 | `fe7459a9099ad6abcf4c2cd8acb68c7c8998d5a4004a7897bf4beeb429c26e19` |

2. 独立记录 standalone 模块的 19 个 `.go`、`.md`、module 输入文件，包含原有测试和 CLI 包。`/tmp/peri-fault-wait-independent/source-before.json` 与 `source-after.json` 字节一致，SHA-256 均为 `f70a17e8a99c5ec66f04c2f73b5da2f99f7c50b8e1aba9f851008fe47615a73e`；范围不含相邻 stress 模块或 native 门禁。
3. 作者提供的 RED 日志和原 gates 是历史来源，不计作本轮独立执行。本代理先前只读旧实现看到 zombie 被当作 gone、postsignal 不完整观察立即失败；冻结版本已改变这些路径，本轮才运行其独立验证。

## 实际命令与计数

1. 以下命令从 ROOT `prototypes/v1/stress/faultprobe` 串行执行，均使用 `GOENV=off`，没有调用 CLI 的 `-execute`：

   ```sh
   GOENV=off go test -count=1 -json ./...
   GOENV=off go test -race -count=1 -json ./...
   GOENV=off go vet ./...
   gofmt -l $(rg --files -g '*.go')
   ```

2. normal **32 个唯一顶层测试 PASS，43 个子测试 PASS，0 FAIL，0 test skip，退出 0**。race **32 个唯一顶层测试 PASS，43 个子测试 PASS，0 FAIL，0 test skip，退出 0**。以 JSON `Package::Test` 去重，顶层与子测试分开，不把 package PASS 重复计为测试。
3. CLI 包无测试，不记为通过的测试或 test skip；vet 实际退出 0，gofmt 实际退出 0 且输出为空。独立日志：`/tmp/peri-fault-wait-independent/{normal.jsonl,race.jsonl,vet.log,fmt.log,exits.log}`。
4. normal 日志 SHA-256 `1e43be97c64224a5679a4ccbeb69ca08d0f09abf865e17bc23f09129b8eb4d4d`；race 日志 SHA-256 `98049120084d9d91e2ecdf8b9a9f0827f4a3c8888ee04619c08404ff6f4e0f7c`。独立计数与身份摘要 `summary.json` SHA-256 `f2405ad90583c58e541923073c051ed7845aadf3ab3a84228016bd4b139aa2ad`。

## 严格源码审查与 mock 覆盖

1. **gone 的唯一来源。** `inspectProcess` 仅将 PID probe 的 `ESRCH` 转成 `ErrProcessGone`；ps error、空输出、EPERM、zombie 本身均不是 PID 消失。`parseIdentity` 保留可见 PID/UID/start，zombie 返回独立错误，不能凭 defunct argv 删除缓存。PID 明显不符时不再用随后 probe 掩盖该观察。
2. **signal 前不允许 pending。** `killOwned` 在 intent 前与 signal 边界分别要求完整 PID、caller UID、非空且不变的 start、精确 owned-worker argv；两次都调用现有 worker/cache 校验。首次或第二次 incomplete 立即拒绝 signal，不把 postkill 的等待许可带到 signal 前。Authority、platform、另一 worker、launcher 与当前进程仍不能成为目标。
3. **signal 后只观察一次已发信号。** 只调用一次 `SignalKill`，记录 `fault_signal_sent` 后才开始最多五秒、继承 parent 的 context。50ms 观察间隔不会再次 signal；temporary inspection error、缺 argv 或 zombie 只允许等待，不赋予 signal/delete 权限。
4. **可见身份变化优先失败。** 每次检查 PID、可用 UID/start 和可用 argv 前缀；完整 live argv 必须逐项相同且长度相同。不同 UID、start、argv、额外 argv、可见 PID 复用立即失败，即使之后还有 gone。gone 携带的已观察元数据也先检查，不能用随后 ESRCH 隐藏此前变化。zombie 不具有可靠 live argv，只使用保留下来的身份元数据，不声称其完整 live argv 已匹配。
5. **取消与缓存边界。** 检查 parent/deadline 在 observe 前后及 removal 前；timeout/cancel/evidence failure 不推进删除。只有 Host gone 证明后，才重新核 canonical private directory、caller ownership、无 symlink/special file、遍历数量/大小上限和 `os.SameFile`。缓存目标仍限已确认 worker，其他缓存和 backend 路径不触及。
6. **审计语义保留。** `fault_intent`、`fault_signal_sent`、`fault_process_wait`、`fault_process_gone`、`fault_exit_uncertain`、删除后的 `fault_completed` 分离。signal sent 不等于 gone，gone 不等于已经删除，未知也不变成 completed。新记录不包含原始 argv、环境或 inspect 错误详情；记录失败停止后续动作。
7. **实际通过的 mock 场景。** zombie/缺 argv/transient 后 gone；transient 后原身份再 gone；PID/UID/start/argv 与 partial argv mismatch；gone 携带已变化 start；两次 precheck incomplete；持续 unavailable/zombie/缺 argv 的短 context deadline；parent cancel、观察后取消与 gone 同时出现；signal 后 evidence failure；ESRCH/EPERM、无效 PID、zombie parser。仅缩短测试 context，不声称实际等待完整五秒或重现宿主 OS 的退出竞态。

## 残余限制与冻结

1. Host 的 `ErrProcessGone` 必须遵守 ESRCH 合同；本轮直接审查 LocalHost 实现，但没有实际执行它。mock gone 不证明真实 OS PID 消失，不证明 real SIGKILL syscall 成功，也不证明重新授权 takeover 或物理 I/O 屏障完成。
2. 五秒是观察 context 的截止，不是 OS 调度/ps 回收/文件系统删除的硬实时保证。deadline 紧邻 deletion 的检查不是原子 process/filesystem fence；检查与 signal、检查与 removal 之间仍存在可信私有 fixture 的 TOCTOU，ps start 时间粒度也不能替代 pidfd。
3. 本轮门禁允许主线程在提交与另行精确授权后评估后续动作，不自行批准实际 fault。压力、Docker、真实 fixture teardown 和最终对抗验证均不在本报告执行范围。
4. 本报告完成后冻结；源码和门禁日志保留，无迟到生产修改或额外覆盖扩展。
