# P1：恢复证据完整性——停写证明混代与操作日志先改后验

**状态**：已修复，待合并。
**优先级**：P1。
**类型**：错误（数据正确性边界）。
**创建日期**：2026-10-30。
**来源**：用户提供的设计文档 `Peri-Loom-Issue23-Volume-Recovery-Design.md` §8；explorer 只读确认 + 主 agent 复核（基线 `d0c5417`）。
**确认方式**：代码审计 + explorer 只读确认；未在真实 v7/v8 卷上运行时复现，修复以回归测试锁行为。

## 确认证据

### 1. `--recover` 接受混代 stop proof

正常 clean 门禁要求 stop proof 与 identity 逐字段相等：

- `prototypes/v1/platform/dev.py:806`：
  `if stopped != {**identity, "waited": True, "exit_code": expected_exit}:`
- identity 的 8 个字段由 `dev.py:745-752` 校验：`version/generation/worker_id/nonce/pid/started/waited/exit_code`。

恢复门禁只比较 `waited` 与 `exit_code`：

- `prototypes/v1/platform/dev.py:829-843`：
  `if stopped["waited"] is not True or type(stopped["exit_code"]) is not int:`、
  `if observed is not None and observed != stopped["exit_code"]:`；
  `stopped` 的 `worker_id/nonce/pid/started/generation` 从未参与比较。
- 唯一调用点 `dev.py:1403`（`Project.prepare_recovery`，`--recover` 入口 `dev.py:2220-2229`）。
- launcher 写入方本身总是带全字段：identity `dev.py:2423-2425`，stop
  `dev.py:2483-2486`（`dict(self.owned_identities[worker], waited=True, exit_code=...)`）。

launcher stop record 的 stage 也不绑定 incarnation：

- `dev.py:755-767` 的 `joined_exit` 只按 `name` 取第一个 int `exit_code`。
- stage 写入 `dev.py:2472-2476`、`dev.py:2716-2719` 只有
  `name/cap_ms/waited_ms/exit_code/signal/ack/authority_lock`，没有 identity 摘要。

后果：worker 的 stop 文件若来自另一 incarnation（旧 `pid`/`started`、或复制自另一 worker），
恢复门禁会把它当作本次停写证明；当另一个 worker 提供非零 joined exit 时，`dead` 非空，
混代证据随本次恢复一起放行。若全部 worker 的 stop 都是混代且 exit 0，则被静默视为 clean，
最终以 clean 门禁的错误拒绝，拒绝理由误导操作者（观察）。

### 2. legacy operation journal 在拒绝前截断证据

- `prototypes/v1/platform/operation_journal.go:401-414`：`parse` 后立即设置
  `journal.size`，legacy（`journal.completion == nil`）路径在 `:410` 直接
  `Truncate(journal.size)`；`verifySnapshot`（`:425`）与 `verifyWitness`（`:428`）在其后执行。
- `parse`（`:474-522`）把“完整但校验失败、以换行结束”的记录与“无换行的 torn tail”同样
  归入可丢弃尾部；调用方只对 completion v2 用 `bytes.IndexByte(..., '\n')` 拒绝完整坏尾
  （`:403-405`），legacy v1 无此判断，直接截断。
- completion v2 只在全部校验通过后截断（`:439-441`），与 legacy 顺序不一致。

后果：`verifyWitness` 报 `operation journal rollback detected` 时，文件已经被截断，
事故字节不可再复核；完整坏尾（例如哈希被改写）也被静默删除后继续启动，而不是 fail-closed。

## 根因链

1. 恢复门禁复用 signed stop proof 的宽松字段读取，只把 `waited/exit_code` 当作证据，
   没有把 proof 绑定到当前打开的 identity → 旧 incarnation 的停写证据可用。
2. stage 只携带 `name/exit_code`，没有 identity 摘要 → 没有 signed stop proof 时，
   唯一可用的 joined-exit 证据无法证明属于当前实例。
3. operation journal 把“可丢弃尾部”的判断与“应用截断”合并在校验之前 → 拒绝路径修改证据，
   完整坏尾与 torn append 不再区分。
4. 用户可见后果：恢复可能接受旧写入能力的证明；日志拒绝后现场被改写，无法回答“哪些字节
   曾被当作已确认提交”。

## 影响面

- 用户影响：本地卷 `--recover` 的证据可信度；崩溃后的操作日志事故现场保全。
- 受影响模块/调用点：`dev.py` 的 `verify_recovery_worker_joins` 与 stop stage 写入；
  `operation_journal.go` 的 `replay` 与 `joined_exit` 读取。
- 是否可规避：无。恢复门禁与日志打开都是启动必经路径，只能修代码。
- 不在本 issue：完整崩溃恢复协议（seal v2、backend owner lock、attempt 状态机、
  Controller 迁移）仍按设计文档分阶段实施；本 issue 不改变 v7/v8 卷的恢复结论。

## 修复方案

- 范围：
  1. `dev.py`：恢复门禁要求 signed stop proof 与 identity 全字段相等（只允许
     `waited/exit_code` 按观察值改写）；拒绝任何 identity 字段不一致的 proof。
  2. `dev.py`：launcher stop stage 增加 `identity_sha256`（当前 identity 文件字节摘要）；
     `joined_exit` 只有 stage 的摘要与当前 identity 文件一致时才返回 exit code。
  3. `operation_journal.go`：completion v2（genesis 存在）对任何以换行结束的尾部拒绝；
     legacy 对可解码的完整坏记录拒绝，不可解码的完整尾部按 torn append 处理
     （completion 半记录崩溃矩阵）。只有 torn/不可解码尾部且 snapshot/witness 全部
     校验通过后才截断；任何拒绝路径不得修改文件字节。
  4. 更新 `prototypes/v1/platform/README.md` 中 `--recover` 与 stop proof 的描述。
- 明确不做：不实现设计文档的完整恢复闭环；不修改业务表/schema；不改 root/WAL 格式；
  不新增 seal v2、owner lock、attempt 协议或 Controller 恢复计划；不声称 v7/v8 真实恢复通过。
- 回归测试：
  - `prototypes/v1/platform/tests/owned-stop-proof.test.py`：混代 stop proof 被恢复门禁拒绝；
    stage 缺 identity 摘要或摘要不匹配时不构成 joined-exit 证据；同代非零退出仍被接受。
  - `prototypes/v1/platform/operation_journal_test.go`：完整坏尾（legacy）被拒绝且文件字节不变；
    witness 覆盖的 torn tail 报 rollback 且文件字节不变；未被覆盖的 torn tail 仍被截断（既有语义）。

## 验收标准

1. 混代 stop proof 在 `--recover` 准备阶段被拒绝，错误信息指出 incarnation/joined exit 不一致。
2. 无 signed stop proof 时，只有带正确 identity 摘要的 stage 可作为 joined-exit 证据。
3. legacy v1 的可解码完整坏记录被拒绝，拒绝后日志字节与拒绝前完全一致；
   completion v2 的完整坏尾被拒绝；不可解码的 legacy 尾部仍按 torn append 截断
   （既有 completion 崩溃矩阵）。
4. 既有 torn tail 成功截断语义保留，`TestOperationJournalTornTailIsDroppedNotTrusted` 仍通过。
5. `python3 tests/owned-stop-proof.test.py`、Go 定向用例、`gofmt`、`go vet`、`git diff --check` 通过。

## 验证记录

- `python3 prototypes/v1/platform/tests/owned-stop-proof.test.py` → 53 tests，OK（8 skipped，含新增混代与 stage 摘要用例）。
- `GOFLAGS=-buildvcs=false go test . -run 'TestUnifiedCompletion|TestCompletionCrashMatrixOperationJournal|TestOperationJournal' -count=1` → ok（17.0s）。
- platform 全量（干净 clone，`PERI_RUST_BINARY` 指向已构建的 `peri-loom-v1`）：
  `go test ./... -count=1` → 4/4 包 ok，0 失败，163.156s（platform 163.156s、cmd/peri、sdk/go、sqlcontract）。
- `gofmt -l operation_journal.go operation_journal_test.go` 无输出；`go vet ./...` OK；`git diff --check` OK。
- FAIL-first：新增用例在旧实现上分别复现“混代 stop proof 被接受”“完整坏尾在校验前被截断且字节被改写”；收窄/放宽规则过程中，全量测试又分别捕获
  `completion_written_before_fsync_absent`（不可解码半记录必须仍按 torn append 截断）与
  `TestUnifiedCompletionCorruptAndPartialTails`（completion v2 必须无条件拒绝换行结尾的完整尾部），
  最终规则按 completion 状态分流并全部通过。
- 限制：worktree 的 Go `buildvcs` 受 linked-worktree 环境限制，全量测试在普通 clone 中执行；
  未在真实 v7/v8 卷或 OrbStack 引擎重启场景复测，不能据此宣称完整崩溃恢复通过。
