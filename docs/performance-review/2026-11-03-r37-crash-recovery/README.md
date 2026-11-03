# R37 crash-recovery E2E（2026-11-03）

R37 把 open、只读 statement、只读 cursor、close 的 completion/audit 改为回复后异步持久化。
本目录保留合入前的崩溃恢复 E2E 证据。授权与边界见 issue #30 评论 `6593598631`：

> 崩溃时尾部已确认的只读/open/close 记录可能丢失；若 close 丢失，残留 pending open 仍按现有
> fail-closed 规则阻止 clean restart（不放宽）。

## 环境

- 镜像 `ghcr.io/konghayao/peri-loom@sha256:13ab3a908401c7447ce84109e098862ce3c4481dac7c82dba15fa7f073ffe1c1`
  （v1.1.27 构建，仅用于容器运行时与 dev.py；数据面二进制全部外挂）。
- 平台：`/tmp/bench-bin/peri-platform-r37b`，sha256 `a353454d6f0d6129…`（R37 修复版）。
- worker：`/tmp/bench-bin/peri-loom-v1-r24b`，sha256 `2921e355d9967610…`。
- 每个场景独立全新卷；`--backend local --listen 127.0.0.1:8092 --no-build`，
  `PERI_COMPLETION_JOURNAL=1`，空库或单行小数据集。容器一律 `--stop-timeout 60`，
  停止用 `docker stop -t 60`。
- 探针只存在于独立分支 `perf/issue30-r37-crash-probe`（不合入）：
  - `b7f0d0d`：close 屏障延迟 5s（`PERI_DEFERRED_PROBE_DELAY_MS`）。
  - `248ddf4` / `055d1ee`：跳过统一提交里的 close 记录（`PERI_DEFERRED_PROBE_DROP_CLOSE=1`），
    用于确定性模拟掉电丢失尾记录；`248ddf4` 只跳过 barrier，`055d1ee` 跳过整个 close submit。
- 诊断用 `dev.py` 只额外转发上述两个探针环境变量（`/tmp/r37/dev-crash.py`，不入库）。

## 结果

| 场景 | kill 点 | 重启方式 | 结果 |
| --- | --- | --- | --- |
| A 对照 | ack 一行、延迟屏障排空后 SIGSTOP `dev.py` + SIGKILL 平台 | r37b 直接按同一 `platform.json` 重启 | 行仍可读；优雅关闭后 `verify_no_unresolved_operations` **pass**（`clean-gate.txt`） |
| B 进程崩溃 | close 回复后、close 屏障延迟 5s 时 SIGKILL | r37b 直接重启 | journal 中 close 记录仍在（`journal-B-pre.json`）；重启成功、`SELECT 1` 成功。说明 SIGKILL 不丢已 `write()` 的页缓存，延迟窗口暴露的是机器崩溃/掉电 |
| D 丢失 close 尾 | 探针跳过 close submit，随后 SIGKILL | r37b 直接重启 | 崩溃前 journal 只有 open、`close_session` 计数 0（`journal-D-pre.json`）；重启后 pending open 被隔离：`state=quarantined`、`may_have_committed=true`、`cleanup_outcome=unknown`（`state-D-post.json`）；`SELECT 1` → HTTP 409（`D-read-after.json`）；clean gate 报 `unresolved platform operation prevents clean restart`（`clean-gate.txt`） |

A 的写读输出在 `A-write.json`、`A-read-after.json`；B/D 的客户端输出在 `B-read-after.json`、
`D-read-after.json`；管理面目录在 `*-catalog.json`。二进制与探针提交哈希见 `binaries.txt`。

## 限制

- 未做真实掉电/机器崩溃（无法在 Docker 内可靠模拟页缓存丢失）；D 是确定性故障注入，
  模拟的正是授权中接受的「close 尾记录丢失、open 仍在」状态。
- A/D 的重启是直接启动平台二进制（绕过 `dev.py` 的 clean gate）；clean gate 单独在
  优雅关闭后的物化状态上执行，避免 `operations_checkpoint` 未物化掩盖未决记录的判断。
- D 没有写入；写语句 intent/terminal 不变量由 A（ack 写跨崩溃仍可读）和既有写路径测试覆盖。
- 探针二进制不得合入；验证分支 `perf/issue30-r37-deferred-durability` 只含生产改动
  `8684987` + `1dfa9f4`。
