# Operation snapshot 的状态锁范围（Issue #15）

默认 `PERI_COMPLETION_JOURNAL` 为空或 `0`，仍使用 v1 operation journal。每个已提交 batch 支付一次 journal `Sync`，每 256 个 mutation 或 8 MiB journal 触发同步快照。此前快照的编码、文件和目录屏障、witness 同步及截断都持有共享 state 锁，因此其他数据库的认证与状态查询会等待该快照。

本次保留触发频率、同步返回语义、磁盘格式与全部持久化屏障，只缩短 v1 compaction 的锁范围。不将它表述为已经解决 285 ms 尾延迟：本轮默认配置 H4 诊断未复现该尾部，慢 `cursor_start` 的主要耗时在 native 路径。

## 锁与持久化协议

1. state 锁内结算所有 operation append，确认达到原有 compaction 阈值。
2. 获取 journal writer 独占权与 metadata 锁，重新确认没有未完成 append，再复制已确认 operation map，固定 journal revision、digest 和 registry base revision。设置由 state 锁保护的 compaction 完成门，然后释放 state 与 journal metadata 锁。
3. 编码独立快照并按原顺序执行 snapshot 原子写入、journal witness `Sync`、journal 截断。全程保留 writer 独占权。
4. 用 journal metadata 锁安装快照、计数与截断位置；出错先 poison journal。释放 writer 后，state 锁内记录失败、poison registry，再打开完成门。

operation mutation 的入口和 append 完成后的结算、registry `Update`、关停 checkpoint 均在 state 锁外等待该门。这样不会产生等待 compaction 的乐观 edit，间接让只读路径重新在 state 锁内等待屏障。认证、授权信息、请求状态、已确认 operation 查询与 metrics 可继续完成。

触发 compaction 的调用仍等待它完成；后续 operation 写入仍须等待同一 journal writer。该改动隔离共享状态锁的阻塞，不承诺独立 journal 的下一次写入可以绕过快照。快照复制量受既有 operation retention/capacity 限制，未提高日志阈值。

`Close` 在 state 锁内停止新 maintenance 注册，随后等待已注册任务；失败被锁存并由 `Close` 返回。checkpoint 也等待完成门，因此不能先发布 clean checkpoint，再忽略快照失败。v2 保留原有归档和调度方式，共用的文件写入顺序仍是 archive → sealed snapshot → witness sync → truncate。

## 可观测性

保留 compaction 次数和累计时长，新增以下计数，均经现有 operation metrics 投影暴露：

- `operation_journal_compaction_max_ns`
- `operation_journal_compaction_failures_total`
- `operation_journal_compaction_snapshot_ns_total`：快照编码和原子落盘。
- `operation_journal_compaction_sync_ns_total`：截断前 witness 同步。
- `operation_journal_compaction_truncate_ns_total`

v1 audit 的周期 head 推进未改：每最多 32 条记录推进 durable head，两次 `atomicPrivate` 各有文件与目录屏障，随后当前 audit batch 仍执行自己的 `Sync`。这是一项待按实际 trace 分析的周期成本，不是本次优化绕过的持久化步骤。

## 验证与边界

新增故障测试让 snapshot I/O 主动停住，确认认证、授权、请求状态、operation 查询、全量 Snapshot 与 metrics 均能完成；触发调用、后续 mutation、registry Update 与 checkpoint 仍正确等待。三种失败窗口（snapshot 前、snapshot 后、truncate 前）验证等待方 fail closed，`Close` 不吞错，重新打开后已持久 operation 保持 terminal。另覆盖 Close join、新提交拒绝、三个已提交等待者只生成一份快照。

相关 queue、replay、snapshot、checkpoint、completion v2 与故障回归使用 `go test -race`：56 个顶层测试、包括子测试共 110 个通过，0 失败。新增四项又重复三轮：12 个顶层执行、包括子测试共 21 个通过，0 失败。原快照 crash-prefix、Unknown 保留与 v2 audit archive 测试均包含在上述回归内。

这些是并发正确性和阻塞隔离证据，不是正式端到端性能验收。单个 journal 写入仍串行，普通 operation 结算和 registry 更新的原有等待未整体重构；真实断电与目标磁盘 p99 仍需单独验证。

补充单并发消融：同一 Rust 二进制下，完整候选相对 cursor-only 的两对 H6 p50 分别增加 1.92%、2.42%，p99 分别变化 -20.56%、+3.78%。本次没有一致尾部改善，不应把锁阻塞隔离包装成单连接加速；快照 map 副本确有额外分配成本。主 ABBA 中 H6 第二对 p99 回退 82.46% 的不利结果仍保留，原因未定，不能宣称 Issue #15 已解决。
