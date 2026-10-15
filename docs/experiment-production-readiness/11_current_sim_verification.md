# F08 提交镜像 S3 模拟：独立只读核验

核验对象：[`11_current_sim_report.md`](./11_current_sim_report.md)、`/tmp/peri-current-sim-f08/` 中三项驱动、构建日志及原始事件。未运行实验或操作 Docker 资源；未读取、输出凭据原值。

## 总体评估：PARTIAL

四条被测路径的**观察结果可信**：干净重启成功，SIGKILL 后原卷启动被拒绝，冷备份手动恢复 A 且丢失备份后的已确认 B，预建备用在 60 秒观察窗内未接管。镜像只能定位为本机从指定提交构建的 image ID；其 Go 缓存基底没有独立供应链证明。报告中的“构建输入快照”和 RPO 时间点措辞需要收紧。上述结果不能作为生产 DR/HA 验收。

## 逐维度评估

### 数据完整性：PASS

- `evidence.json` 记录两次严格 SQL 回执及逐值读回、TERM/KILL 的 `docker wait`、marker、ready 次数、重启后的数据库及项目 key 比对。首行在干净重启前后 SHA-256 均为 `59ab111e78a59e13497a759c1b3e1d6b2507ce7f6c474b3d31cd8eba9cdb3e1b`；dirty 重启后进程退出 1，SQL 因 gateway 未 ready 无法读回。
- DR `result.json` 为 `RESTORE_PROVEN_WITH_B_LOSS`：A 恢复、B 未恢复、丢失已确认写入 1 笔。独立重算两个归档的字节数及 SHA-256 均与 `backup-manifest.json` 一致；恢复前后 13 个对象的 key、大小及 SHA-256 清单完全一致。
- HA `evidence.json` 的主服务故障前 ACK 为 `{"executed": true}` 且读回匹配；watcher 在 KILL 前就位，备用 `docker start` 返回 0，随后退出 1、ready=0。56 次 SQL 探测全为 `unavailable`，`recovered=false`。原始事件、脱敏日志和脚本可追溯。

### 方法正确性：PARTIAL

- `git rev-parse HEAD`、tree、Dockerfile SHA-256 分别与报告的提交、tree、`4771c6e5...ecabcf` 一致。标准构建日志确实因 `github.com/gorilla/websocket v1.5.3` 下载超时失败；第二次日志显示 Go 编译步骤完成并写出 `sha256:6c3e07973a6db51b753ad4f4dd28fc5b706e61903cd9959036ecf3033c5405bf`。三项驱动均使用该本地 image ID；`dr_current.py` 覆盖了旧驱动的默认远程镜像检查。`go-cache-history.txt` 显示模块缓存及 `GOPROXY=off`，但不足以独立证明缓存模块的来源或完整性。
- 重启和 HA 驱动将 SQL 回执严格比对为 `{"executed": true}`，并核对读回；DR 底层驱动同样严格比对回执、随后查询 A/B。脏重启日志及备用日志均直接出现 `project stopped: owned stop proof set incomplete or unexpected`。这是首个可观察拒绝原因；脚本没有隔离试验来证明 dirty marker 单独导致拒绝。
- DR 在停止应用及 RustFS 后分别冷归档两个卷，移除本轮源容器及源卷，再在新卷恢复。它验证的是单宿主本地冷备份，未模拟备份周期、跨宿主转移或生产客户端入口。HA 使用脚本内 watcher 自动启动备用，只验证这一夹具路径。

### 结论合理性：PARTIAL

- DR 的单调时钟重算：`(source_fault - ack_a) / 1e9 = 4.474912375s`，`(restored_sql_read - source_fault) / 1e9 = 3.386219833s`，四舍五入的 `4.475s`、`3.386s` 正确。`source_fault` 取两源容器 KILL 与 wait 完成之后，故 RTO 不含故障检测及两次 KILL/wait 耗时；它包含源资源移除、解包、启动与 SQL 读回。`ack_a` 时间点取 **ACK A 已读回之后**，不是 SQL execute 回执返回瞬间。故 `4.475s` 精确表示“故障到 A 读回完成记录”的间隔；若称为 ACK A 的真实年龄，会略微低估。报告应写明这一代理时间点。
- HA 以 `kill_result` 为起点，首个失败探测在 `0.166s`，最后一个在 `60.083s`；共 56 次、全部不可用。报告所称“约 60 秒内”准确，但只能判定在**采样到的 60.083 秒窗口内**未恢复，不能推断最终恢复时间或连续每一瞬间的服务状态。
- 报告明确将本机模拟与发布镜像、生产 DR/HA 区分，并承认 stop proof 拒绝的替代解释，方向合理。

### 可复现性与报告质量：PARTIAL

- 私有目录保留 Dockerfile 副本、两次构建日志、image ID、Go 基底 history、三项驱动及其 SHA-256、事件 JSON 和归档；目录权限为 0700。当前仓库 HEAD 与记录相符，Dockerfile 副本哈希也与仓库文件相同。
- 目录中没有完整源码快照或构建上下文归档，也没有构建前 `git status` 的带时刻记录。因此报告“构建输入快照”及“构建前工作树干净”只能由当前提交/tree 和执行者叙述间接支持，不能仅凭保留材料独立重建当时的构建输入状态。三项驱动依赖仓库中的 `04_dr_driver.py` 和 `dev.py`；重跑时应固定该提交并记录完整构建命令与基底材料。

## 需修正的报告措辞

1. 将“构建输入快照……保存在私有目录”改为“提交/tree 标识、Dockerfile 副本、基底 history、构建日志及 image ID 保存在私有目录”；若确有完整源码快照，补上路径与校验和。将“构建前工作树干净”标为执行时检查记录，或补充可核验的原始 `git status` 证据。
2. 将 DR RPO 说明改为“从 ACK A **读回完成后的单调时钟记录**到两源容器 KILL/wait 完成的间隔，4.475 秒”；保留其不是生产 RPO 承诺的限定。
3. HA 结论继续限定为“56 个采样点均失败，末次失败距 KILL 返回 60.083 秒；最终恢复时间未知”。不要把 `docker start` 返回 0 当作备用 ready。

## 上一轮反馈

相较 [`10_current_sim_verification.md`](./10_current_sim_verification.md)，本轮保留了标准构建失败日志、成功构建日志及本地缓存基底 history，镜像构建链可核验程度提高；供应链来源和完整构建输入快照仍未获独立证明。四项能力的生产适用边界仍需保持限定。
