# 当前工作树镜像 S3 模拟：独立只读核验

核验对象：[`10_current_sim_report.md`](./10_current_sim_report.md)；私有证据在 `/tmp/peri-current-sim-a7c9`。核验只读取证据、脚本与 Docker inspect，没有启动、停止或改写容器和卷；以下不包含凭据或探针原值。

## 判定

**PARTIAL：四项被测路径的观察结果可信，不能据此通过生产 DR/HA 验收。** 干净重启成功、脏重启安全拒绝、单宿主冷备份恢复成功并丢失备份后的 ACK，以及被测备用容器自动启动后接管失败，均有相符的事件与现存资源状态。镜像构建来源只能部分独立核验，时间指标只适用于本次脚本定义的区间。

## 镜像与实验边界

- 本地镜像 `sha256:17be3cf191517993e4ff0c11fbf49da921da2d38f6e8545b16c68d4babddd51c` 存在，Docker inspect 为 `linux/amd64`，创建时间 2026-10-15 08:21:57 UTC，标签 `peri-loom:readiness-20261015-a7c9`。三套驱动均按该 image ID 创建应用；现存应用容器的 image ID 一致。RustFS repo digest `sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c` 对应本地 image ID `sha256:6024cc1b...`，现存 RustFS 容器一致。
- 当前 HEAD 为 `f4047befdbf26d7ba22c2229bccd39f077abab93`；当前两处未提交源码 diff 的 SHA-256 重算为 `fdf171df5471f4293326365115184af38f3bcacc3c82f82e3b0b5a549c4da8ac`，与报告相符。镜像 history 仅能看到最终二进制 COPY 层；私有证据没有完整的构建命令、构建日志或可验证的源码到二进制 attestation。因此“Rust release 和 Go 服务均由该工作树编译”及临时 Go 基底的精确内容是执行者的构建记录，**不能从保留证据独立证明**；也不能把该镜像等同于远程发布镜像。
- 三次有效实验的 owner 分别为重启 `cc6df77737d69a3833118074`、DR `a69ec61009ac13e437fa296ebd346f50`、HA `685bd194a7ae62f88d7ddc2a`。现存容器及卷的名称、owner 标签、镜像与挂载相符；有效 HA 主备均挂载同一专属应用卷。首次 HA 夹具 owner `bc4e6a789e4d6cdf446e494e` 的事件停在 `app configuration differs: Env`，两个应用容器保持 created，未进入故障注入；排序 Env 后的重跑才计产品结果。现存资源状态是本次核验时点快照，不能单独证明实验当时没有操作其他资源。

## 四项事件链

| 场景 | 独立核对结果 |
| --- | --- |
| 干净重启 | 初次 ready=1；首笔 ACK 和 SQL 读回匹配，行摘要 `c8aef85d...aa4a0`。TERM 后 `docker wait=0`，`authority.lock` 十六进制 `636c65616e0a`（`clean\n`）；同一容器重启后 ready=2，数据库和项目 key 相同，重读摘要仍为 `c8aef85d...aa4a0`。**通过本轮模拟。** |
| SIGKILL 脏重启 | 第二笔 ACK 和读回匹配；KILL 后 `docker wait=137`，marker `64697274790a`（`dirty\n`）。再次 `docker start` 返回 0 只表示启动命令被接受；进程随后退出 1，ready 停在 2，证据记录 `gateway_not_ready`，所以故障后没有 SQL 读回。现存应用容器退出 1，marker 未被脚本修复。**恢复失败，拒绝启动被观测。** |
| 故障前冷备份 DR | `result.json` 为 `RESTORE_PROVEN_WITH_B_LOSS`、`pass=true`、恢复 A=true、恢复 B=false、丢失 ACK 数=1；先 ACK A，停源服务并归档应用及 S3 卷，再启动源服务 ACK B，KILL 两源服务，移除本轮源资源，解包并在新卷恢复。两个归档 SHA-256 与清单匹配，恢复前后 13 个 S3 对象清单一致，恢复后 SQL 仅有 A；现存恢复应用与 RustFS 均退出 0，源服务已不存在。**本轮手动恢复通过。** |
| 预建备用 HA | 主服务 ready=1，故障前 SQL ACK 和读回匹配；外部 watcher 在 KILL 前就位，主服务退出 137 后执行 `docker start` 备用并得到返回码 0，备用短暂 running 后退出 1、ready=0，marker 保持 `dirty\n`。56 次固定 TLS 入口 SQL 探测均为 unavailable；首末探测相隔 59.957 秒，KILL 返回至末次探测 60.086 秒；`recovered=false`。现存主备状态分别为退出 137/1。**该接管路径失败；至少约 60 秒未恢复，最终恢复时间未知。** |

## 时间指标与限制

DR 保存的单调时钟纳秒点给出 `(source_fault - ack_a) / 1e9 = 4.330423667`，以及 `(restored_sql_read - source_fault) / 1e9 = 3.695959750`；报告四舍五入为 `4.330s`、`3.696s` 正确。`source_fault` 在驱动确认两源容器 KILL 后才记录，RTO 因而**不含故障检测与两次 KILL 耗时**，但包含本轮源资源移除、解包、启动和读回。RPO 是故障时“最新可恢复 ACK A 的年龄”，不是周期备份策略的最大数据损失保证。归档留在同一宿主，数据量小且镜像预热；没有异机传输或整机丢失。

HA 的 watcher 是实验脚本中的外部线程，不是产品内置故障检测器；这次观察不能证明产品自动 HA。DR 结果字段 `same_endpoint` 是应用所用 S3 endpoint `http://rustfs:9000`；恢复应用的宿主 TLS 端口由 Docker 动态分配，未证明客户端入口不变。实验没有测云 S3、网络分区、多主机、周期备份、长期负载、旧副本复活后的全局 fencing 或真实生产 RPO/RTO。结论应保持为“当前工作树镜像在上述隔离路径上的模拟结果”；生产 DR/HA 门槛仍未通过。
