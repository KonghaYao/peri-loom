# F08 本机构建镜像的隔离 S3 生产能力模拟

执行时间：2026-10-15 10:45–10:47 UTC。被测对象是使用记录的提交 `45946d55ee94725dd8219fbe1fab991391f1459f`（tree `7c10960d2fcc51a0cdac829736d7f4c97df273a8`）、Dockerfile 和本地 Go 缓存基底构建的 `linux/amd64` 镜像 `sha256:6c3e07973a6db51b753ad4f4dd28fc5b706e61903cd9959036ecf3033c5405bf`。执行前 `git status --short --branch` 显示该分支无工作树改动；私有目录未另存这次状态输出，故该点仅为执行记录。**本轮不是已发布 v1.0.1，也不是 [上一轮工作树镜像](./10_current_sim_report.md) 的结果。**

## 构建来源与证据

从仓库根目录使用 `prototypes/v1/deploy/Dockerfile` 构建，其 SHA-256 为 `4771c6e53e6e59df70c3f4b315dc55bb5b5c0a75507a2f2dfa5dcbc2f1ecabcf`。标准命令 `DOCKER_BUILDKIT=1 docker build --platform linux/amd64 --progress plain -f prototypes/v1/deploy/Dockerfile -t peri-loom:sim-45946d5 .` 在 Go 阶段下载 `github.com/gorilla/websocket v1.5.3` 时，访问 `proxy.golang.org` 超时而失败；没有把失败产物用于测试。随后对同一提交和 Dockerfile 增加 `--build-arg GO_IMAGE=peri-go-cached:a7c9` 构建成功。该本机基底 ID 为 `sha256:2800fb0fa526992b9af10324493e7e5919cb4c90c55fcca40cefd7e5301c8074`，镜像 history 显示它在 Go 基底上增加该版本模块缓存并设 `GOPROXY=off`。Go 平台和 CLI 仍在本轮 Docker build 中编译；本轮没有使用旧应用镜像。

提交及 tree 标识、Dockerfile 副本、基底 history、两次完整构建日志、镜像 inspect、驱动及其 SHA-256 保存在私有目录 `/tmp/peri-current-sim-f08/`；没有完整源码或构建上下文归档。本机缓存基底不是独立发布的可验证供应链证明，因而这里证明的是**指定本地镜像**在本次模拟中的行为，不能等同于远程发布 digest。

## 四项判定

| 能力 | 实测结果与判定 |
| --- | --- |
| 干净停机后同卷同桶重启 | **本轮模拟通过。** 第一笔 SQL 执行回执严格等于 `{"executed": true}` 且故障前逐值读回；TERM 后 `docker wait=0`、marker 为 `clean\n`。重启后 ready 事件由 1 增至 2，数据库与项目 key 相同，旧行 SHA-256 均为 `59ab111e78a59e13497a759c1b3e1d6b2507ce7f6c474b3d31cd8eba9cdb3e1b`。 |
| SIGKILL 后原卷直接重启 | **业务恢复失败，本次安全启动拒绝生效。** 第二笔 SQL 的 `{"executed": true}` 与读回已确认；KILL 后 `docker wait=137`、marker 为 `dirty\n`。原卷启动命令返回 0，但进程随即退出 1、ready 仍为 2，故障后无 SQL 读回。脱敏启动日志直接记录 `project stopped: owned stop proof set incomplete or unexpected`；这是可观察的首个拒绝原因，不能据此断言 dirty marker 的单独作用、后续门禁结果或数据已损坏。未改 marker 或 stop proof。 |
| 故障前冷备份、源卷移除后的手动 DR | **本机模拟恢复成功，备份后确认写入丢失一笔；生产 DR 未获验证。** 在独立应用卷与 S3 卷的冷归档中备份 ACK A，随后 ACK B，故障后移除本轮源容器及源卷，并从已校验归档恢复到全新卷。13 个对象的恢复前后清单一致，同库同凭据，恢复后只读到 A。本次脚本代理 RPO `4.475s`、故障注入完成后的恢复段耗时 `3.386s`；A 的起点在读回后，故障起点在两个 wait 后，这些不是生产承诺值。 |
| 预创建备用的同卷接管 | **本机指定路径失败。** 同宿主共享卷与桶、主进程 KILL 后，由已就位的外部 watcher 顺序启动备用；命令返回 0，但备用随后退出 1、ready=0，日志记录同一 stop proof 拒绝。主服务故障前 SQL 回执及读回成功，原固定回环 TLS 入口在约 60 秒观察窗的 56 次 SQL 探测均不可用；`recovered=false`。该 watcher 是测试辅助，不是产品 HA 控制器。 |

## 原始证据与限制

各场景生成独立随机 owner、标签、Docker 网络、卷、容器、S3 桶与回环端口；RustFS 使用本地固定 digest `sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`。应用镜像以本轮 image ID 固定，应用容器无自动 Docker 重启。凭据、连接配置、逐事件记录与归档仅留在权限为 0700 的 `/tmp/peri-current-sim-f08/`，公开报告不含原值。逐项原始记录分别位于：

- 重启：`/tmp/peri-current-sim-f08/peri-s3-r3-3d6a51044564338415d7cd4d/evidence.json` 与 `dirty_restart.log`。
- DR：`/tmp/peri-current-sim-f08/peri-dr-dbed748f6d92409030b967d1b3728c19-h1z20369/result.json`、两个归档及对象清单；归档字节数和 SHA-256 已另行重算匹配，13 个对象的前后清单一致。
- HA：`/tmp/peri-current-sim-f08/peri-s3-ha-0db8122798cf2b385570d4e4/evidence.json` 与 `standby-final.log`。

DR 的 RPO 代理值从 ACK A **读回完成后的单调时钟记录**到两个源容器 KILL、wait 完成计时，表示故障时最新可恢复写入的近似年龄；它略低于从 SQL execute 回执瞬间计算的年龄。RTO 从两个源容器 KILL、wait 完成后起算，到新实例 TLS SQL 读回为止，不含故障检测和 KILL 耗时。恢复后的客户端宿主端口未保持不变。HA 的 56 个采样点均不可用，最后一次失败探测距 KILL 返回 `60.083s`，最终恢复时间未知。实验仅覆盖单宿主 Docker、独立 RustFS、小数据量及这四条故障路径；未覆盖云 S3、跨宿主备份、周期备份、固定恢复入口、网络分区、旧主复活或全局 fencing。因此不能由本轮手动 DR 成功或安全拒绝推定生产 DR/HA 能力。

独立复核见 [11_current_sim_verification.md](./11_current_sim_verification.md)，对抗审查见 [11_current_sim_adversarial.md](./11_current_sim_adversarial.md)。
