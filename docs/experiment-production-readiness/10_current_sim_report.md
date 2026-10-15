# 当前工作树镜像的隔离 S3 生产能力模拟

执行时间：2026-10-15 08:22–08:27 UTC。被测对象是本轮本地构建的 `linux/amd64` 镜像 `sha256:17be3cf191517993e4ff0c11fbf49da921da2d38f6e8545b16c68d4babddd51c`，不是已发布的 v1.0.1。执行者记录的构建输入为 HEAD `f4047befdbf26d7ba22c2229bccd39f077abab93`，另含 `authority.rs`、`authority/paged_index.rs` 未提交 diff，SHA-256 为 `fdf171df5471f4293326365115184af38f3bcacc3c82f82e3b0b5a549c4da8ac`。执行者从该工作树编译 Rust release 和 Go 服务；标准构建因容器内下载 `gorilla/websocket v1.5.3` 超时，最终使用添加该版本本机模块缓存的临时 Go 基底完成构建。完整构建日志与来源证明未保留，故镜像与指定源码的精确对应关系未经独立证明。S3 使用固定 RustFS digest `sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`。

## 判定

| 能力 | 观测与判定 |
| --- | --- |
| S3 干净停机后原卷重启 | **通过本轮模拟**。TERM `docker wait=0`，`authority.lock=clean\n`；同卷同桶重启后 ready 事件由 1 增至 2，数据库与项目 key 保持一致，故障前写入逐值读回且行 SHA-256 相同：`c8aef85d56675a711d590475ae90a89e4e0f7cd960d353e7efa4dc13025aa4a0`。本场景脚本未严格断言 SQL execute 回执值。 |
| S3 SIGKILL 后原卷直接重启 | **业务恢复失败，与安全拒绝逻辑一致**。第二笔写入在 KILL 前已读回；KILL `docker wait=137`，marker 为 `dirty\n`；原卷直接重启后进程退出 1，ready 仍为 2，无法做故障后 SQL 读回。未编辑 marker 或 stop proof；本轮未用结构化错误码独立隔离启动失败原因。 |
| 故障前独立冷备份的手动 DR | **本轮模拟成功，但生产 DR 门槛未证实**。备份应用卷及 13 个 S3 对象后，确认写入 B，再 KILL 并移除本轮模拟源卷；从归档恢复到全新卷，同库同凭据，备份前 ACK A 可读、备份后 ACK B 丢失 1 笔。按脚本定义，模拟 RPO `4.330s`、RTO `3.696s`。这是单宿主手动冷备份结果。 |
| 预创建备用的 S3 同卷自动接管 | **失败**。故障前 SQL ACK 与读回成功；外部测试 watcher 在 KILL 前就位，主进程退出 137 后自动 `docker start` 备用返回 0。备用退出 1，ready 为 0；固定 TLS 入口在约 60 秒内 56 次 SQL 读回全部不可用，marker 保持 `dirty\n`。watcher 是实验辅助，不属于产品 HA 控制器。 |

## 隔离、复核与局限

每个场景生成独立随机 owner、标签、Docker 网络、容器、卷、S3 桶和回环端口。应用镜像以本地 image ID 固定，应用设置为 `--restart no`；没有操作已有共享 RustFS 或其他实验资源。测试脚本、逐事件证据、原值与凭据位于 0700 目录 `/tmp/peri-current-sim-a7c9`；公开报告不保存凭据。实验后仅检查 owner 标签并以 TERM 停止本轮仍运行的容器，卷和已退出容器保留供复核。

HA 首次尝试在注入故障前被测试脚本对 Docker `Env` 列表的顺序敏感比较误拦截。主备环境变量的键值相同；临时脚本改为排序后比较，并用新 owner 重跑完整 60 秒。首次尝试不计入产品结果。DR 与 HA 的原始结果分别为 `RESTORE_PROVEN_WITH_B_LOSS` 和 `recovered=false`。DR 的 RPO 是故障时最新可恢复写入的年龄；RTO 从两次 KILL 和等待完成后起算，恢复后的客户端 TLS 入口也未固定。此次结果只覆盖本机 Docker、独立 RustFS 和所述故障时序；未覆盖云 S3、多主机、网络分区、周期备份或旧副本全局 fencing，因此本轮不能认定具备生产 DR/HA 能力。独立核验和对抗审查见 [核验报告](./10_current_sim_verification.md) 与 [对抗审查](./10_current_sim_adversarial.md)。
