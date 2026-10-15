# 实验 9：S3 共享桶的跨卷冷克隆 fencing 诊断

执行时间：2026-10-15 06:12 UTC。使用发布 v1.0.1 固定镜像 `ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333` 和固定 RustFS digest `rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`。本轮是**跨卷、共享 S3 桶的身份 fencing 诊断，不是自动 HA 或故障接管测试**。

## 隔离与操作

随机 owner 为 `03971fdb3d590d76e5d19ce6ee6f2931`。脚本新建同名前缀的 owner-labelled 网络、RustFS 容器及数据卷、源应用卷、两个空白克隆卷与各自应用容器。唯一桶为 `peri-clone-03971fdb3d590d76e5d19ce6ee6f2931`。应用及 RustFS 端口仅动态发布到 `127.0.0.1`；未使用已有 RustFS、桶、卷或网络。Docker inspect 核验两个克隆与源使用同一镜像、S3 endpoint、桶、区域和 S3 凭据，但挂载不同的应用状态卷。凭据仅保存在 0700 私有目录的 0600 文件。

源实例经 TLS SQL 创建随机表并写入 A，收到 `{"executed": true}`，随后逐值读回。对源发送 TERM，`docker wait` 与 inspect 均证明退出码 0、非 OOM。停机后以只读挂载归档**完整应用状态卷**，归档 SHA-256 为 `b65ba7a0274fe1d10470162503ac60a0d0a3ceb33acf192520ff9e2384eb8012`；归档内 `authority.lock` 是 `clean\n`，并存在 `launcher-terminal.json`。同一归档分别校验后恢复到两个全新卷，再先后发出两个克隆启动命令，之后才等待任一 ready。

## 观察

| UTC | 事件 | 结果 |
| --- | --- | --- |
| 06:12:08–09 | 源 TLS SQL 写入 A、读回、TERM | ACK 且逐值读回；06:12:09.434 退出码 0。 |
| 06:12:09–10 | 完整冷归档及恢复 | clean proof 有效；归档恢复到两个全新卷；源桶列出 13 个对象。 |
| 06:12:10.774 / 11.134 | 克隆 A / B 容器启动 | 两者均已发出启动，使用不同回环 TLS 端口绑定和不同卷。 |
| 06:12:12.583 | 克隆 A ready | 与源数据库、项目 API key 相同，并经 TLS 读回源 A。 |
| 06:12:26.531 | 克隆 B 退出 | 退出码 1、非 OOM、无 ready 事件；启动器日志：`project stopped: authority exited before a joined clean proof`。独立核验还从 B 卷内 Authority 日志找到共享后端 seal 为 dirty、旧远端终止未证实的直接诊断。 |
| 06:12:27 | 观察结束 | A 当时仍运行；B 已退出。桶仍列出 13 个对象，其中两个 authority 对象内容哈希发生变化。 |
| 06:12:27–28 | 正常收尾 | A 和 RustFS 收到 TERM 且退出码 0；源原先退出码 0，B 保留退出码 1。 |

本轮**没有**对任一克隆发出 SQL 写入：脚本只有在两个克隆都 ready 且持续运行时，才会各发出一次唯一 SQL 写入；未知写入结果不会重试。因此没有双活动写入 ACK 或写入后读回证据，也不对共享 S3 的最终一致性作断言。桶对象变化发生在克隆启动后，不能解释为克隆 SQL 写入。

## 判定与边界

**本次共享 S3 桶的跨卷冷克隆未出现双 ready；第二克隆被 Authority 的共享后端 dirty seal 检查拒绝。** 独立核验读取 B 卷内 `logs/authority.log`，其直接原因是旧远端终止未证实；源码 `Seal::acquire` 要求旧 seal 为 clean 才能获取新 seal。上表的启动器日志只是随后传播的错误。B 没有就绪，因此无法直接采集 B 运行时 `connection.json`；同数据库与项目凭据仅由相同冷归档及容器配置间接支持。这与 `local` 后端实验 6 的双活动结果不同，表明本次发布镜像与隔离 RustFS 组合在该时序下存在共享桶的启动 fencing。一次拒绝不能证明所有启动竞态、网络分区或多宿主 S3 环境中都维持全局单主。此诊断也没有自动监测、切换入口或故障后接管，不能计作计划第 4 项的 HA 通过；该项仍应依据实验 8 的自动接管失败判定。

脱敏机器证据见 [`09_s3_clone_evidence.json`](./09_s3_clone_evidence.json)。私有原始结果、凭据、完整归档、B 日志、SHA-256 manifest 和复现入口位于 `/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-s3-clone-03971fdb3d590d76e5d19ce6ee6f2931-dhx6f334`，目录 0700、文件 0600。复现入口 `reproduce.py` 每次生成新的随机 owner 与资源，不复用本轮桶。所有本轮资源保留供核验，所有活动服务已停止。
