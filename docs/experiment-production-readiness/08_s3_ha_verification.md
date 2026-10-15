# 实验 8 独立复核：S3 共享桶自动接管尝试

复核时间：2026-10-15。**报告与保留证据一致：PASS；计划第 4 项测试覆盖：PARTIAL；本次被测配置的 HA 接管能力：FAIL。** 本次只读取报告、私有证据及带 owner `e0036ea74f160534eacc7db4` 标签的 Docker 对象，没有启动、停止、清理或修改资源。

## 已核实的证据

- 私有目录 `/tmp/peri-s3-ha-final/peri-s3-ha-e0036ea74f160534eacc7db4/` 为 `0700`，所检查的文件为 `0600`。重新计算 `manifest.json` 列出的 7 个 SHA-256，全部匹配：脚本、事件证据、两份最终日志和三份 `authority.lock` 快照。私有凭据和探针原值未写入本报告。
- `evidence.json`、脚本和 Docker inspect 一致指向发布镜像 `ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`；主备容器的实际镜像 ID 均为 `sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102`。Docker 中保留一个该 owner 的专属网络、RustFS 容器、RustFS 数据卷、应用共享卷和两只应用容器，标签与名称一致。脚本创建专属桶 `peri-ha-e0036ea74f160534eacc7db4`，事件记录其创建成功；本复核未重新启动已停止的 RustFS 来独立查询桶。
- 主备在故障前均已创建；备用为 `created`。Docker inspect 确认两者同镜像、相同命令和环境、相同应用卷、同一隔离网络、`--backend s3`、`--restart no`，并都配置 `127.0.0.1:61612:9443`。脚本通过应用 CA 校验回环 IP 证书，客户端在故障前后固定使用 `https://127.0.0.1:61612`。
- 06:01:09.883 UTC 的事件记录 SQL `executed=true` 且逐值读回相等。watcher 的 `watcher_armed` 事件为 06:01:09.936，早于 06:01:10.266 的 KILL 派发；当时主实例运行、备用未运行。KILL 返回 0，主实例退出码 137，`OOMKilled=false`。三次只读快照均为十六进制 `64697274790a`（`dirty\n`）。
- watcher 在 06:01:10.475 检测到主实例已退出，并对预创建备用执行一次 `docker start`；06:01:10.676 记录命令返回 0、备用短暂运行。Docker inspect 给出的备用运行时间为 06:01:10.556–06:01:10.824，最终退出码 1。备用日志在 06:01:10.806 记录 `project stopped: owned stop proof set incomplete or unexpected`；备用 ready 事件为 0。watcher 是测试脚本中的外部线程，不是产品或发布部署自带的 HA 控制器。
- 故障后的 56 次固定 URL SQL 读回探测全部为 `unavailable`，没有成功 SQL 响应。首末探测为 06:01:10.652 和 06:02:10.616，单调时钟跨度 59.964 秒；KILL 派发至末次探测约 60.349 秒。最终 `recovered=false`。因此只可报告**至少 60 秒未恢复**，不能给出有限 RTO，也不能经故障后 SQL 证实 ACK 行的持久字节完整性。RustFS 最终以退出码 0 停止，主备应用均已退出。

## 判定边界

相较 [`05_ha_report.md`](./05_ha_report.md)，本轮实质补上了 **S3 后端、隔离共享桶、故障前预创建备用、故障前预置的自动启动尝试**。旧轮是在 60 秒观察结束后才人工创建和启动备用；本轮 watcher 在故障后约 0.41 秒即自动尝试启动，仍因 dirty stop proof 被拒绝。因此，本轮足以判定**这一同卷、同桶、固定入口的冷备用方案接管失败，同时安全拒绝生效**。`docker start` 的命令成功不构成服务恢复。

[`00_plan.md`](./00_plan.md) 第 4 项还要求故障期间持续查询与**写入**、接管后同一身份继续成功读写，以及同卷/跨卷 fencing 和服务入口的独立核验。本轮故障后只有只读探测，且没有可用备用实例，故无法检验故障后写入、成功接管后的身份与 ACK 一致性、跨卷接管或双主 fencing。固定 TLS 入口已核验，但没有运行中的并行备用或多宿主/负载均衡故障域。故该项的**测试闭环仍为 PARTIAL，能力判定为 FAIL**；不能推广为任何外部编排都无法实现 HA，也不能把外部 watcher 称为产品自动接管能力。

证据来源：[`08_s3_ha_report.md`](./08_s3_ha_report.md)、私有目录中的 `reproduce.py`、`evidence.json`、`manifest.json`、只读 marker 快照及脱敏日志、带 owner 标签的 Docker inspect。私有目录含凭据与随机探针原值，保持不公开。
