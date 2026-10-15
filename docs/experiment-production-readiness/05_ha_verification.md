# 实验 5 独立复核：HA 故障注入

复核时间：2026-10-15。**证据真实性与所报单实例观察：PASS；计划 00 第 4 项完整覆盖：PARTIAL；被测配置的自动接管能力：FAIL。** 本复核只读取脚本、私有 JSONL、Docker 事件与带 owner `53b528b79674ee7268f0e16271aa3a7e` 的两个容器和卷；没有重启、清理或修改资源。

## 可确认的事实

- 驱动固定发布镜像 digest；Docker inspect 确认主、备容器使用同一镜像 ID、同一独立本地卷，均有匹配 owner 标签。主容器发布 `127.0.0.1:54215:9443`，备用容器也配置该端口；客户端始终使用 `https://127.0.0.1:54215`。驱动验证 TLS CA、服务端证书与回环 IP，并核对 SQL 响应 request ID。
- 05:31:30.955949 UTC 记录 SQL `executed: true` 且随后读回随机值。05:31:30.980353 发出 KILL，Docker 事件确认信号 9 和主容器退出。主容器最终退出码 137、`OOMKilled=false`。
- KILL 命令返回后约 60.053 秒完成自动观察。首次和末次失败读分别在 05:31:31.207019 与 05:32:31.211858 UTC，相距 60.004 秒；共 59 次 `endpoint_unavailable`，无成功读。事件序列在此阶段只有主容器 `kill`/`die`，没有创建或启动替代容器；驱动代码也没有恢复动作。结束时主容器仍退出，`recovered=false`。因此可说**至少 60 秒未恢复**，不能量化最终恢复时间。
- 自动观察结束后约 24 毫秒，驱动才创建备用容器并手工执行 `docker start`。命令返回 0，但备用进程约 0.30 秒后退出码 1；日志包含 `owned stop proof set incomplete or unexpected`。没有备用实例 ready 或故障后的 SQL 读回。该步骤属于人工诊断，不能计作自动接管。
- Docker inspect 确认两个容器的重启策略都是 `no`。故障前写入可读，但故障后没有可用实例，所以无法判定其持久字节是否完整。

## 适用范围与缺口

驱动明确指定 `--restart no`，且只在观察结束后创建备用容器。因此，这个实验足以证明**该单实例、无重启策略、无运行中的接管者的配置**无法自动接管固定入口；它不能把不可用单独归因于产品 HA 机制。备用进程的 stop proof 拒绝说明手工同卷冷启动也未成功，但仍未测试由编排器自动尝试接管的过程。

根目录 `compose.yaml` 确实只有一个 app 服务且没有 restart 配置，不过它使用 S3 后端、bind mount 与外部 TLS 文件；本实验使用 local 后端、Docker volume 与镜像内开发证书，不能视为该 Compose 部署的端到端运行。Helm 模板为 `replicas: 1`、`Recreate`，状态 PVC 为 `ReadWriteOnce`；本实验没有运行 Kubernetes。单副本 Helm 仍可由 Kubernetes 尝试自动重启，故 Docker 的 `--restart no` 结果不能推出 Helm 重启路径的结果。

计划 [`00_plan.md`](./00_plan.md) 第 4 项要求故障前后持续查询**和写入**、另一实例在无人操作下接管同一身份、测量失败窗口，并独立核验同卷/跨卷 fencing 与服务入口。本轮完成固定入口查询、故障注入和 60 秒无接管判定；没有故障后的成功写入、预置运行中的备用实例、跨卷接管、双主或 fencing 测试。这些未测项不能标为通过。主报告 [`05_ha_report.md`](./05_ha_report.md) 的 59 次/60 秒、人工备用诊断和“本次单实例部署自动接管未通过”结论与证据一致；其对 Compose/Helm 的对照应按上述范围理解。

证据来源：[`05_ha_driver.py`](./05_ha_driver.py)、私有 `evidence.jsonl`（`/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-ha-evidence-bnw57k8d/`）、带 owner 标签的 Docker inspect / events / standby 日志。私有目录含凭据和随机探针原值，不应公开。
