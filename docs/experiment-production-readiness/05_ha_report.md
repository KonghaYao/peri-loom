# 实验 5：固定客户端入口的故障接管观测

执行时间：2026-10-15 05:31–05:32 UTC。脚本 [`05_ha_driver.py`](./05_ha_driver.py) 使用发布镜像固定 digest、新随机 owner `53b528b79674ee7268f0e16271aa3a7e`、独立本地状态卷及回环 TLS 地址。Docker 容器的重启策略为 `no`，与仓库根 Compose 未配置自动重启的单实例部署相符。所有资源带 `peri.ha-experiment.owner` 标签，未触及既有应用。

## 方法与原始观测

应用启动后，脚本验证 CA/服务端证书与回环 IP，通过同一个固定客户端 URL 和项目凭据写入随机 SQL 行，收到 `{"executed": true}` 并立即读回。随后在 05:31:30.980 UTC 发出 `SIGKILL`；Docker 接受 KILL，主实例状态变为退出码 137。脚本在此后的 60 秒**没有执行任何恢复动作**，每约 1 秒向原固定 URL 发起 SQL 读回请求，记录 59 次不可用，首次为 05:31:31.207 UTC，末次为 05:32:31.211 UTC，单调时钟跨度 60.004 秒。主实例一直未恢复，未取得故障后成功读回，因此无法给出有限的恢复中断时间；只可报告观测窗口至少 60 秒未恢复。

60 秒观察结束后，脚本才作为**人工诊断**在同一卷、同一发布端口启动预备容器。`docker start` 返回 0，但预备进程随即退出码 1，日志为 `project stopped: owned stop proof set incomplete or unexpected`，仍不能通过该路径读回故障前的确认写入。该人工步骤不计作自动接管。

| 证据项 | 结果 |
| --- | --- |
| 故障前 SQL | 确认写入且逐值读回，随机值 SHA-256 记录于私有证据 |
| 主实例 KILL | 容器退出码 137 |
| 自动恢复观察 | 固定 TLS URL 60 秒内 59 次探测均不可用；`recovered=false` |
| 人工预备实例 | Docker start 命令 0，应用最终退出码 1，无新 ready |
| 同一身份读回 | 故障后无法执行，故障前写入的持久字节完整性未确定 |

私有证据 `/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-ha-evidence-bnw57k8d/evidence.jsonl` 含 UTC 与单调时间、Docker 状态、探测结果及资源 ID；同目录的 `connection.json`、`probe.json` 含凭据或原值，不应公开。应用卷和两个退出容器保留供复核，均带上述 owner 标签。

## 判定边界

**本次单实例部署的自动接管未通过。** 对照仓库根 `compose.yaml` 的一个 `app` 服务和 Helm 模板的 `replicas: 1`、`Recreate`、`ReadWriteOnce`，这也符合当前部署形态没有冗余接管实例的预期。实验没有运行多宿主负载均衡或外部 HA 控制器，不能断言任何自定义外部编排都不可能实现接管。脚本的持续请求是只读 SQL 探测，没有在故障后获得可写实例，因此也无法检验故障后写入或双主 fencing。
