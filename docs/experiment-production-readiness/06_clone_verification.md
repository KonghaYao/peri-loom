# 实验 6 独立复核：干净冷克隆的双实例写入

复核时间：2026-10-15。**本次诊断的证据结论：PASS；干净冷克隆跨卷互斥能力：FAIL；将结果推广到生产双主风险：PARTIAL。** 复核只读取驱动、私有证据、归档和带 owner `43852f0e51981a690a81aba2c68dcf9f` 的 Docker 元数据；没有修改或停止资源。

## 核验结果

- 驱动固定发布镜像 `ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`。Docker inspect 显示源容器和两个克隆容器均使用同一镜像 ID、UID `10001:10001`，三个独立卷及容器名称和 owner 标签匹配。克隆 A、B 分别使用自己的卷和回环端口，复核时进程 PID 不同且均在运行。
- 源端先收到 SQL `executed: true`，并按驱动读回随机值。随后驱动发送 TERM；`docker wait` 返回 `0`，源容器退出码 `0`、`OOMKilled=false`。归档在源停止后创建；归档内 `authority/authority.lock` 为 `clean\n`，`launcher-terminal.json` 是普通文件。私有归档 SHA-256 与事件记录一致：`b313976b16109ccd09ed9c1ebb51d9ea0957f2950fdf68e677836918613a71ba`。两次解包记录引用这一摘要，驱动在每次解包前重新计算摘要并检查源进程已停止。
- 源、A、B 的 `connection.json` 中数据库名和项目 API key 相同；CA 与服务端证书文件逐字节相同。驱动对每个证书运行 `openssl verify`，并通过经该 CA 验证的回环 TLS 端点访问 SQL。A、B 都读回源端已确认的探针。
- 两个克隆在 05:36:47 UTC 先后启动并就绪。05:36:49.462 至 05:36:51.345 UTC 的记录显示两个不同 PID 的容器持续处于 running 状态，观察窗口约 **1.933 秒**。A 在 05:36:50.037、B 在 05:36:50.599 UTC 分别记录独立表的 SQL 确认和读回；之后又分别读回各自表。驱动查询 `sqlite_master`，记录 A 中没有 B 的表、B 中没有 A 的表。最终状态为 `BOTH_CLONES_ACK_WITH_SAME_IDENTITY`。

## 证据边界

这是**手工从一次干净停机归档复制到两个本地 Docker 卷**的实验，证明被测 `local` 后端在此配置下允许两个相同数据库、项目凭据和 TLS 身份的独立克隆同时服务并分别确认写入；两边的数据已经分叉。它不证明同一共享卷上的并发写入，也没有测试 S3 后端、网络分区、自动接管、编排器、客户端共用入口或长期并发压力。

两次写入按 A 后 B 顺序执行，`running` 状态在写入前后采样，并非逐指令连续观测。结合两次写入之间两个容器持续运行的起止状态与独立端口上的请求，足以支持短窗口的重叠服务结论；不能推出更长时间的稳定性。跨表缺失由驱动记录的 SQL 查询结果支持，复核没有重新发起 SQL 请求，也没有对保留卷做离线字节比较。归档摘要证明私有归档未变，并非源卷与归档逐字节相同的独立证明。

证据来源：[`06_clone_driver.py`](./06_clone_driver.py)、私有目录 `/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-clone-43852f0e51981a690a81aba2c68dcf9f-x0nljxr2/` 中的 `evidence.jsonl`、`cold-state.tar`、连接与证书文件，以及 owner 匹配的 Docker inspect。私有目录及归档含凭据，不应公开。
