# 实验 2：同卷并发启动与正常停机后的新卷冷恢复

执行时间：2026-10-15 04:05–04:08 UTC（本地 Docker daemon，`linux/amd64` 镜像在 `arm64` 宿主上运行）。结论：**同卷第二应用实例未能并行就绪；正常停机后的完整状态卷冷拷贝可在新容器手动恢复并读回已确认写入。** 后者只证明这一手动恢复路径，不证明灾难恢复或高可用。

## 固定输入、隔离和证据

- 固定镜像：`ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`；本地镜像 ID：`sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102`。所有应用容器均以镜像默认用户 `10001:10001`、`--backend local --run-dir /data/peri --listen 0.0.0.0:9443 --tls --no-build` 启动，TLS 端口仅随机映射到 `127.0.0.1`。容器均配置 2 GiB/2 CPU/128 PIDs、无自动重启、丢弃 capabilities、`no-new-privileges`。
- 本轮随机 owner：`3a399bf9c40e9dc8d2015656ed02bdc0`；新建资源均带 `peri.verify-image.owner` 等于该值的标签。容器后缀为 `main`、`second`、`restored`、`timed-restored`；卷后缀为 `data`、`restore-data`、`timed-restore-data`，完整前缀均为 `peri-prod-r2-3a399bf9c40e9dc8d2015656ed02bdc0-`。未触碰第 1 轮及其他资源。
- 私有原始证据：`/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-prod-r2-XXXXXXXX.lNVYs5Ibki/`。`evidence.jsonl` 逐步记录 UTC 时间、Docker 命令返回码与输出、状态、脱敏 SQL 响应和哈希；`summary.json` 汇总结果。`main-connection.json` 等文件含项目凭据，**不得公开复制**。SQL 值只以 SHA-256 或查询结果哈希出现在证据和本报告中。私有脚本 `run.py`、`continue.py`、`timed.py` 保留实际执行命令及测量逻辑。

## A. 同卷第二实例

主实例在 `04:05:59.333644502Z` 记录 `ready=true`。经 TLS CA、服务端证书及回环 IP 校验后，以项目凭据创建随机表并写入随机值，SQL execute 返回 `{"executed": true}`；随后 SELECT 返回 1 行且与写入值相等，查询结果哈希为 `025f698d52a3eca4ea9ccc89453c3485122944994e0af8d63d2deb0caed4cd70`。

主实例仍运行时，在相同卷、相同目标路径、相同镜像和启动参数下启动 `second`。Docker 接受 `start`，但该进程在 `04:06:00.402357954Z` 退出，`ExitCode=1, OOMKilled=false`，没有新 `ready` 事件；其带时间戳 stderr 为：

```text
2026-10-15T04:06:00.390944773Z project stopped: runtime directory already has an active launcher
```

之后主实例经同一 TLS/项目凭据再次成功写入第二个随机值，execute 同样返回 `{"executed": true}`；SELECT 返回两行，与两个已确认写入相等，结果哈希为 `7680ca9e0d4b136b9e93d39eba1bda60f0cd91f1c3a3b9866d653eadcb3cdcca`。因此本次同卷并发启动被拒绝，原实例继续可读写。这没有测试原实例故障后由另一实例自动接管；不能据此给出 HA 中断时间。

## B. 正常停机、新卷冷备份和恢复

在第二次写入和读回之后，对主实例发送 `TERM`。`docker wait` 原始输出为 `0`，最终状态为 `Running=false, ExitCode=0, OOMKilled=false, Pid=0`，`FinishedAt=2026-10-15T04:06:01.494225932Z`。从停止容器复制的 `authority/authority.lock` 是 `clean\n`（十六进制 `636c65616e0a`）；`launcher-terminal.json` 非空，为 938 字节，SHA-256 为 `6fb8dc11b9dfa9eaaa15d7761a2e48fbd11d8507e439c2c13651134951784a2b`。未修改 dirty/clean 标记或绕过任何启动 guard。

主实例停止后，用本轮固定镜像中的 GNU tar、一次性 root 辅助容器，将源卷只读挂载，并将**整个** `/data/peri` 状态树以 `--numeric-owner --xattrs --acls` 复制到全新目标卷；保留文件内容、权限、所有权、修改时间、扩展属性及 ACL。目标卷在复制前创建，恢复应用从未同时写源卷。稳定归档比对使用 `--sort=name --format=posix --xattrs --acls --numeric-owner --pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime`，源卷与两个新卷的 SHA-256 均为 `00667f58ccaacb3cabc676dc22e41d229b551bcf482a0b6a56999e1e0a53a403`。atime/ctime 是归档过程及解包会变化的元数据，未作为相等条件；其余上述元数据和内容均纳入比对。另以 `find` 对比路径、mode、UID、GID、大小、mtime，无差异。

首个新卷 `restore-data` 上的 `restored` 容器使用与原实例相同的命令和 `/data/peri` 路径，在 `04:06:59.849047722Z` 记录新 `ready` 事件。其数据库标识和项目凭据与原容器逐值相等；TLS 校验通过；SELECT 返回两个已确认写入，对应结果哈希仍为 `7680ca9e0d4b136b9e93d39eba1bda60f0cd91f1c3a3b9866d653eadcb3cdcca`。从该容器 `docker start` 到成功读回为 **1.536 秒**。第一次冷拷贝到读回的实际墙钟时间为 58.976 秒，包含归档比较方法的人工排查，不作为自动流程性能指标。

为得到连续流程耗时，再从同一已停止源卷冷拷贝到第二个全新卷 `timed-restore-data`，使用同一启动命令和读回检查。`04:07:35.807641Z` 开始计时；复制完成 0.344 秒，复制及双卷稳定哈希校验完成 0.890 秒，容器就绪 2.149 秒，TLS、同库同凭据及两行 SQL 读回全部完成于 `04:07:38.248065Z`，合计 **2.440 秒**。这是本机小数据量、源卷仍可用、操作者已知步骤且已有固定镜像的手动冷恢复测量，不能外推为实际故障 RTO。就这两个备份前已确认写入而言，读回结果完整；这不构成持续备份的 RPO 证明。

## 结论边界与资源状态

- 本实验支持“运行中的同卷第二实例被安全拒绝，原实例仍可读写”和“clean TERM 后完整本地卷可手动复制到新卷并启动读回”的窄结论。
- **灾难恢复未证实**：源卷可读且由操作者手动复制，没有源卷丢失、远端/定期备份、异机恢复、备份滞后或真实 RPO 测量。**高可用未证实**：没有自动故障检测、切换、共享服务身份接管或故障期间读写连续性测量。第 1 轮的 KILL 后 dirty 安全拒绝仍是另一路径，不能用本次 clean 冷恢复抵消。
- 最初直接比较 tar PAX 归档哈希时，两卷哈希不同；原因是归档包含会随读取/解包改变的 atime/ctime。保留这次原始结果于 `evidence.jsonl`，随后用上文明确的稳定归档参数复核成功。该排查造成首次 58.976 秒墙钟时间中的人工间隔。
- 本轮四个容器和三个卷均保留供复核，标签为上述 owner；`second` 以 1 退出，`main`、`restored`、`timed-restored` 均以 TERM 正常退出 0，目前无本轮运行中的应用容器。私有证据目录也保留。没有清理任何其他资源。
