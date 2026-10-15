# 实验 8：S3 共享桶与预创建备用的自动故障接管

执行时间：2026-10-15 06:01–06:02 UTC。固定发布镜像：`ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`；本机镜像 ID `sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102`。

## 判定

**本次 S3 自动接管失败；dirty guard 的安全拒绝生效。** 故障前 TLS SQL 写入获得 `{"executed": true}` 并即时逐值读回。预先启动的外部测试 watcher 在主实例退出后检测故障并对预创建备用执行 `docker start`，命令返回 0；备用仅短暂运行，随后退出码 1，没有 ready 事件。固定客户端 TLS URL 的 60 秒窗口内 56 次 SQL 读回均不可用，故障前 ACK 行未能在故障后经 SQL 恢复验证。备用日志为 `project stopped: owned stop proof set incomplete or unexpected`。`authority.lock` 保持 `dirty\n`，未修改 dirty 或 stop proof。

这里的 watcher 是**测试控制器的外部辅助**，不属于产品内置 HA 或发布部署的自动接管机制。它只证明即使已安排故障后的自动 `docker start`，同卷、同桶的备用仍被当前安全检查拒绝；不能把启动命令成功当作服务恢复。

## 隔离配置与方法

每次运行脚本都随机生成新 owner。本次 owner 为 `e0036ea74f160534eacc7db4`；资源名称统一以 `peri-s3-ha-e0036ea74f160534eacc7db4` 开头。新建并标注 `peri.s3-ha-experiment.owner` 的专属 Docker 网络、RustFS 容器及数据卷、应用共享卷、主应用容器和备用应用容器；唯一桶为 `peri-ha-e0036ea74f160534eacc7db4`。没有访问或改变已有 RustFS、桶、卷、网络或应用。RustFS 使用本机缓存的 `rustfs/rustfs:1.0.1`；随机 S3 access key 与 secret key 均为十六进制 ASCII，保存在 0700 私有目录的 0600 文件中。

两个应用容器在故障前均已创建，使用相同发布镜像、命令、S3 桶和凭据、应用卷、资源限制、`--restart no`、`--backend s3 --run-dir /data/peri --listen 0.0.0.0:9443 --tls --no-build`，以及同一 `127.0.0.1:61612:9443` 绑定；Docker inspect 验证其配置环境、命令和挂载一致。备用初始为 `created` 且未运行。固定客户端入口为 `https://127.0.0.1:61612`，用应用自身 CA 验证服务端证书的回环 IP。故障前用项目凭据通过 `/v1/sql/open`、`/execute`、`/query`、`/close` 写入随机行并读回。故障后只对同一 URL 做读回探测，不重试任何不确定写入。

watcher 在注入 SIGKILL **之前**作为独立线程启动并记录 `watcher_armed`。它轮询主容器状态，确认主已退出才对预创建备用执行一次 `docker start`。故障后没有人工恢复操作。探测循环在 KILL 命令之后持续 60 秒，每次记录 UTC 与单调时钟、主/备 Docker 状态、备用 ready 数和 SQL 结果。

## 时间线与原始观测

| UTC 时间 | 事件 | 观测 |
| --- | --- | --- |
| 06:01:06.494 | 主备预创建核验 | 备用 `created`，共享卷与桶、运行配置一致。 |
| 06:01:08.969 | 主实例就绪 | 主实例 1 个 ready 事件。 |
| 06:01:09.883 | 故障前 ACK 与即时读回 | `executed=true`，逐值相等；规范化行 SHA-256：`2acd7cf18e4186edb3da7829e0c2c22aaba1686a5c24f77619d203eae95f12f0`。 |
| 06:01:09.936 | watcher 已启动 | 故障注入前确认主运行、备用未运行。 |
| 06:01:10.266 | SIGKILL 发出 | 主实例当时运行，备用未运行。 |
| 06:01:10.475 | watcher 检测退出 | 主实例退出码 137，`OOMKilled=false`；备用仍为 `created`。 |
| 06:01:10.676 | watcher 启动备用 | `docker start` 返回 0；备用短暂 `running`。 |
| 06:01:10.824 | 备用进程结束 | Docker `FinishedAt`；最终退出码 1。 |
| 06:01:10.652–06:02:10.616 | 固定入口持续探测 | 56 次 SQL 读回均为 `unavailable`；首末探测单调时钟跨度 59.964 秒，KILL 至末次探测约 60.29 秒。 |
| 06:02:10.785 | 最终状态 | 主退出码 137；备用退出码 1；备用 ready 事件 0；`recovered=false`。 |

备用带时间戳日志在 06:01:10.806 记录 `project stopped: owned stop proof set incomplete or unexpected`。主容器 `authority.lock` 在 KILL 前、KILL 后及窗口结束时均为十六进制 `64697274790a`（`dirty\n`）；脚本只读取这些字节，没有编辑、删除或重建标记。60 秒内无故障后成功读回，因此恢复中断时间没有有限测值，只能报告**至少 60 秒未恢复**；持久字节是否完整未能由 SQL 验证。

## 复现与证据

私有目录 `/tmp/peri-s3-ha-final/peri-s3-ha-e0036ea74f160534eacc7db4` 为 0700；目录内文件为 0600。`reproduce.py` 每次只生成和操作自身随机 owner 命名及标注的资源，运行命令为 `python3 /tmp/peri-s3-ha-final/peri-s3-ha-e0036ea74f160534eacc7db4/reproduce.py`。`evidence.json` 保存逐事件 UTC、单调时钟、Docker 状态、ready 与 SQL 结果；`primary-final.log`、`standby-final.log` 为脱敏日志；`manifest.json` 给出脚本、证据、日志与只读 marker 快照的 SHA-256。`credentials.json`、`probe.json`、`connection/` 含私密凭据或原始随机值，仅保存在私有目录，不列入公开摘要。

主备应用容器、共享应用卷、RustFS 容器及卷、网络与桶保留供核验，dirty 状态保持原样。实验结束后仍运行的 RustFS 已收到 TERM 并以退出码 0 停止；主备应用均已退出。此结果来自单宿主 Docker 与隔离 RustFS，不等同于多宿主、真实云 S3、负载均衡或 Kubernetes 故障域验证。
