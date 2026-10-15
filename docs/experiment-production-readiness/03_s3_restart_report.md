# 实验 3：独立 S3 后端的干净停机与异常中断原卷恢复

执行时间：2026-10-15 05:13 UTC；环境：本机 Docker Linux daemon。固定发布镜像：`ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`（本机 image ID `sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102`）。

## 判定

| 能力 | 判定 | 依据 |
| --- | --- | --- |
| S3 干净 TERM 后同卷恢复 | **通过** | 停机退出码 0，`authority.lock` 为 `clean\n`；同一应用卷与 S3 桶重启产生新的 ready 事件，同库同项目凭据，首次确认写入经 SQL 读回且行哈希相同。 |
| S3 SIGKILL 后原卷直接恢复 | **可用性失败；安全拒绝通过** | 第二次确认写入在 KILL 前可读；KILL 退出码 137，marker 为 `dirty\n`；原容器与原卷直接启动后进程退出码 1，没有新的 ready 事件，无法执行重启后 SQL 读回。marker 保持 dirty。 |

本实验不能判断 SIGKILL 后持久字节是否完整；SQL 服务未恢复，因此不能宣称已恢复确认写入。`docker start` 命令返回 0 仅表示 Docker 接受启动请求，不表示应用就绪。

## 隔离资源与方法

本轮新建随机 owner `ecad12ba90fcbfab60583971`，并给网络、两个卷和两个容器标注 `peri.experiment.owner`。网络 `peri-s3-r3-ecad12ba90fcbfab60583971-net`；RustFS 容器和卷分别为同前缀 `-rustfs`、`-s3-data`；应用容器和卷分别为 `-app`、`-app-data`；唯一桶 `peri-r3-ecad12ba90fcbfab60583971`。RustFS 镜像为本机缓存的 `rustfs/rustfs:1.0.1`，桶由缓存的 `bitnamilegacy/minio-client:latest` 创建。对象存储未发布宿主端口，应用 TLS 端口仅随机发布到 `127.0.0.1`。应用采用 `--backend s3 --run-dir /data/peri --listen 0.0.0.0:9443 --tls --no-build`，2 GiB/2 CPU/128 PIDs、无自动重启。随机生成的对象存储凭据仅写入 0700 私有目录的 0600 文件，并通过容器环境注入。没有操作现有 RustFS、桶、网络、卷或服务。

使用镜像自身生成的 `connection.json`、项目 API key 和 CA，验证服务端证书对 `127.0.0.1` 有效后，调用 `/v1/sql/open`、`/v1/sql/execute`、`/v1/sql/query`、`/v1/sql/close`。每次写入随机表与随机行值，只有执行返回且立即查询逐值相等，才作为 ACK。报告仅公开 JSON 行数组的 SHA-256，不公开随机值或凭据。重启后逐值核对同库、项目凭据与第一行。SIGKILL 后未修改、删除、重建 dirty marker，也未更换应用卷或桶。

## 逐步证据

| UTC 时间 | 事件 | 观测 |
| --- | --- | --- |
| 05:13:12.132 | 首次就绪 | 新 ready 事件数 1，应用 `Running=true`。 |
| 05:13:12.972 | 第一笔 ACK 与即时读回 | 表 `s3_probe_ba314e876c77`；行 SHA-256 `aaa8f15524a053aa4421af1696930e6567847001755482da344b881de78f83a2`，逐值相等。 |
| 05:13:13.543 | TERM 停机 | `docker wait=0`，`ExitCode=0`，`OOMKilled=false`，`Pid=0`；marker 十六进制 `636c65616e0a`（`clean\n`）。 |
| 05:13:15.670 | 同卷重启 | `docker start` 返回 0；累计 ready 事件数 2，应用运行；数据库与项目凭据逐值相同。 |
| 05:13:16.118 | 第一笔恢复读回 | SQL 行 SHA-256 仍为 `aaa8f15524a053aa4421af1696930e6567847001755482da344b881de78f83a2`，逐值相等。 |
| 05:13:16.603 | 第二笔 ACK 与即时读回 | 表 `s3_probe_482e141cb7d9`；行 SHA-256 `4ac19337a7d2199ef3b31c6d3a9fc94fa76579a847b9cd466a56197179a7095d`，逐值相等。 |
| 05:13:16.874 | SIGKILL | `docker wait=137`，`ExitCode=137`，`OOMKilled=false`，`Pid=0`；marker 十六进制 `64697274790a`（`dirty\n`）。 |
| 05:13:17.673 | 原卷直接重启 | `docker start` 返回 0，但 `ExitCode=1`，`Running=false`；累计 ready 事件仍为 2，marker 仍为 `dirty\n`。 |

重启日志末尾为 `project stopped: owned stop proof set incomplete or unexpected`。两个 ready 事件均属于 KILL 之前的启动，不能当作异常中断后的就绪。此时网关进程不存在，第二笔写入无法做 KILL 后 SQL 读回。

## 原始证据与复现

私有证据目录：`/tmp/peri-s3-experiment/peri-s3-r3-ecad12ba90fcbfab60583971`（0700）。其中 `reproduce.py` 是本轮完整可复现脚本；`evidence.json` 含 UTC 时间戳、各阶段 Docker 状态、退出码、marker 字节和 SQL 行值哈希；`initial.log`、`term.log`、`clean_restart.log`、`kill.log`、`dirty_restart.log` 为脱敏后的带时间戳 Docker 日志；`resource-inspect.json` 只收集资源身份、标签、挂载、端口和状态，不含容器环境变量；`manifest.json` 保存公开证据文件 SHA-256。`credentials.json` 与各阶段 `connection.json` 含凭据，只在私有目录内，不列入 manifest。复现时运行 `python3 /tmp/peri-s3-experiment/peri-s3-r3-ecad12ba90fcbfab60583971/reproduce.py`；脚本每次随机生成全新资源。当前应用容器、应用卷、RustFS 容器与卷、网络、桶保留以供独立核验；dirty marker 原样保留。

首次尝试另有独立资源 `peri-s3-r3-ae0a8cafb5426cfd54742c10`：随机 URL-safe access key 含非字母数字字符，固定镜像的 S3 配置校验报 `invalid S3 access key`，应用初启退出 1。其私有证据位于 `/tmp/peri-s3-experiment/peri-s3-r3-ae0a8cafb5426cfd54742c10`，资源保留并与有效实验明确分开。第二轮改用随机十六进制 access key，未修改首次尝试资源。该 fixture 配置错误不计入两项能力判定。

本机单宿主 Docker/RustFS 结果只证明该环境与固定镜像的行为，不能代替多宿主生产故障演练。

独立核验完成后，有效实验与首次无效 fixture 的 RustFS 容器均在确认各自 owner 标签后以 `TERM` 正常退出 0；应用的 dirty 状态仍原样保留。卷、容器和私有证据继续保留供审计。
