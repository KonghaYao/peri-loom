# 实验 3 独立验证：S3 原卷重启

验证时间：2026-10-15。范围：只读核对 [验收计划](./00_plan.md) 的第 1、2 项、[实验报告](./03_s3_restart_report.md)、私有原始证据及本轮 Docker 资源；未重跑、重启或清理。

## 结论

| 门槛 | 独立判定 | 依据 |
| --- | --- | --- |
| S3 干净 TERM 后同卷恢复 | **PASS** | 首次 SQL execute 成功返回，随即逐值读回；TERM 退出码 0、marker 为 `clean\n`；同一容器和应用卷再次启动产生第二个 ready 事件，同一数据库及项目 API key，第一笔写入再次逐值读回，行哈希相同。 |
| S3 SIGKILL 后原卷直接恢复 | **FAIL（可用性）；PASS（安全拒绝）** | 第二笔写入在 KILL 前 execute 成功返回且逐值读回；KILL 退出码 137、marker 为 `dirty\n`。原容器、原卷直接重启后退出码 1，无第三个 ready 事件，marker 仍为 `dirty\n`，日志显示 `owned stop proof set incomplete or unexpected`。因此无法做 KILL 后 SQL 读回。 |

两项合计 **PARTIAL**：干净恢复成立，异常中断恢复不满足计划中“无需人工绕过安全标记即可重新对外服务”的条件。安全拒绝不代表可用性恢复。`docker start` 返回 0 仅说明 Docker 接受启动请求。

## 核对记录

- 有效证据目录为 `/tmp/peri-s3-experiment/peri-s3-r3-ecad12ba90fcbfab60583971`。`manifest.json` 列出的 11 个文件 SHA-256 均重新计算匹配；目录权限为 0700，凭据与两个 `connection.json` 均为 0600，且未列入 manifest。本验证未输出凭据内容。
- `evidence.json` 的 UTC 顺序完整：05:13:12 首次 ready，05:13:12 第一笔 ACK 与读回，05:13:13 TERM，05:13:15 第二次 ready，05:13:16 第一笔重启后读回与第二笔 ACK，05:13:16 KILL，05:13:17 直接重启失败。带时间戳日志中的 ready 分别发生于 05:13:11.720 和 05:13:15.239；失败日志发生于 05:13:17.225。Docker 状态中的各次 `StartedAt`/`FinishedAt` 与该顺序一致。
- `reproduce.py` 实际调用 `/v1/sql/open`、`/v1/sql/execute`、`/v1/sql/query`、`/v1/sql/close`；只有请求正常返回且查询结果与随机写入值逐值相等才记录 ACK。重启后读回用同样的逐值断言。第一笔行 SHA-256 在重启前后均为 `aaa8f15524a053aa4421af1696930e6567847001755482da344b881de78f83a2`；第二笔 KILL 前行 SHA-256 为 `4ac19337a7d2199ef3b31c6d3a9fc94fa76579a847b9cd466a56197179a7095d`。证据只保存哈希和比较布尔值，未保存原始 SQL 响应体。
- 实时只读 Docker inspect 显示，本轮网络、两个卷、应用及 RustFS 容器均保留并带相同 `peri.experiment.owner=ecad12ba90fcbfab60583971` 标签。应用卷仍挂载到 `/data/peri`，RustFS 卷仍挂载到 `/data`；应用状态为 `exited`、退出码 1，RustFS 为 `running`。应用只将 TLS 端口绑定到 `127.0.0.1`，RustFS 没有宿主端口发布。
- 固定发布 digest `ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333` 在本机镜像 `RepoDigests` 中，image ID `sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102` 与应用容器一致。容器命令确为 `--backend s3`；环境中的桶名与私有证据的独立桶 `peri-r3-ecad12ba90fcbfab60583971` 一致，端点是本轮网络内的 `rustfs:9000`。仅比较了环境值，没有输出密钥。
- 首次失败 fixture 使用独立 owner `ae0a8cafb5426cfd54742c10`、桶和 Docker 资源。其 access key 含非字母数字字符，`app-tree/logs/authority.log` 记录 `invalid S3 access key`，首次就绪事件数为 0、应用退出码 1；没有 SQL 写入，不能计入重启能力判定。有效实验改用十六进制 access key，未复用首次 fixture 的资源。

## 局限与复现性

这是本机单 Docker daemon 和隔离 RustFS 桶的模拟；未验证多宿主或真实云 S3。桶创建由脚本的 `mc mb` 成功返回及事件记录支持，本次未独立调用 S3 API 枚举桶或对象。SQL ACK 和逐值比较由脚本及其 `evidence.json` 记录支持，本次因应用已停止没有再次在线查询；KILL 后持久数据字节是否完整也无法由本实验判定。

`reproduce.py` 位于上述有效证据目录，完整记录资源创建、TLS 校验、SQL、故障注入和证据采集；每次运行会随机生成新的 owner、资源、桶和写入值。它依赖本机 Docker、缓存镜像及仓库内 `prototypes/v1/platform/dev.py`，因此是环境相关的可复现脚本，不是封存的离线证明。本次只读验证没有执行该脚本。私有证据和仍保留的资源供后续核验；不得公开其中凭据或连接文件。
