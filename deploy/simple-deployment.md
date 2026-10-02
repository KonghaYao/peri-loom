# Simple 单机部署

Simple 模式使用一个主进程、一个 HTTP 端口和一个本地数据目录。运行时不需要 PostgreSQL、S3、Worker、独立 WAL 节点、Nginx 或 Node。它适合开发与小型可信用户部署；同一台机器上的备份不能保护磁盘损坏。

## 启动

预编译二进制：

```sh
./peri-loom serve --mode simple --data-dir ./data
```

从源码构建可运行 `bash scripts/build-simple.sh`，产物为 `target/release/peri-loom`。构建环境需要仓库指定的 Rust、Node 和支持 proto3 optional 的 protoc（流水线使用 28.3）。Node 只在构建时使用。实例数据应放在源码仓库之外；源码压缩包不能作为实例备份。

执行队列与结果帧均有上限，可用 `--queue-capacity` 和 `--max-result-frame-bytes` 调整。默认单行结果必须小于 256 KiB，超限会明确报错；大结果应通过 NDJSON 或 Hrana cursor 流式读取。单机模式不承诺逐库硬内存隔离。

首版达到打开库上限时明确拒绝新库请求，需要停止闲置库释放容量；暂不做自动 LRU 回收。后台任务以持久状态轮询恢复，不引入事件总线。整实例导出采用停机方式；Simple 到 distributed 的跨模式数据迁移尚不支持原地切换。

默认监听 `127.0.0.1:8080`。可用 `--listen`、`--max-open-databases`、`--max-sessions-per-database`、`--queue-capacity`、`--log-level` 调整设置。`--tls-cert` 和 `--tls-key` 必须同时提供。

## 本地开发

本地源码开发可执行 `bash scripts/dev-simple.sh`：debug 后端监听 `127.0.0.1:18081`，Vite 热更新页面位于 `http://127.0.0.1:5174`，默认沿用仓库下的 `data`。按 Ctrl+C 同时停止两者。可通过 `PERI_LOOM_API_PORT`、`PERI_LOOM_WEB_PORT`、`PERI_LOOM_DATA_DIR` 覆盖设置；开发脚本需要 Python 3 做端口检查。这里的 Vite 仅用于开发，发布版仍是一个内嵌 Web 的二进制。

## mise 安装与 GitHub Release

Simple 以原生二进制发布，不需要独立 Compose。首次正式 Release 发布后，使用 mise 的 [GitHub backend](https://mise.jdx.dev/dev-tools/backends/github.html) 安装：

```sh
mise use -g github:KonghaYao/peri-loom@latest
mise exec github:KonghaYao/peri-loom@latest -- peri-loom serve --mode simple --data-dir ./data
```

启用 mise shell integration 后可直接执行 `peri-loom serve --mode simple --data-dir ./data`。固定版本可将 `latest` 替换为 Release 版本；升级前先停止进程并导出数据。服务运行时只需要可执行文件和数据目录，不需要 mise 常驻。

[publish-simple.yml](../.github/workflows/publish-simple.yml) 构建内嵌 Web 后编译以下平台，每个平台都解压产物并启动真实二进制验证：

| 平台 | Release 附件后缀 | 构建 runner |
| --- | --- | --- |
| Linux x64 | `x86_64-unknown-linux-gnu.tar.gz` | `ubuntu-22.04` |
| Linux ARM64 | `aarch64-unknown-linux-gnu.tar.gz` | `ubuntu-22.04-arm` |
| macOS Apple Silicon | `aarch64-apple-darwin.tar.gz` | `macos-14` |

Linux 产物面向 glibc 2.35+（如 Ubuntu 22.04+），不用于 Alpine/musl；macOS 最低部署目标为 13。Windows 不在本次支持范围内。

- 仅推送 `v*` 标签时触发构建；普通分支推送、PR 不触发，也不提供手动触发入口。构建上传三个平台的 Actions artifacts，保留 14 天。
- 全部构建成功后上传 GitHub Release，再使用 mise 在三个平台实际下载安装并复验。带 `-` 的版本标签发布为 prerelease，不替换 `latest`。
- 附件形如 `peri-loom-v1.2.3-aarch64-apple-darwin.tar.gz`，解压只有 `peri-loom`。每个包附带 `.sha256`，Release 同时提供 `SHA256SUMS`；可在安装前校验，mise 项目可用 `mise.lock` 固定版本与校验值。
- 已发布版本不会原地替换二进制；重试只允许继续未完成的 Release 草稿。代码未推送、工作流未运行时，安装命令不能凭本地构建自动获得远端 Release。

## Docker 镜像

`Dockerfile.simple` 构建内嵌 Web 的单进程镜像。推送 `main` 或 `v*` 标签时，[独立镜像工作流](../.github/workflows/publish-simple-image.yml) 会构建 Linux x64 镜像并发布到 `ghcr.io/konghayao/peri-loom/simple`；标签规则与 distributed 镜像一致（`main`、版本号及 `latest`）。首次发布后若 GHCR 包仍为私有，需要先登录。

使用已发布镜像：

```sh
docker run -d --name peri-loom-simple --restart unless-stopped \
  -p 127.0.0.1:8080:8080 -v peri_data:/data \
  ghcr.io/konghayao/peri-loom/simple:latest
```

也可以从源码构建：

```sh
docker build -f Dockerfile.simple -t peri-loom:simple .
docker run --rm --name peri-loom-simple -p 127.0.0.1:8080:8080 -v peri_data:/data peri-loom:simple
```

镜像使用 `/data` 持久卷，监听容器内的 `0.0.0.0:8080`，并通过 `/readyz` 报告健康状态。运行二进制时按 Ctrl+C 正常停止；容器可用 `docker stop peri-loom-simple` 停止。

首次启动生成随机管理员密码，写入 `data/secrets/initial-admin.json`，以及持久化的 JWT 签名密钥。读取凭据后应按本机权限管理该文件；登录与授权继续使用现有 API。实例锁阻止两个进程同时打开同一数据目录。不要把该目录放在共享网络盘，也不要将同一目录直接切换为 distributed 模式。

## 连接数据库与申请 Token

建库成功后会显示 libSQL 和 TursoDB 的连接地址、端口；数据库详情首屏也保留这些信息。两种 SDK 共用当前入口端口，通过数据库路径区分各库。

在**数据库详情 → 数据库连接 → 申请本库 Token** 一键申请，将本次弹窗中的 Token 复制到应用的 `DB_TOKEN` 环境变量，并填入 SDK 的 `authToken`。明文只显示一次，关闭后无法再次查看；后台只保存哈希。管理台的账号密码/JWT 用于管理登录，不能当作 SDK Token。

每个数据库最多保留一个未吊销 Token。再次申请需明确轮换，旧 Token 随即失效；同一个 Token 不能跨库访问、管理数据库生命周期或签发其他凭据。建库、启停、备份和恢复等管理操作需要管理 JWT。凭据列表可查看绑定库、状态和吊销凭据，申请和轮换回到对应数据库详情。SQL SDK 需要 `db:write`，`db:read` 仅允许读取数据库信息，并不代表 SQL 只读权限。

管理 API `POST /api/v1/tokens` 必须提供 `database_id` 与 `name`；省略权限时默认 `db:read`、`db:write`，`rotate: true` 表示明确轮换本库 Token。需要管理 JWT 和 `token:admin` 权限。旧版未绑定数据库的 Token 不再接受认证，需要在对应数据库详情重新申请；升级不会影响管理员登录 JWT。

## 备份与恢复

数据库备份由后台作业通过引擎 checkpoint 产生一致文件集。操作完成后会校验本地快照的压缩数据与 SHA-256，恢复时在暂存目录验证全部文件后替换目标库。数据库恢复会使原有会话失效。快照在 `data/objects`，临时文件在 `data/tmp`；不要只复制单个 `main.db` 作为在线备份。

整实例迁移或离机保存需先正常停止服务，再运行：

```sh
./peri-loom export --data-dir ./data --output ./peri-loom-export
./peri-loom import --input ./peri-loom-export --data-dir ./restored-data
```

导出要求源目录可取得排他实例锁，并拒绝未完成作业或操作。若被拒绝，先启动 Simple 服务，让恢复作业完成，再正常停止后重试。导出包含元数据、数据库文件及本地 WAL、对象、身份与密钥；不包含实例锁和可清理临时文件。导入校验文件清单、大小、SHA-256、实例身份及格式版本，仅发布到**不存在**的新目录，不覆盖现有数据。导入保留用户、令牌和 JWT 密钥。当前版本要求导出方与导入方的二进制包版本相同；升级时先留存旧版本二进制与导出，完成验证后再迁移，不能把新的数据目录交给旧版程序原地回退。

## API 与能力

`GET /api/v1/deployment` 返回 `mode`、`contract_version`、`durability` 及能力布尔值。Simple 的 `durability` 为 `local_fsync`，不提供远程 quorum LSN、跨 Worker 迁移或逐库硬隔离。查询响应的 `wal_lsn` 与会话响应的 `worker_id` 在 Simple 中为 `null`；distributed 仍返回原有值。Simple 不支持的集群动作返回 `NOT_IMPLEMENTED`，错误 detail 的 `reason` 为 `UNSUPPORTED_IN_DEPLOYMENT_MODE`。Web 根据能力隐藏相应入口，保留登录、SQL、操作、备份和恢复。

本地可靠提交依赖操作系统、文件系统和硬件正确履行同步语义。强杀重启测试只能验证进程崩溃恢复，不能替代断电或丢失未同步写测试。部署前应完成目标文件系统上的提交、checkpoint、恢复及故障注入验收；当前实现阶段的实际测试记录见 [simple-deployment-progress.md](simple-deployment-progress.md)。
