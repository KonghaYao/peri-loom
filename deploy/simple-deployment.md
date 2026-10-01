# Simple 单机部署

Simple 模式使用一个主进程、一个 HTTP 端口和一个本地数据目录。运行时不需要 PostgreSQL、S3、Worker、独立 WAL 节点、Nginx 或 Node。它适合开发与小型可信用户部署；同一台机器上的备份不能保护磁盘损坏。

## 启动

预编译二进制：

```sh
./peri-loom serve --mode simple --data-dir ./data
```

从源码构建可运行 `bash scripts/build-simple.sh`，产物为 `target/release/peri-loom`。构建环境需要仓库指定的 Rust、Node 和 protoc 3.x；已有其他 protoc 版本时可用 `PROTOC` 指向 3.x 可执行文件。Node 只在构建时使用。实例数据应放在源码仓库之外；源码压缩包不能作为实例备份。

执行队列与结果帧均有上限，可用 `--queue-capacity` 和 `--max-result-frame-bytes` 调整。默认单行结果必须小于 256 KiB，超限会明确报错；大结果应通过 NDJSON 或 Hrana cursor 流式读取。单机模式不承诺逐库硬内存隔离。

首版达到打开库上限时明确拒绝新库请求，需要停止闲置库释放容量；暂不做自动 LRU 回收。后台任务以持久状态轮询恢复，不引入事件总线。整实例导出采用停机方式；Simple 到 distributed 的跨模式数据迁移尚不支持原地切换。

默认监听 `127.0.0.1:8080`。容器镜像使用 `docker-compose.simple.yml`，默认将宿主的 `127.0.0.1:8080` 映射到容器端口，并使用 `peri_data` 命名卷：

```sh
docker compose -f docker-compose.simple.yml up --build -d
```

如需其他端口，可设 `PERI_LOOM_PORT`。也可用 `--listen`、`--max-open-databases`、`--max-sessions-per-database`、`--queue-capacity`、`--log-level` 调整单机设置。`--tls-cert` 和 `--tls-key` 必须同时提供；未配置 TLS 时默认只在回环地址使用。Simple 不读取分布式模式的数据库、对象存储或 WAL 服务地址。

首次启动生成随机管理员密码，写入 `data/secrets/initial-admin.json`，以及持久化的 JWT 签名密钥。读取凭据后应按本机权限管理该文件；登录与授权继续使用现有 API。实例锁阻止两个进程同时打开同一数据目录。不要把该目录放在共享网络盘，也不要将同一目录直接切换为 distributed 模式。

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
