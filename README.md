# peri-loom

基于自建 TursoDB Engine（OSS fork）的 Diskless / Serverless 数据库平台。

核心演进目标：

> `database = file` → `database = manifest + immutable segments + WAL`

## 架构一句话

> Go 控制面（Controller / Gateway / Key Broker）+ Rust 数据面（Authority / Workers）+ Object Storage 权威。
> **Storage 是最终 fencing authority**：Go 可以暂时做错调度，但不能因此导致数据损坏。

- **Rust Authority**：唯一持有 raw `ObjectStore` 的受信任角色；一个 durable root CAS 串行化 refs、WAL 发布、commit history、reader pins、upload intents 与 GC。
- **Rust Worker**：multi-DB runtime；本地磁盘只做 cache（RAM / SSD segment cache），lazy load，epoch / lease fencing。
- **Go Controller**：placement、lease 签发续期、failure detection、failover / migration 编排。
- **Go Gateway**：`/v1/sql/*`、`/v1/databases/*`、多租户与配额、内嵌管理控制台。
- **Key Broker**：native 页加密 / KMS；只有 Authority 接收 S3 凭据。
- **引擎**：Turso OSS fork（`prototypes/p0/turso` submodule，固定 gitlink `d0c23d6a16f457d382059a84f84f9c4463c85a84`）。

## 本地启动

```sh
git submodule update --init prototypes/p0/turso
python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-local --tls
```

启动器构建并启动 Authority、两台加密 Worker、Controller、Gateway 和 Key Broker。
`connection.json` 包含私有测试 API Key 与开发 CA 路径，**不可公开**；访问其中的
Gateway 地址即可打开内嵌管理控制台。真实 S3 仅由 Authority 接收凭据。

### Docker Compose（生产部署配置）

仓库根目录的 `compose.yaml` 使用固定镜像、外部 S3 和项目内的 `deploy/state`、
`deploy/tls` 绑定目录，不使用 Docker 命名卷。先按
[`prototypes/v1/deploy/README.md`](prototypes/v1/deploy/README.md) 的「Production Compose configuration」
准备证书、目录、S3 和私有 `deploy/production.env`，再运行
`docker compose --env-file deploy/production.env up -d --no-build --pull always`。

**当前仍不能用于实际生产上线**：启动器会拒绝复用已有状态目录，重启/故障恢复尚未完成验收。
请勿删除持久化数据以绕过保护。需要本地构建体验时，使用上面的 Python 启动方式或
`prototypes/v1/deploy/compose.yaml` 开发配置。

## 目录结构

```text
prototypes/v1/            Rust Authority / Workers + Go 平台（当前主线，独立 workspace）
  src/                    Rust：authority / worker / storage / wal / encryption / ownership / gc
  controller/             Go：ownership controller（独立 Go module）
  platform/               Go：gateway / broker / admin / controller 集成 + 内嵌控制台
  tests/                  原生集成与验收（含真实 S3 / RustFS 用例）
prototypes/p0/            P0 Core Prototype + Turso fork submodule
scripts/                  工具链安装与性能工具（perf-reliability）
third_party/sqlite/       SQLite 官方源码 submodule（测试语料，不链接进平台）
```

## 文档入口

- 本地平台运行、连接与安全边界：[`prototypes/v1/platform/README.md`](prototypes/v1/platform/README.md)
- V1 构建与各阶段命令：[`prototypes/v1/README.md`](prototypes/v1/README.md)
- Docker / Compose 部署及镜像验收：[`prototypes/v1/deploy/README.md`](prototypes/v1/deploy/README.md)
- P0 内核与 Turso 分支：[`prototypes/p0/README.md`](prototypes/p0/README.md)

## 本地构建

```bash
# Rust 数据面（独立 workspace）
cargo build --manifest-path prototypes/v1/Cargo.toml --locked
cargo test  --manifest-path prototypes/v1/Cargo.toml --locked

# Go 平台
cd prototypes/v1/platform && go build ./cmd/peri-platform ./cmd/peri
cd prototypes/v1/controller && go build ./cmd/peri-controller
```

## 国内环境（已内置）

| 组件 | 镜像 | 配置位置 |
|---|---|---|
| crates.io | `rsproxy.cn`（sparse） | `.cargo/config.toml`（随仓库提交） |
| Rust 工具链 | `rsproxy.cn` dist | `rust-toolchain.toml` |

Rust 工具链固定为 `1.98.1`（见 `rust-toolchain.toml`），CI 与生产不得浮动使用 `latest`。

### 首次安装工具链

```bash
./scripts/install-toolchain.sh   # 通过 rsproxy 安装 rustup + 1.98.1 + protoc 25.1
```
