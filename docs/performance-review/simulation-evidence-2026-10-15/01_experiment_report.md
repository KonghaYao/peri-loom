# 实验 1：故障路径与发布配置的隔离复核

## 基本信息

- 时间：2026-10-15 UTC；宿主 macOS 26.5.1 arm64，Docker 客户端/服务端 29.4.0，Rust 1.98.1。
- 当前源码身份：`HEAD=9bfe7e9901442e5628cddd2345d235ccd20b3bac`，另有未提交的 `authority.rs` 引用和 `paged_index.rs`；逐文件 SHA-256 在 [身份记录](./01_identity.json)及[命令记录](./01_commands.json)。Rust 测试前后所记录的四个关键源码 SHA-256 相同。
- 假设：已有本地故障模拟与发布配置测试在当前工作树下通过；这些结果只证明各自测试的模拟路径。
- 约束：没有连接 S3、启动/停止任何 Docker 容器、访问现有服务或更改 `trial-ground`。发布配置检查只解析 Compose，测试使用临时目录、内存对象存储与 `127.0.0.1:0` 回环服务。

## 实验方法与原始数据

逐条命令、工作目录、退出码、耗时和日志文件列在 [01_commands.json](./01_commands.json)。Compose 检查所需的变量均设为示例值（`s3.example.invalid` 与测试凭据）；只运行 `config --quiet`，未连接该地址。未把私有凭据写入证据。

| 检查 | 结果 | 原始日志 |
| --- | --- | --- |
| 根 Compose、部署 Compose、部署加 MinIO profile 配置解析 | 3 项退出码均为 0 | [根](./01_compose_root.txt)、[部署](./01_compose_deploy.txt)、[MinIO](./01_compose_minio.txt) |
| Compose 配置单测 | 2/2 通过，退出 0 | [01_compose_unit.txt](./01_compose_unit.txt) |
| 镜像验证器逻辑单测 | 38/38 通过，退出 0 | [01_image_verifier_unit.txt](./01_image_verifier_unit.txt) |
| Rust `authority`、`ownership_view`、`recovery`、`p4_remote`、`p4_runtime`、`remote_database` 集成测试 | 六组共 86/86 通过，退出 0，32.947 秒；另有测试内部子进程显示 1/1 通过 | [01_rust_fault_paths.txt](./01_rust_fault_paths.txt) |

Rust 测试包含本地故障模型：旧会话不应派发物理上传、模糊写入不能授权、续租与接管后旧句柄受 fence、已确认 WAL 冷恢复、损坏根拒绝、pin 保护恢复依赖、torn WAL 不恢复未提交行。这些名称与断言可在被测六个 `tests/*.rs` 文件中复核。编译日志中 `paged_index` 的 dead-code 警告显示该模块尚未接入生产路径；其存在不构成 F01/F02 已修复的证据。

## 与已有 Docker/S3 演练的关系

发现仓库已有 [v1.0.1 四项生产能力模拟结论](../../experiment-production-readiness/conclusion.md)，以及第 1–9 轮报告、独立核验与对抗审查。它们固定发布镜像 `ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`，对应发布 tag `v1.0.1` 的源码基线 `48ce7bf679570f0dba5edb468b5bbc341ae17ae4`；当前 `HEAD` 已有后续提交，且本轮还含未提交模块。`git diff v1.0.1..HEAD` 显示 Rust `io.rs`、`wal.rs`、`worker_rpc/sessions.rs` 及部署 README 等改变。因此旧镜像的端到端结果不能当作当前工作树镜像的测试结果。本轮没有重建或运行镜像。

旧镜像演练足以回答**固定 v1.0.1 镜像在模拟条件下**的四项问题：干净停机同卷恢复通过；SIGKILL 后 dirty 原卷恢复失败并安全拒绝；预先冷备份的手动灾备在单宿主模拟中读回备份前 A、丢失备份后 B，缺生产备份策略和跨故障域证据；S3 同卷预建备用由测试 watcher 启动后仍退出，固定入口 60 秒内无服务。详见[结论](../../experiment-production-readiness/conclusion.md)和[独立对抗复核](../../experiment-production-readiness/07_adversarial_review.md)。这些实测失败不被本轮单测通过所推翻。

## 结论与局限

**DONE_WITH_CONCERNS。** 当前源码的所选本地故障单测与发布配置静态检查全部通过；固定 v1.0.1 发布镜像已有更强的 Docker/S3 证据，结论仍是未通过四项整体生产验收。本轮没有当前工作树镜像构建、真实镜像验证器运行、外部 S3、故障后业务端到端复测，也没有 T01–T12 的冻结条件性能/资源数据；不能宣称 F01–F12 或 T01–T12 通过。若需要当前源码的四项生产能力判定，应在代码冻结后生成可追溯镜像，再按已有演练复测差异涉及的路径。
