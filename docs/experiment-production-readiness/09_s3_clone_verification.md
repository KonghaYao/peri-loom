# 实验 9 独立核验：共享 S3 桶跨卷冷克隆

**总评：PASS（限于本轮观察与启动拒绝机制）；外推为普遍 HA / 全局单主保证：PARTIAL。** 本次只读核验 `09_s3_clone_report.md`、驱动及脱敏 JSON，私有原始结果、归档和日志，并现场检查保留的 owner-labelled Docker 资源。没有重跑实验、写入桶、修改或清理资源。

## 证据链

| 核验项 | 结果 | 依据 |
| --- | --- | --- |
| 固定发布镜像与独立资源 | PASS | Docker 本地 digest 映射：应用 `ghcr.io/konghayao/peri-loom@sha256:901f37f2…` → image ID `sha256:81ef392b…`；RustFS `rustfs/rustfs@sha256:1803faef…` → `sha256:6024cc1b…`。源、A、B 均为同一应用 image ID；RustFS 为指定 image ID。owner `03971fdb3d590d76e5d19ce6ee6f2931` 下存在专用 bridge 网络、RustFS 容器及 `s3-data` 卷、三个不同的应用卷；均有 `peri.dr.owner` 标签。创建时间集中在 06:12:04 UTC。四个容器仅配置动态 `127.0.0.1` 端口绑定，均已停止。 |
| 同一 S3 目标与凭据 | PASS | 现场 `docker inspect` 中，源、A、B 的 `S3_ENDPOINT`、`S3_BUCKET`、`S3_REGION`、`S3_ACCESS_KEY_ID`、`S3_SECRET_ACCESS_KEY` 五项非空且逐项相等；RustFS 的 access/secret 与应用对应项相等。endpoint 为 `http://rustfs:9000`，桶为本轮唯一 `peri-clone-<owner>`。三个应用命令行相同，挂载不同卷。凭据值没有写入本文。 |
| 数据库与项目 API key | A：PASS；B：PARTIAL | 私有源、A `connection.json` 的数据库和项目 API key 相等，A 经 TLS 读回源表。B 未 ready，未生成可供比较的运行时 `connection.json`；B 与 A 从同一归档分别恢复，足以说明启动前文件来源相同，不能直接证明 B 运行时最终配置。 |
| 源 clean 与完整归档 | PASS | 私有 `result.json` 记录源 SQL `{"executed":true}`、逐值读回匹配、TERM 后 06:12:09.434 UTC 退出 0 且非 OOM。`cold-app-state.tar` SHA-256 为 `b65ba7a0274fe1d10470162503ac60a0d0a3ceb33acf192520ff9e2384eb8012`，与报告、manifest、恢复事件一致；归档有 53 个条目，含完整状态目录、`authority/authority.lock` 的 `clean\n` 和 `launcher-terminal.json`。驱动通过只读源挂载制作归档，恢复前校验同一 SHA-256，并向两个新卷分别解包。 |
| 启动时序与结果 | PASS | Docker 状态的 A/B `StartedAt` 分别为 06:12:10.774 / 06:12:11.134 UTC；驱动先 `create_app` 启动两者，再依次等待 ready。A 于 06:12:12.583 ready，源表 TLS 读回匹配；B 于 06:12:26.531 退出 1、非 OOM，驱动未观测到 ready；06:12:27.238 观察时 A 仍运行。A 与 RustFS 随后 TERM 退出 0。驱动只有在两者都 ready 且运行时才尝试克隆 SQL 写入，本轮没有执行该分支。 |
| B 拒绝原因 | PASS（本轮） | 私有 `clone-b.log` 精确为 `project stopped: authority exited before a joined clean proof`。停止容器内 `logs/authority.log` 另有更直接的一行：`dirty backend authority seal; old remote terminal is unproven; state-dir replacement and --reconciled cannot authorize takeover`；B 的本地 `authority.lock` 为 `dirty\n`。`prototypes/v1/src/authority_rpc/seal.rs` 的 `Seal::acquire` 读取 `authority-control/single-authority-seal`，要求旧记录 `clean` 才能以条件写取得新 seal。源停机与最终观察间，桶中该 seal 对象及另一个 authority 对象的哈希变化，总对象数保持 13。日志和代码共同支持“本轮 B 因共享后端 seal 为 dirty 而拒绝启动”；报告所引的是 launcher 收尾错误，并非 authority 最具体的诊断。 |
| 私有证据完整性与公开 JSON | PASS | 私有目录权限 `0700`，所有现有文件 `0600`；`manifest.json` 中归档、B 日志、凭据、探针、复现脚本和原始结果的 SHA-256 与当前文件一致。逐项扫描公开 `09_s3_clone_evidence.json`：没有私有 S3 access/secret、项目 API key、SQL 探针表名/值、TLS 私钥原文或连接配置原值；只保留相等性布尔值、事件时间、摘要、随机 owner/桶名及 Docker ID。无需删除或脱敏。 |

## 判定范围

本轮**确证** A ready 且能读源数据，B 未 ready、退出 1，且 B authority 因同一 S3 后端的 dirty seal 拒绝启动；共享桶启动 fencing 的本轮诊断成立。`docker logs` 对 B 返回空文本，诊断来自其卷内日志与私有 `clone-b.log`，应在复核时区分日志来源。

本轮**没有证明**所有并发启动竞态、网络分区、多宿主部署或长期运行时仍保持全局单主；也没有双 ready、双写 ACK 或写后读回结果。源和克隆均运行在同一 Docker 网络及单个 RustFS 实例上。B 的运行时数据库/API key 因启动失败无法直接采集。单轮结果不构成自动监测、接管或 HA 通过证据；实验 8 对自动接管的结论仍独立适用。
