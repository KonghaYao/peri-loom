# 架构研究备忘（2026-10-13）

目的：把 Turso、Tailscale、Neon 自述的架构选择与故障经验转成 peri-loom 的**挑战假设**，不是断言 peri-loom 与其实现相同，也不把竞品的 SLO/语义强加于本项目。对应测试见 `ORDERED_SCENARIOS.md` 的 S30–S40。以下只引用官方的一手工程资料；文章反映发表时的设计，不代表这些产品此刻的全部实现。

## Turso：对象存储原生与多租户摊销

- [Turso Cloud Goes Diskless](https://turso.tech/blog/turso-cloud-goes-diskless)：作者区分“本地可作缓存”和“本地作为持久主副本”，讨论将 durable 写入交给对象存储之后的请求延迟、跨可用区访问成本、海量低活跃数据库下的小写入请求摊销，以及 checkpoint 后把近期 WAL 折入持久数据。启发 S30–S32：**ACK 时远端真正持久**、热冷混合时对象存储请求与尾延迟、多库共享基础设施上的相互干扰。文中 S3 Express One Zone 和合批策略是 Turso 的具体选择，**不是本项目已采用的组件或保证**。

## Tailscale：分片 SQLite、恢复与罕见竞态

- [How Tailscale helped find the SQLite WAL-Reset bug](https://tailscale.com/blog/sqlite-wal-reset-bug)：Tailscale 描述按 shard 划分控制面，每个 shard 一个 SQLite 库、一个 Go 进程访问；从 S3 备份的 `integrity_check` 报警追查到 checkpoint 与并发写之间的罕见 WAL-reset 竞态。文章亦说明升级后一度出现计算表达式索引的假阳性不一致，不能把所有校验报警等同于真实数据损坏。启发 S33–S35、S40：备份必须定期实际恢复，低概率时间窗口需要可控交错和法证记录，验证升级前后索引语义及校验器行为。SQLite 的具体 WAL-reset 缺陷**不等于 Turso fork/本项目也有同一缺陷**。
- [Tailscale control/data planes](https://tailscale.com/docs/concepts/control-data-planes)：控制面故障不应被表述为既有数据流全部立即消失。类比本项目时，仅验证该 DB/tenant 的隔离、已有会话和新会话的行为；不声称 peri-loom 具备 Tailscale 网络数据面特性。

## Neon：时间点读取、不可变层与读路径可用性

- [GetPage@LSN — A Deep Dive into Neon’s Storage Engine](https://neon.com/blog/get-page-at-lsn)：Neon 自述的设计将计算和持久化拆分，WAL 流向 safekeepers、pageserver 构建不可变 delta/image layers；读取指定 LSN 的页面需组合相应历史图像与增量；branch 是写时复制，compaction/GC 要兼顾历史可读性和读放大。启发 S36–S39：读版本边界、深增量链尾延迟、GC/分支竞争和“数据安全但读取暂时不可用”的区分。Neon 的 LSN/Paxos/safekeeper/pageserver **不是 peri-loom 组件**；对应替代观测是 manifest commit、branch refs、reader pin、segment 链与 Authority/Worker。

## 横向推导规则

- **可迁移的是故障形态，不是实现方式**：对象存储成本、ACK 持久化、备份恢复、checkpoint/GC 的发布顺序与缓存缺失的延迟都需要独立 oracle。
- **不能类推的性质**：Turso 的 S3 Express/合批时延、Tailscale 的网络数据面存活、Neon 的 quorum/LSN 可见性和 PostgreSQL 语义，都不能当成本项目当前的实现或 PASS 门槛。
