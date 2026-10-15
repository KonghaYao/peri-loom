# 实验 4 独立复核：预先备份灾难恢复

判定：**PASS（隔离单宿主演练的手动冷备份恢复）/ PARTIAL（生产灾难恢复能力验收）**。`04_dr_report.md` 对成功运行的核心事实与私有证据一致：故障前归档恢复到新应用卷和新 RustFS 卷，原容器停止，备份前确认的 A 可读，备份后确认的 B 丢失。原卷并未物理损坏或删除，亦未验证跨主机、定期备份、自动恢复、旧副本 fencing 或生产 RPO/RTO。本复核检查脚本、原始文件和 Docker inspect，并对保留的恢复服务独立执行一次 TLS SQL 只读查询；没有重跑写入或更改持久数据。

## 独立证据

- 成功运行 owner 为 `a245460bf8a0d3083fb30f6151361b11`。私有证据目录权限为 `0700`；`result.json` 为 `RESTORE_PROVEN_WITH_B_LOSS`、`pass=true`、`stage=complete`。应用归档为 256000 字节，重算 SHA-256 为 `f2d4063c1226d3a629591b99574584a50eadbe52be33befb47260bc760ae9ae9`；S3 归档为 337920 字节，重算值为 `ad8faca4fc4a49f6df47015bdcf66451096f3885c3dbf82e9e5e5dec1f9861e4`。两者均与 `backup-manifest.json` 的长度和摘要一致，且 manifest owner 与本轮相同。
- 备份和恢复后的对象清单均有 13 个互异 key，逐项 key、长度及对象 SHA-256 相等；两个清单文件各自的 SHA-256 均为 `f9658502a3aeda813b22bf3bc9e9050914e25d4162e54aa4f1b9d9d7c2d6aae0`。脚本在原 S3 停止前逐项 GET 生成前清单，在新 RustFS 启动后逐项 GET 生成后清单。清单比对可信，但本复核没有重新向运行中的 S3 发请求。
- Docker 按 `peri.dr.owner` 标签保留 8 个容器、4 个卷和 1 个网络。两个源服务容器均停止，退出码 137；两个恢复服务正在运行。恢复应用仅挂载 `app-restore`，恢复 RustFS 仅挂载 `s3-restore`，均未挂载两个源卷。备份容器以只读方式挂载对应源卷；提取容器以只读方式挂载归档目录并写入对应恢复卷，均以退出码 0 结束。源和恢复应用镜像 ID 相同，源和恢复 RustFS 镜像 ID 相同；两应用的容器配置均使用 `S3_ENDPOINT=http://rustfs:9000` 和同一隔离桶名。
- 两份私有连接文件的数据库结构、项目凭据、全局凭据、原始 endpoint 和 CA 路径逐值相等；没有输出凭据或数据库值。脚本用相同数据库和项目凭据执行 TLS SQL，写入 A 后收到 `{"executed": true}` 并读回 A；备份后写入 B 收到同样确认并读回 A、B；恢复后查询只得 A。`result.json` 记录 `ack_a_restored=true`、`ack_b_restored=false`、`lost_acknowledged_writes=1`。正常停机后复制出的 Authority 标记经重读为 `clean\n`。
- **独立在线 SQL 复核**：通过 Docker inspect 核对恢复容器 owner 标签、运行状态及 loopback 发布端口，使用私有连接文件中的项目凭据和 CA 经 TLS 打开同一数据库。只读查询发现唯一 `dr_` 表，查询该表仅返回一行；该行值的 SHA-256 与 `ack_a` 事件摘要相同，与 `ack_b` 摘要不同，且 B 摘要不在结果中。关闭查询会话后未改动持久数据。此举提高了“恢复后 A 在、B 不在”的证据置信度；原始 A/B 写入确认仍只由驱动控制流和结果记录证明，因当时未保存独立 SQL 请求响应 transcript。

## 时间指标和范围

脚本以同一进程的 `time.monotonic()` 计算：**RPO 4.045 秒**为 `source_fault` 到最近可恢复的已确认写入 A 的时间差；**RTO 2.983 秒**为源应用及 S3 KILL 完成后，到新应用 TLS SQL 成功读回 A 的时间差。备份完成到已确认但丢失的 B 为 2.471 秒，实际丢失一笔确认写入。UTC 事件在处理和写入报告后记录，仅可交叉检查先后顺序：A 05:26:49.141743、备份 05:26:50.338795、B 05:26:52.809396、故障 05:26:53.239654、完成 05:26:56.170550。原始 monotonic 时间戳未持久化，故无法由日志独立精确复算三个秒数。RTO 不含备份制作，也不含自动检测、调度和异机数据传输；这两个数不能作为生产承诺。

故障时源服务被 KILL，恢复容器没有挂载源卷；这是**恢复路径对原卷无依赖**的证据。四个 Docker 卷仍存在，原始数据并未物理失效。新 RustFS 从原 S3 卷归档恢复包含同名桶的状态，不是通过 S3 API 新建空桶后逐对象导入。计划第 3 项所说的源卷及源对象存储不可用于恢复，在本轮以停止源服务和隔离挂载模拟；物理损毁、跨宿主恢复及旧副本再次启动后的双主风险均未覆盖。报告已明确这些限制，因此“模拟演练通过”成立，“生产 DR 已验收”不成立。

另外两个保留的运行是 `PARTIAL`：05:20 UTC 一轮停在 `backup_complete`，05:24 UTC 一轮停在 `source_fault` 并报 `S3 listing failed`。它们没有完成恢复；报告所述旧端口与恢复 S3 就绪问题与阶段相容，但现存第一轮 `result.json` 未记录错误细节，不能仅凭原始结果独立证明具体根因。

## 增强重跑独立复核（05:42 UTC）

增强重跑 owner 为 `9ed915ac7dc64c4d8919a1a5a10186e1`，私有证据目录为 `/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-dr-9ed915ac7dc64c4d8919a1a5a10186e1-ixtwvo7t`，权限 `0700`。本次 `result.json` 为 `RESTORE_PROVEN_WITH_B_LOSS`、`pass=true`、`stage=complete`；以下均是只读独立复核，未输出连接凭据或 SQL 值。

- 从 `timepoints_monotonic_ns` 原始整数重新计算：`(source_fault − ack_a) / 10⁹ = 4.098342` 秒，四舍五入三位为 **RPO 4.098 秒**；`(restored_sql_read − source_fault) / 10⁹ = 3.040093625` 秒，为 **RTO 3.040 秒**；`(ack_b − backup_complete) / 10⁹ = 2.43325125` 秒，为备份到丢失的 B 确认间隔 **2.433 秒**。三项均与报告字段一致。`restored_sql_read` 事件还记录一行、首值哈希匹配 A、B 不在。存储的时间点能独立复算算术，但时间点由同一驱动采集，仍不是外部时钟测量。
- 应用归档 256000 字节，重算 SHA-256 `06dc2555d354db54c1ba6a07014804658d2c0bc3343a542ac43e63cf3970dcd2`；S3 归档 337920 字节，重算 SHA-256 `28fa9ed54653bf473fdf2b7e3c59df46bcadc551763495e76a30adec6b681b51`。两者长度、摘要及 owner 均与 `backup-manifest.json` 一致。备份与恢复对象清单各含 13 个互异 key，逐项完全相等，两个清单文件 SHA-256 同为 `29fca46e66b655e2dec1b394150f961fb3bb3ea916e07d6b1573199f2f8cd98d`。
- Docker owner 标签下保留 8 个容器、4 个卷、1 个网络。源应用与源 RustFS 均已停止，退出码 137；恢复应用和 RustFS 正在运行且仅挂载各自恢复卷。两份私有连接文件的数据库标识、项目凭据、全局凭据及 endpoint 逐值相等；原应用 clean Authority 标记仍为 `clean\n`。
- **独立在线 TLS SQL**：核对恢复容器 owner、运行状态及 loopback 端口后，以该轮私有项目凭据和 CA 打开恢复数据库，查询唯一 `dr_` 表。结果仅一行，其值的 SHA-256 与该轮 `ack_a` 哈希精确匹配，`ack_b` 哈希不在结果中；与 `restored_sql_read` 事件的行数、首值哈希和 B 缺失标记一致。查询会话已关闭，未写入持久数据。这进一步独立证实增强重跑的恢复后 A 在、B 无；故障前 A/B 的写入 ACK 依旧依赖驱动记录。

**判定仍为 PASS（本机手动预先备份恢复）/ PARTIAL（生产 DR 验收）**。增强重跑修补了首次运行无法精确复算时间差和缺少恢复 SQL 结果摘要的证据缺口；它没有改变故障模型。源卷保留、恢复仅逻辑隔离源卷，备份与恢复均在同一宿主，数据量很小且镜像已缓存；没有物理卷损坏、异机搬运、周期性备份、自动故障检测、自动恢复、旧副本 fencing 或生产 RPO/RTO 保证。

## 最终强故障重跑独立复核（05:51 UTC）

最终重跑 owner 为 `eeea7514d9d9e9c1a424ab265b1bf0e7`，私有证据目录 `/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-dr-eeea7514d9d9e9c1a424ab265b1bf0e7-os69fdv2` 权限为 `0700`。`result.json` 为 `RESTORE_PROVEN_WITH_B_LOSS`、`pass=true`、`stage=complete`、`source_resources_retired=true`。以下是独立只读核验，没有输出凭据或 SQL 值。

- **源资源确实不可用**：按 owner 标签列出 4 个容器、2 个卷和 1 个网络；四个容器只包括两项恢复服务和两项提取任务，两个卷只包括 `app-restore`、`s3-restore`。逐个 `docker container inspect` 两个源服务和 `docker volume inspect` 两个源卷均返回不存在。驱动先按 owner 与 Docker ID 核对已停止的两个源服务及两个备份容器，再删除四个容器与两个源卷，`source_resources_retired` 事件记录了该步骤。恢复服务运行中，仅挂载对应新卷，提取容器退出码为 0。由此，最终重跑比前两次更强地证明恢复没有使用源卷或源 RustFS 状态；源卷删除属于本轮隔离资源的模拟故障，并非真实磁盘毁损。
- **独立归档和对象**：应用归档为 256000 字节，重算 SHA-256 `16acab56bb06ab016f30a218803108eb7ff2ee049afddd0e19db2a149e246a33`；S3 归档为 337920 字节，重算 SHA-256 `6134d68f2f91138b5a4632ff44f9534322cfcf896c9ccfd00d6fdb64b7c9cd48`。两者与 `backup-manifest.json` 中的大小、摘要及 owner 相符。备份前与恢复后对象清单各有 13 个互异 key，逐项完全相等；两个清单文件 SHA-256 同为 `76ac3bc570edfee7ce969f729af6d7201f4d2f2d68c052af3b64cf9107d67238`。应用 clean Authority 标记核对为 `clean\n`。
- **同身份与在线 SQL**：源和恢复连接文件中的数据库标识、项目凭据、全局凭据、endpoint 和 CA 路径逐值相同。核对恢复容器 owner、运行状态和 loopback TLS 端口后，使用该轮私有连接文件与 CA 做独立只读 SQL 查询。唯一 `dr_` 表仅有一行，其值 SHA-256 与 `ack_a` 事件精确匹配；`ack_b` 摘要不在结果中，与驱动记录的 `restored_sql_read` 行数、首值摘要和 B 缺失标记一致。查询会话已关闭，没有写入持久数据。历史 A/B 写入确认仍依赖驱动执行时的 ACK 和立即读回记录。
- **时间可复算**：`timepoints_monotonic_ns` 给出 `(source_fault − ack_a)/10⁹ = 4.028149125` 秒，三位小数为 **RPO 4.028 秒**；`(restored_sql_read − source_fault)/10⁹ = 3.363335292` 秒，为 **RTO 3.363 秒**；`(ack_b − backup_complete)/10⁹ = 2.5163445` 秒，为备份后 B 确认间隔 **2.516 秒**。三项均与 `result.json` 相符。RTO 包含删除本轮源资源、提取归档、启动恢复服务与 TLS SQL 读回；不含预先备份的制作。

**最终判定：PASS（隔离单宿主中，源容器及源卷删除后的预先备份手动恢复）；PARTIAL（生产 DR 能力验收）。** 最终重跑满足本计划第 3 项在模拟环境中“源卷及源对象存储不可用于恢复”的更强条件，并实际观察到一笔备份后确认写入丢失。归档保存在同一宿主私有目录，恢复依赖人工预置脚本和已缓存镜像；没有异机备份或恢复、周期性备份、独立故障检测、自动恢复、大数据量测试、旧副本自动 fencing 或可承诺的生产 RPO/RTO。原始时点由同一驱动采集，虽可复算算术，仍非外部独立计时。
