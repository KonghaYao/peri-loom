# 第 10 轮预注册：当前机器的真 S3 冷缓存 SQL 正确性（2026-10-15）

## 范围

对**已提交**版本上已有的 `minio_network_fresh_cache_sql_recovery` ignored 测试只执行一次，独立于正式 stress harness 和 challenger。测试在 `trial-ground` 自有、回环绑定且 CPU 0.5/内存 512 MiB 限额的 RustFS 容器内新建随机独立桶；显式 `PERI_S3_TEST_RETAIN=1` 确保任何结果都不自动删除桶。测试创建包含两行的表，先 checkpoint，提交两行后丢弃活跃数据库/本地 cache，用新的空 cache 读回 count=2，再 checkpoint、丢弃本地 cache、再次冷打开读回 count=2 和 integrity=`ok`。这些均是现有测试内固定的断言，不增加额外 SQL 发压或自动重试；S3 客户端允许 2 次网络重试，不单列重试次数。

## 保全和停止

运行前检查自有容器的 Compose project、digest-pinned 镜像、回环映射、卷、资源配额及凭据与私有夹具匹配；源码 Git HEAD 记录于运行意图，必须是干净工作树；0700 私有目录、0600 日志和意图。只允许该测试恰好运行 1 次、1 个线程、上限 120 秒；达到上限仅结束**本次自有 cargo 进程组**，保留日志及远端桶，所有结果视为 Unknown 而不是通过。若任何一步失败，停止本轮、不复跑、不清理现场。现有测试会有意删除本地 cache 来证明冷打开，因此失败时缓存目录可能不保留，不能声称完整进程现场。

记录首次退出状态与原始日志；成功仍需核对用例名及 `1 passed; 0 failed; 0 ignored`，再次检查自有容器身份，并在独立只读 `mc ls` 中核对新桶及对象条目（不读对象字节）。如果日志交错或只读复核失败，明确标注包装器 FAIL，不倒填 PASS。不存在端到端 Gateway、TLS、并发吞吐、业务 SLO/ACK/s/p99 或 P0–P6 联合门禁；只能称“当前机器上已提交源码的单项隔离 S3 SQL 正确性复测”。
