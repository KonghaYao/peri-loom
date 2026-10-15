# 第 1 轮：性能测试准入实验（2026-10-14）

## 假设与方法

假设：现有平台在独立环境中可先通过 P0–P6 联合正确性门禁，再开始商业数据库基准测试。本轮只验证已具备的离线门禁和资源，**没有运行数据库压力测试**。不得用本轮结果证明吞吐、延迟或生产适用性。

测试源码：`c1eea11caffd893390001517149380e824aeb6a2`，但工作树包含并发进行的未提交变更和未跟踪文件；此哈希**不能**代表完整构建内容。测试使用仓库中已有的 Rust debug 二进制，尚未核对其与当前源码的一致性。

## 原始证据与结果

| 检查 | 结果 | 证据位置 |
| --- | --- | --- |
| `python3 -m unittest discover -s trial-ground/tests -p 'test_*.py' -v` | 7/7 PASS；仅夹具安全单测 | `trial-ground/private/challenger-preflight-2026-10-14.md` |
| `python3 prototypes/v1/stress/provision.py --self-test` | 19/19 安全单测 + 1/1 凭据单测 PASS；无网络调用 | 同上 |
| `cd prototypes/v1/stress && go test ./...` | PASS；无实际 SQL 负载 | 同上 |
| `cd prototypes/v1/platform && GOMAXPROCS=2 PERI_RUST_BINARY=<仓库 debug 二进制> go test -p 1 -timeout 180s ./...` | FAIL：`TestP6VerifyScopedTokensAndNativeReadonly` 中一次应拒绝的 `/v1/sql/open` 返回 HTTP 200；后续整包超时 180 秒 | `trial-ground/private/platform-go-test-2026-10-14.log`（私有原始日志） |
| `python3 trial-ground/trialctl.py check` | `images_pinned=false`，`main_ready_file_present=false`；独立存储尚未启动 | `trial-ground/private/challenger-preflight-2026-10-14.md` |

首次可见失败位于 `prototypes/v1/platform/p6_verify_security_test.go:100`。该测试包含多处拒绝断言，目前尚不能从日志判断命中的是哪一个；平台文档同时允许全局管理密钥对既有数据库执行 SQL，须先排除测试期望与接口合同不一致。**不能**据此推断特定租户泄露，也**不能**忽略这一安全门禁失败。

## 性能指标

| 指标 | 本轮状态 |
| --- | --- |
| ACK/s、offered/actual/admitted、p50/p95/p99/max、错误/Unknown、客户端丢弃 | NOT MEASURED |
| CPU/RSS、磁盘/网络、对象存储请求与延迟、队列等待、恢复时间、受害租户进展 | NOT MEASURED |
| S01 最小事务验证、S04 租户边界、S06 吞吐拐点、S08 noisy neighbor | SKIP：联合门禁及独占夹具未就绪 |

## 停止与下一步

按 `challenger/ORDERED_SCENARIOS.md` 的停止条件，保留首次失败和未知状态，不发起后续真实负载，不改门槛覆盖失败结果。先定位并修复安全测试失败、验证构建与源码身份、在独立隔离夹具取得实际 P0–P6 联合 PASS。随后冻结机器预算、SLO、种子和阈值，再顺序执行 S01/S04 正确性验证与 S06/S08 容量实验；生成第二轮报告并进行独立复核与对抗审阅。未经真实可重复样本和企业验收，不能宣称 SpaceX 认证或可用于商用生产。
