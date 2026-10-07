# R15 隔离 SDK/UI 作者事实（等待独立原始证据审核）
- 判定：SDK3 本轮成功；UI 唯一实际运行 FAILED，组合验收 HOLD。本页不是独立整体验收或性能结论。
- Main explicit GO 后仅用新 R15/19453 的健康 default demo；新 Authority/bucket 与旧 R14 潜在 Unknown 隔离，不是旧 UI 重试、回滚或恢复证明。
- 私有证据目录：`/private/tmp/peri-sdk-ui-isolated-ygstvdhd`（0700）；凭据只在内存或派生 0600 文件，Go 文件完成后删除，公开记录 secret=false。
- SDK 命令：matching Go `peri-cli sql --query <固定只读 oracle> --request-id <本轮 ID> --endpoint <new endpoint> --database <authorized identity> --credential-file <private file> --ca-file <proper CA>`；TS `NODE_EXTRA_CA_CERTS=<proper CA> node <frozen sdk-live.mjs> <private connection>`；Rust `<new frozen example> <private connection>`。

| Case | 实际 exit / 秒 | 核验类型与 oracle | Query request_id |
| --- | --- | --- | --- |
| Go i64 | 0 / 0.989 | JSON 两列精确整数：9223372036854775807、-9007199254740993 | `r15-go-i64` |
| Go integrity | 0 / 0.087 | JSON 一行一列 `ok` | `r15-go-integrity` |
| TypeScript | 0 / 0.234 | native SELECT 两列 bigint、readonly status 成功 | `ts-live-exact-integers` |
| Rust | 0 / 0.213 | native SELECT 两列 `Value::as_i64()`、readonly status 成功 | `rust-live-exact-integers` |

- SDK 安全既有审计共16条：4 open、4 query、4 close 各 `ok`，另2 status各 admitted/complete；每新会话仅一次业务调用与成功 oracle 后一次 CloseACK，未观察 Unknown/失败重试。
- SDK public Close IDs：Go `50e13e49784222713f7fba31c8826f0c` / `76e28db3eea70ac18090e41dcc3b8dce`；TS `467ae19a3636fee974d2cfa000f39441`；Rust `054ca709932738ec8060e08d8ea3ea5b`。完整安全 ID/action/outcome 与各 exit 在 `sdk-actual.json`。
- SDK 全部 proper CA，未禁用 TLS 校验；TS 实际 dist SHA `68ce175fb7128499c81c1868321943b19a77ce9e3a9b80854d427d5f2d2a6a07`，没有宣称本轮重新编译 TS。
- Go matching CLI SHA `26f7a378f3b1b6cda38d329308adc99285478e197141b9e6ae5d5f7d96b61291`；Rust new 0500 example SHA `16ec821d1ae2c6baeb69c5f17c8f5922adacb6a1ec3815c66929c4f505dc51a9`；SDK选定7项 before/after一致。
- Server 仍是 frozen Native `64ec5bd6e7b143b95151c9a52934c03f42a2a5396297f4cbe91c06de33c5f85e` / Go `c25245389361dd602def477e36d65059e0834796178ba9c09bd3377623f0028d`，文件哈希与 main-launch 记录一致，未冒称未来 Root 诊断版本。
- 私有 v3 作者离线14 PASS；Gauss 独立14 PASS/语法/5项哈希后 Main另给 UI GO，proof SHA `a88fe0f6e4d5822a922b4d139e75ebb9e27dbeb805c9892e3909165cd325a0f5`，路径 `/private/tmp/peri-ui-v3-independent-ila7prkv/proof.json`。
- 唯一 UI 命令：`python3 /private/tmp/peri-sdk-ui-isolated-ygstvdhd/ui-run-v3.py`，实际 exit1；捕获器 exit1；root helper PID18326/UID501 完整身份已捕获，自然 exit code1/signal=null，未向 helper 发信号。
- `ui-capture-v3-result.json` 的闭集 first_safe_cause=`childSignal` 表示 child 非0退出，不代表实际收到 signal；原 child stdout/stderr仅记录存在，不能还原具体失败 phase。
- UI records=0；exact i64 JSON/NDJSON、refs graph、CloseACK、storage0 均 NOT PROVEN，未将 helper 工具状态当模型结果通过。
- `ui-existing-audit-after-sdk.json` 记录 SDK 最后 known Close 之后新增0事件；审计缺席不是未派发证明，UI 潜在 Unknown 不 Close/reopen、探测或重跑。
- Chrome PID18269/UID501：signal前完整 PID/UID/start/ARGV两次匹配，SIGTERM后实际 joined exit0；`SingletonLock` 为断裂 symlink，lexists=true，不能声称 lock removed/clean profile。
- UI只用新 own profile/CDP loopback与单 leaf-SPKI例外，不用 global TLS ignore、用户浏览器或 keychain；该浏览器方式不证明 CA-chain 验收，storage无凭据仍未验。
- `ui-owner-v3-result.json` 的10项 UI选定输入 before/after一致；已捕获 `/json.js`、`/refs.js`、`/rpc.js` 与 Root对应字节相同，未捕获 app/index，不声称完整 served lineage。
- 本轮无自动第二次运行、失败后 SQL Close/额外 HTTP诊断；Root/SDK源码与旧 R14原始证据未修改。日志、锁状态和本轮失败均保留。
- 后续：Gauss独立 raw审核；UI首因仍待闭环，需要新的授权隔离实验。Root P4 HOLD/完整 Rust watchdog失败与历史压力失败不被本轮 SDK成功覆盖；performance=false。
