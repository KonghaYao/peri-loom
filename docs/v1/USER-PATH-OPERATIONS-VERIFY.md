# 用户运维路径与总入口：独立静态核验

## 结论与范围

1. **最终静态核验 PASS。** 修正后的运维篇命令、启动流程、凭据隔离、关闭判据及压力授权描述与当前源码一致；总入口的阅读顺序、记录模板与执行边界正确。
2. 本报告只核验 `USER-PATH-OPERATIONS.md` 与 `USER-PATH.md`；不核验本代理自己编写的 `USER-PATH-PLATFORM.md`，也不对数据库篇、SDK 篇作独立验收结论。
3. 核验只读文档与源码，仅写本报告。未执行正文命令、真实网络/API/SQL、fixture 启动、build、测试负载或信号；没有修改作者文档、生产代码、`CLAUDE.md`，没有提交。
4. PASS 指文档的静态正确性，不是新实例已启动、压力已完成、恢复已通过、性能提升或全阶段联合 PASS。所有真实执行仍须遵循文档规定的授权与证据要求。

## 冻结输入

1. 父仓库观察 HEAD：`9819a47f88fd3f51a551f1c01f328563cd5d14c3`。共享工作树仍有其他作者的修改；本报告依据直接读取的当前文件，不把 HEAD 单独当作全部 dirty/untracked 内容证明。
2. 运维篇：`docs/v1/USER-PATH-OPERATIONS.md`，257 行，最终 SHA-256：
   `d767bafd9adca9c96f35bcebe0d92102c6f1ee771d9c2ca74c3cae2b9bbb7c7d`。
3. 总入口：`docs/v1/USER-PATH.md`，38 行，SHA-256：
   `87c3e446078704dc88386e44e9de31fc5ff912a67807424affc7627f141eafb1`。
4. 最终语法与链接检查前重新核对上述两个输入的 SHA，均与作者冻结值一致。总入口只检查路径引用、顺序与安全约束，不借链接存在宣称对应篇已验收。

## 发现、作者修正与复核

1. **初始路径问题，不予通过原版本。** 作者初始运维篇 SHA 为
   `432fe71e1d053013241abe81eb6927bbba87019afa01c5660405a1d7c7a704ae`，将锁写成 `authority/Authority.lock`。
   主线程指出疑点后，本代理独立对照 `prototypes/v1/src/authority_rpc/startup.rs:32` 的
   `directory.join("authority.lock")` 与 `prototypes/v1/platform/dev.py:213` 的 Authority 状态目录参数，确认实际为 `authority/authority.lock`。
   `startup.rs:61` 写入精确 `clean\n`。反馈作者修正；不引用旧报告的大写误写，也不触碰真实锁文件。
2. **第一次修正并非最终放行。** 作者修正路径后冻结值为
   `d2523106573940db418ebc077b78fa6f44b0a41a9e9894cfbcb713cbe94b3bc4`；继续独立核查后发现下列两项问题。
3. **Release 产物路径问题。** 原命令隐式继承 Cargo target 配置，却将 `PERI_RUST_BINARY` 固定指向默认 `target/release`，存在误选旧二进制的可能。
   作者已显式取得 rustc host，并给 Cargo 同时指定 `--target` 与 `--target-dir`，变量指向对应 host 下的 release 产物；也要求确认 host 非空、构建成功并记录实际二进制 SHA。
   该修正避免把 `CARGO_TARGET_DIR` 或默认跨 target 配置生成的其他产物误认成命令输出；本代理没有执行构建来验证性能或运行结果。
4. **故障 oracle 过强问题。** 原文把通用 faultprobe 选择目标 worker 的依据称为当前 Authority binding 确认。
   对照 `faultprobe/owner.go:6`、`faultprobe/database.go:27`，实际是 management status 的 placement/grant 校验，加私有 topology 与进程身份核验。
   作者已明确这不是新的实时 Authority lease/物理 I/O 终止证明，并区分专用 joint 脚本的额外 Authority state 校验。
5. 上述三项均由原作者修正文档，本代理只反馈与复核；最终输入为 `d767bafd…`。保留初始问题与修正顺序，不将最终 PASS 写成所有版本均正确。

## 逐项源码核验

1. **源码与依赖准备：PASS。** 子模块初始化路径为 `prototypes/p0/turso`，工具链来自根 `rust-toolchain.toml`。
   文档要求记录实际 HEAD、工作树差异与子模块身份，没有授权更新依赖、清理其他人工作树或修改 `CLAUDE.md`。
2. **本地 TLS 启动：PASS。** 对照 `platform/dev.py:108`、`:206`、`:304`：`--run-dir`、`--backend local`、`--tls`、`--listen`、`--no-build` 均为真实参数。
   新私有目录使用 canonical 路径与 `umask 077`；launcher 未指定 `--no-build` 时执行 locked Cargo build 和 Go build。
   整栈 READY 位于认证 provisioning 之后，种子是 `local/default/local_default/demo/main`；不是任意端口监听即就绪。
3. **S3 与 TLS 凭据隔离：PASS。** 对照 `dev.py:58`、`:206` 与 `src/config.rs`：S3 变量只注入 Authority，worker/Go 使用 allowlist 环境与各自内部 token。
   S3 bucket 必须由管理员预先准备，存储 identity、backend、listen、TLS 不能在旧目录中静默重绑。
   文档不提供真实 secret、不要求打印环境、不允许禁用 TLS 校验，也没有授权清理现有用户 bucket。
4. **CLI 与私有凭据文件：PASS。** 对照 `cmd/peri/main.go:37` 与 `cmd/peri/credentials.go:15`：`PERI_ENDPOINT`、`PERI_CA_FILE`、`PERI_CREDENTIAL_FILE` 是实际入口。
   CLI credential 文件检查 owner、私有权限、常规文件、长度与 `O_NOFOLLOW`。文档派生文件使用 `O_EXCL`、`O_NOFOLLOW`、0600，不输出 credential。
   `status`、`sql --query 'SELECT 1'`、`sql --query 'PRAGMA integrity_check'` 与 CLI 实际命令及互斥 query/execute 规则一致。
5. **指标与健康范围：PASS。** 对照 `management.go:156`：全局 admin 才能 `GET /v1/metrics`；不存在本文虚构的 `/health`。
   示例 HTTPS 客户端信任指定 CA、禁用 proxy/redirect、限制响应体并检查 envelope；完整响应只保存到私有文件。
   文档区分实际 metrics、unavailable/null、diagnostics 与实时 lease/终止证明，不把一次 SELECT 或 integrity 结果推广为全部数据库健康。
6. **关闭与重启：PASS。** 对照 `dev.py:255`、`platform/service.go:142`、`authority_rpc/server.rs:116`：关闭顺序为平台、worker、Authority，并等待进程结果。
   Authority 请求线程 join、session store 释放及 quiescent 成立后才写 clean；shutdown 回复或 PID 消失不是整栈 clean。
   文档只给经核对的本人 launcher 单 PID SIGTERM 与 `wait`，保留非零退出；不提供删除 dirty marker 或自动 `--reconciled` 的强行恢复步骤。
   重启复用原 connection 并检查数据库 status，与 `dev.py` 一致。
7. **压力 provisioning：PASS。** 对照 `stress/provision.py:100`、`:244`、`:304`：新 canonical owned `peri-stress-*` 私有目录、S3/TLS 文件与 CA 校验是真实前置条件。
   `--plan` 只读本地，不创建输出或 reservation；`--execute` 才进行管理请求与一次性 reservation。
   `parent-baseline.json` 是 parent runner 的 ConnectionConfig，不是 launcher connection 的直接替代；未知结果保留 reservation/输出，不重放或删除证据。
8. **压力 runner 与授权：PASS。** 对照 `stress/cmd/peri-stress/main.go:19`、`stress/gate.go`、`stress/scenarios.go`、`stress/runner.go:36`：所有示例 flags 与场景名均存在。
   实际运行要求与 source_root 当前 HEAD 一致的 main joint gate；`-list` 在读取 connection、调用 runner 前返回。
   Gate 是操作者声明而非密码学证明；dirty/untracked/binary provenance 须额外记录。源码与文档均不把短 gate、SKIP、部分完成或原始 ACK/s 推广成全套性能 PASS。
9. **串行、未知结果与 raw evidence：PASS。** parent runner 使用 global/output 锁，输出 `run_<id>/run.json` 和 `operations.jsonl`。
   文档要求保留首次失败与未知结果，不删除 stale locks 来并行启动、不提高配额来制造成功、不重试 SQL 或清 poisoned session。
   未来压力示例仍需单独当轮授权，本次没有实际运行。
10. **故障与清理边界：PASS。** 对照 `platform/tests/joint-s3-failover.py:24`：专用脚本硬编码历史 fixture，不能通用替换目录直接运行。
    通用 faultprobe 的 `-execute`、`-joint-pass`、`-rust-binary`、`-allow-worker-sigkill` 均为真实开关；`-list` 不触发 probe。
    对照 `faultprobe/host.go:129`，目标限已验证 worker 单 PID，退出证明之后才触及其已验证 cache；保留 TOCTOU 与缺少实时 lease oracle 的限制，不扩展到 Authority/platform/launcher、用户容器或 S3 删除。
11. **总入口：PASS。** 38 行入口链接均存在，阅读顺序先运维再平台/数据库/SDK，再授权后的恢复与压力。
    它要求版本、实际操作、脱敏证据、PASS/FAIL/NOT RUN 分开记录；禁止未知结果后重放事务，不将本地原型宣传为生产 HA。

## 静态检查与可追溯锚点

1. 独立抽取最终运维文档的 **13 个 shell block**，逐个以 `/bin/zsh -n` 解析；**13/13 PASS**，没有执行 block。
   抽取其中 **2 个 Python heredoc**，以 `ast.parse` 解析；**2/2 PASS**，没有运行其中的文件读取、网络或写入逻辑。
2. 最终两份文档的 Markdown 链接目标存在检查 PASS，`git diff --check` PASS。
   语法通过仅说明可解析，实际行为的结论来自以上源码逐项对照，不等同于已运行这些命令。
3. 核验时直接源码 SHA-256 锚点：
   - `platform/dev.py`：`55a76fd63db9be44827d6456a7b0c55205b3079f4920bc02311d6bbdb5deb83d`。
   - `src/authority_rpc/startup.rs`：`4474ea7e68cf880338c05776345f13bb97604cc365b81d83dd6bbf11cea1e0cb`。
   - `src/service_cli.rs`：`693c26dbf045fb6b15ed2be359b04327b3151d7f781424ddacc791991045d1a2`。
   - `stress/gate.go`：`13868e1953064304c8d45efce5e3dd44ef78eb005b22eb34fb8726d9d56ec2f7`。
   此处路径以前缀 `prototypes/v1/` 为基准；这些锚点不是对全仓库或二进制的重新门禁。
4. 最终没有未解决的本次文档 blocker。其他篇交叉验收、F 的代码门禁、真实 S3/故障/压力及主线程最终提交各有独立责任，本文不代替它们。

## 发布限制：独立增量复核

1. **增量静态 PASS。** 运维篇仅在源码准备步骤新增发布限制 4 行，现为 261 行，最新冻结 SHA-256 为 `2cc655b2b7cc9dd67593b6eec375dd3144088c3594458936312cc935a0846305`；本值取代上文首次完成核验时的 `d767bafd…` 输入身份，上文原检查过程作为历史记录保留。
2. 独立删除该唯一新增段后计算 SHA，精确恢复 `d767bafd9adca9c96f35bcebe0d92102c6f1ee771d9c2ca74c3cae2b9bbb7c7d`，证明其余字节未变；总入口仍为 `87c3e446…`，没有重复全套语法或运行门禁。
3. 本地只读确认 parent gitlink 与已完整 checkout 的 Turso HEAD 均为 `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`，子模块 origin 配置为对应 fork。提交尚未 push 是维护者声明；本代理没有联网确认远端可达性，也没有 fetch/push，不能保证全新网络 clone 当前可以取得该固定提交。
4. 新段准确限制复现范围：已有完整本机 checkout 可使用本地依赖；全新 checkout 取决于维护者发布。fetch 失败须停止联系维护者，不能 vendor、改 gitlink、换提交或自行 push 掩盖缺失。没有扩大启动、凭据、故障、清理或压力授权。
5. 本次只追加报告，不改作者文档、平台篇、生产代码或 `CLAUDE.md`；未网络/API/SQL、build、load、信号或提交。结论仍限文档静态正确性。
