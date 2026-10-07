# 用户验收路径：启动、运维与实验复现

本篇负责运维路径；数据库业务操作、平台权限管理由对应用户路径文档展开。所有命令从仓库根目录执行，路径变量由操作者为**自己的新隔离实例**设置。下面是可复现操作说明，不代表本次文档编写已经执行了构建、API、压力、故障或信号操作。

已完成的功能验收来源见 `docs/v1/P6-VERIFICATION.md`、`docs/v1/P6-LAUNCHER-REVIEW.md` 和 `docs/v1/JOINT-VERIFICATION.md`。这些报告不等于任意新版本、任意新 fixture 或后续压力轮次自动通过。不要使用其他人的运行目录、连接文件、端口、PID、bucket 或缓存来照抄执行。

## 一、准备源码与独立运行目录

1. **确认实际源码与依赖。** 切换到自己的 checkout；初始化固定的 Turso 子模块。使用仓库 `rust-toolchain.toml` 指定的工具链；确认 Go、Python 3 和 OpenSSL 可用。Python 在开启开发 TLS 时调用 OpenSSL。

   ```sh
   cd /path/to/peri-loom
   git rev-parse HEAD
   git status --short
   git submodule update --init prototypes/p0/turso
   git -C prototypes/p0/turso rev-parse HEAD
   ```

   预期证据：实际 HEAD、工作树差异和子模块 gitlink。不要修改 `CLAUDE.md`、清理不属于自己的改动、更新依赖或把未知源码直接当作已验收版本。

   发布限制：当前验收提交仅保存在本地，父仓库和 Turso 子模块的对应提交尚未 push；已有完整子模块的本机 checkout 不受影响。
   全新网络 checkout 能否 fetch 固定 gitlink，取决于维护者是否已在对应 fork 发布该提交，不能保证现在可从全新 clone 重现。
   若子模块 fetch 失败，停止并联系维护者发布；不要 vendor、改 gitlink 或换成其他提交掩盖缺失，也不要据此自行 push。

2. **创建私有目录并记录根路径。** 以下示例对 macOS 的 `/tmp` 别名做 canonical 化，也可在 Linux 使用。后续步骤继续使用同一终端中的变量。

   ```sh
   umask 077
   export RUN_DIR="$(mktemp -d /tmp/peri-ops-tls.XXXXXXXX)"
   export RUN_DIR="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$RUN_DIR")"
   ```

   预期证据：新目录属于当前用户，权限为 0700；其下敏感文件为 0600。不要将该目录放进源码、构建上下文、共享盘或截图中。

3. **选择未占用的 loopback 端口。** 本文用 `127.0.0.1:19443` 作为示例，不代表它在你的机器上空闲。若端口已由别人的服务占用，换端口，不停止该服务。记录自己选择的端口及后续 `ready` 输出。

## 二、启动完整 Local + TLS 项目

1. **使用正式 launcher，而不是逐个拼接服务。** 在新目录启动：

   ```sh
   python3 prototypes/v1/platform/dev.py \
     --run-dir "$RUN_DIR" --backend local --tls \
     --listen 127.0.0.1:19443 \
     > "$RUN_DIR/launcher.stdout.log" \
     2> "$RUN_DIR/launcher.stderr.log" &
   LAUNCHER_PID=$!
   ```

   未指定 `--no-build` 时，launcher 实际调用 locked Cargo build 和 Go build，然后启动一台 Authority、两个加密 worker、Controller/Gateway/KMS Broker。这是单 Authority 开发部署，不是 HA。

2. **等待完整就绪记录。** 查看自己的 `launcher.stdout.log`，必须出现 `ready:true`、`service:"local-project"`，并给出 gateway 地址、`connection_file` 和 `topology_file`。不要把某个 TCP 端口已监听、一个子进程已输出 ready 或后台 shell 成功当作整栈 READY。

   预期文件：`connection.json`、`topology.json`、`launch.json`、`platform.json`、私有 `logs/`、`authority/`、`controller/`、`platform/`、`keys/` 和两个 cache 目录。首次启动实际创建组织 `local`、项目 `default` 和加密数据库 `local_default/demo/main`；READY 包含认证 provisioning 成功，而不只是静态配置完成。

3. **正确验证 TLS。** `connection.json` 的 `ca_file` 指向本实例 CA。默认 `--tls` 生成独立 CA 与签发的 leaf 证书；只对该隔离实例信任它。禁止 `curl -k`、`NODE_TLS_REJECT_UNAUTHORIZED=0` 或其他跳过证书验证的方式。

   已有证书时，先通过私有环境机制设置 `PERI_TLS_CERT`、`PERI_TLS_KEY`，并按信任链设置 `PERI_TLS_CA`，再启动同一命令。私钥文件不能进入源码、日志或 argv 内容；外部 listener 必须使用 TLS，暴露网络前还需自行评估认证及部署边界。

4. **需要 release 变体时，先显式构建，再禁用 launcher 重建。** 以下是未来操作命令；仅用于自己的新实例：

   ```sh
   export PERI_NATIVE_TARGET="$(rustc -vV | sed -n 's/^host: //p')"
   cargo build --locked --release --manifest-path prototypes/v1/Cargo.toml \
     --target "$PERI_NATIVE_TARGET" --target-dir "$PWD/prototypes/v1/target"
   export PERI_RUST_BINARY="$PWD/prototypes/v1/target/$PERI_NATIVE_TARGET/release/peri-loom-v1"
   export PERI_PLATFORM_BINARY="$RUN_DIR/peri-platform"
   (cd prototypes/v1/platform && go build -o "$PERI_PLATFORM_BINARY" ./cmd/peri-platform)
   ```

   然后在步骤 1 的 launcher 参数中增加 `--no-build`。本例显式使用当前 rustc host target 和输出目录，不继承 `CARGO_TARGET_DIR` 或跨 target 默认值后误选旧产物；应确认 host 值非空、构建成功，并记录两个实际二进制的 SHA256，而非只记录 mtime 或 profile 名称。debug→release 是部署变体，不自动构成性能提升证据。

## 三、启动完整 S3 + TLS 项目

1. **准备专用存储与新的运行目录。** 由存储管理员创建专用测试 bucket；不要复用用户业务 bucket。新建自己的 `RUN_DIR`，与 Local 实例分开；沿用第二节同样的 TLS 与 READY 验证流程。

2. **只通过私有环境注入真实凭据。** launcher 读取 `S3_ENDPOINT`、`S3_BUCKET`、`S3_ACCESS_KEY_ID`、`S3_SECRET_ACCESS_KEY`，可显式设置 `S3_REGION`。使用用户批准的私有终端/秘密管理机制；本文不提供真实值。

   禁止在命令参数、shell history、源码、run 配置、CI 输出、`env`/`printenv` dump 中保存或展示真实 S3 密钥。launcher 的子进程环境是 allowlist：原始 S3 变量只传给 Authority，worker 和 Go 平台不接收 raw backend 凭据。不要手动给 worker 增加这些变量。

3. **在已注入私有环境的终端启动。**

   ```sh
   python3 prototypes/v1/platform/dev.py \
     --run-dir "$RUN_DIR" --backend s3 --tls \
     --listen 127.0.0.1:19443 \
     > "$RUN_DIR/launcher.stdout.log" \
     2> "$RUN_DIR/launcher.stderr.log" &
   LAUNCHER_PID=$!
   ```

   release 预构建实例可增加 `--no-build`，但必须先确认上一节的二进制变量指向实际存在的对应版本。不要把 Local 的旧运行目录改成 S3，或修改已有目录的 endpoint/bucket/region/TLS/listener；launcher 会拒绝存储身份或运行配置的静默重绑定。

4. **验证成功范围，不擅自清理存储。** READY 后通过下节公共 gateway 进行认证读取、状态与指标检查。若启动失败，保留日志及私有目录，不盲目重放未知 mutation，不删除 S3 对象或 bucket 来制造成功。专用 bucket 的最终清理由拥有明确授权的管理员负责；此操作说明不授权触碰现有 RustFS 容器或其他用户资源。

## 四、凭据、健康检查与指标

1. **只读取必要连接字段，不打印整份配置。** `connection.json` 同时含 project/global API key；`platform.json` 和 `launch.json` 还含内部服务凭据。不要 `cat` 后粘贴进工单。可以为 CLI 派生仅属于本实例的私有 credential 文件：

   ```sh
   python3 - <<'PY'
   import json, os, pathlib
   directory = pathlib.Path(os.environ["RUN_DIR"])
   connection = json.loads((directory / "connection.json").read_text())
   destination = directory / "project-key.txt"
   descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
   with os.fdopen(descriptor, "w") as output:
       output.write(connection["project_api_key"] + "\n")
   print("private project credential file created")
   PY
   export PERI_ENDPOINT="$(python3 -c 'import json,os,pathlib; print(json.loads((pathlib.Path(os.environ["RUN_DIR"])/"connection.json").read_text())["endpoint"])')"
   export PERI_CA_FILE="$(python3 -c 'import json,os,pathlib; print(json.loads((pathlib.Path(os.environ["RUN_DIR"])/"connection.json").read_text())["ca_file"])')"
   export PERI_CREDENTIAL_FILE="$RUN_DIR/project-key.txt"
   ```

   `O_EXCL` 防止覆盖旧凭据；已有文件时应检查其归属与版本，不反复复制。这里输出的是文件完成消息和非秘密 endpoint/CA 路径，不输出 key。不要在命令中拼接真实 `Authorization` header。global key 用于全局管理/metrics，不能代替 project key 打开 SQL session。

2. **构建真实 CLI 并做只读健康检查。**

   ```sh
   (cd prototypes/v1/platform && go build -o "$RUN_DIR/peri" ./cmd/peri)
   "$RUN_DIR/peri" status --database local_default/demo/main
   "$RUN_DIR/peri" sql --database local_default/demo/main --query 'SELECT 1'
   "$RUN_DIR/peri" sql --database local_default/demo/main --query 'PRAGMA integrity_check'
   ```

   预期证据：真实 placement/status、`[[1]]` 与 `[["ok"]]`。它们分别验证控制面可观察性、认证 SQL 通路与 native 数据完整性；任何单项都不是所有服务/所有 DB 的健康证明。客户端不会自动重放 SQL。若返回未知结果，应停止后续写入并保留 request ID，不循环重试来得到绿灯。

3. **使用真实 GET `/v1/metrics`，不是虚构的 `/health`。** 该接口需要 global admin key；project key 被拒绝是权限边界，不是全局 metrics 成功。以下命令通过私有文件取 key、验证本实例 CA、禁用 proxy/redirect，并把完整结果只存于私有目录：

   ```sh
   python3 - <<'PY'
   import json, os, pathlib, ssl, urllib.request
   directory = pathlib.Path(os.environ["RUN_DIR"])
   connection = json.loads((directory / "connection.json").read_text())
   class NoRedirect(urllib.request.HTTPRedirectHandler):
       def redirect_request(self, request, response, code, message, headers, destination):
           return None
   context = ssl.create_default_context(cafile=connection["ca_file"])
   client = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=context))
   request = urllib.request.Request(connection["endpoint"] + "/v1/metrics", headers={"Authorization": "Bearer " + connection["global_api_key"]}, method="GET")
   with client.open(request, timeout=10) as response:
       encoded = response.read(1024 * 1024 + 1)
       if response.status != 200 or len(encoded) > 1024 * 1024:
           raise SystemExit("metrics response unavailable or too large")
   result = json.loads(encoded)
   if result.get("version") != 1 or result.get("ok") is not True or result.get("error") is not None:
       raise SystemExit("metrics envelope rejected")
   destination = directory / "metrics-response.json"
   descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
   with os.fdopen(descriptor, "wb") as output:
       output.write(encoded)
   metrics = result["result"]
   print(json.dumps({name: metrics.get(name) for name in ("active_permits", "unknown_held_permits", "errors_total")}, ensure_ascii=False))
   PY
   ```

   每次保留新的私有输出文件名；此例重复执行会拒绝覆盖。不要为共享方便改成打印凭据、完整私有配置或原始环境。

4. **按指标真实含义验收。** 查看 `active_permits`、`unknown_held_permits`、`errors_total`、`native` 和对应 `measurement_scopes`。预期空闲正常实例没有未知占用；但已发生的未知调用不能靠清零指标解除。`unavailable`、`null` 或未暴露的 cache/队列数据不等于零；RSS 不是 native payload 资源计数。

   `/v1/databases/status` 的 refs/native diagnostics 可明确 unavailable，这让 placement 仍可观察，不代表获得了 lease、完成 fencing barrier 或物理 I/O 已终止。独立 `/refs` 仍 fail-closed。读取、迁移、权限管理分别使用真实响应，不用 optional inspection 替代所有权证明。

## 五、日常关闭与同目录重启

1. **先结束已知业务 session，停止新工作。** 对结果已知的交互 CLI 使用其明确的 close/`:quit` 路径。`execution_completed:true` 的 native SQL 错误不意味着事务自动回滚；必要时在同 session 明确 `ROLLBACK`。EOF、timeout、unknown outcome 不能当作执行终止，也不能为了正常关闭而盲目 retry、rollback、close 或重放旧 session。

2. **只关闭自己启动的 launcher。** 在原终端核对 `jobs -l`、自己的 `topology.json` 中 `launcher_pid` 和当前进程身份，确认是同一私有目录的原 launcher；缺失、变化或 PID 重用则停止，不发信号。

   ```sh
   kill -TERM "$LAUNCHER_PID"
   if wait "$LAUNCHER_PID"; then
     CLOSE_EXIT=0
   else
     CLOSE_EXIT=$?
   fi
   printf 'launcher exit=%s\n' "$CLOSE_EXIT"
   ```

   该命令只用于经核验的本人实例，不使用 `killall`、`pkill`、负 PID/process group、端口抢占或停止其他人的容器。launcher 负责先停止平台，再分别请求 worker/Authority shutdown，并等待各进程结果；不要绕过它逐个随机 kill 子进程。

3. **区分 clean、进程消失与保守失败。** 成功 shutdown 应有实际 joined exit 0 和各服务完成证据。`authority/authority.lock` 的精确 `clean\n` 只证明 Authority 自己的标记；不能把平台 exit 1、未知 session、被 kill 的 worker 或所有 PID 消失转换成整栈 clean。超时后 launcher 可能杀掉仍运行的子进程并报错，这同样不是 cancellation-as-success。

4. **仅在确认安全后用同目录重启。** 保留原 backend/listener/TLS 配置、Authority/controller/platform 状态、签名 key 和 KMS keyring；S3 实例用同一已确认存储身份及私有环境。重新执行原启动命令，验证新 READY、更新后的 `topology.json`、先前 ACK 行、integrity 与指标。

   clean restart 会复用原 `connection.json` 并实际检查数据库 status，不重新签发所有凭据。unclean Authority restart 默认拒绝；不要编辑/删除 dirty marker、metadata、keyring、旧 WAL 或缓存来“修复”。先保留证据并由维护者判断是否需要终止证明或恢复流程；本篇不提供强行 dirty restart 指令。Local 的运行目录含真实存储，删除它不是无害 cache 清理。

## 六、压力测试的可复现操作路径

本节只给未来、明确授权后的步骤，不宣称本文编写期间已运行任何压力轮次。真实结果以当轮 raw artifacts 和独立复验报告为准，不能从功能 JOINT PASS 推导吞吐量结论。

1. **先领取 main 的执行授权与 gate。** runner 的 `-joint-pass` 必须指向 main 对实际 `source_root` Git HEAD 签发的 JSON：`version:1`、`joint_pass:true`、`confirmed_by:"main"`、准确 `source_git`、完整 P0–P6 stages、真实 RFC3339 `observed_at`。这是操作授权声明，不是密码学签名；不能自行修改 HEAD 或伪造 gate 来解除禁止。冻结完整源码清单和实际二进制 SHA，特别记录 dirty/untracked 内容。

2. **创建新的、健康的 release S3/TLS fixture。** basename 需以 `peri-stress-` 开头且至少 24 字符，目录必须 canonical、owned、0700。示例仅创建自己的目录：

   ```sh
   export RUN_DIR="$(mktemp -d /tmp/peri-stress-release.XXXXXXXX)"
   export RUN_DIR="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$RUN_DIR")"
   ```

   按第二、三节预构建 release 并启动。不要复用故障后 exit 1 的 fixture，更不能把正文中的示例目录替换为 main 当前占用的实例。开发配置默认的项目并发/速率配额不应被临时提高来让实验过关。

3. **先 plan provisioning，再申请执行。**

   ```sh
   python3 prototypes/v1/stress/provision.py --plan \
     --run-dir "$RUN_DIR" --output "$RUN_DIR/parent-baseline.json" --label baseline
   ```

   plan 只读本地私有文件/CA，不发送服务请求、不写输出或保留 label。确认两个真实 worker、S3/TLS 身份与新 isolated 项目/DB 计划。获得 main 明确授权后，才将 `--plan` 改为 `--execute`；执行真实项目/密钥/DB 创建，输出私有 `ConnectionConfig`，它**不是** launcher `connection.json` 的同义替代。

4. **保留 provision 的一次性证据。** 输出 `parent-baseline.json` 含真实 project key、bootstrap management key、独立 noisy project、多个 DB、CA、当前 PIDs 和迁移目标，必须 0600。项目/数据库名前缀是护栏，不是所有权证明。未知 mutation 结果时保留 `.stress-provision-<label>.json` 和输出，不删除 reservation、不换 ID 自动重放；先审查状态再决定新独立 fixture。

5. **构建独立 pressure runner，先核对能力目录。**

   ```sh
   (cd prototypes/v1/stress && go build -o "$RUN_DIR/peri-stress" ./cmd/peri-stress)
   "$RUN_DIR/peri-stress" -list
   ```

   `-list` 不连接、不施加负载。记录 SUPPORTED、REQUIRES_CONFIG 与 SKIP；`cold_vs_warm`、`migrate_fault`、lease/controller loss、storage fault、snapshot/PITR-under-load 等 SKIP 不能算 PASS。parent runner 不向 topology PID 发信号，也不会调用预留 `fault_endpoint`。

6. **仅在当轮获准后运行定向场景。** 在变量 `JOINT_GATE` 指向 main 的有效 gate 后，以下示例覆盖有序 architecture 场景：

   ```sh
   "$RUN_DIR/peri-stress" \
     -connection "$RUN_DIR/parent-baseline.json" -joint-pass "$JOINT_GATE" \
     -output "$RUN_DIR/results-baseline" -cases architecture \
     -rows 1000 -concurrency 1,2,4,8 \
     -warmup 5s -smoke 10s -measure 30s \
     -offered-rate 50 -request-timeout 10s -seed 20261007
   ```

   SQL 定向轮可显式选择 `-cases point_read,mixed_oltp,rollback,constraint_error_reuse` 等真实 catalog 名称。每次使用新 isolated 数据集和输出；`-rows` 只允许 1000/10000，concurrency 只允许 1/2/4/8。完整 `all` 可能因安全上限停止，不能把部分完成算全套 PASS。短于正式阶段要求的运行会标为 `short_gate_not_benchmark`，不能用于性能结论。

7. **保留并独立审查 raw artifacts。** 输出根目录内实际 `run_<id>/run.json`、`operations.jsonl` 是关键证据；保留 source/dirty/diff/binary hashes、seed、真实 wall time/monotonic elapsed、request/business IDs、ACK ledger、rows/hash、错误、unknown outcomes、offers/queue drops 和资源样本。missing/unavailable 不改为零；写入 ACK 不等于所有 offered workflows 成功；400、429、409、超时和 unknown 均按真实合同分类。

   runner 使用 `/tmp/peri-loom-stress.global.lock` 和输出内 `.stress.lock` 串行化，不并发跑另一个工具。未知 SQL 结果会停止后续 load；不要清 poisoned session、改 deadline、自动重试、吞异常或删 raw log。不要用 native payload budget 冒充 RSS，也不要从 `execute` 延迟推导所有 COMMIT 延迟。

8. **先复验基线，再修复并重复同条件轮次。** 第一轮、第二轮、独立 F 验证、针对性修复与复测报告必须能追溯同一 workload、配额、profile、storage/TLS、seed、阶段时长和 source/binary provenance。比较前单列 warm/cold、累计 schema/WAL/cache 与部署差异；不把 debug/release 或一次手动复测当 A/B 性能证明。最终 adversarial review 与性能结论需要单独通知开始，不能用本操作说明替代。

## 七、故障演练与清理边界

1. **与日常实例、压力 baseline 分开。** 故障演练必须单独新建 canonical owned fixture，并明确授权唯一目录、实际 owner PID、cache、源码 SHA 和次数。保留 topology、process UID/parent/start/argv/SID、cache inode 与原生二进制 profile/hash。禁止用户进程、S3 bucket/对象管理、Authority/platform/launcher 故障或任意 shell 注入。

2. **理解现有 joint 脚本不可通用套用。** `prototypes/v1/platform/tests/joint-s3-failover.py` 的当前验收版本硬编码当时的精确 fixture；它不是“换一个 `--run-dir` 就能安全运行”的通用工具。`--plan` 和 `--self-test` 不执行真实故障。实际 `--execute` 必须由维护者核对/冻结脚本并重新只读预检后逐次授权；本文不提供对旧 fixture 的执行命令。

3. **使用通用 faultprobe 前另行授权。** 入口是独立模块 `prototypes/v1/stress/faultprobe/cmd/peri-faultprobe`，`-list` 是无网络能力说明。真实运行需要 `-execute`、有效 joint gate、私有 connection/topology、准确 `-rust-binary`；`owner_crash` 还需 `-allow-worker-sigkill`。其 ACK-boundary、零 in-flight 设计不等于 joint 脚本的未 COMMIT999场景；`metadata_starvation` 的观察与缺失 terminal/Authority oracle 不能宣称完整 PASS。

4. **保留不确定性，而不是扩大 kill/delete 范围。** 通用 faultprobe 仅依据当前已验证的 management placement/grant 与 private topology/进程身份选择目标 worker；这不是新的 Authority 实时租约或物理 I/O 终止证明。joint 专用脚本另有实际 Authority binding 校验，不能把它的更强 oracle 自动归给其他工具。两者都只能单 PID kill，原 PID 确认消失后才能处理其已验证 cache。argv 暂时不可读不是 PID gone；等待/reuse/timeout 均需 fail-closed。进程检查到 signal、inode 检查到删除仍有 TOCTOU，不能宣称防御恶意同 UID 改写者。旧 session 409 不证明“复活旧 worker”的 native 物理写 fencing。

5. **将恢复 oracle 与 shutdown 分开验收。** ACK ledger、未提交行不存在、integrity、新 ACK、epoch/owner 与旧 session 拒绝通过，才是实际恢复证据；随后 launcher exit 1 仍必须保留为非 clean。不能为取得 exit 0 自动 close/retry 旧 poisoned session、修改 dirty marker 或抹掉失败记录。已有 joint 功能 PASS 保留了首次等待失败和最后 conservative teardown 限制，没有压力/HA结论。

6. **清理只针对自己的已停资源。** private connection、KMS keyring、签名 key 和日志必须按既定保留策略保护；先证明 own runner/launcher/children 已停止，再由授权维护者清理 own container/volume/fixtures。不要自动删除 global/stale locks，也不要删除 `.faultprobe.used` 或 reservation 来重复故障 fixture。保留 required raw artifacts，避免在审阅前丢弃失败证据；不要删除用户存储、源代码或 `CLAUDE.md`。

## 验收记录入口

1. **启动与安全边界：** `prototypes/v1/platform/README.md`、`dev.py`、`docs/v1/P6-LAUNCHER-REVIEW.md`。
2. **已完成的功能与部署范围：** `docs/v1/P6-VERIFICATION.md`、`docs/v1/JOINT-VERIFICATION.md`；新版本须检查复用范围，不自动继承结论。
3. **压力与故障工具合同：** `prototypes/v1/stress/README.md`、`provision.py`、`cmd/peri-stress/main.go`、`faultprobe/README.md`；README 的开发期状态不能替代当轮 raw log。
4. **最终交付判据：** 用户操作顺序、实际命令/response/exit/sourcehash、权限与资源边界、失败/unknown/SKIP、独立报告均可追溯。本文仅完成静态操作路径整理，整套文档独立校验及最终 adversarial verification 等待明确通知，不在此提前宣称执行完成。
