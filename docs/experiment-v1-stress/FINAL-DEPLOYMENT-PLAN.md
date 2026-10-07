# 最新 Linux Docker 复验计划（尚未执行，非 PASS）

**当前禁止执行：** E 独占实际压力 runner；本文只读源码/历史日志后编写。
以下所有 Docker、构建、SDK、UI、关闭和清理步骤，均须等 main 确认测量结束并另行授权。
本次未调用 Docker daemon、网络/API/SQL、build/tests/load 或信号；不触碰用户 RustFS、既有 app/container 或秘密。

## 已核对的复用范围

1. 当前观察提交为 `792a7e1b7659bccde15f97b8e7ca9a887da7d0ee`。
   旧 P6 记录的 tag 是 `peri-loom:p6-verify`，image 为
   `sha256:bab9892875497a976ae96022bfc3eea79daea02f17325f2889772461c565db10`，`linux/arm64`。
   仅据 `/tmp/peri-p6-docker-final-image.txt` 与 `peri-p6-docker-build-offline.log`，**未查询目前 daemon 是否仍持有该 tag/cache**。
2. 已有 seed `/private/tmp/peri-p6-cargo-seed.t5qk6mqs` 是私有目录；历史验证记录为 273 个锁 checksum archive、273 个 sparse version/checksum、缺失 0、秘密/config 复制 0。
   当前 `Cargo.lock` SHA `e070b8dc077f27f0d2d29d20de11e222fb0a6ee1d6531f622550d3f3a9de34dd` 与旧 P6 输入 manifest 一致。
   本次没有重新逐个 hash archive；执行前仍需确认 seed 未变/可验证，缺失或不匹配即停止，不复制 Cargo credentials/config。
3. 小范围直接比较 `/tmp/peri-p6-docker-production-final.json`：Dockerfile/Cargo manifests 未变，
   但 authority、ownership、Rust SDK 已变，`ownership_view.rs` 是新输入；Gateway、launcher、UI 已变，`web/rpc.js` 是新输入。
   旧镜像不能当成最新 Linux 验收。`792a7e1` 的 docs-only 增量本身不改变 native COPY，
   **不等于旧 P6 到当前所有 Rust 输入未变**；当前新镜像仍须构建，允许 BuildKit 增量复用，不许偷换旧 binary。

## 授权后的有序执行步骤

1. **冻结输入与秘密隔离。** main 确认 E runner、相关观察任务全部结束，分配唯一执行者、空闲端口与新私有目录。
   记录实际执行 HEAD、Turso gitlink/HEAD、dirty/untracked、Dockerfile/`.dockerignore`、compiled-input manifest 与 SDK/UI helper 身份。
   manifest 覆盖 fork、Rust manifests/src、controller、platform 和 launcher；按旧清单规则记录 symlink 的 literal target，不能以跟随链接的内容替代。
   旧 4268 条/`5b35e773…` 是历史部署输入身份，不直接赋给当前源码。
2. **隔离 build context。** 源码布局仍以 repo root 为根，Dockerfile 是 `prototypes/v1/deploy/Dockerfile`。
   推荐在新临时目录制作与该 root 布局一致的 allowlist context：只包含 Dockerfile、`.dockerignore`、固定 fork、Rust manifests/src、controller、platform 的已冻结所需文件。
   排除 `.git`、target、运行目录、node_modules、SDK dist、日志、环境、证书/私钥、connection/config、KMS 文件与 cache；检查 symlink 不引入外部秘密。
   不能只依赖现有 `.dockerignore`：它有常见排除项，但不是任意位置秘密文件的完整 allowlist。
   context 的实际 COPY 输入须逐项对应冻结 manifest；seed 是独立只读 named context，不放进源代码 context。
3. **确认旧缓存后构建新 tag。** 以下是未来命令，`BUILD_CONTEXT` 指上一步安全 root，`NEW_IMAGE` 为 main 分配的新 tag，不能覆盖旧验收 tag。

   ```sh
   docker image inspect peri-loom:p6-verify
   docker build --platform linux/arm64 --progress=plain \
     -f "$BUILD_CONTEXT/prototypes/v1/deploy/Dockerfile" -t "$NEW_IMAGE" \
     --build-context local-cargo=/private/tmp/peri-p6-cargo-seed.t5qk6mqs \
     --build-arg CARGO_OFFLINE=true "$BUILD_CONTEXT"
   docker image inspect "$NEW_IMAGE"
   ```

   不加 `--no-cache`/`--pull`，不 prune。现有 cache IDs 是 `peri-loom-rust-registry`、`peri-loom-rust-target`，均 `sharing=locked`。
   Rust stage 将 release binary 从 target cache 安装到 `/out`；Go stage 构建平台与 CLI，CGO 开启。
   保存逐阶段 CACHED/实际编译与新 image ID/architecture；tag 相同不代表产物相同。
   Cargo offline 不等于整个 Docker build 绝对离线：base metadata、未缓存 apt runtime layer 仍可能需要网络；cache/依赖不可用时停止反馈，不自动扩大网络授权或重试旧在线失败。
4. **仅创建 own Local/TLS 容器。** 使用新唯一 container、volume、label 和经确认空闲的 loopback 端口；以下变量不得指向既有资源。

   ```sh
   docker volume create --label "peri.final.owner=$RUN_LABEL" "$OWN_VOLUME"
   docker run -d --init --name "$OWN_CONTAINER" \
     --label "peri.final.owner=$RUN_LABEL" --stop-timeout 45 \
     --cap-drop ALL --security-opt no-new-privileges:true \
     -p "127.0.0.1:$OWN_PORT:9443" \
     --mount "type=volume,source=$OWN_VOLUME,target=/data/peri" "$NEW_IMAGE_ID"
   docker logs "$OWN_CONTAINER"
   ```

   默认 CMD 是 Local、`0.0.0.0:9443`、TLS、`--no-build`，运行 uid 10001；新卷初始化后确认 `/data/peri` 私有可写。
   使用 launcher 在 own volume 内生成的新开发 CA/leaf，无外部 TLS secret 或 S3 凭据进入镜像/context；须等 `service:"local-project"` 完整 READY。
   如果改用外部证书，必须三文件 CA/leaf/key，leaf 有 127.0.0.1 SAN，private key 对 uid10001 可读但不能 world-readable；仅 readonly 挂载。
5. **建立私有客户端连接。** 只从 own container 复制 `connection.json` 与 `keys/dev-ca.crt` 到 own 0700 目录。
   在私有派生 connection 中将 endpoint 改为 `https://127.0.0.1:$OWN_PORT`，ca_file 改为复制 CA 的绝对路径，仍保持 0600；不要打印 connection/global/project key。
   容器内 CLI 用原内部 endpoint/CA，从原 connection 私有派生 credential 文件，不能把 Key 放到 docker exec argv 或 stdout。
6. **真实应用复验，不做压力。** 容器内 `/usr/local/bin/peri` 做 status、有限 exact-int64 SELECT、integrity 与已授权 checkpoint，并显式关闭健康会话；这是实际 Linux Go CLI/SDK。
   外部 Rust helper 用当前已验证的 `p6_sdk_smoke`，TypeScript 使用当前构建产物与
   `NODE_EXTRA_CA_CERTS="$OWN_CA" node prototypes/v1/platform/tests/sdk-live.mjs "$OWN_CONNECTION"`。
   两者检查 Linux 服务的 verified-TLS status、精确整数与显式 close；外部 helper 不冒称在 Linux 容器内运行。
   helper 产物若不能绑定当前 SDK 输入，另行许可后准备，不使用旧 SDK 的 PASS 替代当前验证；镜像本身不含 Node/TS helper 或 Rust example binary。
7. **真实 UI 复验。** 新独立浏览器 profile/CDP port，仅信任 own CA，不关闭证书校验，不复用别人的浏览器。
   `PERI_UI_CONNECTION_FILE="$OWN_CONNECTION" PERI_UI_CDP_PORT="$OWN_CDP_PORT" node prototypes/v1/platform/tests/ui-smoke.mjs` 检查 catalog、opaque session、exact int64 JSON/NDJSON、refs、close、无 credential browser storage。
   再核当前严格 JSON/NDJSON 的 ID/唯一 complete/rows/EOF 行为；成功面板 result-only，证据脱敏，不截录 Key。
   现有 UI smoke 的正常成功路径不等于真实服务已覆盖全部 malformed/unknown 情况；这些继续引用当前 F 的明确单元证据，不通过伪造生产回复补结论。
8. **joined shutdown 单独判定。** 应先结束结果已知的会话；未知结果停止并保留，不为退出绿灯盲目 close/retry。

   ```sh
   docker stop --time 45 "$OWN_CONTAINER"
   docker wait "$OWN_CONTAINER"
   docker inspect --format '{{.State.ExitCode}} {{.State.OOMKilled}}' "$OWN_CONTAINER"
   docker cp "$OWN_CONTAINER:/data/peri/authority/authority.lock" "$OWN_EVIDENCE/authority.lock"
   ```

   保存容器日志与实际 joined exit；只有 exit0、无 OOM/超时强杀、own marker 精确 `clean\n` 并且服务完成证据一致才可记 clean。
   `docker stop` 命令退出0、PID消失、marker 单项或旧 P6 的 group SIGINT 都不能代替此判据；保留非零/dirty 首次失败。
9. **交付与 own-only 清理。** 比较 before-build/after-build/after-runtime 编译输入 manifest，记录新 image ID、内部三 binary SHA、TLS/SDK/UI/关闭各自日志与限制。
   main/独立 verifier 审查后再按显式 label/ID 仅移除 own stopped container/volume，不全局 prune、不删旧 image/容器、用户 RustFS 或存储。
   此轮只验证新 Linux Local/TLS image；不是新增 S3、压力性能、HA、dirty 自动恢复或所有阶段联合 PASS。

## 静态参考

1. `prototypes/v1/deploy/Dockerfile`、根 `.dockerignore`、deploy `README.md`/`compose.yaml`，平台 `dev.py`/SDK/UI helper。
2. 历史证据：`docs/v1/P6-VERIFICATION.md` 的 Final Linux deployment 段，
   `/tmp/peri-p6-docker-{build-offline,final-equivalence,live-cli,live-rust-sdk,live-ts-sdk,live-shutdown}.log`。
3. 历史成功仅供命令/判据参考；此前在线 build 失败仍是失败，本文没有新 Linux image 或执行结果。
