# Native single-container deployment

## Production Compose configuration

The repository-root `compose.yaml` describes the intended single-host S3 deployment:
one prebuilt, immutable application image, an existing dedicated S3-compatible bucket,
host-provided TLS, and repository-relative bind mounts. **It is not yet safe to put into
production**: the source launcher now admits a second launch only after a joined clean
stop, matching worker proofs, a clean Authority marker, and no unresolved platform
operations. The `v1.0.1` preview image includes this fix; `v1.0.0` predates it.
Dirty restart, disaster recovery, and HA have not been validated for production.
Do not delete `deploy/state`, reset the ownership
marker, or reuse the S3 bucket with a fresh state directory to bypass that guard.

Prerequisites on a Linux Docker host:

- Docker Engine with the Compose plugin; access to a reviewed Linux image
  identified by digest (`PERI_IMAGE`) whose architecture matches the Docker host —
  the release tag is a manifest list covering `linux/amd64` and `linux/arm64`. Published tags here are preview releases,
  not production-certified images. Local builds additionally require the Turso
  submodule and the Rust/Go dependencies described below.
- An already-created, dedicated S3-compatible bucket with an endpoint reachable
  **from the container**, region, and scoped read/write credentials. Provide
  `S3_SESSION_TOKEN` if temporary credentials are used. The application does not
  create the bucket or provide S3/MinIO in the root production Compose file.
- A trusted server certificate chain `deploy/tls/tls.crt`, its matching private
  key `deploy/tls/tls.key`, and the issuer CA bundle `deploy/tls/ca.crt`. The leaf
  must have a SAN for the public gateway hostname and `IP:127.0.0.1` (internal
  seed requests). Keep the CA signing key elsewhere. Do not use the disposable
  development certificate below for a public endpoint.
- A private persistent `deploy/state` directory on a backed-up disk; the container
  runs as uid 10001 and requires ownership of this directory and read access to
  `deploy/tls`. Use a local Docker daemon: relative bind paths are on the daemon
  host, not on a remote CLI client. Restrict port access and arrange TLS trust for
  connecting clients.

From the repository root on the Docker host, after placing the three TLS files:

```sh
cp production.env.example deploy/production.env
# Edit deploy/production.env: set PERI_IMAGE to a reviewed digest, PERI_BIND_IP / PERI_GATEWAY_PORT,
# and all S3_* values. Do not use the example placeholders.
chmod 600 deploy/production.env
sudo install -d -m 0700 -o 10001 -g 10001 deploy/state
sudo install -d -m 0700 -o 10001 -g 10001 deploy/tls
sudo chown 10001:10001 deploy/tls/tls.crt deploy/tls/tls.key deploy/tls/ca.crt
sudo chmod 0600 deploy/tls/tls.crt deploy/tls/tls.key deploy/tls/ca.crt
docker compose --env-file deploy/production.env config --quiet
docker compose --env-file deploy/production.env up -d --no-build --pull always
docker compose --env-file deploy/production.env logs app
```

All host bind paths are relative to the repository root; keep that directory at a
stable path. `deploy/state` contains keys and private API credentials: protect
and back up both it and the external S3 objects; consistent recovery has not
been validated. Avoid `docker compose down --volumes` and never share
`deploy/state` between projects. The default
`PERI_BIND_IP=127.0.0.1` limits access to localhost; choose a public bind address
only with a matching certificate and appropriate firewall policy. The gateway is
HTTPS on host port `PERI_GATEWAY_PORT` (container port 9443). `S3_*` credentials
from `deploy/production.env` are passed to the launcher, which forwards them only
to the Authority; Docker administrators can inspect container environments. Do
not print `docker compose config` without `--quiet`, or commit
`deploy/production.env`/TLS keys. No separate
database/queue/MinIO container is required: Authority, workers, controller,
gateway, and key broker run in the one application container.

The Compose file sets `--backend s3` and TLS explicitly; changing backend, S3
identity, bind address or security parameters for an existing state directory
is not supported. No automatic restart policy is configured: dirty restart remains
denied. Operators must not mistake an initial successful start for production readiness.

## CI and image releases

`.github/workflows/v1-image.yml` runs only when a `v*` tag is pushed. A reusable
`baseline` job calls `.github/workflows/v1-baseline.yml` via `workflow_call`;
that workflow contains the P0 release gate (`.github/workflows/p0-gate.yml` →
`scripts/p0-gate.sh`, which checks that `prototypes/p0/turso` matches the pinned
gitlink, runs every `prototypes/p0` test, and runs the pinned Turso SQL smoke)
and the full official JS SDK matrix. The standalone `v1-baseline` push trigger
uses `tags-ignore: v*`, so a release tag has exactly one P0/matrix run. `build`
depends on `[check, baseline]`, so a failing gate blocks the image builds, the
multi-architecture manifest, and the GitHub Release. The matrix declares 27/27
scripts and default-executes 26/27 per round for 2 rounds: 52 passed runs and 2
known-gap skips. E1 (`http-abort-write-cursor-live.mjs`) is the explicit known
gap because HTTP v3 write-cursor abort has no acknowledged rollback proof.
`PERI_HRANA_INCLUDE_KNOWN_GAPS=1` executes E1 but does not guarantee a pass; this
is not a full Hrana compatibility claim. Matrix evidence is passed through
`scripts/redact-matrix-evidence.sh` and retained as a workflow artifact. The tag
must match `v<prototypes/v1/Cargo.toml version>`. A `check` job validates the version and
the Compose/verifier configuration, then a `build` matrix runs one **native** runner
per architecture (`ubuntu-24.04` for `linux/amd64`, `ubuntu-24.04-arm` for
`linux/arm64`). Each runner builds only its own platform, runs `verify-image.py`
against that image on that architecture (SQL, encrypted native storage, clean
shutdown, same-volume restart, recovery of an acknowledged SQL write), records
`uname -m`/`ldd`/`docker image inspect` as architecture evidence, and pushes
`ghcr.io/<owner>/<repo>:<tag>-<arch>`. A final `publish` job merges those two
digests into the multi-architecture manifest `ghcr.io/<owner>/<repo>:<tag>` with
`docker buildx imagetools create` and opens a **pre-release** GitHub Release. No
`latest` image is published. GHCR access
depends on the package's visibility and the registry credentials of the client.

No emulated or cross-compiled build is part of the release path: the Go build uses
CGO and the Rust/Turso build links the platform C library, so a foreign-architecture
binary must be compiled and tested where it will run. The Dockerfile additionally
fails the build when an embedded binary's ELF machine does not match the requested
platform, and an architecture-scoped `TARGETARCH` is part of every BuildKit cache
id so two architectures can never share one `target` or module cache.

For a disposable local deployment using a published preview image, set
`PERI_IMAGE=ghcr.io/<owner>/<repo>:<tag>`, prepare `deploy/dev-tls` with
`tls.crt`, `tls.key`, and `ca.crt` readable by uid 10001, and create
`deploy/dev-state` (uid 10001, mode 0700), then run:

```sh
docker compose -f prototypes/v1/deploy/compose.yaml up -d --no-build --pull always
```

The default Compose mount for `/data/peri` is a **host directory bind mount**, which
on Docker Desktop and OrbStack is served by a VM file-sharing layer rather than by
the VM's own filesystem. To place the same run directory in a Docker named volume
inside the VM instead:

```sh
docker compose -f prototypes/v1/deploy/compose.yaml \
  -f prototypes/v1/deploy/compose.named-volume.yaml up -d --no-build
```

Set `PERI_STATE_VOLUME` to name the volume. Both mounts are persistent; a named
volume is not interchangeable with a bind mount for backup, host-side inspection or
disaster recovery — read it through a container, and never remove it to shortcut the
ownership guard. The multi-architecture measurements in
`.peri/plans/native-image-and-arch-matrix-report.md` show that, on an Apple Silicon
host, the choice between these two persistent mounts and the emulated/native
architecture are separate contributors to per-request latency, and that neither one
alone explains it.

This is a development deployment, not a production upgrade procedure. The source
launcher accepts a verified clean restart; dirty or uncertain shutdowns remain denied.
Do not remove the directory or reset the ownership marker to bypass that guard.
The CI image test uses one fresh volume across two launches with local storage;
it does not test external S3 or high availability.

## Verify an existing full Linux image

From the repository root, with Docker, Python 3 and OpenSSL available:

```sh
python3 prototypes/v1/deploy/verify-image.py --image peri-loom:fast-full-ea0f7bf
IMAGE=peri-loom:fast-full-ea0f7bf python3 prototypes/v1/deploy/verify-image.py
python3 -m unittest discover -s prototypes/v1/deploy/tests -p 'test_verify_image.py'
```

Verification never builds, pulls, prunes, or touches existing deployments. It pins
the inspected Linux image ID, requires image/runtime UID 10001, and compares the
embedded full launcher hash with the current `platform/dev.py`. Each invocation
creates a random ownership-labelled container and volume, publishes only an
automatically allocated loopback port, and applies 2 GiB memory, 2 CPU, 128 PID,
no-restart, dropped-capability and no-new-privileges constraints. Docker initializes
the volume from the image's private `/data/peri`; no host UID-owned state mount is
needed. Use a local Docker daemon; a remote daemon's loopback is not the client's.

The launcher creates a distinct development issuer CA and server leaf. The tool
copies the connection and public certificates into a fresh 0700 temporary directory,
rewrites the host fixture endpoint/CA path, verifies the CA chain, server purpose
and loopback IP with OpenSSL, then runs the existing `whole-smoke.py` exactly once
with normal Python certificate verification. It writes an additional acknowledged
SQL probe, stops the container cleanly, restarts that same container and volume,
and verifies the probe remains readable with the same database and credential.
Readiness polling does not dispatch SQL. This uses the default full launcher, not
the separate observer profile.

```sh
python3 prototypes/v1/deploy/verify-image.py --image "$IMAGE" --backend s3
```

S3 is opt-in and uses only `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, and optional `S3_SESSION_TOKEN` from
the environment. Supply an **existing independent disposable bucket**, never a
business bucket: the full smoke changes database objects and leaves them in S3.
The tool does not create/delete buckets or remove external objects. Values never
appear in Docker argv or reports; Docker administrators can still inspect container
environment. Local mode does not forward S3 environment.

The final JSON prints the private result directory and cleanup status; exit 0
requires the exact helper result schema, all 12 checks (including
`native_encrypted_sql_ack`), its stdout acknowledgement, verified TLS, matching
fixture database, positive authority puts, both worker metrics and no audit
unavailability, same-volume restart, acknowledged SQL recovery, final shutdown
proof and a successfully written/verified manifest.
There is no invented separate SQL ACK field: the helper emits that named check.
`smoke-process.json` retains the
original smoke exit code and output (known fixture/S3 credentials redacted),
`smoke.json` retains successful smoke results, and `result.json` records inspected
image identity, resource settings, runtime UID/PID, restart proof, stopped
ExitCode/OOM/PID and the authority `clean\n` marker. `manifest.json` hashes these evidence files and
host verification/launcher/smoke/Dockerfile sources, never connections or secrets.
PASS requires all four evidence files (`smoke-process.json`, `smoke.json`,
`authority.lock`, `result.json`) and all four source hashes. The published result
starts with `pass:false`; only after manifest write/readback and evidence/source
hash rechecks does an atomic replacement publish `pass:true`. Hash, manifest-write
or final-publication failure returns failure without leaving a positive result.
These host hashes do not attest the image's compiled Rust/Go source provenance.
The existing smoke labels its successful backend `external_s3_fixture` even in
local mode; `result.json.backend` is the selected backend.

Failure is conservatively `Unknown`, without automatic SQL retry or an additional
Close after an uncertain outcome. The original smoke currently masks exception details with an exception type;
the verifier preserves that output rather than inventing a more precise outcome.
SIGINT/SIGTERM and tool exceptions attempt only ownership-verified cleanup. Shutdown
sends TERM and waits (default 90 seconds), never force-kills. Removal requires an
exited container with ExitCode 0, no OOM, PID 0 and the clean authority marker;
launcher ExitCode 0 supplies its joined platform/worker shutdown proof. If ownership,
stop or clean proof is uncertain, container/volume and private fixture are retained
for manual investigation, not automatically restarted or deleted. The private
namespace is also always retained after dispatched smoke or restart-probe failure/timeout, even
if the container subsequently stops with ExitCode 0 and a clean marker: process
shutdown does not resolve an Unknown SQL outcome or establish no side effects.
The private
fixture contains credentials and must not be shared; it is removed after proven
cleanup. SIGKILL/host loss cannot execute cleanup. Readiness/smoke deadlines default
to 120/300 seconds and are configurable with `--ready-timeout`, `--smoke-timeout`
and `--stop-timeout`. The workflow covers clean local-volume restart on Linux;
external S3 is opt-in and dirty restart, disaster recovery, and HA remain separate gates.

Local and RustFS whole11 runs are bounded functional checks, not HA or
performance acceptance. Retain run outputs outside the source repository.

The build context is the **full repository root**, including the locally initialized
`prototypes/p0/turso` fork and final sibling modules `prototypes/v1/controller` and
`prototypes/v1/platform`. Do not substitute an upstream Turso checkout: the native
durability hook must be present. The Dockerfile builds Rust 1.98.1 and Go 1.23 with
CGO enabled, including the management CLI. It does not publish the fork.
Compose resolves `../../..` from `prototypes/v1/deploy` to the repository root and
uses `prototypes/v1/deploy/Dockerfile` relative to that build context. The examples
below run from the repository root.

The runtime launcher is copied with explicit mode `0555`, so a private build
context created with `umask 077` cannot make it unreadable by uid 10001.
The native build copies only the pinned fork, `Cargo.toml`, `Cargo.lock`, and `src`.
Go, tests, examples, and documentation do not invalidate the native compile layer.
BuildKit keeps separate locked registry and target caches; the native binary is
copied out of the target cache into `/out` before the runtime stage consumes it.
The normal build uses the public registry and needs no local seed:

```sh
docker build -f prototypes/v1/deploy/Dockerfile -t peri-loom:local .
```

For a disconnected build, prepare a private temporary seed containing only
`registry/cache/index.crates.io-1949cf8c6b5b557f/*.crate` archives whose SHA256 matches
every registry package in the root `prototypes/v1/Cargo.lock`, the corresponding
`registry/index/index.crates.io-1949cf8c6b5b557f/.cache` sparse metadata with each
locked version/checksum verified, and that public index's `config.json`.
Cached alternate-index metadata may supply missing entries only after validating
the locked version/checksum; the seed retains the crates.io logical origin.
Do not copy Cargo credentials, config, private registry metadata, environment files,
or unverified archives. Missing or mismatched dependencies must fail preparation.
The optional named context replaces an otherwise empty scratch stage, is mounted
read-only, and seeds only the registry cache. Offline Cargo does not contact a mirror:

```sh
docker build -f prototypes/v1/deploy/Dockerfile -t peri-loom:local \
  --build-context local-cargo="$VERIFIED_CARGO_SEED" --build-arg CARGO_OFFLINE=true .
```

One application container runs `prototypes/v1/platform/dev.py`, a Rust authority, two Rust workers,
and the Go platform (controller, HTTPS gateway, key broker). Internal authority,
worker and broker RPC listen on literal loopback addresses in this shared network
namespace. Splitting them into separate containers with service hostnames is not a
supported topology. Only the gateway's HTTPS port 9443 is exposed.

## TLS and local object storage

Provide **three files**, `tls.crt`, `tls.key`, and `ca.crt`, in a private
`deploy/dev-tls` directory. `tls.crt` is the server leaf (plus any intermediate chain), `tls.key`
is its private key, and `ca.crt` is the actual trusted issuer/root CA bundle. Compose
and Helm set `PERI_TLS_CA=/run/tls/ca.crt`; a leaf or incomplete server chain must not
be assumed to provide the trust root. The key
must be readable by container uid 10001, not world-readable. Use a valid certificate
for the external gateway name **and IP SAN 127.0.0.1**, because the launcher's seed
requests use loopback. External clients must trust the certificate issuer; no SDK
offers an insecure verification bypass. For a disposable local development CA:

```sh
umask 077
mkdir -p /tmp/peri-dev-tls
openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
  -subj '/CN=peri-loom disposable development CA' \
  -addext basicConstraints=critical,CA:TRUE \
  -addext keyUsage=critical,keyCertSign,cRLSign \
  -keyout /tmp/peri-dev-tls/ca.key -out /tmp/peri-dev-tls/ca.crt
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
  -keyout /tmp/peri-dev-tls/tls.key -out /tmp/peri-dev-tls/tls.csr
printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=serverAuth' \
  'subjectAltName=DNS:localhost,IP:127.0.0.1' > /tmp/peri-dev-tls/tls.ext
openssl x509 -req -in /tmp/peri-dev-tls/tls.csr \
  -CA /tmp/peri-dev-tls/ca.crt -CAkey /tmp/peri-dev-tls/ca.key -CAcreateserial \
  -days 7 -sha256 -extfile /tmp/peri-dev-tls/tls.ext -out /tmp/peri-dev-tls/tls.crt
sudo install -d -m 0700 -o 10001 -g 10001 deploy/dev-tls
cp /tmp/peri-dev-tls/tls.crt /tmp/peri-dev-tls/tls.key /tmp/peri-dev-tls/ca.crt \
  deploy/dev-tls/
sudo chown 10001:10001 deploy/dev-tls/tls.crt deploy/dev-tls/tls.key deploy/dev-tls/ca.crt
sudo chmod 0600 deploy/dev-tls/tls.crt deploy/dev-tls/tls.key deploy/dev-tls/ca.crt
```

Keep the CA signing key outside the mount; it is not needed by the application.
The leaf is CA:FALSE with serverAuth; do not serve the CA certificate as the TLS
end entity. On Linux arrange mount-directory and leaf-key ownership for uid 10001
before mounting; do not make them public.
The disposable certificate is not a production certificate. From the repository root:

```sh
sudo install -d -m 0700 -o 10001 -g 10001 deploy/dev-state
docker compose -f prototypes/v1/deploy/compose.yaml up --build -d
docker compose -f prototypes/v1/deploy/compose.yaml logs app
docker compose -f prototypes/v1/deploy/compose.yaml stop
```

The default host port binds 127.0.0.1. Change `PERI_BIND_IP` only with an appropriate
certificate and network policy. The container always invokes the launcher with
`--run-dir /data/peri --backend local --listen 0.0.0.0:9443 --tls --no-build`.

The persistent run directory is private and stores synthetic service credentials,
signing/KMS keys, authority/controller/platform state, and local backend objects.
New state seeds organization `local`, project `default`, and encrypted database
`local_default/demo/main`. Restart does not mint another project API key.
`connection.json` and `topology.json` have mode 0600; the former contains synthetic
API keys and must not be shared or collected as ordinary diagnostics. Its endpoint
is container-loopback: external SDK callers use the published HTTPS endpoint instead.

SIGTERM/SIGINT 首次到达时，launcher 固定一个 monotonic deadline。
`dev.py --shutdown-budget-seconds` 默认 40 秒，阶段上限为 platform 15 秒、
两个 worker 共用 13 秒、authority 12 秒；RPC 与 join 共用阶段剩余时间，
仅两个 worker 可并行。每个阶段上限都严格高于该组件自身的停机预算（platform
与两个 Rust 进程均为 10 秒；Rust 侧还能用 `PERI_SHUTDOWN_BUDGET_MS` 覆盖为
1..20 秒，launcher 白名单不转发该变量，绕过 launcher 直接运行 Rust 进程时
由运维自行保证它小于对应阶段上限）：上限先到的结果是 launcher 在停机中途
SIGKILL，丢掉组件本可自己报告的 unclean 退出。launcher 同时把 platform 的阶段
上限通过 `PERI_SHUTDOWN_STAGE_CAP_MS` 显式声明给子进程，并把本次部署的整体
grace 通过 `PERI_SHUTDOWN_OUTER_GRACE_MS` 一并声明，platform 在启动时拒绝高于
默认值又未声明上限、预算不严格小于已声明上限、或声明上限达到/超过该 grace 的
配置：上限大于真实外层强杀时限的声明是永不失败的检查，不是被覆盖的停机。三个
部署入口的 45 秒 grace 留出 5 秒收尾裕量，`tests/test_shutdown_budget.py` 校验
它们与 launcher 默认值一致，校验阶段上限仍高于从两侧实现源码读出的组件预算，并
校验 launcher 声明给 platform 的两个变量名与上限均落在 grace 之内。启动前拒绝
阶段上限总和大于预算、或预算加收尾裕量大于部署 grace 的配置；当前 profile
只接受 40 秒，不能仅增加 CLI 数字而不重新评审阶段与编排配置。

清理入口原子落盘 HMAC 签名的 `launcher-stop.json`（`in_progress`）；
超时或失败先落盘 `unclean` 再 kill，不无界等待 kill 后退出。
前序阶段失败后，下游不再请求 graceful shutdown，而是保持隔离并强制停止，
避免把失败的依赖链继续包装为 clean。此诊断文件绝不是复用授权；只有完整验证
worker stop proofs、authority clean marker 和现有 operation 判据后，才生成
`launcher-terminal.json` 并将诊断状态改为 `clean`。磁盘写入失败时不会越过
“先落盘再 kill”的门，外层强杀仍可能发生；这些时间上限不保证磁盘 fsync 的延迟。
被强杀的阶段记录它收到的信号（`signal`），`exit_code` 只在 launcher 确实回收
到退出码时才为整数，来不及回收时为 `null`：这是“未观测到退出码”，不是“退出码为
0”。强杀、超时和未解决操作均不能凭诊断文件复用，且不提供 `--reconciled` 透传；
终端证明失败时 `launcher-stop.json` 的 `reason` 会带上具体成因，便于区分。
复用还要求 platform payload 是停机物化后的最终视图：`operation_storage_version`
为 1 时必须存在正整数 `operations_checkpoint` 且等于 `revision`，否则一律以
“未物化”拒绝，未知存储版本同样 fail-closed（旧快照看不到只写侧车的 operation，
未物化与未解析在诊断中可区分）；operation 存储版本缺失或为 0 的旧格式 payload
沿用原有判据。复用门与停机判定调用同一份规则，曾进入 Unknown/quarantine 的
物化记录继续阻止复用。物化标记证明的是“sidecar 视图已写进 payload”，不是
“这次 stop 是 clean”：判定仍要求 terminal 终局为 `clean`，被放弃的
checkpoint goroutine 也可能在 unclean 之后继续提交物化，因此两者缺一不可。

Helm 暂保留 TCP readiness；它只证明监听可达，不证明 drain 状态。Go readiness
端点尚未接线，不能先改成 HTTP probe，也不增加可能误报 healthy 的 healthcheck。
容器级外层强杀回归使用已有 Python 镜像与模拟慢停 platform，不代表 Rust/S3
seal/fencing 的端到端验收：

```sh
PERI_SHUTDOWN_TEST_IMAGE=<local-python-image-id> \
  python3 -m unittest discover -s prototypes/v1/deploy/tests -p 'test_outer_grace.py' -v
```

## Existing external S3

Export `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION` (default `us-east-1`),
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, and optional `S3_SESSION_TOKEN` through a
private environment injector. The bucket must already exist. Do not paste real
credentials into YAML, argv, source control, or logs:

```sh
sudo install -d -m 0700 -o 10001 -g 10001 deploy/s3-dev-state
export PERI_DEV_STATE_DIR=../../../deploy/s3-dev-state
docker compose -f prototypes/v1/deploy/compose.yaml \
  -f prototypes/v1/deploy/compose.s3.yaml up --build -d
```

The launcher forwards S3 environment variables **only to the Rust authority** and
clears them from workers and Go services. It writes no real S3 secrets to run files.
Container administrators can inspect injected environment variables; this deployment
is not a hostile-container isolation boundary. Avoid printing expanded Compose
configuration, since it includes injected secrets. Check syntax with `config --quiet`.
Backend mode and listener are persistent identity: use a separate state directory rather
than silently rebinding an existing local deployment to a different backend.

## Isolated MinIO development profile

Use a separate Compose project, new `PERI_TEST_BUCKET`, reviewed pinned image digests
in `MINIO_IMAGE`/`MINIO_MC_IMAGE`, and synthetic URL-safe test credentials in
`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`. First create `deploy/minio-state` with
permissions suitable for the pinned MinIO image, and `deploy/minio-app-state`
owned by uid 10001 with mode 0700. This profile uses these separate directories,
does not expose MinIO host ports, and never changes an existing RustFS instance:

```sh
docker compose -p peri-isolated -f prototypes/v1/deploy/compose.yaml \
  -f prototypes/v1/deploy/compose.minio.yaml --profile minio up --build -d
```

Bucket initialization waits at most 120 seconds and only creates/reuses this profile's
isolated bucket. Its client receives credentials by environment, not expanded argv.
Never use real external S3 credentials for this disposable profile.

## Kubernetes / Helm

Build and load/publish the locally built application image through your normal trusted
registry workflow. Create an existing TLS secret with **all three data keys**
`tls.crt`, `tls.key`, and `ca.crt` (CA signing keys are never included), and, for S3,
an existing credential
secret with keys `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, optional `S3_SESSION_TOKEN`.
No chart values contain secret material:

```sh
kubectl create secret generic peri-gateway-tls --type=kubernetes.io/tls \
  --from-file=tls.crt=/tmp/peri-dev-tls-mount/tls.crt \
  --from-file=tls.key=/tmp/peri-dev-tls-mount/tls.key \
  --from-file=ca.crt=/tmp/peri-dev-tls-mount/ca.crt
helm template peri prototypes/v1/deploy/helm/peri --set tlsSecret=peri-gateway-tls
helm upgrade --install peri prototypes/v1/deploy/helm/peri --set tlsSecret=peri-gateway-tls
```

For S3 also set `backend=s3`, `s3.endpoint`, `s3.bucket`, and `s3.credentialSecret`.
The chart is deliberately **one replica, Recreate, one application container, one
ReadWriteOnce PVC**. It is not HA and must not be scaled. The PVC mounts at `/data`,
allowing uid 10001 to create `/data/peri` with mode 0700; root-matching fsGroup policy
avoids broadening private run-directory permissions on subsequent mounts. TLS keys
are mounted group-readable from the existing secret. The readiness probe establishes
only TCP listener availability, not database readiness or a durability proof.

The launcher ready event has `ready`, `service`, `address`, `connection_file`, and
`topology_file`. Its bin overrides are `PERI_RUST_BINARY` and `PERI_PLATFORM_BINARY`.
This chart/template is not a substitute for live authority, SQL, restart, and S3 gates.
For isolated launcher development without external mounts, `--tls` generates its
own private development CA and signed server leaf; `connection.json.ca_file` points
to that CA. Mounted deployment certificates use the explicit `PERI_TLS_CA` trust
file instead. Neither mode disables TLS verification.
