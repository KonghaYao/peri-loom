# Native single-container deployment

## Production Compose configuration

The repository-root `compose.yaml` describes the intended single-host S3 deployment:
one prebuilt, immutable application image, an existing dedicated S3-compatible bucket,
host-provided TLS, and repository-relative bind mounts. **It is not yet safe to put into
production**: `platform/dev.py` currently rejects any second launch against the same
`launch.json`, even after a clean stop. Clean restart, disaster recovery, S3 durability,
and HA have not been validated. Do not delete `deploy/state`, reset the ownership
marker, or reuse the S3 bucket with a fresh state directory to bypass that guard.

Prerequisites on a Linux Docker host:

- Docker Engine with the Compose plugin; access to a reviewed Linux amd64 image
  identified by digest (`PERI_IMAGE`). Published tags here are preview releases,
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
is not supported. No automatic restart policy is configured because restarting
currently fails safely. Operators must not mistake an initial successful start
for production readiness.

## CI and image releases

`.github/workflows/v1-image.yml` builds the full image on relevant branch pushes,
pull requests, and manual runs. It uses `verify-image.py` to test the actual
container over TLS, including SQL, encrypted native storage, and clean shutdown.
A tag matching `v<prototypes/v1/Cargo.toml version>` rebuilds and verifies the
image before publishing `ghcr.io/<owner>/<repo>:<tag>` and a **pre-release** GitHub
Release. Only Linux amd64 is tested; no `latest` image is published. GHCR access
depends on the package's visibility and the registry credentials of the client.

For a disposable local deployment using a published preview image, set
`PERI_IMAGE=ghcr.io/<owner>/<repo>:<tag>`, prepare `deploy/dev-tls` with
`tls.crt`, `tls.key`, and `ca.crt` readable by uid 10001, and create
`deploy/dev-state` (uid 10001, mode 0700), then run:

```sh
docker compose -f prototypes/v1/deploy/compose.yaml up -d --no-build --pull always
```

This is a development deployment, not a production upgrade procedure. A normal
restart using an existing persistent state directory is rejected by the launcher.
Do not remove the directory or reset the ownership marker to get past that rejection.
The CI image test uses a fresh volume and local
storage; it does not test S3, existing-volume restarts, or high availability.

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
with normal Python certificate verification. Readiness polling does not dispatch
SQL. This uses the default full launcher, not the separate observer profile.

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
unavailability, plus shutdown proof and a successfully written/verified manifest.
There is no invented separate SQL ACK field: the helper emits that named check.
`smoke-process.json` retains the
original smoke exit code and output (known fixture/S3 credentials redacted),
`smoke.json` retains successful smoke results, and `result.json` records inspected
image identity, resource settings, runtime UID/PID, stopped ExitCode/OOM/PID and
the authority `clean\n` marker. `manifest.json` hashes these evidence files and
host verification/launcher/smoke/Dockerfile sources, never connections or secrets.
PASS requires all four evidence files (`smoke-process.json`, `smoke.json`,
`authority.lock`, `result.json`) and all four source hashes. The published result
starts with `pass:false`; only after manifest write/readback and evidence/source
hash rechecks does an atomic replacement publish `pass:true`. Hash, manifest-write
or final-publication failure returns failure without leaving a positive result.
These host hashes do not attest the image's compiled Rust/Go source provenance.
The existing smoke labels its successful backend `external_s3_fixture` even in
local mode; `result.json.backend` is the selected backend.

Failure is conservatively `Unknown`, without automatic SQL retry or additional
Close. The original smoke currently masks exception details with an exception type;
the verifier preserves that output rather than inventing a more precise outcome.
SIGINT/SIGTERM and tool exceptions attempt only ownership-verified cleanup. Shutdown
sends TERM and waits (default 90 seconds), never force-kills. Removal requires an
exited container with ExitCode 0, no OOM, PID 0 and the clean authority marker;
launcher ExitCode 0 supplies its joined platform/worker shutdown proof. If ownership,
stop or clean proof is uncertain, container/volume and private fixture are retained
for manual investigation, not automatically restarted or deleted. The private
namespace is also always retained after dispatched smoke failure/timeout, even
if the container subsequently stops with ExitCode 0 and a clean marker: process
shutdown does not resolve an Unknown SQL outcome or establish no side effects.
The private
fixture contains credentials and must not be shared; it is removed after proven
cleanup. SIGKILL/host loss cannot execute cleanup. Readiness/smoke deadlines default
to 120/300 seconds and are configurable with `--ready-timeout`, `--smoke-timeout`
and `--stop-timeout`. This is D04/R02 tooling, not completion of all whole7tasks
or a live Linux/S3/restart gate.

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

SIGTERM goes to the launcher, which shuts down platform services and joins normal
worker/authority RPC shutdowns so clean markers can be proven. The 45-second Compose
grace period covers normal shutdown; forced kills are dirty restarts, not clean proof.

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
