ARG RUST_IMAGE=rust:1.98.1-bookworm
FROM ${RUST_IMAGE}

# The tag is the repository-pinned toolchain (Cargo.toml requires Rust >=1.98,
# CI installs 1.98.1). A per-architecture digest is not fabricated here; pin the
# image ID/digest in the consuming workflow when one is available.
ARG TARGETARCH=unknown
ARG REVISION=unknown

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC \
    HOME=/home/peri \
    CARGO_HOME=/cargo-cache \
    CARGO_TARGET_DIR=/cargo-target \
    RUSTUP_HOME=/usr/local/rustup \
    RUSTUP_TOOLCHAIN=1.98.1 \
    TMPDIR=/build/tmp

# build-essential covers the C/C++ objects in Cargo.lock; procps is required by
# the resource sampler. Go, Node and Docker CLI are deliberately absent: this
# image runs the native Rust/Python harness only.
# TMPDIR is /build/tmp at runtime; create it before apt because maintainer
# scripts (ca-certificates) call mktemp through TMPDIR.
# rust-toolchain.toml names rustfmt and clippy. Install them at build time: at
# run time the root filesystem is read-only, so rustup cannot sync a missing
# component into RUSTUP_HOME. RUSTUP_TOOLCHAIN pins every proxy below.
RUN mkdir -p /build/tmp \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        file \
        git \
        procps \
        python3 \
        util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 peri \
    && useradd --uid 10001 --gid 10001 --create-home \
        --home-dir /home/peri --shell /bin/bash peri \
    && install -d -m 0755 -o peri -g peri \
        /opt/perf /workspace /cargo-cache /cargo-target /build /build/tmp \
        /perf/store /perf/cache /perf/evidence /perf/metrics \
        /reports /inputs /artifacts /trust \
    && test "$(id -u peri)" = "10001" \
    && python3 -c 'import sys; assert sys.version_info[:2] == (3, 11), sys.version' \
    && rustc --version | grep -Eq '^rustc 1\.98\.1( |$)' \
    && rustup component add --toolchain 1.98.1 rustfmt clippy \
    && cargo fmt --version \
    && cargo clippy --version

# Only the entrypoint enters the image. Source, .git, Cargo.lock, evidence and
# credentials stay on the host and are mounted read-only at run time.
COPY --chmod=0555 entrypoint.py /opt/perf/entrypoint.py

LABEL org.opencontainers.image.title="peri-loom Issue 8 performance harness" \
      org.opencontainers.image.description="Pinned Rust/Python toolchain and native Issue 8 harness entrypoint" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.base.name="rust:1.98.1-bookworm" \
      io.peri-loom.image.arch="${TARGETARCH}" \
      io.peri-loom.toolchain.rust="1.98.1" \
      io.peri-loom.toolchain.python="3.11"

WORKDIR /workspace
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "/opt/perf/entrypoint.py"]
CMD ["test"]
