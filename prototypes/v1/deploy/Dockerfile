ARG RUST_IMAGE=rust:1.98.1-bookworm
ARG GO_IMAGE=golang:1.23-bookworm
FROM scratch AS local-cargo

FROM ${RUST_IMAGE} AS rust-build
ARG CARGO_OFFLINE=false
WORKDIR /source
COPY prototypes/p0/turso prototypes/p0/turso
COPY prototypes/v1/Cargo.toml prototypes/v1/Cargo.lock prototypes/v1/
COPY prototypes/v1/src prototypes/v1/src
RUN --mount=type=bind,from=local-cargo,target=/seed,ro --mount=type=cache,id=peri-loom-rust-registry,target=/usr/local/cargo/registry,sharing=locked --mount=type=cache,id=peri-loom-rust-target,target=/source/prototypes/v1/target,sharing=locked if [ -d /seed/registry ]; then cp -R /seed/registry/. /usr/local/cargo/registry/; fi && test -f prototypes/p0/turso/core/Cargo.toml && CARGO_NET_OFFLINE="$CARGO_OFFLINE" cargo build --locked --release --manifest-path prototypes/v1/Cargo.toml --bin peri-loom-v1 && install -D prototypes/v1/target/release/peri-loom-v1 /out/peri-loom-v1

FROM ${GO_IMAGE} AS go-build
ENV CGO_ENABLED=1
WORKDIR /source
COPY prototypes/v1/controller controller
COPY prototypes/v1/platform platform
WORKDIR /source/platform
RUN go mod edit -replace=peri-loom/controller=../controller && go build -trimpath -o /out/peri-platform ./cmd/peri-platform && go build -trimpath -o /out/peri ./cmd/peri

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends python3 openssl ca-certificates libgcc-s1 && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home peri && install -d -m 0700 -o peri -g peri /data/peri
WORKDIR /app
COPY --from=rust-build /out/peri-loom-v1 /usr/local/bin/peri-loom-v1
COPY --from=go-build /out/peri-platform /usr/local/bin/peri-platform
COPY --from=go-build /out/peri /usr/local/bin/peri
COPY prototypes/v1/platform/dev.py /app/prototypes/v1/platform/dev.py
ENV PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 9443
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "/app/prototypes/v1/platform/dev.py"]
CMD ["--run-dir", "/data/peri", "--backend", "local", "--listen", "0.0.0.0:9443", "--tls", "--no-build"]
