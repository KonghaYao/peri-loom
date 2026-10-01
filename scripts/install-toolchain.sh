#!/usr/bin/env bash
# 通过 rsproxy.cn 国内镜像安装固定版本 Rust 工具链 + protoc。
# 幂等：可重复执行。
set -euo pipefail

RUST_VERSION="1.98.1"
PROTOC_VERSION="25.1"
RUSTUP_DIST_SERVER="${RUSTUP_DIST_SERVER:-https://rsproxy.cn}"
RUSTUP_UPDATE_ROOT="${RUSTUP_UPDATE_ROOT:-https://rsproxy.cn/rustup}"
export RUSTUP_DIST_SERVER RUSTUP_UPDATE_ROOT

log() { printf '\033[36m[toolchain]\033[0m %s\n' "$*"; }

# --------------------------------------------------------------- Rust
if ! command -v rustup >/dev/null 2>&1; then
  log "安装 rustup（镜像：${RUSTUP_DIST_SERVER}）"
  curl -sSf "${RUSTUP_DIST_SERVER}/rustup-init.sh" -o /tmp/rustup-init.sh
  sh /tmp/rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain "${RUST_VERSION}"
else
  log "rustup 已存在：$(rustup --version 2>/dev/null | head -1)"
fi

export PATH="${HOME}/.cargo/bin:${PATH}"

if ! rustup toolchain list 2>/dev/null | grep -q "^${RUST_VERSION}"; then
  log "安装工具链 ${RUST_VERSION}"
  rustup toolchain install "${RUST_VERSION}" --profile minimal --component rustfmt --component clippy
fi
rustup default "${RUST_VERSION}" >/dev/null
log "rustc: $(rustc --version)"
log "cargo: $(cargo --version)"

# --------------------------------------------------------------- protoc
need_protoc=1
if command -v protoc >/dev/null 2>&1; then
  current="$(protoc --version 2>/dev/null | awk '{print $2}')"
  # 需要 >= 3.15 以支持 proto3 语义（本仓库用 protoc 25.x）
  if [ "$(printf '%s\n3.15\n' "${current}" | sort -V | head -1)" = "3.15" ]; then
    log "protoc 已满足：${current}"
    need_protoc=0
  fi
fi

if [ "${need_protoc}" = "1" ]; then
  log "安装 protoc ${PROTOC_VERSION}"
  tmp="$(mktemp -d)"
  if curl -sSL --max-time 300 -o "${tmp}/protoc.zip" \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"; then
    command -v unzip >/dev/null 2>&1 || { apt-get update -qq && apt-get install -y -qq unzip; }
    unzip -q "${tmp}/protoc.zip" -d "${tmp}/out"
    install -m 0755 "${tmp}/out/bin/protoc" /usr/local/bin/protoc
    mkdir -p /usr/local/include
    cp -r "${tmp}/out/include/google" /usr/local/include/
    log "protoc: $(protoc --version)"
  else
    log "GitHub 下载失败，回退到 apt（apt 已配置 aliyun 镜像）"
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq protobuf-compiler
    log "protoc: $(protoc --version)"
  fi
  rm -rf "${tmp}"
fi

log "完成。请确保 ${HOME}/.cargo/bin 在 PATH 中。"
