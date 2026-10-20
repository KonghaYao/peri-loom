#!/usr/bin/env bash
# P0 发布门禁：本地发布检查与 CI（v1-baseline、v1-image）共用的唯一入口。
# 覆盖 prototypes/p0 的全部测试目标，以及锁定 Turso checkout 的 SQL 冒烟。
# 任何一项失败都以非零状态退出；镜像发布工作流依赖本门禁，失败时不会构建或发布。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
P0_DIR="${REPO_ROOT}/prototypes/p0"
TURSO_DIR="${P0_DIR}/turso"

log() { printf '\033[36m[p0-gate]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[p0-gate]\033[0m %s\n' "$*" >&2; exit 1; }

# 测试必须跑在父仓库锁定的 Turso commit 上；submodule 未初始化或停在别的 commit 时，
# 通过测试的只是另一份代码。
expected="$(git -C "${REPO_ROOT}" ls-tree HEAD prototypes/p0/turso | awk '{print $3}')"
test -n "${expected}" || fail "HEAD 未锁定 prototypes/p0/turso"
actual="$(git -C "${TURSO_DIR}" rev-parse HEAD 2>/dev/null)" || fail "prototypes/p0/turso 未初始化"
test "${actual}" = "${expected}" || fail "prototypes/p0/turso 停在 ${actual}，父仓库锁定 ${expected}"

log "P0 test set：acceptance / final_verify / statement_safety"
cargo test --locked --no-fail-fast --manifest-path "${P0_DIR}/Cargo.toml"

# SQL 兼容门禁使用固定 64 MiB hash 预算并禁用快照更新；不要在这里改预算或启用快照更新。
log "Pinned Turso SQL smoke（native Rust backend）"
(
  cd "${TURSO_DIR}"
  cargo run --locked --manifest-path testing/sqltest/Cargo.toml --bin sqltest -- run \
    sqlite/conformance/sqlite-sqltests/transactions.sqltest \
    sqlite/conformance/sqlite-sqltests/constraint-error-message-suffix.sqltest \
    sqlite/conformance/sqlite-sqltests/view-rowid.sqltest \
    --backend rust --skip-matrix --snapshot-mode=no \
    --hash-join-memory-budget 67108864 --jobs 2
  cargo run --locked --manifest-path testing/sqltest/Cargo.toml --bin sqltest -- run \
    sqlite/conformance/sqlite-sqltests/returning.sqltest \
    --filter insert-returning-single-column \
    --backend rust --skip-matrix --snapshot-mode=no \
    --hash-join-memory-budget 67108864 --jobs 2
)

log "P0 gate passed"
