#!/usr/bin/env bash
# 为本地 / Compose 开发生成 secrets 文件（架构 §17.14）。
# 生产环境请改用公司 PKI / Secret Manager，不要把生成结果提交到仓库。
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/deploy/secrets"
mkdir -p "${DIR}"

gen() {
  local file="$1"
  if [ -s "${DIR}/${file}" ]; then
    echo "已存在，跳过：${file}"
    return
  fi
  # 32 字节 URL-safe 随机串
  head -c 48 /dev/urandom | base64 | tr -d '/+=' | head -c 40 > "${DIR}/${file}"
  # 0644 而不是 0600：Docker Compose 的文件型 secret 是以 bind mount 形式注入的，
  # 容器内的非 root 进程（db-server 以 uid 10001 运行）读不到 0400 root:root 的文件。
  # 该目录已被 .gitignore 排除；生产环境请用编排器管理的 secret（支持 uid/gid 作用域）
  # 或在节点上限制该目录的访问权限。
  chmod 644 "${DIR}/${file}"
  echo "生成：${file}"
}

gen postgres_password.txt
gen bootstrap_admin_password.txt
gen jwt_secret.txt
gen s3_access_key.txt
gen s3_secret_key.txt
gen grafana_admin_password.txt

echo "完成。目录：${DIR}"
