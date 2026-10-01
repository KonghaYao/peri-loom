# DB Platform 开发入口。所有命令均可在国内网络环境下直接执行。
SHELL := /bin/bash
CARGO ?= cargo
NPM   ?= npm

.DEFAULT_GOAL := help

.PHONY: help
help: ## 显示可用命令
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------ 工具链
.PHONY: toolchain
toolchain: ## 通过 rsproxy 安装固定版本 Rust 工具链与 protoc
	./scripts/install-toolchain.sh

# ------------------------------------------------------------------ 构建
.PHONY: check
check: ## 编译校验（workspace 全量）
	$(CARGO) check --workspace --all-targets

.PHONY: build
build: ## 构建 release 二进制（db-server / db-worker / db-runtime / wal-service）
	$(CARGO) build --release --bins

.PHONY: fmt
fmt: ## 格式化
	$(CARGO) fmt --all

.PHONY: clippy
clippy: ## lint（警告视为错误）
	$(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: test
test: ## 单元 / 集成测试
	$(CARGO) test --workspace

.PHONY: test-nextest
test-nextest: ## 使用 cargo-nextest 运行测试（如已安装）
	$(CARGO) nextest run --workspace

.PHONY: deny
deny: ## 依赖许可证与安全审计（如已安装 cargo-deny）
	$(CARGO) deny check

# ------------------------------------------------------------------ 本地运行
.PHONY: compose-up
compose-up: ## 启动标准 Compose 拓扑
	docker compose up -d --build

.PHONY: compose-down
compose-down: ## 停止并清理 Compose 拓扑
	docker compose down -v

.PHONY: compose-logs
compose-logs: ## 跟踪核心服务日志
	docker compose logs -f db-server db-worker-1 wal-1

.PHONY: pull
pull: ## 通过国内 registry 镜像预拉基础镜像
	./scripts/docker-pull.sh

# ------------------------------------------------------------------ Web
.PHONY: web-install
web-install: ## 安装前端依赖（npmmirror）
	cd web && $(NPM) install

.PHONY: web-dev
web-dev: ## 前端开发服务器
	cd web && $(NPM) run dev

.PHONY: web-build
web-build: ## 前端生产构建
	cd web && $(NPM) run build

# ------------------------------------------------------------------ 契约
.PHONY: openapi
openapi: ## 导出 OpenAPI 契约到 web/openapi.json（前端 client 生成源）
	$(CARGO) run -p db-server -- --dump-openapi > web/openapi.json

.PHONY: web-client
web-client: openapi ## 由 OpenAPI 生成前端 TypeScript client
	cd web && $(NPM) run gen:api

# ------------------------------------------------------------------ 验收
.PHONY: acceptance
acceptance: ## 运行验收脚本（并发启动 / 故障注入 / durability / 路由）
	./scripts/acceptance.sh

.PHONY: clean
clean: ## 清理构建产物
	$(CARGO) clean
