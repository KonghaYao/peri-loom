#
# DBA Panel 镜像（架构 §17.14 Web Container）：
#   Vite 构建静态文件 -> nginx 提供 SPA 并反向代理 /api 与 /data 到 db-server。
#   因此 Browser 与 API 默认同 Origin，不需要宽泛 CORS。

ARG NODE_IMAGE=node:22-alpine
ARG NGINX_IMAGE=nginx:1.27-alpine

# ---------------------------------------------------------------- build
FROM ${NODE_IMAGE} AS build

WORKDIR /app

# 国内 npm 镜像（构建机与 CI 均无需直连 registry.npmjs.org）
RUN npm config set registry https://registry.npmmirror.com \
 && npm config set fund false \
 && npm config set audit false

COPY package.json package-lock.json* .npmrc* ./
RUN npm install --no-audit --no-fund

COPY . .
RUN npm run build

# ---------------------------------------------------------------- runtime
FROM ${NGINX_IMAGE} AS runtime

COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
  CMD wget -qO- http://127.0.0.1/healthz || exit 1

CMD ["nginx", "-g", "daemon off;"]
