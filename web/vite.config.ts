import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 开发服务器代理：Browser 与 API 同 Origin（与 web/nginx/default.conf 行为一致）。
//   /api/*  -> db-server 管理面
//   /data/* -> db-server 数据面（NDJSON 流式，必须禁止缓冲）
const DB_SERVER = 'http://127.0.0.1:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      // 用正则前缀，避免 '/data' 误匹配 SPA 路由 '/databases'
      '^/api/': { target: DB_SERVER, changeOrigin: true },
      // 流式响应：Vite 代理基于 http-proxy，不缓冲响应体
      '^/data/': { target: DB_SERVER, changeOrigin: true },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      output: {
        // 拆分 vendor，降低单文件体积并改善缓存命中
        manualChunks: {
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
          antd: ['antd', '@ant-design/icons'],
          query: ['@tanstack/react-query'],
        },
      },
    },
  },
});
