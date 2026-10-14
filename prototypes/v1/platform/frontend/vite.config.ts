import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const gateway = process.env.PERI_GATEWAY_URL ?? "https://127.0.0.1:9443";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    proxy: Object.fromEntries(
      ["/v1", "/rpc.js", "/json.js", "/refs.js"].map((path) => [
        path,
        { target: gateway, changeOrigin: true, secure: true },
      ]),
    ),
  },
  build: {
    outDir: "../web",
    emptyOutDir: false,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        entryFileNames: "console.js",
        chunkFileNames: "assets/[name]-[hash].js",
        assetFileNames: (asset) =>
          asset.names.some((name) => name.endsWith(".css"))
            ? "console.css"
            : "assets/[name]-[hash][extname]",
      },
    },
  },
});
