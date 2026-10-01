import { defineConfig } from 'orval';

// 契约来源：db-server 用 `--dump-openapi`（或运行时 GET /api/v1/openapi.json）导出到 web/openapi.json。
// 生成物写入 src/api/generated/，**仅作类型参考**；运行时不依赖生成代码（见 src/api/client.ts）。
export default defineConfig({
  'peri-loom': {
    input: {
      target: './openapi.json',
    },
    output: {
      target: './src/api/generated/endpoints.ts',
      client: 'react-query',
      // 用 fetch 而非 axios：生成代码不引入额外运行时依赖
      httpClient: 'fetch',
      mode: 'split',
      clean: true,
      prettier: false,
      override: {
        // 生成文件头部注释
        header: () => [
          '// 本文件由 orval 生成（npm run gen:api），仅作类型 / 契约参考，请勿手工修改。',
          '// 运行时请使用 src/api/client.ts。',
        ],
      },
    },
  },
});
