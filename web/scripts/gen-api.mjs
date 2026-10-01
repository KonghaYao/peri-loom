#!/usr/bin/env node
/**
 * gen:api 预检：确保 openapi.json 存在再调用 orval。
 *
 * 契约来源（二选一）：
 *   1) 运行时导出：curl -s http://127.0.0.1:8080/api/v1/openapi.json > web/openapi.json
 *   2) 离线导出：db-server --dump-openapi > web/openapi.json
 *
 * 生成物在 src/api/generated/，**仅作类型参考**；运行时请使用手写的 src/api/client.ts。
 */
import { existsSync, statSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const specPath = resolve(webRoot, 'openapi.json');

if (!existsSync(specPath) || statSync(specPath).size === 0) {
  console.error(
    [
      '',
      `[gen:api] 未找到 OpenAPI 契约文件：${specPath}`,
      '',
      '请先生成契约（任选其一）：',
      '  db-server --dump-openapi > web/openapi.json',
      '  curl -s http://127.0.0.1:8080/api/v1/openapi.json > web/openapi.json',
      '',
      '提示：openapi.json 不入库（见 .gitignore），生成代码仅作类型参考。',
      '',
    ].join('\n'),
  );
  process.exit(1);
}

const result = spawnSync('orval', ['--config', 'orval.config.ts'], {
  cwd: webRoot,
  stdio: 'inherit',
  shell: process.platform === 'win32',
});

process.exit(result.status ?? 1);
