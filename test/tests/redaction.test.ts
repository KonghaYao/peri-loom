/**
 * ============================================================================
 *  tests/redaction.test.ts —— 测试基座自身的输出脱敏
 * ============================================================================
 *  为什么值得一条用例：这套测试要把平台的响应、错误、请求头打到终端，
 *  而凭据（Bearer token）绝不允许出现在任何输出里。脱敏不是"记得别打印"，
 *  而是有一条**可执行的不变量**：任何要打印的文本都先过 redact()。
 *  这里就把这条不变量钉住——否则某天有人加了句 debug 日志，泄漏是静默发生的。
 *
 *  纯本地用例：不需要平台在跑，因此不放 preflight（平台挂了它也应该通过）。
 *  运行：npm test
 * ============================================================================
 */

import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { redact, tryResolveToken } from '../src/config.ts';

/** 形状合法但签名随便的 JWT：用来验证"按形状兜底"的那一层。 */
const FAKE_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJub3QtYS1yZWFsLXVzZXIifQ.not-a-real-signature';

describe('输出脱敏', () => {
  test('打印请求头时，Authorization 只能留下 Bearer <REDACTED>', () => {
    const line = redact(`Authorization: Bearer ${FAKE_JWT}`);

    assert.equal(
      line,
      'Authorization: Bearer <REDACTED>',
      `请求头脱敏结果不对：${line}`,
    );
    assert.ok(!line.includes(FAKE_JWT), '脱敏后仍能看到 token 原文');
  });

  test('任何 JWT 形状的串都会被抹掉（即使不是当前凭据）', () => {
    assert.ok(!redact(`别人的 token=${FAKE_JWT}`).includes(FAKE_JWT), 'JWT 形状的串没有被抹掉');
    assert.match(redact(`别人的 token=${FAKE_JWT}`), /<REDACTED>/, '应当留下 <REDACTED> 占位');
  });

  test('数据库 API Token 无需载入配置也会被脱敏', () => {
    const token = `dbp_${'0123456789abcdef'.repeat(4)}`;
    assert.equal(redact(`authToken=${token}`), 'authToken=<REDACTED>');
  });

  test('当前配置的真实凭据不会出现在输出里', (t) => {
    const token = tryResolveToken();
    if (!token) {
      // 没有凭据时前置检查会给出可读失败，这里只是没东西可验
      t.skip('未配置 DB_PLATFORM_TOKEN 且 /tmp/.peri-token 不存在');
      return;
    }

    const line = redact(`token=${token}`);
    assert.ok(!line.includes(token), '真实凭据没有被脱敏：输出里能看到原文');
    assert.match(line, /<REDACTED>/, '真实凭据应被替换成 <REDACTED>');
  });
});
