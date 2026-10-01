// k6 压测脚本：热 DB 查询（架构 §16「热 DB 查询」）。
//
//   k6 run --vus 64 --duration 30s \
//     -e BASE_URL=http://127.0.0.1:8080 -e DB_ID=<uuid> -e TOKEN=<jwt> \
//     deploy/k6/hot-query.js
//
// 说明：
//   * DB 必须已经 WARM/HOT 且数据进入 page cache，否则测到的是冷启动而不是热路径。
//   * 这里测量的是「客户端观测到的端到端延迟」。平台附加延迟（Server Router +
//     Worker Dispatcher 的 P95 <= 3ms）应结合 Server 内部指标
//     dbplatform_request_latency_micros{stage="route"} 一起判读。
//   * handleSummary 会输出 acceptance.sh 可直接解析的键值行。

import http from 'k6/http'
import { check } from 'k6'
import { Trend, Counter, Rate } from 'k6/metrics'

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080'
const DB_ID = __ENV.DB_ID || ''
const TOKEN = __ENV.TOKEN || ''

const platformLatency = new Trend('platform_latency_ms', true)
const errors = new Counter('platform_errors')
const errorRate = new Rate('platform_error_rate')

export const options = {
  scenarios: {
    hot_query: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 64),
      duration: __ENV.DURATION || '30s',
    },
  },
  thresholds: {
    // 与架构 §16 的验收阈值保持一致（P95 <= 10ms 端到端）
    'platform_latency_ms': ['p(95)<=10', 'p(99)<=25'],
    'platform_error_rate': ['rate<0.0001'],
  },
}

function headers() {
  const h = { 'content-type': 'application/json' }
  if (TOKEN) h['Authorization'] = `Bearer ${TOKEN}`
  return h
}

export default function () {
  if (!DB_ID) {
    throw new Error('必须通过 -e DB_ID=<uuid> 指定已预热的目标数据库')
  }
  const url = `${BASE_URL}/data/v1/databases/${DB_ID}/query`
  const body = JSON.stringify({ sql: 'SELECT 1' })

  const res = http.post(url, body, {
    headers: headers(),
    tags: { stage: 'data_query' },
  })

  platformLatency.add(res.timings.duration)

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'no platform error': (r) => !String(r.body || '').includes('"error"'),
  })
  errorRate.add(!ok)
  if (!ok) errors.add(1)
}

export function handleSummary(data) {
  const p95 = data.metrics.platform_latency_ms
    ? data.metrics.platform_latency_ms.values['p(95)']
    : 0
  const p99 = data.metrics.platform_latency_ms
    ? data.metrics.platform_latency_ms.values['p(99)']
    : 0
  const rps = data.metrics.http_reqs ? data.metrics.http_reqs.values.rate : 0
  const errRate = data.metrics.platform_error_rate
    ? data.metrics.platform_error_rate.values.rate
    : 0

  const lines = [
    `platform_p95_ms=${p95.toFixed(3)}`,
    `platform_p99_ms=${p99.toFixed(3)}`,
    `achieved_rps=${rps.toFixed(1)}`,
    `error_rate=${errRate.toFixed(6)}`,
  ].join('\n')

  return {
    stdout: lines + '\n',
  }
}
