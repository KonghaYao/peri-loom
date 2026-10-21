# sqld 相对性能门禁（五项完整接口 p50 ≤ 1.5×）

日期：2026-10-20
基线：`913a20b`（v1.1.10）
状态：**门禁工具与输入契约已实现；当前没有 v1.1.10 的同机同条件配对实测证据，因此不存在 PASS 结论。**

本文只说明门禁如何拒绝不可信证据、如何从原始记录重算指标，以及解释边界。
门禁本身不发起请求、不启动 peri-loom 或 sqld、不产生任何测量值。
实现为 Python 3 标准库，无第三方依赖。

审计来源：`peri-loom-v1.1.10-serverless-branch-sql-audit-2026-10-20.md` 第 6 节 Q01 与第 9.2 节。
审计报告中 2026-10-20 v1.1.6 优化计划的历史 p50 表是**目标来源**，不是本轮或 v1.1.10 的实测成绩；
本门禁也不使用任何固定毫秒阈值替代新的配对 sqld 分母。

---

## 1. 固定五场景

场景名固定，缺一个、重复、改名都判 `INVALID`：

| 场景 | 说明 |
|---|---|
| `single_read` | 单请求读 |
| `single_write` | 单请求写 |
| `sequential_read_50rps` | 50 RPS 连续读 |
| `mixed_read` | 四流混合负载中的读 |
| `mixed_write` | 四流混合负载中的写 |

五项分别判定：`peri p50 <= 1.5 × sqld p50`。
不得对五项取平均，也不得用四项通过掩盖一项失败。

## 2. 一个 campaign = 一个池

每个证据文件只允许一个对照池。以下维度固定在 `campaign` 中，文件内所有 run 必须一致：

| 维度 | 允许值 | 约束 |
|---|---|---|
| `backend` | `native` / `s3` | 不得混池 |
| `cache` | `cold` / `warm` | 不得混池 |
| `maintenance` | `on` / `off` | 不得混池 |
| `tls` | `{enabled: true, profile: "..."}` | 性能验收必须启用 TLS |
| `durability` | 非空字符串 | 两侧必须声明同一配置 |
| `dataset` | `{ref, sha256}` | 固定数据集 |
| `request_script` | `{ref, sha256}` | 固定请求脚本 |
| `rounds` | 整数，且 `>= 3` | 每个 arm/scenario 必须恰好有 `1..rounds` 轮 |

每个 run 还必须重复 `dimensions.backend`、`dimensions.cache`、`dimensions.maintenance`，
其中这三个字段必须与 campaign 完全一致；不一致即 `INVALID`。

需要比较 native 与 S3、cold 与 warm、维护开与关时，分别生成证据文件、分别运行门禁、
分别保存 verdict。**不得把多个 campaign 的结果合并、平均或互相替代。**

## 3. 固定元数据

`metadata` 必填，缺字段或格式错误判 `INVALID`：

| 字段 | 格式 |
|---|---|
| `peri_sha` | 40 位小写十六进制 |
| `turso_submodule_sha` | 40 位小写十六进制 |
| `sqld_image_digest` | `sha256:<64 位小写十六进制>` |
| `arch` | 非空字符串 |
| `hardware.cpu` | 非空字符串 |
| `hardware.memory_bytes` | 正整数 |
| `hardware.disk` | 非空字符串 |
| `hardware.network` | 非空字符串 |

可选命令参数 `--expect-peri-sha`、`--expect-turso-sha` 用于把证据固定到预期提交；
不匹配判 `INVALID`。门禁无法证明声明的 SHA 就是实际运行的二进制，这属于采集器的责任和
剩余证据缺口。

## 4. G0 正确性阻断

`correctness` 是必填对象：

| 字段 | 要求 | 不满足时 |
|---|---|---|
| `g0_passed` | 布尔值，必须为 `true` 才能进入性能达标 | `false` → `HOLD`；缺失/类型错误 → `INVALID` |
| `unknown_results` | 非负整数，必须为 `0` | 非 0 → `INVALID` |
| `oracle` | 非空字符串（例如 `SP01`） | 缺失 → `INVALID` |
| `evidence` | 非空字符串，指向 oracle 证据 | 缺失 → `INVALID` |

`HOLD` 时仍会输出五场景比值供诊断，但所有 scenario 标记为 `accepted=false`，
Markdown 显示 `BLOCKED`，**不得解释为性能达标**。

## 5. 每个 run 的必填字段

| 字段 | 要求 |
|---|---|
| `run_id` | 文件内唯一、非空字符串 |
| `arm` | `peri` / `sqld` |
| `scenario` | 五个固定场景之一 |
| `round` | `1..campaign.rounds` |
| `dimensions` | `{backend, cache, maintenance}`，必须与 campaign 一致 |
| `planned_requests` | 整数，且 `>= 30` |
| `samples_ns` | 每个请求一个正整数纳秒记录，至少 30 个 |
| `errors` | 恰好包含 `rejected`、`failed`、`unknown`、`excluded` 四个非负整数计数 |
| `summary` | 可选；**从不被信任**，只用于比对告警 |

请求账必须闭合：

```text
len(samples_ns) + rejected + failed + unknown + excluded == planned_requests
```

以下任一情况判 `INVALID`，不能通过：

- `errors` 缺键、值不是非负整数、出现未分类键（例如 `timeout`，应归入 `failed`）；
- `unknown > 0`（Unknown 未结算）；
- `excluded > 0`（样本被剔除）；
- `rejected > 0` 或 `failed > 0`（只测成功子集会使比较失真）；
- 请求账不闭合；
- 每个 arm/scenario 缺少 `1..rounds` 中的任一轮，或出现额外轮次；
- `planned_requests` 在同一场景的不同 run 之间不一致；
- 同一 arm/scenario 的 round 重复；
- 样本数少于 30，或样本不是正整数纳秒。

`summary` 中若提供 `p50_ns` / `p95_ns` / `p99_ns`，门禁会与原始样本重算值比较；
不一致只产生 warning，判定始终使用重算值。这样既不让错误汇总影响结论，也能暴露采集器问题。

## 6. 输入结构示意

以下为结构示意，占位符不是合法输入：

```text
{
  "schema_version": "sqld-relative-gate/1",
  "campaign": {
    "id": "<campaign-id>",
    "backend": "native",
    "cache": "warm",
    "maintenance": "off",
    "tls": {"enabled": true, "profile": "<tls-profile>"},
    "durability": "<durability-profile>",
    "dataset": {"ref": "<dataset-ref>", "sha256": "<64 hex>"},
    "request_script": {"ref": "<script-ref>", "sha256": "<64 hex>"},
    "rounds": 3
  },
  "metadata": {
    "peri_sha": "<40 hex>",
    "turso_submodule_sha": "<40 hex>",
    "sqld_image_digest": "sha256:<64 hex>",
    "arch": "<arch>",
    "hardware": {
      "cpu": "<cpu>",
      "memory_bytes": 0,
      "disk": "<disk>",
      "network": "<network>"
    }
  },
  "correctness": {
    "g0_passed": true,
    "oracle": "SP01",
    "evidence": "<oracle evidence ref>",
    "unknown_results": 0
  },
  "runs": [
    {
      "run_id": "<unique-id>",
      "arm": "peri",
      "scenario": "single_read",
      "round": 1,
      "dimensions": {"backend": "native", "cache": "warm", "maintenance": "off"},
      "planned_requests": 0,
      "samples_ns": ["<raw ns integers>"],
      "errors": {"rejected": 0, "failed": 0, "unknown": 0, "excluded": 0}
    }
  ]
}
```

一个完整证据文件需要：五个场景 × 两个 arm × `rounds` 轮；每轮 `planned_requests >= 30`。
示例中的 `0` 和占位符只表示字段位置，会被门禁拒绝。

## 7. 判定规则

1. 对每个 arm/scenario，把各轮原始 `samples_ns` 合并后排序，用 nearest-rank 重算：
   - p50：第 `ceil(50 × N / 100)` 个；
   - p95：第 `ceil(95 × N / 100)` 个；
   - p99：第 `ceil(99 × N / 100)` 个。
2. 逐场景用整数比较，避免浮点边界误差：

   ```text
   peri_p50 × 2 <= sqld_p50 × 3
   ```

   等价于 `peri_p50 <= 1.5 × sqld_p50`。
3. 五项分别给出 `PASS` / `FAIL`；总体只有在五项全部通过时才 `PASS`。
4. 输出 JSON verdict 与 Markdown 表；Markdown 表包含两侧 p50/p95/p99、比值、
   1.5× 上限（向下取整到纳秒）、样本数和逐场景结论。

## 8. 状态与退出码

| 状态 | 含义 | 退出码 |
|---|---|---:|
| `PASS` | 证据结构完整、G0 通过、零错误/剔除，五项均 ≤ 1.5× | 0 |
| `FAIL` | 证据有效且 G0 通过，但至少一项超过 1.5× | 1 |
| `INVALID` | 缺字段、轮次不足、错误未结算、样本剔除、混池、场景/arm 不完整等 | 2 |
| `HOLD` | 没有证据、证据文件不存在，或 G0 未通过；不能进入性能达标 | 3 |

`INVALID` 与 `HOLD` 都不是通过。缺失输入时工具仍输出结构化 JSON，而不是默认成功。

## 9. 命令

```bash
# 对账一份原始证据；生成 JSON 与 Markdown
python3 tools/sqld-relative-gate/gate.py \
  --input evidence.json \
  --json-out verdict.json \
  --markdown-out verdict.md

# 没有证据时：输出 HOLD JSON，退出码 3
python3 tools/sqld-relative-gate/gate.py

# 固定到预期提交
python3 tools/sqld-relative-gate/gate.py \
  --input evidence.json \
  --expect-peri-sha <full-sha> \
  --expect-turso-sha <full-sha>

# 单元测试
python3 -m unittest discover -s tools/sqld-relative-gate -v
```

只给 `--input` 时，JSON verdict 打印到 stdout；给 `--json-out` 时写入文件。
`--markdown-out` 可选。

## 10. 解释边界

- `PASS` 只表示：在同一 campaign、同一固定元数据、G0 通过、零错误/剔除的条件下，
  五项 p50 均不超过配对 sqld 的 1.5 倍。
- 它**不**证明 p95/p99 尾延迟、goodput、长期稳定性或容量；SP01 的 G1–G5 仍需独立执行。
- 它**不**证明 S3 backend、cold start、维护开启、多 branch 或 serverless 规模；
  这些必须各自作为独立 campaign 运行。
- 它**不**重新执行 SP01/G0 oracle；`correctness.g0_passed` 是输入声明，
  必须由 `correctness.evidence` 指向的独立证据支撑。
- 它**不**验证采集器是否如实记录了硬件、TLS、durability 和两侧配置差异；
  门禁只校验文件内一致性和必填身份。
- 它**不**单独建模 native RPC 与公开 Hrana 路径；一个 campaign 必须固定一条用户接口路径，
  并在 `request_script.ref` 或采集说明中留痕，不能把两条路径混成一个分母。
- 它**不**校验 EXPLAIN/query plan、索引、返回行数与字节；这些仍须随证据保存，
  作为“两侧处理同一工作量”的补充证明。
- 普通共享 CI 的噪声结果只能作为回归探针，不能凭它宣称达标；正式结论需要受控 runner。
- 不得把不同优化版本、不同机器或不同 campaign 的最佳结果拼接成一次 `PASS`。

## 11. 当前证据缺口

截至 2026-10-20，已知缺口如下，均未被本工具填补：

1. 没有 v1.1.10 的同机同条件五场景 peri/sqld 配对原始记录。
2. 没有冻结二进制、sqld image digest、硬件和 TLS/durability 配置的完整采集包。
3. 没有与本轮 v1.1.10 对应的 G0 独立 oracle 证据；因此即使有延迟记录也不能进入性能达标。
4. 没有分别覆盖 native/S3、cold/warm、维护开/关的独立 campaign。
5. 没有可复现的固定 request script、dataset hash 和五场景原始逐请求样本。
6. 没有 SP01 G1–G5 的尾延迟、600 秒/1800 秒长期运行和 cold/graceful open 结果。
7. 本工具仅通过合成单元测试验证判定逻辑；尚未用真实 v1.1.10 证据运行过 `PASS`。

在这些缺口关闭前，正确结论只能是 `HOLD` 或 `INVALID`，不能是性能达标。
