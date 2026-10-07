# Round 4 control：FAIL，发生 OS suspend，无有效性能结论

## 结论与边界

本轮唯一实际 runner 为 `r4csat`，run ID `95b311e297b4f1fb`，实际 exit **1**，raw 状态 **FAIL / sql_error**。C1 warmup、smoke完成；C1 measure未完成，随后整个control序列停止。C2/C4/C8、control point和candidate全部 **NOT EXECUTED**，不计PASS。

运行窗口存在实际OS Idle Sleep / DarkWake及显著wall-vs-monotonic分离，不能把失败measure当作正常运行性能片段。没有完整30s measure、没有完整control，更没有paired性能比较PASS。原typed SQL错误仍FAIL，不因环境证据改成PASS或安全重试。

两个持久oracle实际PASS：preflight与C1 post-smoke，均ledger rows=4。没有post-measure或最终oracle。所有已执行phase合计unknown=0、cleanup_unknown=0、correctness_failures=0；这些计数不代表缺失的最终oracle通过，也不能证明失败请求无已提交prefix。

报告只核对既有private raw/provenance/power日志；不执行API、SQL、provision、build、signal或重跑。Round1–3与本轮首失败证据不改分类、不覆盖。

## 授权、source 与服务身份

实际执行source HEAD：`21c509d74bdc92c140915f18d6fb647c82b90ef7`。预声明为 `04_PLAN.md`，最终SHA256 `22515f4de8a641b4b84c1bc1ab7a56bcc3cf4f9a094946620c25979f3dab944f`，main确认已由commit `6964557`纳入历史。执行前后root仅用户 `CLAUDE.md` dirty；该用户文件排除于不可变runtime代码，仍真实记入provenance，不改其内容。

Gate：`/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round4.json`，private0600。它是main matching-HEAD operator attestation（普通private JSON），**不是密码学签名证明**。C/James、F完整gates结束和builders停止为main的启动授权前提；本报告不代替F独立复验那些gates。

| Artifact | 实际路径 / 完整SHA256 |
| --- | --- |
| Preserved old native control | `/tmp/peri-stress-native-baseline-bin` |
| Native SHA256 | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Gateway | `/tmp/peri-stress-platform-correlation-bin` |
| Gateway SHA256 | `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` |
| New drain harness | `/tmp/peri-stress-phase-drain-bin` |
| Harness SHA256 | `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8` |

Preserved native不是当前HEAD重新构建的artifact；这是有意control variant。新drain harness与旧c186不同，不把历史吞吐作无条件等价比较。

Fixture：`/private/tmp/peri-stress-r4-control-8dKNuhKU`，fresh whole-project、proper CA HTTPS19446、两个worker、独立fresh S3 bucket。目录0700，connection0600，不公开credential。Bucket初始化证据为该fixture的 `bucket-initialization.json`。

| Role | PID | `ps` start identity（host observed） |
| --- | --- | --- |
| Launcher | 99485 | Wed Oct 7 11:31:43 2026 |
| Authority | 99521 | Wed Oct 7 11:31:44 2026 |
| Platform/Gateway | 99522 | Wed Oct 7 11:31:44 2026 |
| worker-a | 99524 | Wed Oct 7 11:31:44 2026 |
| worker-b | 99526 | Wed Oct 7 11:31:44 2026 |

Host实际capacity：18 logical CPUs、51,539,607,552 bytes（48GiB）物理内存。Private provenance记录上述所有PID/executable/start identity/SHA、source/dirty、gate、bucket初始化hash与精确argv。

## 实际命令与一次性顺序

一次safe provision实际exit0：

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-r4-control-8dKNuhKU \
  --output /private/tmp/peri-stress-r4-control-8dKNuhKU/stress-r4csat.json \
  --label r4csat
```

唯一一次runner：

```sh
/tmp/peri-stress-phase-drain-bin \
  -connection /private/tmp/peri-stress-r4-control-8dKNuhKU/stress-r4csat.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-round4.json \
  -cases saturation_admission -rows 1000 -seed 20261007 \
  -concurrency 1,2,4,8 -warmup 2s -smoke 10s -measure 30s \
  -offered-rate 200 -max-process-rss-bytes 536870912
```

保持默认request timeout10s、generated256MiB、artifact64MiB；未调高quota/预算、未加SQL retry。唯一runner PID7753实际退出，全host/output locks释放；`r4cpoint` config与label reservation不存在。失败后没有诊断SQL、rollback/reopen、盲close、cache删除或candidate启动。

## 已执行阶段与oracle

| C / Phase | 状态 | Offered | Attempted / completed | ACK | Client drop | Queue expired | SQL error |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 warmup | COMPLETE | 400 | 49 / 49 | 49 | 350 | 1 | 0 |
| 1 smoke | COMPLETE | 2000 | 247 / 247 | 247 | 1752 | 1 | 0 |
| 1 measure | FAIL，提前结束 | 1707 | 194 / 194 | 193 | 1512 | 1 | 1 |
| 合计 | PARTIAL | 4107 | 490 / 490 | 489 | 3614 | 3 | 1 |

各phase的unknown、cleanup_unknown、timeouts、correctness_failures均为0。Client drop是客户端offer admission drop，**不是server quota rejection**。Failed operation的 `correct=false` 不表示已ACK row mismatch；它没有ACK，phase `correctness_failures=0`。

Preflight PASS与post-smoke PASS实际核对账户hash、accountsum、ledger；ledger均4。该case为read workload，489个workflow ACK不是489条write ledger。C1失败measure之后没有新持久oracle；不可用此前oracle倒填最终PASS。

首错误request ID：`stress_95b311e297b4f1fb_000000004534`，业务ID `stress_95b311e297b4f1fb_000000004526`，`POST /v1/sql/query`，HTTP400、173 response bytes，typed `sql_error`，`execution_completed=true`、`may_have_committed=true`、`unknown=false`、`timed_out=false`。实际response body/native message未保存，**不能特指BUSY、View、lease、VM budget或其他native根因**。

New-drain合同保留首SQL失败、停止新offers/dequeue；没有把FAIL改PASS。本次失败发生于C1，并无其他并发session可用于证明peer自然drain改善，因此unknown0不能单独作为多inflight drain验证。Typed terminal SQLnegative也不保证无committed prefix/implicit rollback；未额外执行恢复mutation。

## OS Sleep/Wake与时钟异常：实际环境证据

以下时间均为raw/OS日志 **host-wall observed timestamps**，不作外部校时推断；elapsed为原始Go monotonic计数。Wall差仅用于异常审计，不替代elapsed或用于性能吞吐。

| Window | Host-wall observed start → finish（UTC） | Wall差 | Raw monotonic elapsed |
| --- | --- | --- | --- |
| Whole runner | 2026-10-07T03:35:37.801848Z → 03:40:19.379930Z | 281.578082s | 24.983863042s |
| C1 warmup | 03:35:41.652349Z → 03:35:43.666201Z | 2.013852s | 2.013834167s |
| C1 smoke | 03:35:43.749011Z → 03:35:53.785425Z | 10.036414s | 10.036327792s |
| C1 measure | 03:35:54.168240Z → 03:40:19.293236Z | 265.124996s | 8.530917208s |
| Failed RPC | 03:36:02.169269Z → 03:40:19.293062Z | 257.123793s | 0.529775584s |

Fixture `power-events.log`实际记录：

- `2026-10-07 11:36:00 +0800`：Idle Sleep，259secs。
- `2026-10-07 11:40:19 +0800`：DarkWake from Deep Idle。
- 之后还有11:41:04 Maintenance Sleep、11:41:14 DarkWake与11:41:49 FullWake；这些不倒填为已结束runner的SQL阶段。

DarkWake时刻对应failed RPC的UTC03:40:19终态，runner/measure/RPC的wall与monotonic均显著分离。这是**实际suspend/timing confounder**，不能描述为正常连续awake测量，也不能把monotonic较短误读成请求在host-wall内快速完成。OS证据本身不提供未保存的native错误文本或唯一因果链。

Warmup/smoke虽然在该IdleSleep记录之前完成，仍只是本次partial run的功能片段；本报告不从它们发表稳定性能结论，不从latency quantiles推断suspend期间真实服务质量或最大容量。

## 资源、metrics与背景局限

Raw含25个实际resource samples；sampled RSS maxima：authority38,879,232、platform22,364,160、worker-a14,106,624、worker-b32,653,312 bytes，未观测到512MiB单进程guard触发。它们是实际进程RSS，不是cache payload代替RSS，不保证采样间峰值。

独立background JSONL共5个样本（含preflight），sampled external cargo/rustc CPU max0。Preflight有WindowServer、编辑器renderer等实际背景CPU，且OS suspend造成观测空档；**不声称零干扰、最大容量或纯CPU/锁性能归因**。管理/native metrics只保留raw实际map及measurement scopes；全局/进程累计观测不是本run专属增量，缺失值不造0。

Generated request bytes charged204,144；run目录两项raw合计1,793,958bytes。没有resource/artifact abort记录；这不改变unexpectedSQL FAIL与suspend使测量无效的结论。

## 停止与未覆盖

| 项目 | 实际状态 |
| --- | --- |
| Control C1 warmup / smoke | COMPLETE，持久oracle边界到post-smoke |
| Control C1 30s measure | FAIL，未完成；无post-measure/final oracle |
| Control saturation C2/C4/C8 | NOT EXECUTED |
| Control point `r4cpoint` | NOT PROVISIONED / NOT EXECUTED |
| Control mixed | OUT OF SCOPE，原预声明不安排 |
| Candidate saturation / point / mixed | NOT EXECUTED |
| Candidate architecture6 / commonSQL13 | NOT EXECUTED |
| snapshot-under-load | 原计划explicit SKIP，未取得coverage |

Main在确认所有E runner停止后正常SIGTERM自己launcher99485；main报告PTY72702 actualexit0、全部5服务PID gone、Authority clean。这是**main交接的实际停机事实**，不是E实施signal或额外API观测。旧raw/fixture全部保留；Round5仅为新的预声明，需main新commit/gate/启动和明确授权，不在本报告中执行。

## 冻结证据索引

Base：`/private/tmp/peri-stress-results-h3wtrdm2`。

| 文件 | SHA256 |
| --- | --- |
| `run_95b311e297b4f1fb/run.json` | `1a44137bc81bc2762e146c9be2cd8c790dcaa93271421f554d1d06adeb7aa933` |
| `run_95b311e297b4f1fb/operations.jsonl` | `c15da398ce52091f11a75f51b7d0058ceaf07ab45e01c3fd76ba4ccfbb9c9ecd` |
| `round4-control-r4csat.provenance.json` | `050b0946a2c123d10b8fcce34034fde95ecf7ae9ba61413f5a046aa867be6e09` |
| `joint-pass-round4.json` | `ee0f1b1714967e8d7326756c52e2fa48e6ffe15d894986892a39ccecfdf421ee` |
| Fixture `power-events.log` | `58c8f3343b5e043b0fd7d12153b4e825bbddb620f47b744d4adbb0068239913e` |

同base的 `round4-control-r4csat` 前缀另保留 `.background.jsonl`、`.provision.stdout.log`、`.provision.exit-code`（0）、`.stdout.log`、`.exit-code`（1）、`.outcome.json`、`.wrapper-stop.json`。没有成功completion文件，不用wrapper STOP掩盖实际runner exit1。

作者判定：**Round4 control FAIL / 全矩阵PARTIAL / 无性能结论**。F独立 `04_control_verification.md` 负责复核原始计数、oracle边界、firstnegative与suspend/timing证据；本报告不提前替F宣布验收。
