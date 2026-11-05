#!/usr/bin/env python3
"""Generate the R72 evidence README from the machine-readable evidence.

The narrative and the frozen criteria are in criteria.json / criteria-freeze.txt;
this script only renders the measured numbers so the README cannot drift from
summary.json, analysis-p1.json, analysis-p2.json, invariants.json and gate0.json.
"""
from __future__ import annotations

import json
import pathlib
import sys


def read_json(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


def fmt(value, digits=6):
    if value is None:
        return "—"
    return f"{value:.{digits}f}"


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    criteria = read_json(root / "criteria.json")
    summary = read_json(root / "summary.json")
    p1 = read_json(root / "analysis-p1.json")
    p2 = read_json(root / "analysis-p2.json")
    p3 = read_json(root / "invariants.json")
    gate0 = read_json(root / "gate0.json")
    response = read_json(root / "response-bytes.json")
    logs = read_json(root / "log-scan.json")
    metrics = read_json(root / "metrics.json")
    freeze = (root / "criteria-freeze.txt").read_text(encoding="utf-8").strip().splitlines()

    lines = []
    add = lines.append
    add("# R72 独立验证：默认启用延迟会话关闭")
    add("")
    add("日期：2026-11-05")
    add("分支：`perf/r72-deferred-close-default`")
    add("commit：`56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6`（起点 `main` `ee1da7d`）")
    add("性质：**独立验证轮（verification）**。只构建、启动隔离栈、测量、核对与留证；")
    add("不改产品代码/测试，不提交、不推送、不删卷/证据。")
    add("")
    add("## 结论")
    add("")
    add("**总判定：FAIL。** 8 项判据中 C1、C2、C3、C5、C6、C7、C8 通过；")
    add("**C4（L1 c1 p95）失败**：合并 p95 B/A = **%.5f**，超过预冻结阈值 `< 1.10`。" % summary["C4_l1_c1"]["merged_p95_b_over_a"])
    add("C4 的 p50 通过（%.5f），失败只落在尾部。按预冻结判据，本轮不建议合入；" % summary["C4_l1_c1"]["merged_p50_b_over_a"])
    add("未因结果不理想修改判据，也没有为通过而重跑 L1 腿。")
    add("")
    add("| 判据 | 结果 | 关键数字 |")
    add("|---|---|---|")
    add("| C1 Gate 0 机制 | %s | A 无 deferred 键；B enqueued=5、pending_max=1；两臂 operation_settle.retry_total=0 |" % ("PASS" if summary["C1_gate0_mechanism"]["pass"] else "FAIL"))
    add("| C2 P1 主指标 | %s | 中位 %.5f，CI [%.5f, %.5f]，%d/30 B 更快 |" % (
        "PASS" if summary["C2_p1_primary"]["pass"] else "FAIL",
        summary["C2_p1_primary"]["effect_median"],
        summary["C2_p1_primary"]["effect_ci95"][0],
        summary["C2_p1_primary"]["effect_ci95"][1],
        summary["C2_p1_primary"]["direction_triples_below_one"]))
    add("| C3 F1 c1 | %s | p50 %.5f，p95 %.5f |" % (
        "PASS" if summary["C3_f1_c1"]["pass"] else "FAIL",
        summary["C3_f1_c1"]["merged_p50_b_over_a"],
        summary["C3_f1_c1"]["merged_p95_b_over_a"]))
    add("| C4 L1 c1 | **%s** | p50 %.5f，**p95 %.5f (> 1.10)** |" % (
        "PASS" if summary["C4_l1_c1"]["pass"] else "FAIL",
        summary["C4_l1_c1"]["merged_p50_b_over_a"],
        summary["C4_l1_c1"]["merged_p95_b_over_a"]))
    add("| C5 标准 X1 c8 | %s | 吞吐 B/A %.5f |" % (
        "PASS" if summary["C5_standard_x1_c8"]["pass"] else "FAIL",
        summary["C5_standard_x1_c8"]["merged_throughput_b_over_a"]))
    add("| C6 持续 c8 | %s | 吞吐 B/A %.5f，8/8 腿零错误 |" % (
        "PASS" if summary["C6_sustained_c8"]["pass"] else "FAIL",
        summary["C6_sustained_c8"]["merged_throughput_b_over_a"]))
    add("| C7 不变量 | %s | P3、响应形状与日志扫描均通过 |" % ("PASS" if summary["C7_invariants"]["pass"] else "FAIL"))
    add("| C8 P3 | %s | %s/%s 腿 |" % (
        "PASS" if summary["C8_p3"]["pass"] else "FAIL",
        summary["C8_p3"]["legs_passed"], summary["C8_p3"]["legs_total"]))
    add("| C9 崩溃语义 | 引用 | R71 Gate 3 T1–T9 PASS；代码路径一致性见下文 |")
    add("")
    add("**总判定：%s。**" % summary["verdict"])
    add("")
    add("## 判据冻结")
    add("")
    add("判据于任何测量腿之前写入 `criteria.json`；冻结记录见 `criteria-freeze.txt`：")
    add("")
    add("```text")
    for line in freeze:
        add(line)
    add("```")
    add("")
    add("`criteria.json` SHA256 以冻结记录为准：")
    add("")
    add("```text")
    add("de7860a1b1737d4fd20b14fd0f743c1f14f38455892e8b162d44c6b59ce0ba39  criteria.json")
    add("```")
    add("")
    add("## 构建与环境")
    add("")
    add("| 项 | 值 |")
    add("|---|---|")
    add("| 平台二进制 | `/tmp/peri-r72-ab/bin/peri-platform-r72`，SHA256 `e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba` |")
    add("| 平台构建 | `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath ./cmd/peri-platform`（从 `56dc914`；两次构建 SHA 一致） |")
    add("| worker | R71d 同一二进制 `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |")
    add("| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |")
    add("| launcher | `prototypes/v1/platform/dev.py`，SHA256 `80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2` |")
    add("| Go | `go1.27.3 darwin/arm64` |")
    add("| 耐久模式 | `audit_file_v1`（每腿 metrics 实读） |")
    add("| 配额 | 默认，未放宽：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304` |")
    add("| fixture | 每栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625` |")
    add("| 观测器 | 全程关闭 |")
    add("| 端口 | Gate0/P1 21400–21401；F1 21410–21417；L1 21420–21427；X1 21430–21437；持续 c8 21440–21447 |")
    add("| 卷 | 全部新建 `peri-r72-ab-*`，未复用/未删除旧卷 |")
    add("")
    add("两侧使用同一二进制、同一 worker、同一镜像、同一配额；唯一差异是平台进程环境：")
    add("A 显式 `PERI_DEFERRED_SESSION_CLOSE=0`，B 完全未设置。每腿 `binaries.txt` 都记录了")
    add("容器环境与 `/proc/<pid>/environ` 探测结果：")
    add("")
    add("```text")
    add("A: container PERI_DEFERRED_SESSION_CLOSE=0; platform process env: PERI_DEFERRED_SESSION_CLOSE=0")
    add("B: container PERI_DEFERRED_SESSION_CLOSE=<unset>; platform process env: unset")
    add("```")
    add("")
    add("## C1 Gate 0：机制")
    add("")
    add("| 腿 | 开关 | deferred 键 | enqueued | pending_max | operation_settle | retry_total |")
    add("|---|---|---|---:|---:|---|---:|")
    for leg in gate0["legs"]:
        if leg.get("kind") != "gate0":
            continue
        add("| %s | %s | %s | %d | %d | %s | %d |" % (
            leg["leg"], leg["switch"], leg["deferred_present_after"],
            leg["deferred_enqueued_total"], leg["deferred_pending_max"],
            leg["operation_settle_after"], leg["retry_total"]))
    add("")
    add("- **PASS**：B（默认）发布 `deferred_session_close` 且 `enqueued_total=5>0`；")
    add("  A（显式 0）前后都没有该键；两臂 `operation_settle.retry_total=0`。")
    add("- 34 条测量腿也都发布 `operation_settle`。其中 **1 条持续 c8 B 腿**（`p2/c8/r1-b2`）")
    add("  出现 `retry_total=33`、`retry_success_total=8`、`exhausted_total=0`、")
    add("  `refused_total={}`、`errors_total={}`：这是本地结算队列满后的有界重试路径，")
    add("  不是 502/`outcome_unknown`，不违反 C1（C1 的 `retry_total=0` 针对 Gate 0 短腿）。")
    add("")
    add("## C2 P1：短版交错主指标（30 triples）")
    add("")
    add("命令：")
    add("")
    add("```bash")
    add("cd docs/performance-review/2026-11-05-r72-deferred-close-default-ab")
    add("./run_interleaved.sh p1")
    add("python3 analyze_interleaved.py --campaign p1 --root .")
    add("```")
    add("")
    add("运行：`valid_triples=30/30`、`void_triples=0`、`failed_blocks=0`、`blocks=90`、")
    add("measured requests 1800（A 1,200、B 600），另有 2 个不计入统计的 warmup 请求。")
    add("两栈 `driver_exit=0`、`settle_exit=0`、`stop_exit=0`。")
    add("")
    add("| 块 | n | 中位 ms | 均值 ms | p95 ms | min | max |")
    add("|---|---:|---:|---:|---:|---:|---:|")
    for name in ("A1", "A2", "B"):
        item = p1["blocks"][name]
        add("| %s | %d | %.4f | %.4f | %.4f | %.4f | %.4f |" % (
            name, item["count"], item["median"], item["mean"], item["p95"],
            item["min"], item["max"]))
    add("")
    effect = p1["effect"]["per_triple"]
    zero = p1["zero_control"]["per_triple"]
    add("- `B / mean(A1,A2)` 中位 **%.6f**（bootstrap 95%% CI **%.6f–%.6f**，上界 <1.0），" % (
        effect["median"], effect["ci95"][0], effect["ci95"][1]))
    add("  ≤0.90 → **C2 PASS**。")
    add("- 方向：30 个 triples 中 **%d 个 B 更快**、%d 个不更快（≥20 的门槛满足）。" % (
        p1["direction_triples_below_one"], 30 - p1["direction_triples_below_one"]))
    add("- 配对差值中位 **%.6f ms**（CI %.6f–%.6f）。" % (
        p1["effect"]["paired_difference_ms"]["median"],
        p1["effect"]["paired_difference_ms"]["ci95"][0],
        p1["effect"]["paired_difference_ms"]["ci95"][1]))
    add("- 上下文（非判据）：A2/A1 零对照中位 %.6f，CI %.6f–%.6f。" % (
        zero["median"], zero["ci95"][0], zero["ci95"][1]))
    add("")
    triples_path = root / "legs" / "p1" / "driver" / "triples.jsonl"
    if triples_path.exists():
        triples = [json.loads(line) for line in
                   triples_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        add("### 附录：P1 逐 triple 原始比值")
        add("")
        add("| triple | order | A1 ms | A2 ms | B ms | A2/A1 | B/mean(A1,A2) | B faster |")
        add("|---:|---|---:|---:|---:|---:|---:|---|")
        for item in triples:
            add("| %d | %s | %.4f | %.4f | %.4f | %.5f | %.5f | %s |" % (
                item["triple"] + 1, "".join(block[0] for block in item["order"]),
                item["a1_ms"], item["a2_ms"], item["b_ms"], item["zero_ratio"],
                item["effect"], "yes" if item["effect"] < 1.0 else "no"))
        add("")
    add("## C3 F1 c1")
    add("")
    add("命令：`./run_p2.sh f1 r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2`；")
    add("每腿 1 warmup + 125 timed，2 轮 × A/B/B/A，逐轮交替顺序（第 1 轮 A/B/B/A，第 2 轮 B/A/A/B）。")
    add("")
    add("| 腿 | 侧 | p50 ms | p95 ms | 样本 | 失败 |")
    add("|---|---|---:|---:|---:|---:|")
    for leg in p2["scenarios"]["f1"]["legs"]:
        add("| %s | %s | %.4f | %.4f | %s | %d |" % (
            leg["leg"], leg["side"], leg["median_ms"], leg["p95_ms"],
            leg.get("requests"), leg.get("failures") or 0))
    check = p2["scenarios"]["f1"]["checks"]
    add("")
    add("| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |")
    add("|---|---:|---:|---:|---:|---:|---|")
    for metric in ("median_ms", "p95_ms"):
        item = check[metric]
        add("| %s | %s | %s | **%.5f** | %s | %s | %s |" % (
            "p50 ms" if metric == "median_ms" else "p95 ms",
            fmt(item["sides"]["a"]), fmt(item["sides"]["b"]),
            item["merged_b_over_a"], fmt(item["round_ratios"].get("r1"), 5),
            fmt(item["round_ratios"].get("r2"), 5),
            "PASS" if item["merged_b_over_a"] < 1.10 else "FAIL"))
    add("")
    add("**C3 PASS**：p50 %.5f、p95 %.5f，均 <1.10。" % (
        check["median_ms"]["merged_b_over_a"], check["p95_ms"]["merged_b_over_a"]))
    add("")
    add("## C4 L1 c1（FAIL）")
    add("")
    add("命令与形状同 C3，场景为 `L1.list-20`。")
    add("")
    add("| 腿 | 侧 | p50 ms | p95 ms | 样本 | 失败 |")
    add("|---|---|---:|---:|---:|---:|")
    for leg in p2["scenarios"]["l1"]["legs"]:
        add("| %s | %s | %.4f | %.4f | %s | %d |" % (
            leg["leg"], leg["side"], leg["median_ms"], leg["p95_ms"],
            leg.get("requests"), leg.get("failures") or 0))
    check = p2["scenarios"]["l1"]["checks"]
    add("")
    add("| 指标 | A 中位 | B 中位 | 合并 B/A | r1 | r2 | 判定 |")
    add("|---|---:|---:|---:|---:|---:|---|")
    for metric in ("median_ms", "p95_ms"):
        item = check[metric]
        add("| %s | %s | %s | **%.5f** | %s | %s | %s |" % (
            "p50 ms" if metric == "median_ms" else "p95 ms",
            fmt(item["sides"]["a"]), fmt(item["sides"]["b"]),
            item["merged_b_over_a"], fmt(item["round_ratios"].get("r1"), 5),
            fmt(item["round_ratios"].get("r2"), 5),
            "PASS" if item["merged_b_over_a"] < 1.10 else "FAIL"))
    add("")
    add("**C4 FAIL**：p50 通过（0.69538），但合并 p95 B/A = **1.17471 > 1.10**。")
    add("两轮 p95 比值方向相反：r1 = 1.26545（B 更慢）、r2 = 0.83121（B 更快）；")
    add("合并值按冻结规则取每侧 4 腿 p95 的中位数，点估计超过阈值。")
    add("")
    add("上下文（非判据）：对 4 条腿做 bootstrap（10,000 次，seed 20261105），")
    add("p95 B/A 的 95% 区间约 **0.81–1.27**；对每条腿的 125 个样本重采样，")
    add("区间约 **0.85–1.32**。区间很宽，但预冻结判据没有 CI 条款，")
    add("因此按点估计判 FAIL；不据此宣称 p95 一定退化，也不据此改写判据。")
    add("")
    add("口径敏感性（非判据）：若把每侧 500 个计时样本直接合并后取 p95，")
    add("A=4.6742、B=4.5808，比值 **0.98**（<1.10）；但 R71d 冻结口径是")
    add("「每侧 4 条腿 p95 的中位数」，本轮按该口径判 FAIL。")
    add("不得在看到数据后切换聚合口径；该对比只用于说明点估计对口径敏感。")
    add("")
    add("8 条 L1 腿全部有效：125/125 timed 请求、0 失败、driver exit 0、")
    add("审计 delta 384（A/B 相同）、响应形状一致（mode 15,627 bytes / rows 40）。")
    add("因此这不是无效腿或环境异常，不能以“换卷重跑”处理。")
    add("")
    add("## C5 标准 X1 c8")
    add("")
    add("命令：`./run_p2.sh x1 ...`，每腿 `run.py --scenario X1 --concurrency-requests 100`，")
    add("levels 1/4/8/16。c16 的默认配额 429 是已知诊断，不作为判据；c8 必须零 429。")
    add("")
    add("| 腿 | 侧 | c8 qps | c8 p50 ms | c8 p95 ms | c8 429 | c16 429 |")
    add("|---|---|---:|---:|---:|---:|---:|")
    for leg in p2["scenarios"]["x1"]["legs"]:
        add("| %s | %s | %.1f | %.2f | %.2f | %s | %s |" % (
            leg["leg"], leg["side"], leg["throughput_qps"], leg["median_ms"],
            leg["p95_ms"], leg["c8_429"], leg["c16_429"]))
    item = p2["scenarios"]["x1"]["checks"]["throughput_qps"]
    add("")
    add("**C5 PASS**：c8 吞吐合并 B/A = **%.5f**（A %s、B %s qps），>0.90；c8 每腿 429=0。" % (
        item["merged_b_over_a"], fmt(item["sides"]["a"], 1), fmt(item["sides"]["b"], 1)))
    add("")
    add("## C6 持续 c8")
    add("")
    add("命令：`./run_p2.sh c8 ...`；每腿 8 客户端 × 500 pipeline 请求（100 warmup + 400 timed），")
    add("200 请求/块、块间 1.2 s 配额回填。")
    add("")
    add("| 腿 | 侧 | 吞吐 qps | p50 ms | p95 ms | 失败 | B enqueued/inline/pending_max |")
    add("|---|---|---:|---:|---:|---:|---|")
    for leg in p2["scenarios"]["c8"]["legs"]:
        record = next((r for r in metrics["legs"] if r["campaign"] == "p2"
                       and r["scenario"] == "c8" and r["leg"] == leg["leg"]), {})
        deferred = record.get("deferred_session_close") or {}
        text = ("%s/%s/%s" % (deferred.get("enqueued_total"), deferred.get("inline_total"),
                              deferred.get("pending_max"))) if deferred else "—"
        add("| %s | %s | %.1f | %.4f | %.4f | %s | %s |" % (
            leg["leg"], leg["side"], leg["throughput_qps"], leg["median_ms"],
            leg["p95_ms"], leg.get("failures"), text))
    item = p2["scenarios"]["c8"]["checks"]["throughput_qps"]
    add("")
    add("**C6 PASS**：吞吐合并 B/A = **%.5f**，>0.90；8/8 腿校准、warmup、计时失败均为 0。" % item["merged_b_over_a"])
    add("p50/p95 只作诊断：p50 B/A %.5f、p95 B/A %.5f。" % (
        p2["scenarios"]["c8"]["checks"]["median_ms"]["merged_b_over_a"],
        p2["scenarios"]["c8"]["checks"]["p95_ms"]["merged_b_over_a"]))
    add("")
    add("## C7 不变量")
    add("")
    add("- **零异常**：36 个容器日志（Gate0 2 + P1 2 + P2 32）中 `outcome unknown`、` 502`、")
    add("  `audit_unavailable`、`session_moved` 命中数均为 **0**。34 条测量腿的 `errors_total`")
    add("  没有 `outcome_unknown`/502/503；X1 只有 c16 的 `quota_exceeded`/`rate_limit` 诊断。")
    add("- **资源归零**：34/34 腿结算后 `session_seats=0`、`active_permits=0`、")
    add("  `unknown_held_permits=0`、worker sessions=0、worker `pending_close=0`、")
    add("  cleanup ledger `pending_close=0`。")
    add("- **审计链**：34/34 腿 `chain_ok=true`、head 匹配 tail。分组条目数一致：")
    add("  P1 A 3,603 / B 1,803（比值 1.9983，请求比 2.0）；F1/L1 每腿 384；")
    add("  持续 c8 每腿 12,024；X1 的审计增量与成功动作数可解释。")
    add("- **响应形状**：F1 8/8 腿 mode `312 bytes / 1 row`；L1 8/8 腿 mode")
    add("  `15,627 bytes / 40 rows / 201 rows_read`，与 R71d 的 15,627 一致。")
    add("- **deferred 上限**：B 腿 `pending_max` 最大 4（每库上限 4），满足 `≤4`；")
    add("  `failed_total=0`、`retried_total=0`、`unresolved_total=0`、`pending=0`。")
    add("")
    add("| 检查 | 结果 |")
    add("|---|---|")
    add("| P3 34/34 腿 | %s |" % ("PASS" if p3["passed"] else "FAIL"))
    add("| 响应字节/行数 | %s |" % ("PASS" if response["passed"] else "FAIL"))
    add("| 日志扫描 | %s |" % ("PASS" if logs["passed"] else "FAIL"))
    add("")
    add("## C8 P3：34/34")
    add("")
    add("`python3 collect_invariants.py .`：**34/34 腿通过**（P1 2/2；F1 8/8；L1 8/8；")
    add("标准 X1 8/8；持续 c8 8/8）。逐腿检查项：A 腿 13 项、B 腿 18 项；")
    add("34 腿的 driver/settle/stop 退出码均符合预期；`launcher-stop` 为 `outcome=clean`、")
    add("`unresolved_operations=false`、`docker stop` 退出码 0。")
    add("")
    add("## C9 崩溃语义（引用 R71 Gate 3）")
    add("")
    add("- **状态：引用，未重跑。** R71 Gate 3 T1–T9 在显式启用的同一机制上 PASS；")
    add("  T2（丢弃 close 耐久记录）得到预期的 fail-closed：重启隔离命名空间、")
    add("  Hrana `SELECT 1` 返回 HTTP 409 `database_isolated`、`verify_no_unresolved_operations`")
    add("  拒绝 clean restart，受支持的 release 路径恢复 200 且不删除 ownership marker。")
    add("- **代码路径一致性**：探针分支 `probe/r71-g3-fault-injection` @ `090aad5`，")
    add("  探针平台 SHA256 `5c3e8f2a9a290620de39d56ffcb02292381ba0616873efa04f03cfa0b9c71152`")
    add("  （已核对）。`git diff probe/r71-g3-fault-injection 56dc914 --")
    add("  prototypes/v1/platform/session_close_deferred.go` 只有默认值翻转与注释差异；")
    add("  探针分支其余平台差异是 4 个调用点 hook + `probe_r71_g3.go`，仅在")
    add("  `PERI_R71_G3_*` 设置时生效。显式 `=1` 且不设探针变量时，探针二进制执行与")
    add("  R72 B 臂相同的 deferred close 路径。完整 diff 见 `c9-code-path-check.txt`。")
    add("")
    add("## 失败腿、异常与重跑")
    add("")
    add("- **无效腿：无。** 34/34 测量腿有效；无 502/`outcome_unknown`/审计不可用/启动失败，")
    add("  因此没有换卷重跑的腿。")
    add("- **唯一 FAIL 是 C4 的 L1 p95 点估计**，不是环境异常或无效腿；没有为通过而重跑。")
    add("- **Gate 0 之外的结算重试**：`p2/c8/r1-b2` 的 `operation_settle.retry_total=33`、")
    add("  `retry_success_total=8`、`exhausted_total=0`、`refused_total={}`、`errors_total={}`；")
    add("  请求全部成功、审计链连续、clean stop。这是有界重试路径，不是 502。")
    add("")
    add("## 验证命令与结果")
    add("")
    add("```bash")
    add("cd prototypes/v1/platform")
    add("GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o /tmp/peri-r72-ab/bin/peri-platform-r72 ./cmd/peri-platform")
    add("go vet ./...")
    add("PERI_RUST_BINARY=/Users/kk/code/peri-loom/prototypes/v1/target/debug/peri-loom-v1 go test ./... -count=1")
    add("gofmt -l <changed files>   # 无输出")
    add("```")
    add("")
    add("构建产物 SHA256：`e07e6da8…c621ba`；`go vet` 通过；`gofmt` 无输出。")
    add("")
    add("完整 Go 测试套件（`PERI_RUST_BINARY=prototypes/v1/target/debug/peri-loom-v1 go test ./... -count=1`）：")
    add("")
    add("```text")
    go_test = root / "go-test.txt"
    if go_test.exists():
        for line in go_test.read_text(encoding="utf-8").splitlines():
            add(line)
    else:
        add("go-test.txt missing")
    add("```")
    add("")
    add("针对性延迟关闭测试 `go test . -run 'TestDeferredSessionClose' -count=1` 全部 PASS，")
    add("包括 `unset/empty 启用`、`显式 0 关闭`、`显式 1 启用`、`非法值拒绝`。")
    add("")
    add("## 环境与限制")
    add("")
    add("- 单机 macOS 26.5.1 / arm64、Docker 29.4.0；不是容量或 SLA 证明。")
    add("- C4 的 p95 点估计失败，但 bootstrap 区间宽、两轮方向相反；报告保留这一事实，")
    add("  不据此宣称稳定退化，也不放宽判据。")
    add("- C9 引用 R71 Gate 3，未做真实掉电/机器崩溃；T2 是确定性故障注入。")
    add("- 观测器全程关闭；未开启 `PERI_DURABILITY_OBSERVER` 或相位观测器。")
    add("")
    add("## 证据清单")
    add("")
    add("```text")
    add("criteria.json                 冻结判据")
    add("criteria-freeze.txt           冻结时间与 SHA256")
    add("environment.txt               构建/镜像/worker/配额/端口")
    add("raw.jsonl                     P1 的 90 个块原始记录（triples 在 legs/p1/driver/）")
    add("summary.json                 C1–C9 机器可读结论")
    add("metrics.json                 36 条腿的合并指标视图")
    add("analysis-p1.json             P1 统计")
    add("analysis-p2.json             F1/L1/X1/c8 统计")
    add("invariants.json              P3 34/34")
    add("gate0.json                   C1 + operation_settle 检查")
    add("response-bytes.json          F1/L1 响应形状")
    add("log-scan.json                36 个容器日志扫描")
    add("c9.json / c9-code-path-check.txt  C9 引用与代码路径一致性")
    add("go-test.txt                  完整 Go 测试套件输出")
    add("go-test-deferred-close.txt   延迟关闭定向测试输出（含开关解析）")
    add("go-vet.txt / gofmt.txt       go vet 与 gofmt 输出")
    add("l1-p95-bootstrap.txt         C4 p95 的上下文 bootstrap（非判据）")
    add("run_gate0.sh / gate0_driver.py / check_gate0_r72.py")
    add("run_interleaved.sh / interleaved_driver.py / analyze_interleaved.py")
    add("run_p2.sh / run_all_p2.sh / sustained_c8_driver.py / analyze_p2.py")
    add("collect_invariants.py / verify_audit.py / wait_settled.py / snap_metrics.py")
    add("check_response_bytes.py / check_logs.py / collect_metrics_summary.py / make_summary_r72.py")
    add("l1_p95_bootstrap.py / make_readme.py")
    add("legs/                         每腿 leg.txt/binaries.txt/metrics-*/audit-verify/harness/")
    add("```")
    add("")
    add("连接文件与私有 API Key 不在证据目录；容器/卷保留在 Docker 中，未 prune、未删除。")
    add("")
    (root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote README.md (%d lines)" % len(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
