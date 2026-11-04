#!/usr/bin/env python3
"""Invariant scan for the official v1.2.7 release A/B window.

Reads legs/<scenario>/<leg>/ and prints the checks that are not part of the
latency aggregate: per-leg binary identity, fixture completeness, protocol
outcomes, stop proof, resource counters, and durability mechanism counters.
"""

import json
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent
LEGS = HERE / "legs"
EXPECT = {
    "A": {
        "platform": "2a4a41d84011021e7c7b4d556f737a9582c5d7a48dec6a6c3887f6fbbae8294d",
        "worker": "61f742362c04d095b6f23754e22bf6aa2d807ac7154feaf4af211406282faeff",
    },
    "B": {
        "platform": "95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912",
        "worker": "b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff",
    },
}
FIXTURE = {"threads": 201, "messages": 8625, "content_chars": 20177069}
IMAGE_DIGEST = "sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff"


def sides(leg):
    return leg[2].upper()


def main():
    legdirs = sorted(p for p in LEGS.glob("*/*") if p.is_dir())
    problems = []
    sha_ok = 0
    image_ok = 0
    stops = 0
    fixtures = 0
    outcomes = 0
    status_429 = 0
    rows_seen = 0
    counters = {}
    audit_fsync = {}
    journal = {}

    for legdir in legdirs:
        scenario = legdir.parent.name
        leg = legdir.name
        side = sides(leg)

        binaries = legdir / "binaries.txt"
        if binaries.exists():
            found = dict((line.split()[1], line.split()[0])
                         for line in binaries.read_text().splitlines())
            if (found.get("/usr/local/bin/peri-platform") == EXPECT[side]["platform"]
                    and found.get("/usr/local/bin/peri-loom-v1") == EXPECT[side]["worker"]):
                sha_ok += 1
            else:
                problems.append("%s/%s binary sha mismatch" % (scenario, leg))
        else:
            problems.append("%s/%s missing binaries.txt" % (scenario, leg))

        digest = legdir / "image-digest.txt"
        if digest.exists() and IMAGE_DIGEST in digest.read_text():
            image_ok += 1
        else:
            problems.append("%s/%s image digest mismatch" % (scenario, leg))

        summary = json.loads((legdir / "summary.json").read_text())
        fixture = summary.get("fixture") or {}
        if fixture.get("complete") and all(fixture.get(k) == v for k, v in FIXTURE.items()):
            fixtures += 1
        else:
            problems.append("%s/%s fixture incomplete" % (scenario, leg))
        if summary.get("warnings") or summary.get("fatal"):
            problems.append("%s/%s warnings/fatal" % (scenario, leg))

        for row in summary.get("rows", []):
            rows_seen += 1
            bad = {k: v for k, v in row.get("outcomes", {}).items()
                   if k not in ("ok", "expected_limit")}
            if bad:
                problems.append("%s/%s %s outcomes %r" % (scenario, leg, row["protocol"], bad))
            bad_status = [str(k) for k in row.get("status_counts", {}) if str(k) != "200"]
            if bad_status:
                problems.append("%s/%s %s status %r" % (scenario, leg, row["protocol"], bad_status))
            outcomes += row.get("samples", 0)
        for row in summary.get("concurrency", []):
            rows_seen += 1
            status_429 += row.get("status_429", 0)
            if row.get("errors"):
                problems.append("%s/%s c%d errors" % (scenario, leg, row["level"]))
            if row.get("verdict") != "PASS":
                problems.append("%s/%s c%d verdict %r" % (scenario, leg, row["level"], row.get("verdict")))

        stop_doc = json.loads((legdir / "launcher-stop.json").read_text())
        payload = stop_doc.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if payload.get("outcome") == "clean" and not payload.get("unresolved_operations"):
            stops += 1
        else:
            problems.append("%s/%s stop=%r unresolved=%r" % (
                scenario, leg, payload.get("outcome"), payload.get("unresolved_operations")))
        exit_file = legdir / "stop-exit.txt"
        if not exit_file.exists() or exit_file.read_text().strip() != "0":
            problems.append("%s/%s stop exit not 0" % (scenario, leg))

        metrics = json.loads((legdir / "metrics-after.json").read_text())
        before = json.loads((legdir / "metrics-before.json").read_text())
        audit = metrics.get("audit", {})
        audit_before = before.get("audit", {})
        operation = metrics.get("state_operation_cost", {})
        operation_before = before.get("state_operation_cost", {})
        for name in ("rejected_requests_total", "session_seats", "active_permits",
                     "unknown_held_permits"):
            if metrics.get(name, 0) not in (0, None):
                problems.append("%s/%s %s=%r" % (scenario, leg, name, metrics.get(name)))
        for name in ("dropped_total", "head_checkpoint_failures_total",
                     "in_flight_count", "queue_count"):
            if audit.get(name, 0) not in (0, None):
                problems.append("%s/%s audit.%s=%r" % (scenario, leg, name, audit.get(name)))
        if operation.get("operation_journal_compaction_failures_total", 0) not in (0, None):
            problems.append("%s/%s journal compaction failures" % (scenario, leg))
        if audit.get("durability_source") != "audit_file_v1":
            problems.append("%s/%s durability_source=%r" % (
                scenario, leg, audit.get("durability_source")))

        key = (scenario, side)
        audit_fsync.setdefault(key, []).append(
            audit.get("fsync_count", 0) - audit_before.get("fsync_count", 0))
        journal.setdefault(key, []).append((
            operation.get("operation_journal_appends_total", 0)
            - operation_before.get("operation_journal_appends_total", 0),
            operation.get("operation_journal_syncs_total", 0)
            - operation_before.get("operation_journal_syncs_total", 0),
            operation.get("operation_journal_coalesced_total", 0)
            - operation_before.get("operation_journal_coalesced_total", 0),
            operation.get("operation_journal_batch_max", 0),
        ))
        counters[key] = counters.get(key, 0) + 1

    print("legs=%d binary_sha_ok=%d image_digest_ok=%d fixture_ok=%d stop_clean=%d"
          % (len(legdirs), sha_ok, image_ok, fixtures, stops))
    print("summary_rows=%d timed_samples=%d status_429=%d problems=%d"
          % (rows_seen, outcomes, status_429, len(problems)))
    for problem in problems:
        print("PROBLEM", problem)

    print("\n| 场景 | 侧 | 腿数 | audit fsync 增量中位 | journal appends 增量中位 | journal syncs 增量中位 | coalesced 增量中位 | batch_max 中位 |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for key in sorted(audit_fsync):
        scenario, side = key
        fsync = audit_fsync[key]
        appends = [item[0] for item in journal[key]]
        syncs = [item[1] for item in journal[key]]
        coalesced = [item[2] for item in journal[key]]
        batch = [item[3] for item in journal[key]]
        print("| %s | %s | %d | %s | %s | %s | %s | %s |" % (
            scenario, side, counters[key], statistics.median(fsync),
            statistics.median(appends), statistics.median(syncs),
            statistics.median(coalesced), statistics.median(batch)))

    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
