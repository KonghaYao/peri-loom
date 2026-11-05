#!/usr/bin/env python3
"""Gate 2 analysis: eligibility ratio, close latency and cleanliness.

Reads each leg directory written by run_gate2.sh:
  legs/<leg>/leg.txt          leg identity, flag state and platform sha
  legs/<leg>/summary.json     driver window counters (the decision source)
  legs/<leg>/metrics-*.json   whole-leg metrics snapshots (key-presence proof)

Prints a markdown table and writes analysis.json. The decision thresholds are
the Gate 2 brief's: the flag-on read leg must enqueue > 0 with an eligibility
ratio >= 0.90, the flag-off leg must publish no deferred counter, and the
write leg must enqueue 0 with no failures.
"""
from __future__ import annotations

import json
import pathlib
import sys

THRESHOLD = 0.90


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def read_leg(path: pathlib.Path) -> dict:
    identity = {}
    for line in (path / "leg.txt").read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            identity[key.strip()] = value.strip()
    summary = read_json(path / "summary.json") or {}
    before = read_json(path / "window-before.json")
    after = read_json(path / "window-after.json")
    whole_after = read_json(path / "metrics-after.json")
    window = summary.get("window") or {}
    return {
        "leg": identity.get("leg", path.name),
        "flag": identity.get("flag", "unknown"),
        "kind": identity.get("kind", "unknown"),
        "platform_sha256": identity.get("platform_sha256", ""),
        "container": identity.get("container", ""),
        "volume": identity.get("volume", ""),
        "port": identity.get("port", ""),
        "summary": summary,
        "window": window,
        "deferred_key_in_window": bool(before is not None and after is not None
                                       and "deferred_session_close" in after),
        "deferred_key_after_leg": bool(whole_after is not None
                                       and "deferred_session_close" in whole_after),
        "exit_code": read_json(path / "exit-code.json"),
    }


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    leg_dirs = sorted(path for path in root.glob("legs/*") if path.is_dir())
    legs = [read_leg(path) for path in leg_dirs]
    by_name = {leg["leg"]: leg for leg in legs}
    checks = []

    def check(name: str, condition: bool, detail: str) -> None:
        checks.append({"check": name, "pass": bool(condition), "detail": detail})

    read_off = next((leg for leg in legs
                     if leg["kind"] == "read" and leg["flag"] == "off"), None)
    read_on = next((leg for leg in legs
                    if leg["kind"] == "read" and leg["flag"] == "on"), None)
    write_on = next((leg for leg in legs
                     if leg["kind"] == "write" and leg["flag"] == "on"), None)

    if read_off:
        check("flag-off publishes no deferred key in window",
              not read_off["deferred_key_in_window"],
              "window-after has deferred_session_close=%s" % read_off["deferred_key_in_window"])
        check("flag-off publishes no deferred key after leg",
              not read_off["deferred_key_after_leg"],
              "metrics-after has deferred_session_close=%s" % read_off["deferred_key_after_leg"])
        check("flag-off read leg has no failures",
              read_off["summary"].get("failures") == 0,
              "failures=%r" % read_off["summary"].get("failures"))
    if read_on:
        window = read_on["window"]
        deferred = window.get("deferred_session_close") or {}
        enqueued = deferred.get("enqueued_total", 0)
        ratio = window.get("eligibility_ratio")
        check("flag-on read leg enqueues closes", enqueued > 0,
              "enqueued_total=%d" % enqueued)
        check("flag-on read eligibility ratio >= 0.90",
              ratio is not None and ratio >= THRESHOLD,
              "ratio=%r over close_actions=%r" % (ratio, window.get("close_actions")))
        check("flag-on read leg has no failures",
              read_on["summary"].get("failures") == 0,
              "failures=%r" % read_on["summary"].get("failures"))
        check("flag-on read window has no error counters",
              not window.get("errors_total"),
              "errors_total=%r" % (window.get("errors_total"),))
        check("flag-on read window has no refusals",
              not window.get("rejection_reasons_total"),
              "rejection_reasons_total=%r" % (window.get("rejection_reasons_total"),))
        check("flag-on read ends with zero unresolved closes",
              int(deferred.get("unresolved_total") or 0) == 0,
              "unresolved_total=%r" % deferred.get("unresolved_total"))
    if write_on:
        window = write_on["window"]
        deferred = window.get("deferred_session_close") or {}
        check("flag-on write enqueues nothing",
              int(deferred.get("enqueued_total") or 0) == 0,
              "enqueued_total=%r over close_actions=%r"
              % (deferred.get("enqueued_total"), window.get("close_actions")))
        check("flag-on write leg has no failures",
              write_on["summary"].get("failures") == 0,
              "failures=%r" % write_on["summary"].get("failures"))
        check("flag-on write window has no error counters",
              not window.get("errors_total"),
              "errors_total=%r" % (window.get("errors_total"),))
    for leg in legs:
        window = leg["window"]
        check("%s ends with zero session seats" % leg["leg"],
              int(window.get("session_seats_after") or 0) == 0,
              "session_seats_after=%r" % window.get("session_seats_after"))
        check("%s ends with zero active permits" % leg["leg"],
              int(window.get("active_permits_after") or 0) == 0,
              "active_permits_after=%r" % window.get("active_permits_after"))

    latency = {}
    if read_off and read_on:
        left = read_off["window"].get("close_latency_mean_us")
        right = read_on["window"].get("close_latency_mean_us")
        if left and right:
            latency = {"off_us": left, "on_us": right,
                       "ratio_on_over_off": round(right / left, 4),
                       "change_percent": round((right / left - 1) * 100, 2)}

    document = {"root": str(root), "legs": legs, "checks": checks,
                "checks_passed": sum(1 for item in checks if item["pass"]),
                "checks_total": len(checks), "close_latency": latency}
    (root / "analysis.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("| leg | flag | kind | requests | failures | close actions | close mean µs "
          "| enqueued | inline | failed | unresolved | pending_max | eligibility | seats | permits |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for leg in legs:
        window = leg["window"]
        deferred = window.get("deferred_session_close") or {}
        print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |"
              % (leg["leg"], leg["flag"], leg["kind"],
                 leg["summary"].get("requests"), leg["summary"].get("failures"),
                 window.get("close_actions"), window.get("close_latency_mean_us"),
                 deferred.get("enqueued_total", "—"),
                 deferred.get("inline_total", "—"),
                 deferred.get("failed_total", "—"),
                 deferred.get("unresolved_total", "—"),
                 deferred.get("pending_max", "—"),
                 window.get("eligibility_ratio", "—"),
                 window.get("session_seats_after"),
                 window.get("active_permits_after")))
    print()
    for item in checks:
        print("%s  %s — %s" % ("PASS" if item["pass"] else "FAIL",
                               item["check"], item["detail"]))
    if latency:
        print("\nclose mean: off=%sus on=%sus ratio=%s (%s%%)"
              % (latency["off_us"], latency["on_us"],
                 latency["ratio_on_over_off"], latency["change_percent"]))
    print("\n%d/%d checks passed" % (document["checks_passed"],
                                     document["checks_total"]))
    return 0 if document["checks_passed"] == document["checks_total"] else 1


if __name__ == "__main__":
    sys.exit(main())
