#!/usr/bin/env python3
"""T1 burst: four concurrent Hrana pipeline [execute SELECT 1, close] requests."""
import argparse, json, sys, threading, time

REPO = "/Users/kk/code/peri-loom"
sys.path.insert(0, REPO + "/scripts/perf-reliability")
from issue28.hrana import (  # noqa: E402
    FatalTransportError, HranaClient, HranaError, Target, parse_pipeline_json
)

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("connection_file")
    parser.add_argument("out")
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    target = Target.from_file(args.connection_file)
    clients = [HranaClient(target, timeout=60) for _ in range(args.concurrency)]
    barrier = threading.Barrier(args.concurrency + 1)
    results = []
    lock = threading.Lock()

    def worker(index):
        client = clients[index]
        barrier.wait()
        started = time.perf_counter_ns()
        record = {"worker": index, "duration_ns": 0, "status": 0, "ok": False, "error": ""}
        try:
            raw = client.pipeline_json(REQUESTS)
        except (FatalTransportError, HranaError, OSError) as error:
            record["duration_ns"] = time.perf_counter_ns() - started
            record["error"] = type(error).__name__
            with lock:
                results.append(record)
            return
        record["duration_ns"] = time.perf_counter_ns() - started
        record["status"] = raw.status
        if raw.status != 200:
            record["error"] = "http_%d" % raw.status
        else:
            try:
                reply = parse_pipeline_json(raw.body)
                record["ok"] = all(item.ok for item in reply.results)
                if not record["ok"]:
                    record["error"] = first_error(reply)
            except HranaError:
                record["error"] = "decode"
        with lock:
            results.append(record)

    def first_error(reply):
        for item in reply.results:
            if not item.ok:
                return item.error_code or "rejected"
        return "rejected"

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(args.concurrency)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=60)
    for client in clients:
        try:
            client.close()
        except Exception:
            pass
    results.sort(key=lambda item: item["worker"])
    ok = len(results) == args.concurrency and all(item["ok"] for item in results)
    document = {"concurrency": args.concurrency, "ok": ok, "results": results}
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"ok": ok, "results": results}, ensure_ascii=False))
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
