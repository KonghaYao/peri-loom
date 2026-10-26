#!/usr/bin/env python3
"""Independent recomputation entry point for Issue 11 attribution evidence."""
import argparse
import json
from pathlib import Path
import sys

PERF_ROOT = Path(__file__).resolve().parents[2]
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11.attribution import aggregate  # noqa: E402


def _operation_request_ids(path):
    referenced = set()
    seen = set()
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as failure:
        raise aggregate.AttributionError("operations_unreadable: "
                                         + type(failure).__name__) from None
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            raise aggregate.AttributionError("operations_json: "
                                             + str(line_number)) from None
        if not isinstance(record, dict) or record.get("record_type") != "operation":
            raise aggregate.AttributionError("operations_record_type: "
                                             + str(line_number))
        request_ids = record.get("http_request_ids")
        if not isinstance(request_ids, list) \
                or any(not isinstance(item, str) for item in request_ids):
            raise aggregate.AttributionError("operations_request_ids: "
                                             + str(line_number))
        if record.get("op_id") in seen:
            raise aggregate.AttributionError("operations_duplicate_op_id: "
                                             + str(record.get("op_id")))
        seen.add(record.get("op_id"))
        referenced.update(request_ids)
    return referenced


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description="Recompute Issue 11 attribution from raw JSONL only.")
    parser.add_argument("--request-attribution", required=True,
                        help="request-attribution.jsonl path")
    parser.add_argument("--operations", default=None,
                        help="optional operations.jsonl path for request-id join checks")
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_arguments(argv)
    try:
        documents = aggregate.load_requests(arguments.request_attribution)
        result = aggregate.aggregate(documents)
        if arguments.operations:
            referenced = _operation_request_ids(arguments.operations)
            operation_requests = {document["request_id"] for document in documents
                                  if document["phase"] not in {"setup", "cleanup"}}
            if referenced != operation_requests:
                raise aggregate.AttributionError("operation_request_set_mismatch")
        result["source"] = {
            "request_attribution": str(arguments.request_attribution),
            "operations": str(arguments.operations) if arguments.operations else None,
            "summary_used": False,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except aggregate.AttributionError as failure:
        print("attribution recompute error: " + str(failure), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
