#!/usr/bin/env python3
"""Generate deterministic test SQL and historical-smoke input records; no DB calls."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def historical_fixture(payload_bytes=256):
    if payload_bytes < 1:
        raise ValueError('payload_bytes >= 1 required')
    yield 'CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT);\n'
    values = [f"({i},'{('x' * payload_bytes)}')" for i in range(64)]
    yield 'INSERT INTO bench VALUES ' + ','.join(values) + ';\n'


def fixture(rows=64, payload_bytes=256):
    if rows < 64 or payload_bytes < 1:
        raise ValueError('rows >= 64 and payload_bytes >= 1 are required')
    yield (ROOT / 'sql/00-schema.sql').read_text()
    yield 'BEGIN;\n'
    # Small batches keep setup within the existing protocol's payload limits.
    for table, size in [('bench', 64), ('kv', rows)]:
        for start in range(0, size, 64):
            values = []
            for i in range(start, min(size, start + 64)):
                payload = 'x' * payload_bytes
                values.append(f"({i},'{payload}')" if table == 'bench' else
                              f"({i},{i % 100},0,{i},'{payload}')")
            yield f'INSERT INTO {table} VALUES ' + ','.join(values) + ';\n'
    yield 'INSERT INTO accounts VALUES (1,1000000000),(2,1000000000);\n'
    yield 'COMMIT;\n'


def smoke_records(mode, operations=100, rows=64, payload_bytes=256, lane=0):
    # This schedule matches the current runner: mixed writes on odd indices.
    expected = ['x' * payload_bytes for _ in range(rows)]
    for index in range(operations):
        row = index % rows
        write = mode == 'write' or (mode == 'mixed' and index % 2 == 1)
        if write:
            payload = ('y' if (index // rows) % 2 == 0 else 'z') * payload_bytes
            sql = f"UPDATE bench SET payload='{payload}' WHERE id={row}"
            expected[row] = payload
        else:
            sql = f'SELECT payload FROM bench WHERE id={row}'
        yield {'case': 'S0', 'lane': lane, 'index': index,
               'op': 'exec' if write else 'query', 'sql': sql,
               'expected_query_result': None if write else [[expected[row]]],
               'expected_payload_after_ack': expected[row], 'row_id': row}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=64)
    parser.add_argument('--payload-bytes', type=int, default=256)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--mode', choices=['fixture', 'read', 'write', 'mixed'], default='fixture')
    parser.add_argument('--profile', choices=['suite','historical'], default='suite')
    parser.add_argument('--operations', type=int, default=100)
    args = parser.parse_args()
    if args.rows < 64 or args.payload_bytes < 1 or args.operations < 1:
        parser.error('rows >= 64, payload-bytes >= 1, operations >= 1 required')
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open('w') as out:
        if args.mode == 'fixture':
            if args.profile == 'historical':
                if args.rows != 64:
                    parser.error('historical profile is fixed at 64 rows')
                out.writelines(historical_fixture(args.payload_bytes))
            else:
                out.writelines(fixture(args.rows, args.payload_bytes))
        else:
            if args.rows != 64:
                parser.error('S0 is fixed at 64 rows; use fixture mode for scale data')
            for item in smoke_records(args.mode, args.operations, 64, args.payload_bytes):
                out.write(json.dumps(item, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    main()
