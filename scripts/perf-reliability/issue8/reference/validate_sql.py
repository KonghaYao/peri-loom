#!/usr/bin/env python3
"""Validate fixture/query semantics with Python SQLite, not peri-loom performance."""
import json
import sqlite3
from pathlib import Path
from generate_fixture import fixture, historical_fixture, smoke_records

ROOT = Path(__file__).resolve().parent


def statements(name):
    text = (ROOT / 'sql' / name).read_text()
    text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    return [s.strip() for s in text.split(';') if s.strip()]


def main():
    checks = []
    historical = sqlite3.connect(':memory:', isolation_level=None)
    historical.executescript(''.join(historical_fixture()))
    assert historical.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [('bench',)]
    assert historical.execute('SELECT COUNT(*),SUM(LENGTH(payload)) FROM bench').fetchone() == (64,16384)
    historical.close()
    checks.append('historical fixture has only the original bench table')
    db = sqlite3.connect(':memory:', isolation_level=None)
    db.executescript(''.join(fixture(100000)))
    assert db.execute('SELECT COUNT(*), SUM(value), SUM(LENGTH(payload)) FROM kv').fetchone() == (100000,4999950000,25600000)
    checks.append('100000-row deterministic fixture')
    for mode in ['read','write','mixed']:
        db.execute('UPDATE bench SET payload=?', ('x' * 256,))
        expected = ['x' * 256 for _ in range(64)]
        for record in smoke_records(mode, operations=1024):
            cursor = db.execute(record['sql'])
            if record['op'] == 'query':
                assert [list(row) for row in cursor.fetchall()] == record['expected_query_result']
            else:
                assert cursor.rowcount == 1
                expected[record['row_id']] = record['expected_payload_after_ack']
        assert db.execute('SELECT id,payload FROM bench ORDER BY id').fetchall() == list(enumerate(expected))
        checks.append(mode + ' historical schedule and complete oracle')
    params = {'id':17, 'delta':7, 'payload':'y'*256}
    db.execute(statements('12-update.sql')[0], params)
    assert db.execute(statements('14-point-read.sql')[0], {'id':17}).fetchone() == (17,17,1,24,'y'*256)
    checks.append('single-row mutation and point read')
    for i in range(100):
        db.execute(statements('13-insert.sql')[0], {'id':i, 'tenant_id':i%100, 'revision':1, 'payload':'z'*256})
    assert db.execute('SELECT COUNT(*) FROM events').fetchone() == (100,)
    checks.append('append insertion')
    actual = db.execute(statements('15-range-read.sql')[0], {'tenant_id':17,'after_id':17}).fetchall()
    assert [row[0] for row in actual] == list(range(117,10118,100))[:100]
    assert all(row[1:]==(0,row[0],'x'*256) for row in actual)
    checks.append('indexed range read and full row oracle')
    for batch in [16,64]:
        db.execute('BEGIN IMMEDIATE')
        for i in range(batch):
            db.execute(statements('12-update.sql')[0], {'id':i,'delta':1,'payload':'z'*256})
        db.execute('COMMIT')
        checks.append(f'explicit transaction {batch} updates')
    before=db.execute(statements('14-point-read.sql')[0], {'id':0}).fetchone()
    db.execute('BEGIN IMMEDIATE')
    db.execute(statements('12-update.sql')[0], {'id':0,'delta':999,'payload':'r'*256})
    db.execute('ROLLBACK')
    assert db.execute(statements('14-point-read.sql')[0], {'id':0}).fetchone()==before
    checks.append('rollback preserves data')
    transfer={'amount':9,'source_id':1,'target_id':2,'op_id':'run-a:lane-0:op-1'}
    for sql in statements('20-transfer.sql'):
        db.execute(sql,transfer)
    assert db.execute('SELECT id,balance FROM accounts ORDER BY id').fetchall()==[(1,999999991),(2,1000000009)]
    assert db.execute('SELECT * FROM transfer_receipts').fetchall()==[('run-a:lane-0:op-1',1,2,9)]
    checks.append('atomic transfer and receipt content')
    try:
        for sql in statements('20-transfer.sql'):
            db.execute(sql,transfer)
        raise AssertionError('duplicate receipt unexpectedly accepted')
    except sqlite3.IntegrityError:
        db.execute('ROLLBACK')
    assert db.execute('SELECT id,balance FROM accounts ORDER BY id').fetchall()==[(1,999999991),(2,1000000009)]
    checks.append('duplicate operation does not apply twice after rollback')
    assert db.execute(statements('16-scan.sql')[0]).fetchone()==(100000,81,4999950087,25600000)
    checks.append('aggregate consistency cross-check')
    assert len(statements('30-oracle.sql'))==5
    for sql in statements('30-oracle.sql'):
        db.execute(sql).fetchall()
    checks.append('all full-content oracle queries execute')
    result={'status':'PASS_SQL_REFERENCE_ONLY','sqlite_version':sqlite3.sqlite_version,
            'checks_passed':len(checks),'checks':checks,
            'peri_loom_execution':'NOT_RUN','durability_fault_tests':'NOT_RUN',
            'performance_measurement':'NOT_RUN'}
    (ROOT / 'sql-validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
