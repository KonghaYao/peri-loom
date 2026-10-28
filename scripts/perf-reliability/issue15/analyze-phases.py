#!/usr/bin/env python3
"""Read-only phase/attempt accounting for the frozen schema-2 H4/H6 ABBA.

This is not the Goal auditor and never assigns a performance PASS. It accepts
completed and interrupted campaigns, but rejects contradictory or malformed raw
evidence. Missing failed-request timing is reported as unknown, never as zero.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import re
import sys


PHASES = ('setup', 'prelude', 'prelude_cleanup', 'warmup', 'timed', 'cleanup')
OP_PHASES = ('prelude', 'warmup', 'timed')
ACTIONS = ('write', 'read', 'close')
ORDER = ('A1', 'B1', 'B2', 'A2')
IDENTITY = ('sdk_sha256', 'sdk_lock_sha256', 'config_sha256',
            'client_sha256', 'observer_sha256', 'node')
RAW_FILES = ('run.json', 'summary.json', 'operations.jsonl', 'requests.jsonl')
EPSILON = 1e-6


class InvalidEvidence(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def field(record, key):
    require(isinstance(record, dict) and key in record, f'missing field: {key}')
    return record[key]


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f'invalid integer: {name}')
    return value


def number(value, name):
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
            f'invalid nonnegative time: {name}')
    return value


def text(value, name):
    require(isinstance(value, str) and bool(value), f'invalid string: {name}')
    return value


def hexadecimal(value, width, name):
    require(isinstance(value, str) and re.fullmatch(f'[0-9a-f]{{{width}}}', value),
            f'invalid hash: {name}')
    return value


def same_number(actual, expected, name):
    number(actual, name)
    require(abs(actual - expected) <= EPSILON, f'inconsistent duration/count: {name}')


def decode(raw, name):
    def reject_constant(value):
        raise InvalidEvidence(f'non-finite JSON number in {name}: {value}')
    def unique_keys(items):
        result = {}
        for key, value in items:
            require(key not in result, f'duplicate JSON key in {name}: {key}')
            result[key] = value
        return result
    try:
        return json.loads(raw, parse_constant=reject_constant, object_pairs_hook=unique_keys)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise InvalidEvidence(f'invalid JSON in {name}') from error


class Inputs:
    def __init__(self, root):
        # Resolve the root once so containment compares against the same canonical
        # path as path.resolve(); on macOS /var/folders is a symlink to /private/var.
        self.root = Path(root).resolve()
        self.hashes = {}

    def read_bytes(self, relative):
        relative = Path(relative)
        require(not relative.is_absolute() and '..' not in relative.parts, 'unsafe input path')
        path = self.root / relative
        require(path.is_file() and not path.is_symlink(), f'missing/nonregular input: {relative}')
        require(path.resolve().is_relative_to(self.root), f'input escapes root: {relative}')
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        key = relative.as_posix()
        require(key not in self.hashes or self.hashes[key] == digest, f'input changed: {key}')
        self.hashes[key] = digest
        return data

    def json(self, relative):
        return decode(self.read_bytes(relative), str(relative))

    def rows(self, relative):
        data = self.read_bytes(relative)
        require(not data or data.endswith(b'\n'), f'truncated JSONL: {relative}')
        rows = []
        for line_number, line in enumerate(data.splitlines(), 1):
            require(bool(line.strip()), f'blank JSONL record: {relative}:{line_number}')
            record = decode(line, f'{relative}:{line_number}')
            require(isinstance(record, dict), f'non-object JSONL record: {relative}:{line_number}')
            rows.append(record)
        return rows

    def recheck(self):
        for relative, expected in list(self.hashes.items()):
            require(hashlib.sha256(self.read_bytes(relative)).hexdigest() == expected,
                    f'input changed during analysis: {relative}')


def statistics(values, missing=0):
    values = sorted(values)
    n = len(values)
    return {'n': n, 'missing': missing,
            **{f'p{p}_ms': values[math.ceil(n * p / 100) - 1] if n else None
               for p in (50, 95, 99)},
            'max_ms': values[-1] if n else None,
            'over_100ms': sum(value > 100 for value in values),
            'over_100ms_rate': sum(value > 100 for value in values) / n if n else None,
            'excess_over_100ms_sum': sum(max(value - 100, 0) for value in values),
            'excess_over_100ms_per_observed':
                sum(max(value - 100, 0) for value in values) / n if n else None}


def summarize(operations):
    def metric(key, selected):
        available = [row[key] for row in selected if row.get(key) is not None]
        return statistics(available, len(selected) - len(available))
    successful = [op for op in operations if op['outcome'] == 'success']
    result = {
        'issued_operations': len(operations), 'successful_operations': len(successful),
        'business_success_ms': metric('duration_ms', successful),
        'attempt_elapsed_ms': metric('duration_ms', operations),
        'connect_object_construction_ms': metric('connect_ms', operations),
        'verification_wait_ms': metric('verification_wait_ms', operations),
        'evidence_flush_ms': metric('evidence_flush_ms', operations),
        'client_gaps_ms': metric('client_gaps_ms', operations),
        'actions': {},
    }
    for name in ACTIONS:
        actions = [action for op in operations for action in op['actions'] if action['action'] == name]
        confirmed = [action for action in actions if action['confirmed']]
        result['actions'][name] = {
            'attempts': len(actions), 'confirmed': len(confirmed),
            'sdk_confirmed_ms': metric('duration_ms', confirmed),
            'sdk_attempt_elapsed_ms': metric('duration_ms', actions),
            'issued_to_return_confirmed_ms': metric('issued_to_return_ms', confirmed),
            **{key: metric(key, actions) for key in
               ('before_dispatch_ms', 'dispatch_to_headers_ms', 'headers_to_sdk_ms')},
        }
    slow = [op for op in operations if op['duration_ms'] > 100]
    result['slow_largest_action'] = dict(Counter(op['largest_action'] for op in slow))
    result['slow_largest_observed_component'] = dict(Counter(op['largest_component'] for op in slow))
    result['slow_with_incomplete_components'] = sum(not op['components_complete'] for op in slow)
    return result


def validate_request(request, run_id, batons):
    require(integer(field(request, 'schema'), 'request schema') == 3, 'unsupported request schema')
    request_id = text(field(request, 'request_id'), 'request_id')
    require(field(request, 'run_id') == run_id, f'cross-run request: {request_id}')
    phase = field(request, 'phase')
    require(phase in PHASES, f'unsupported request phase: {phase}')
    integer(field(request, 'sample_index'), 'request sample_index')
    action = field(request, 'action')
    require(action in ACTIONS, 'unsupported request action')
    session = text(field(request, 'session_id'), 'session_id')
    require(field(request, 'method') == 'POST', 'unexpected request method')
    suffix = '/v3/pipeline' if action == 'close' else '/v3/cursor'
    require(text(field(request, 'path'), 'path').endswith(suffix), 'unexpected protocol path')
    sql = field(request, 'sql')
    require(isinstance(sql, list), 'missing/invalid SQL request list')
    if action == 'close':
        require(sql == [], 'close request carries SQL')
    else:
        require(len(sql) == 2 and isinstance(sql[0], str) and bool(sql[0])
                and sql[1] == 'SELECT 1', 'SQL/probe structure mismatch')
        if phase in OP_PHASES:
            expected_sql = ('INSERT INTO perf_events(id,value,payload) VALUES(?,?,?)'
                            if action == 'write' else 'SELECT value,payload FROM perf_events WHERE id=?')
            require(sql[0] == expected_sql, 'operation SQL does not match fixed workload')
    hexadecimal(field(request, 'request_body_sha256'), 64, 'request body')
    require('baton_in_sha256' in request and request['baton_in_sha256'] == batons.get(session),
            f'baton identity mismatch: {request_id}')
    outcome = field(request, 'outcome')
    require(outcome in ('success', 'outcome_unknown'), 'unknown request outcome vocabulary')
    number(field(request, 'started_ms'), 'request started')
    for key in ('dispatch_ms', 'headers_ms', 'clone_body_observed_ms', 'completed_ms',
                'clone_decode_completed_ms'):
        if key in request:
            number(request[key], key)
    if 'dispatch_ms' in request:
        require(request['started_ms'] <= request['dispatch_ms'], 'request starts after dispatch')
    if 'headers_ms' in request:
        require('dispatch_ms' in request and request['dispatch_ms'] <= request['headers_ms'],
                'headers without preceding dispatch')
    if outcome == 'success':
        require(field(request, 'status') == 200, 'successful ACK without HTTP 200')
        number(field(request, 'dispatch_ms'), 'dispatch')
        number(field(request, 'headers_ms'), 'headers')
        number(field(request, 'clone_body_observed_ms'), 'clone consumed')
        require(request['headers_ms'] <= request['clone_body_observed_ms'], 'clone before headers')
        if action == 'close':
            require(field(request, 'close_acknowledged') is True
                    and field(request, 'close_baton_explicit_null') is True
                    and integer(field(request, 'close_result_count'), 'close result count') == 1
                    and integer(field(request, 'step_count'), 'close step count') == 1
                    and field(request, 'baton_out_sha256') is None, 'invalid close ACK')
        else:
            steps = field(request, 'cursor_completed_steps')
            require(isinstance(steps, list) and all(type(step) is int for step in steps)
                    and steps == [0, 1]
                    and field(request, 'cursor_probe_complete') is True
                    and integer(field(request, 'step_count'), 'cursor step count') == 2
                    and field(request, 'close_acknowledged') is False, 'incomplete cursor ACK/probe')
            hexadecimal(field(request, 'baton_out_sha256'), 64, 'output baton')
        batons[session] = request['baton_out_sha256']
    elif request.get('close_acknowledged') is True:
        raise InvalidEvidence('unknown request also claims close ACK')
    return request_id


def validate_operation(operation, requests, run, used, sequence):
    require(integer(field(operation, 'schema'), 'operation schema') == 1, 'unsupported operation schema')
    op_id = text(field(operation, 'op_id'), 'op_id')
    require(field(operation, 'run_id') == run['run_id'], 'cross-run operation')
    phase = field(operation, 'phase')
    require(phase in OP_PHASES, 'unexpected operation phase')
    index = integer(field(operation, 'sample_index'), 'sample_index')
    scenario = 'H4' if phase == 'prelude' else run['scenario']
    require(field(operation, 'scenario') == scenario, 'operation scenario mismatch')
    require(integer(field(operation, 'retry_count'), 'retry_count') == 0, 'SQL replay recorded')
    outcome = field(operation, 'outcome')
    require(outcome in ('success', 'failed_or_unknown'), 'unknown operation outcome vocabulary')
    issued = number(field(operation, 'issued_ms'), 'issued')
    completed = number(field(operation, 'completed_ms'), 'completed')
    require(issued <= completed, 'operation completed before issued')
    duration = number(field(operation, 'duration_ms'), 'duration')
    same_number(duration, completed - issued, 'operation duration')
    for key in ('verification_wait_ms', 'evidence_flush_ms'):
        number(field(operation, key), key)
    if phase != 'prelude':
        number(field(operation, 'connect_ms'), 'connect_ms')
    actions = field(operation, 'actions')
    require(isinstance(actions, list), 'actions must be a list')
    for holder in [operation, *actions]:
        ids = field(holder, 'request_ids')
        require(isinstance(ids, list) and all(isinstance(rid, str) and bool(rid) for rid in ids),
                'operation/action request IDs must be lists of nonempty strings')
    expected = ['write', 'read'] + (['close'] if scenario == 'H6' else [])
    names = [field(action, 'action') for action in actions]
    require(names == expected[:len(names)] and len(names) <= len(expected), 'action order or replay')
    require(outcome != 'success' or names == expected, 'successful operation lacks actions')
    require(field(operation, 'request_ids') == [rid for action in actions
            for rid in field(action, 'request_ids')], 'operation/request ID list mismatch')
    normalized = []
    prior_end = issued
    sessions = set()
    for action in actions:
        require(len(field(action, 'request_ids')) == 1, 'missing or replayed action request')
        request_id = action['request_ids'][0]
        require(request_id in requests and request_id not in used, 'missing/reused request ID')
        used.add(request_id)
        request = requests[request_id]
        require(field(request, 'op_id') == op_id and request['phase'] == phase
                and request['sample_index'] == index and request['action'] == action['action'],
                'action/request identity mismatch')
        sessions.add(request['session_id'])
        start = number(field(action, 'started_ms'), 'action started')
        end = number(field(action, 'sdk_resolved_ms'), 'SDK returned')
        require(prior_end <= start <= end <= completed, 'overlapping or invalid action times')
        require(field(action, 'completed_ms') == end, 'SDK/action end disagreement')
        same_number(field(action, 'duration_ms'), end - start, 'action duration')
        verified = number(field(action, 'verification_completed_ms'), 'verification end')
        require(verified >= end, 'action verified before SDK return')
        for evidence_time in ('clone_body_observed_ms', 'clone_decode_completed_ms'):
            if evidence_time in request:
                require(request[evidence_time] <= verified, 'action verified before response evidence consumed')
        action_outcome = field(action, 'outcome')
        require(action_outcome in ('success', 'outcome_unknown'), 'invalid action outcome')
        confirmed = action_outcome == 'success' and request['outcome'] == 'success'
        require(action_outcome != 'success' or confirmed, 'action success lacks confirmed request')
        require(outcome != 'success' or confirmed, 'successful operation has unknown action')
        dispatch, headers = request.get('dispatch_ms'), request.get('headers_ms')
        require(start <= request['started_ms'] <= end, 'request outside action')
        if dispatch is not None:
            require(start <= dispatch <= end, 'dispatch outside action')
        if headers is not None:
            require(headers <= end, 'headers after SDK returned')
        normalized.append({
            'action': action['action'], 'request_id': request_id, 'confirmed': confirmed,
            'duration_ms': end - start, 'issued_to_return_ms': end - issued,
            'before_dispatch_ms': dispatch - start if dispatch is not None else None,
            'dispatch_to_headers_ms': headers - dispatch if headers is not None else None,
            'headers_to_sdk_ms': end - headers if headers is not None else None,
        })
        prior_end = end
    require(len(sessions) <= 1, 'operation crossed SDK sessions')
    if actions:
        require(completed == actions[-1]['sdk_resolved_ms'], 'operation end not final SDK return')
    write = next((action for action in normalized if action['action'] == 'write'), None)
    read = next((action for action in normalized if action['action'] == 'read'), None)
    effect = operation.get('write_effect')
    if effect is not None:
        require(write is not None and effect.get('request_id') == write['request_id'], 'write effect ID mismatch')
        require(field(effect, 'kind') == 'insert' and field(effect, 'table') == 'perf_events', 'wrong write effect')
        require(field(effect, 'id') == 10_000_000 + sequence
                and field(effect, 'value') == effect['id'], 'write effect sequence mismatch')
        prefix = 'i' + str(effect['id']).zfill(8) + ':'
        expected_hash = hashlib.sha256((prefix + '0' * (256 - len(prefix))).encode()).hexdigest()
        require(field(effect, 'payload_sha256') == expected_hash, 'write payload hash mismatch')
    readback = operation.get('readback_effect')
    if readback is not None:
        require(effect is not None and read is not None, 'readback without write/read action')
        require(readback == {'value': effect['value'], 'payload_sha256': effect['payload_sha256']},
                'readback value/payload mismatch')
    exact_readback = readback is not None and read is not None and read['confirmed']
    require(outcome != 'success' or (effect is not None and exact_readback), 'successful operation lacks exact readback')
    gaps = duration - sum(action['duration_ms'] for action in normalized)
    require(gaps >= -EPSILON, 'action sum exceeds business elapsed')
    components = {'client_gaps': max(gaps, 0)}
    complete = True
    for action in normalized:
        for key in ('before_dispatch_ms', 'dispatch_to_headers_ms', 'headers_to_sdk_ms'):
            value = action[key]
            if value is None:
                complete = False
            else:
                components[action['action'] + ':' + key] = value
    return {
        'op_id': op_id, 'phase': phase, 'sample_index': index, 'outcome': outcome,
        'issued_ms': issued, 'completed_ms': completed, 'duration_ms': duration,
        'actions': normalized, 'connect_ms': operation.get('connect_ms'),
        'client_gaps_ms': max(gaps, 0), 'verification_wait_ms': operation['verification_wait_ms'],
        'evidence_flush_ms': operation['evidence_flush_ms'], 'exact_readback': exact_readback,
        'write_effect_present': effect is not None,
        'write_confirmed_but_business_failed': bool(write and write['confirmed'] and outcome != 'success'),
        'components_complete': complete,
        'largest_action': max(normalized, key=lambda item: item['duration_ms'])['action'] if normalized else None,
        'largest_component': max(components, key=components.get),
        'last_verification_ms': max((action['verification_completed_ms'] for action in actions), default=None),
        'session_id': next(iter(sessions), None),
    }


def planned_counts(settings, scenario):
    widths = 2 if scenario == 'H4' else 3
    return {
        'setup': (None, 6),
        'prelude': (settings['prelude_writes'], settings['prelude_writes'] * 2),
        'prelude_cleanup': (None, int(settings['prelude_writes'] > 0)),
        'warmup': (settings['warmup'], settings['warmup'] * widths),
        'timed': (settings['samples'], settings['samples'] * widths),
        'cleanup': (None, int(scenario == 'H4')),
    }


def ledger(operations, requests, plans):
    mapped = {action['request_id']: action for op in operations for action in op['actions']}
    result = {}
    for phase in PHASES:
        ops = [op for op in operations if op['phase'] == phase]
        reqs = [request for request in requests if request['phase'] == phase]
        def confirmed(request):
            return request['outcome'] == 'success' and (
                request['request_id'] not in mapped or mapped[request['request_id']]['confirmed'])
        count_ops = len(ops) if phase in OP_PHASES else None
        result[phase] = {
            'planned_operations': plans[phase][0], 'planned_http_requests': plans[phase][1],
            'operation_records_supported': phase in OP_PHASES,
            'issued_operations': count_ops, 'http_requests_observed': len(reqs),
            'http_acks': sum(request['outcome'] == 'success' for request in reqs),
            'write_acks': sum(request['action'] == 'write' and confirmed(request) for request in reqs),
            'close_acks': sum(request['action'] == 'close' and confirmed(request) for request in reqs),
            'exact_readbacks': sum(op['exact_readback'] for op in ops) if phase in OP_PHASES else None,
            'successful_operations': sum(op['outcome'] == 'success' for op in ops) if phase in OP_PHASES else None,
            'failed_or_unknown_operations': sum(op['outcome'] != 'success' for op in ops) if phase in OP_PHASES else None,
            'unknown_http_outcomes': sum(request['outcome'] == 'outcome_unknown' for request in reqs),
            'write_confirmed_but_business_failed': sum(op['write_confirmed_but_business_failed'] for op in ops),
            'http_429': sum(request.get('status') == 429 for request in reqs),
        }
    return result


def analyze_client(inputs, relative, settings, label, scenario, all_ids):
    manifest = inputs.json(relative / 'SHA256SUMS.json')
    for name in RAW_FILES:
        data = inputs.read_bytes(relative / name)
        require(field(manifest, name) == hashlib.sha256(data).hexdigest(), f'raw SHA mismatch: {relative}/{name}')
    run = inputs.json(relative / 'run.json')
    summary = inputs.json(relative / 'summary.json')
    operations = inputs.rows(relative / 'operations.jsonl')
    rows = inputs.rows(relative / 'requests.jsonl')
    require(integer(field(run, 'schema'), 'run schema') == 2, 'only timing schema 2 is supported')
    require(field(run, 'sdk') == '@tursodatabase/serverless@1.4.1', 'unsupported SDK')
    require(field(run, 'scenario') == scenario and field(run, 'arm') == label[0], 'run/path identity mismatch')
    require(field(run, 'fixture_rows') == 1000 and field(run, 'payload_bytes') == 256, 'unsupported fixture')
    for run_name, setting_name in [('samples_requested', 'samples'), ('warmup_requested', 'warmup'),
                                   ('prelude_writes', 'prelude_writes'), ('period_ms', 'period_ms')]:
        require(integer(field(run, run_name), run_name) == settings[setting_name], f'run/settings mismatch: {run_name}')
    run_id = text(field(run, 'run_id'), 'run_id')
    require(run_id not in all_ids['runs'], 'duplicate run ID')
    all_ids['runs'].add(run_id)
    identity = {key: text(field(run, key), key) for key in IDENTITY}
    for key in IDENTITY[:-1]:
        hexadecimal(identity[key], 64, key)
    require(identity['client_sha256'] == settings['helper_sha256']['issue15-sdk-client.mjs']
            and identity['observer_sha256'] == settings['helper_sha256']['goal-observe.mjs'], 'client/observer identity differs from frozen helpers')
    requests, batons = {}, {}
    last_phase = -1
    previous_request_start = None
    for request in rows:
        request_id = validate_request(request, run_id, batons)
        require(request_id not in all_ids['requests'], 'duplicate request ID')
        all_ids['requests'].add(request_id)
        requests[request_id] = request
        phase_position = PHASES.index(request['phase'])
        require(phase_position >= last_phase, 'request phase moved backward')
        last_phase = phase_position
        require(previous_request_start is None or request['started_ms'] >= previous_request_start,
                'request issuance records are out of order')
        previous_request_start = request['started_ms']
    normalized, used = [], set()
    phase_indices = Counter()
    last_phase = -1
    for sequence, operation in enumerate(operations):
        op = validate_operation(operation, requests, run, used, sequence)
        require(op['op_id'] not in all_ids['operations'], 'duplicate operation ID')
        all_ids['operations'].add(op['op_id'])
        require(op['sample_index'] == phase_indices[op['phase']], 'missing/repeated sample index')
        phase_indices[op['phase']] += 1
        position = OP_PHASES.index(op['phase'])
        require(position >= last_phase, 'operation phase moved backward')
        last_phase = position
        if normalized:
            previous = normalized[-1]
            require(previous['outcome'] == 'success', 'operation continued after failure')
            require(op['issued_ms'] - previous['issued_ms'] >= settings['period_ms'] - EPSILON,
                    'minimum start interval violated')
            require(op['issued_ms'] >= previous['completed_ms'], 'operations overlap')
            require(previous['last_verification_ms'] is not None
                    and op['issued_ms'] >= previous['last_verification_ms'], 'operation preceded verification')
            require(op['issued_ms'] + EPSILON >= previous['last_verification_ms'] + previous['evidence_flush_ms']
                    and op['issued_ms'] + EPSILON >= previous['completed_ms']
                        + previous['verification_wait_ms'] + previous['evidence_flush_ms'],
                    'operation preceded evidence flush lower bound')
        op['run'] = relative.parent.as_posix()
        normalized.append(op)
    operation_request_ids = [action['request_id'] for op in normalized for action in op['actions']]
    require([request['request_id'] for request in rows if request['phase'] in OP_PHASES] == operation_request_ids,
            'orphan, missing, or out-of-order operation request')
    setup = [request for request in rows if request['phase'] == 'setup']
    require([request['action'] for request in setup] == (['write'] * 5 + ['close'])[:len(setup)],
            'setup must be a prefix of five writes and one close')
    setup_sessions = {request['session_id'] for request in setup}
    require(len(setup_sessions) <= 1, 'setup crossed SDK sessions')
    require(all(request['sample_index'] == 0 for request in setup), 'setup sample index is not zero')
    for request in rows:
        if request['phase'] not in OP_PHASES:
            require('op_id' not in request, 'unmapped setup/cleanup operation ID')
            if request['phase'] in ('cleanup', 'prelude_cleanup'):
                require(request['action'] == 'close', 'non-close cleanup request')
                expected_index = settings['prelude_writes'] if request['phase'] == 'prelude_cleanup' else 0
                require(request['sample_index'] == expected_index, 'cleanup sample index mismatch')
    # Session construction is part of the benchmark contract, not a TCP claim.
    active = [op for op in normalized if op['phase'] != 'prelude']
    aging_sessions = {op['session_id'] for op in normalized if op['phase'] == 'prelude' and op['session_id']}
    require(len(aging_sessions) <= 1, 'prelude did not reuse its logical session')
    active_sessions = {op['session_id'] for op in active if op['session_id']}
    require(not setup_sessions.intersection(aging_sessions | active_sessions)
            and not aging_sessions.intersection(active_sessions), 'setup/prelude/timed session identity reused')
    for request in rows:
        if request['phase'] == 'prelude_cleanup':
            require(request['session_id'] in aging_sessions, 'prelude close belongs to another session')
        elif request['phase'] == 'cleanup':
            require(request['session_id'] in active_sessions, 'persistent close belongs to another session')
    if scenario == 'H4':
        require(len({op['session_id'] for op in active if op['session_id']}) <= 1, 'H4 did not reuse logical session')
    else:
        sessions = [op['session_id'] for op in active if op['session_id']]
        require(len(set(sessions)) == len(sessions), 'H6 reused logical session')
    plans = planned_counts(settings, scenario)
    accounts = ledger(normalized, rows, plans)
    for phase in OP_PHASES:
        require(phase_indices[phase] <= plans[phase][0], 'more operations than planned')
    for phase in PHASES:
        require(accounts[phase]['http_requests_observed'] <= plans[phase][1], 'more requests than planned')
        if any(PHASES.index(request['phase']) > PHASES.index(phase) for request in rows):
            require(accounts[phase]['http_requests_observed'] == plans[phase][1],
                    f'advanced past incomplete phase: {phase}')
            require(accounts[phase]['unknown_http_outcomes'] == 0
                    and not accounts[phase]['failed_or_unknown_operations'],
                    f'advanced past failed phase: {phase}')
            if phase in OP_PHASES:
                require(phase_indices[phase] == plans[phase][0], f'advanced past missing operations: {phase}')
    status = field(summary, 'status')
    require(status in ('COMPLETED', 'STOPPED'), 'unsupported client status')
    require(integer(field(summary, 'schema'), 'summary schema') == 1 and field(summary, 'scenario') == scenario
            and field(summary, 'arm') == label[0]
            and integer(field(summary, 'requested'), 'summary requested') == settings['samples'], 'summary identity mismatch')
    timed = [op for op in normalized if op['phase'] == 'timed']
    require(integer(field(summary, 'count'), 'summary count') == len(timed)
            and integer(field(summary, 'success'), 'summary success') == accounts['timed']['successful_operations']
            and integer(field(summary, 'prelude_completed'), 'summary prelude completed') == accounts['prelude']['successful_operations'], 'summary counts disagree with raw')
    timed_stats = statistics([op['duration_ms'] for op in timed])
    for key in ('p50_ms', 'p95_ms', 'p99_ms', 'max_ms', 'over_100ms'):
        if key == 'over_100ms':
            integer(field(summary, key), key)
        elif field(summary, key) is not None:
            number(summary[key], key)
        require(field(summary, key) == timed_stats[key], f'summary latency disagrees: {key}')
    require(integer(field(summary, 'http_requests_timed'), 'summary HTTP count') == accounts['timed']['http_requests_observed']
            and integer(field(summary, 'close_acknowledgements_timed'), 'summary close count') == accounts['timed']['close_acks'], 'summary request counts disagree')
    for outcome in ('success', 'outcome_unknown'):
        integer(field(field(summary, 'request_outcomes'), outcome), 'summary outcome count')
    require(field(summary, 'request_outcomes') == {
        'success': accounts['timed']['http_acks'],
        'outcome_unknown': accounts['timed']['unknown_http_outcomes']}, 'summary HTTP outcomes disagree')
    require(integer(field(summary, 'quota_http_429'), 'summary 429 count') == sum(row['http_429'] for row in accounts.values()), 'summary 429 count disagrees')
    if status == 'COMPLETED':
        require(field(summary, 'failure') is None, 'completed client has failure')
        require(all(op['outcome'] == 'success' for op in normalized)
                and all(request['outcome'] == 'success' for request in rows), 'completed client has unknown/failure')
        require(all(accounts[phase]['http_requests_observed'] == plans[phase][1] for phase in PHASES), 'completed client missing requests')
        require(all(phase_indices[phase] == plans[phase][0] for phase in OP_PHASES), 'completed client missing operations')
    else:
        require(field(summary, 'failure') is not None, 'stopped client lacks failure explanation')
    return {
        'status': 'COMPLETED' if status == 'COMPLETED' else 'INTERRUPTED',
        'identity': identity, 'run_id': run_id, 'scenario': scenario, 'arm': label,
        'ledger': accounts, 'phase_metrics': {phase: summarize([op for op in normalized if op['phase'] == phase]) for phase in OP_PHASES},
        'windows_250': [{'first_index': start, 'last_index': min(start + 249, len(timed) - 1),
                         'metrics': summarize(timed[start:start + 250])} for start in range(0, len(timed), 250)],
        'timed_start_rate_per_second': (len(timed) - 1) * 1000 / (timed[-1]['issued_ms'] - timed[0]['issued_ms']) if len(timed) > 1 else None,
        'timed_start_rate_is_capacity': False,
    }, normalized


def analyze(root):
    inputs = Inputs(root)
    settings = inputs.json('abba-settings.json')
    progress = inputs.json('abba-progress.json')
    require(integer(field(settings, 'schema'), 'settings schema') == 2
            and integer(field(progress, 'schema'), 'progress schema') == 2, 'unsupported campaign schema')
    for name in ('rounds', 'samples', 'warmup', 'prelude_writes', 'period_ms'):
        integer(field(settings, name), name, int(name in ('rounds', 'samples')))
    require(field(settings, 'order') == list(ORDER), 'unsupported ABBA order')
    require(field(settings, 'scenarios') in ('H4,H6', 'H6,H4'), 'only complete H4/H6 campaign plans supported')
    require(type(field(settings, 'trace')) is bool, 'trace identity missing')
    helper_hashes = field(settings, 'helper_sha256')
    require(isinstance(helper_hashes, dict) and {'issue15-sdk-client.mjs', 'goal-observe.mjs'} <= helper_hashes.keys(), 'missing frozen client identities')
    for name, expected in helper_hashes.items():
        hexadecimal(expected, 64, 'helper hash')
        require(hashlib.sha256(inputs.read_bytes(Path('helpers') / name)).hexdigest() == expected,
                f'frozen helper SHA mismatch: {name}')
    for role in ('baseline', 'candidate'):
        source = field(field(settings, 'sources'), role)
        hexadecimal(field(source, 'commit'), 40, 'source commit')
        hexadecimal(field(source, 'turso_gitlink'), 40, 'Turso gitlink')
        require(field(source, 'dirty') is False, 'recorded source was dirty')
    for name in ('baseline_rust', 'baseline_platform', 'candidate_rust', 'candidate_platform'):
        hexadecimal(field(field(settings, 'binary_sha256'), name), 64, 'binary SHA')
    expected_arms = [(round_id, arm) for round_id in range(1, settings['rounds'] + 1) for arm in ORDER]
    arms = field(progress, 'arms')
    require(isinstance(arms, list) and len(arms) <= len(expected_arms), 'invalid progress arms')
    for position, arm in enumerate(arms):
        require((integer(field(arm, 'round'), 'progress round', 1), field(arm, 'label')) == expected_arms[position], 'progress order/repeated arm')
        require(type(field(arm, 'returncode')) is int, 'invalid process returncode')
        require(position == len(arms) - 1 or arm['returncode'] == 0, 'continued ABBA after failed arm')
    expected_runs = [f'R{round_id}/{arm}/{scenario}' for round_id, arm in expected_arms for scenario in settings['scenarios'].split(',')]
    actual_runs = {path.parent.parent.relative_to(root).as_posix() for path in root.glob('R*/*/H*/client/run.json')}
    require(actual_runs <= set(expected_runs), 'unplanned client run in campaign')
    last_observable_arm = len(arms) - 1 if arms and arms[-1]['returncode'] != 0 else len(arms)
    for name in actual_runs:
        round_name, arm, _ = name.split('/')
        require(expected_arms.index((int(round_name[1:]), arm)) <= last_observable_arm,
                'client recorded beyond failed/missing progress arm')
    all_ids = defaultdict(set)
    reports, all_operations = {}, []
    identity = None
    for name in expected_runs:
        _, arm, scenario = name.split('/')
        if name not in actual_runs:
            reports[name] = {'status': 'NOT_OBSERVED', 'reason': 'No client record; absence does not prove zero SQL dispatches.'}
            continue
        report, operations = analyze_client(inputs, Path(name) / 'client', settings, arm, scenario, all_ids)
        if identity is None:
            identity = report['identity']
        require(report['identity'] == identity, 'SDK/runtime/client identity changed across clients')
        reports[name] = report
        all_operations.extend(operations)
    for arm in arms:
        if arm['returncode'] == 0:
            require(all(reports[f"R{arm['round']}/{arm['label']}/{scenario}"]['status'] == 'COMPLETED'
                        for scenario in settings['scenarios'].split(',')), 'successful arm has missing/incomplete client')
    declared_complete = progress.get('status') == 'COMPLETED'
    if declared_complete:
        require(len(arms) == len(expected_arms) and all(arm['returncode'] == 0 for arm in arms)
                and all(report['status'] == 'COMPLETED' for report in reports.values()), 'campaign declared complete with missing/failed evidence')
    pooled = {}
    for round_id in range(1, settings['rounds'] + 1):
        for side in ('A', 'B'):
            for scenario in settings['scenarios'].split(','):
                members = [f'R{round_id}/{side}{number}/{scenario}' for number in (1, 2)]
                selected = [op for op in all_operations if op['run'] in members and op['phase'] == 'timed']
                pooled[f'R{round_id}/{side}/{scenario}'] = {
                    'members': {name: reports[name]['status'] for name in members},
                    'members_complete': all(reports[name]['status'] == 'COMPLETED' for name in members),
                    'planned_timed_operations': settings['samples'] * 2,
                    'metrics': summarize(selected),
                }
    total_ledger = {}
    for phase in PHASES:
        rows = [report['ledger'][phase] for report in reports.values() if 'ledger' in report]
        fields = rows[0].keys() if rows else ()
        counts = {key: (sum(row[key] for row in rows) if all(row[key] is not None for row in rows) else None)
                  for key in fields if key != 'operation_records_supported'}
        plans = [planned_counts(settings, name.split('/')[-1])[phase] for name in expected_runs]
        counts['planned_operations'] = sum(plan[0] for plan in plans) if phase in OP_PHASES else None
        counts['planned_http_requests'] = sum(plan[1] for plan in plans)
        counts['unobserved_client_runs'] = len(expected_runs) - len(actual_runs)
        total_ledger[phase] = counts
    if not actual_runs:
        raise InvalidEvidence('no observed client inputs; no zero-failure inference is possible')
    inputs.recheck()
    interrupted = any(arm['returncode'] != 0 for arm in arms) or any(
        report['status'] == 'INTERRUPTED' for report in reports.values())
    result = {
        'schema': 1, 'analysis': 'OFFLINE_PHASE_ACCOUNTING',
        'analyzer_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'campaign_status': 'COMPLETED' if declared_complete else ('INTERRUPTED' if interrupted else 'INCOMPLETE'),
        'performance_acceptance': 'NOT_EVALUATED', 'replaces_goal_audit': False,
        'input_root': str(root), 'server_trace_enabled': settings['trace'],
        'planned_client_runs': len(expected_runs), 'observed_client_runs': len(actual_runs),
        'sources_recorded': settings['sources'], 'binary_sha256_recorded': settings['binary_sha256'],
        'source_and_binary_files_reverified': False, 'client_identity': identity,
        'ledger': total_ledger, 'runs': reports, 'pooled_timed': pooled,
        'raw_sha256': inputs.hashes,
        'limits': [
            'This validates the recorded client contract, not physical durability, MACs, quotas, recovery or all Goal gates.',
            'H4/H6 business timing is unchanged. SDK write includes the autocommit probe and H6 first-session open.',
            'Successful latency is separated from failed-attempt elapsed time; failures and Unknown remain in the ledger.',
            'Unobserved clients are not evidence of zero attempts or zero failures.',
            'Missing failed-request components are unknown, not zero. Setup/cleanup have no SDK action spans.',
            'Phase quantiles do not add. Largest observed component does not identify a server cause.',
            'H6 is a new logical SDK session, not necessarily a new TCP/TLS connection.',
            'Clone consumption is post-sample evidence, not the original response EOF.',
            'C1 paced observed rate is not capacity. No cross-round, cross-campaign, or trace-mode pooling.',
        ],
    }
    return result, all_operations


def write_outputs(output, report, operations):
    (output / 'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    for name, selected in (
        ('slow-operations.jsonl', [op for op in operations if op['duration_ms'] > 100]),
        ('failed-operations.jsonl', [op for op in operations if op['outcome'] != 'success']),
    ):
        with (output / name).open('x') as stream:
            for operation in selected:
                stream.write(json.dumps(operation, ensure_ascii=False, allow_nan=False) + '\n')
    lines = ['# Offline phase accounting', '',
             f"Campaign: **{report['campaign_status']}**. Performance acceptance: **NOT_EVALUATED**.",
             '', 'This output does not replace the frozen Goal audit or certify a performance fix.', '',
             '| Round/side/scenario | Complete members | Observed timed | Business p99 ms | Write SDK p99 ms |',
             '|---|---|---:|---:|---:|']
    for name, group in report['pooled_timed'].items():
        metric = group['metrics']
        def display(value):
            return 'unknown/no samples' if value is None else f'{value:.3f}'
        lines.append(f"| {name} | {group['members_complete']} | {metric['issued_operations']}/{group['planned_timed_operations']} | "
                     f"{display(metric['business_success_ms']['p99_ms'])} | {display(metric['actions']['write']['sdk_confirmed_ms']['p99_ms'])} |")
    lines.extend(['', 'All phases, including prelude/setup failures, are in report.json. '
                  'slow-operations.jsonl retains every observed >100ms operation in every phase; '
                  'failed-operations.jsonl retains failed operation records. '
                  'Setup/cleanup request outcomes remain in the ledger and original hashed raw files.', '',
                  'Missing timings are never replaced with zero. See report.json limits and raw_sha256.', ''])
    (output / 'README.md').write_text('\n'.join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args(argv)
    root, output = args.campaign.resolve(), args.out.resolve()
    try:
        require(root.is_dir(), 'campaign directory missing')
        require(not output.exists(), 'output already exists; never overwrite an analysis')
        require(not output.is_relative_to(root) and not root.is_relative_to(output), 'input/output trees must not overlap')
        output.mkdir(parents=True)
        try:
            report, operations = analyze(root)
            write_outputs(output, report, operations)
        except (InvalidEvidence, OSError, TypeError, KeyError) as error:
            (output / 'invalid-input.json').write_text(json.dumps({
                'analysis': 'INVALID_EVIDENCE', 'performance_acceptance': 'NOT_EVALUATED',
                'input_root': str(root), 'error': str(error)}, indent=2) + '\n')
            raise InvalidEvidence(str(error)) from error
    except (InvalidEvidence, OSError) as error:
        print(json.dumps({'analysis': 'INVALID_EVIDENCE', 'error': str(error)}), file=sys.stderr)
        return 2
    print(json.dumps({'analysis': report['analysis'], 'campaign_status': report['campaign_status'],
                      'performance_acceptance': 'NOT_EVALUATED', 'output': str(output),
                      'timed_operations': report['ledger']['timed'].get('issued_operations'),
                      'observed_all_phase_unknown_http': sum(phase['unknown_http_outcomes'] for phase in report['ledger'].values()),
                      'unobserved_client_runs': report['planned_client_runs'] - report['observed_client_runs']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
