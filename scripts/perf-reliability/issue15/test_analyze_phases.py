#!/usr/bin/env python3
"""Offline corruption, interruption and denominator tests; never launches services."""
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout
import copy
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location('analyze_phases', Path(__file__).with_name('analyze-phases.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def summary_stats(values):
    values = sorted(values)
    return {**{f'p{p}_ms': values[math.ceil(len(values) * p / 100) - 1] if values else None
               for p in (50, 95, 99)},
            'max_ms': max(values) if values else None,
            'over_100ms': sum(value > 100 for value in values)}


class Fixture:
    def __init__(self, scenario='H4', *, failure=None, no_headers=False,
                 samples=2, warmup=1, prelude=1, prefix='one', arm='A'):
        self.scenario, self.prefix, self.arm = scenario, prefix, arm
        self.requests, self.operations, self.batons = [], [], {}
        self.clock = 10.0
        self.run_id = prefix + '-run'
        self.settings = {'schema': 2, 'rounds': 1, 'samples': samples, 'warmup': warmup,
                         'prelude_writes': prelude, 'period_ms': 50, 'trace': False,
                         'order': ['A1', 'B1', 'B2', 'A2'], 'scenarios': 'H4,H6',
                         'helper_sha256': {'issue15-sdk-client.mjs': sha('client'),
                                           'goal-observe.mjs': sha('observer')},
                         'sources': {side: {'commit': 'a' * 40, 'turso_gitlink': 'b' * 40,
                                            'dirty': False} for side in ('baseline', 'candidate')},
                         'binary_sha256': {name: 'c' * 64 for name in ('baseline_rust',
                             'baseline_platform', 'candidate_rust', 'candidate_platform')}}
        self.run = {'schema': 2, 'run_id': self.run_id, 'scenario': scenario, 'arm': arm,
                    'sdk': '@tursodatabase/serverless@1.4.1', 'fixture_rows': 1000, 'payload_bytes': 256,
                    'samples_requested': samples, 'warmup_requested': warmup,
                    'prelude_writes': prelude, 'period_ms': 50,
                    'client_sha256': sha('client'), 'observer_sha256': sha('observer'),
                    'config_sha256': sha('config'), 'sdk_sha256': sha('sdk'),
                    'sdk_lock_sha256': sha('lock'), 'node': 'synthetic-node'}
        for _ in range(5):
            self.action('setup', 0, 'write', 'setup', None)
        self.action('setup', 0, 'close', 'setup', None)
        stopped = False
        for phase, count in [('prelude', prelude), ('warmup', warmup), ('timed', samples)]:
            for index in range(count):
                self.clock += 100
                issued = self.clock
                op_id = f'{prefix}-op-{len(self.operations)}'
                op_scenario = 'H4' if phase == 'prelude' else scenario
                session = 'aging' if phase == 'prelude' else (
                    'persistent' if scenario == 'H4' else op_id)
                operation = {'schema': 1, 'op_id': op_id, 'run_id': self.run_id,
                             'phase': phase, 'sample_index': index, 'scenario': op_scenario,
                             'outcome': 'success', 'retry_count': 0, 'actions': [],
                             'issued_ms': issued, 'verification_wait_ms': 0.3,
                             'evidence_flush_ms': 0.2}
                if phase != 'prelude':
                    operation['connect_ms'] = 0.2
                self.clock += 0.5
                for name in ['write', 'read'] + (['close'] if op_scenario == 'H6' else []):
                    failed = failure == (phase, index, name)
                    action = self.action(phase, index, name, session, op_id,
                                         failed=failed, no_headers=no_headers and failed)
                    operation['actions'].append(action)
                    if failed:
                        operation['outcome'] = 'failed_or_unknown'
                        stopped = True
                        break
                    if name == 'write':
                        effect_id = 10_000_000 + len(self.operations)
                        prefix_text = 'i' + str(effect_id).zfill(8) + ':'
                        operation['write_effect'] = {'request_id': action['request_ids'][0],
                            'kind': 'insert', 'table': 'perf_events', 'id': effect_id,
                            'value': effect_id, 'payload_sha256': sha(prefix_text + '0' * (256 - len(prefix_text)))}
                    elif name == 'read':
                        effect = operation['write_effect']
                        operation['readback_effect'] = {'value': effect['value'], 'payload_sha256': effect['payload_sha256']}
                operation['request_ids'] = [rid for action in operation['actions'] for rid in action['request_ids']]
                operation['completed_ms'] = operation['actions'][-1]['sdk_resolved_ms']
                operation['duration_ms'] = operation['completed_ms'] - issued
                self.operations.append(operation)
                if stopped:
                    break
            if stopped:
                break
            if phase == 'prelude' and prelude:
                self.action('prelude_cleanup', prelude, 'close', 'aging', None)
        if not stopped and scenario == 'H4':
            self.action('cleanup', 0, 'close', 'persistent', None)
        timed = [op for op in self.operations if op['phase'] == 'timed']
        timed_requests = [request for request in self.requests if request['phase'] == 'timed']
        self.summary = {'schema': 1, 'scenario': scenario, 'arm': arm, 'requested': samples,
                        'status': 'STOPPED' if stopped else 'COMPLETED',
                        'count': len(timed), 'success': sum(op['outcome'] == 'success' for op in timed),
                        'failure': [{'message': 'synthetic failure'}] if stopped else None,
                        'prelude_completed': sum(op['phase'] == 'prelude' and op['outcome'] == 'success' for op in self.operations),
                        **summary_stats([op['duration_ms'] for op in timed]),
                        'http_requests_timed': len(timed_requests),
                        'close_acknowledgements_timed': sum(request['close_acknowledged'] for request in timed_requests),
                        'request_outcomes': {name: sum(request['outcome'] == name for request in timed_requests)
                                             for name in ('success', 'outcome_unknown')},
                        'quota_http_429': 0}

    def action(self, phase, index, name, session, op_id, *, failed=False, no_headers=False):
        request_id = f'{self.prefix}-request-{len(self.requests)}'
        start = self.clock
        end = start + 2
        request = {'schema': 3, 'request_id': request_id, 'run_id': self.run_id,
                   'phase': phase, 'sample_index': index, 'session_id': session,
                   'action': name, 'method': 'POST',
                   'path': '/db/test/demo/main/v3/' + ('pipeline' if name == 'close' else 'cursor'),
                   'request_body_sha256': sha(request_id), 'started_ms': start + 0.1,
                   'dispatch_ms': start + 0.2, 'baton_in_sha256': self.batons.get(session),
                   'step_count': 1 if name == 'close' else 2,
                   'close_acknowledged': False, 'outcome': 'outcome_unknown' if failed else 'success'}
        request['sql'] = [] if name == 'close' else [
            ('INSERT INTO perf_events(id,value,payload) VALUES(?,?,?)' if name == 'write'
             else 'SELECT value,payload FROM perf_events WHERE id=?'), 'SELECT 1']
        if op_id is not None:
            request['op_id'] = op_id
        if not no_headers:
            request.update(headers_ms=start + 1, status=502 if failed else 200,
                           clone_body_observed_ms=end + 0.1, completed_ms=end + 0.1)
        if not failed:
            if name == 'close':
                request.update(close_acknowledged=True, close_baton_explicit_null=True,
                               close_result_count=1, baton_out_sha256=None)
            else:
                request.update(cursor_completed_steps=[0, 1], cursor_probe_complete=True,
                               baton_out_sha256=sha(request_id + '-baton'))
            self.batons[session] = request['baton_out_sha256']
        self.requests.append(request)
        self.clock = end + 0.5
        return {'action': name, 'request_ids': [request_id], 'started_ms': start,
                'completed_ms': end, 'sdk_resolved_ms': end, 'duration_ms': end - start,
                'verification_completed_ms': end + 0.3,
                'outcome': 'outcome_unknown' if failed else 'success'}

    def write(self, root, relative=Path('R1/A1/H4/client')):
        directory = root / relative
        directory.mkdir(parents=True)
        for name, value in [('run.json', self.run), ('summary.json', self.summary)]:
            (directory / name).write_text(json.dumps(value) + '\n')
        for name, rows in [('operations.jsonl', self.operations), ('requests.jsonl', self.requests)]:
            (directory / name).write_text(''.join(json.dumps(row) + '\n' for row in rows))
        self.rehash(directory)
        return relative

    @staticmethod
    def rehash(directory):
        (directory / 'SHA256SUMS.json').write_text(json.dumps({
            name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
            for name in MODULE.RAW_FILES}) + '\n')


class PhaseAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'input'
        self.root.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def analyze_fixture(self, fixture):
        relative = fixture.write(self.root, Path('R1/A1') / fixture.scenario / 'client')
        return MODULE.analyze_client(MODULE.Inputs(self.root), relative, fixture.settings,
                                     'A1', fixture.scenario, defaultdict(set))

    def test_nearest_rank_and_no_zero_for_empty(self):
        self.assertEqual(MODULE.statistics(list(range(1, 2001)))['p99_ms'], 1980)
        empty = MODULE.statistics([], missing=3)
        self.assertIsNone(empty['p99_ms'])
        self.assertIsNone(empty['over_100ms_rate'])
        self.assertEqual(empty['missing'], 3)

    def test_completed_h6_prelude_is_h4_and_close_is_explicit(self):
        report, operations = self.analyze_fixture(Fixture('H6'))
        self.assertEqual(report['status'], 'COMPLETED')
        self.assertEqual(report['ledger']['prelude']['close_acks'], 0)
        self.assertEqual(report['ledger']['timed']['close_acks'], 2)
        self.assertEqual(report['phase_metrics']['timed']['actions']['write']['sdk_confirmed_ms']['p99_ms'], 2)
        self.assertEqual(report['phase_metrics']['timed']['actions']['write']['issued_to_return_confirmed_ms']['p99_ms'], 2.5)
        self.assertEqual(report['phase_metrics']['timed']['client_gaps_ms']['p99_ms'], 1.5)
        self.assertEqual(report['ledger']['setup']['http_requests_observed'], 6)
        self.assertIsNone(report['ledger']['setup']['issued_operations'])
        self.assertEqual(len(operations), 4)

    def test_prelude_unknown_is_not_hidden_by_zero_timed(self):
        fixture = Fixture(prelude=2, failure=('prelude', 1, 'write'))
        report, operations = self.analyze_fixture(fixture)
        self.assertEqual(report['status'], 'INTERRUPTED')
        self.assertEqual(report['ledger']['prelude']['planned_operations'], 2)
        self.assertEqual(report['ledger']['prelude']['issued_operations'], 2)
        self.assertEqual(report['ledger']['prelude']['write_acks'], 1)
        self.assertEqual(report['ledger']['prelude']['unknown_http_outcomes'], 1)
        self.assertEqual(report['ledger']['timed']['issued_operations'], 0)
        self.assertEqual(report['ledger']['timed']['planned_operations'], 2)
        self.assertIsNone(report['phase_metrics']['timed']['business_success_ms']['p99_ms'])
        self.assertEqual(operations[-1]['outcome'], 'failed_or_unknown')

    def test_write_ack_followed_by_read_failure_is_business_failure(self):
        report, _ = self.analyze_fixture(Fixture(prelude=0, warmup=0, failure=('timed', 0, 'read')))
        ledger = report['ledger']['timed']
        self.assertEqual(ledger['write_acks'], 1)
        self.assertEqual(ledger['exact_readbacks'], 0)
        self.assertEqual(ledger['successful_operations'], 0)
        self.assertEqual(ledger['write_confirmed_but_business_failed'], 1)
        self.assertEqual(report['phase_metrics']['timed']['actions']['write']['confirmed'], 1)
        self.assertEqual(report['phase_metrics']['timed']['actions']['read']['confirmed'], 0)

    def test_missing_headers_on_failed_transport_are_unknown_not_zero(self):
        fixture = Fixture(prelude=0, warmup=0, failure=('timed', 0, 'write'), no_headers=True)
        report, operations = self.analyze_fixture(fixture)
        metric = report['phase_metrics']['timed']['actions']['write']['dispatch_to_headers_ms']
        self.assertEqual(metric['missing'], 1)
        self.assertEqual(metric['n'], 0)
        self.assertIsNone(metric['p99_ms'])
        self.assertFalse(operations[0]['components_complete'])

    def test_unconfirmed_close_keeps_write_and_read_but_fails_business(self):
        report, _ = self.analyze_fixture(Fixture('H6', prelude=0, warmup=0, failure=('timed', 0, 'close')))
        ledger = report['ledger']['timed']
        self.assertEqual((ledger['write_acks'], ledger['exact_readbacks'], ledger['close_acks'],
                          ledger['successful_operations']), (1, 1, 0, 0))

    def rejected(self, mutate):
        fixture = Fixture()
        mutate(fixture)
        with self.assertRaises(MODULE.InvalidEvidence):
            self.analyze_fixture(fixture)

    def test_duplicate_request_id_rejected(self):
        self.rejected(lambda fixture: fixture.requests.append(copy.deepcopy(fixture.requests[-1])))

    def test_h6_independent_session_request_blocks_cannot_be_reordered(self):
        fixture = Fixture('H6', prelude=0, warmup=0, samples=2)
        fixture.requests = fixture.requests[:6] + fixture.requests[9:12] + fixture.requests[6:9]
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'out of order'):
            self.analyze_fixture(fixture)

    def test_setup_read_actions_cannot_stand_in_for_fixture_writes(self):
        fixture = Fixture('H6', prelude=0, warmup=0, samples=2)
        for request in fixture.requests[:5]:
            request['action'] = 'read'
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'five writes'):
            self.analyze_fixture(fixture)

    def test_boolean_cursor_steps_are_not_integer_step_ids(self):
        fixture = Fixture('H6', prelude=0, warmup=0, samples=2)
        fixture.requests[0]['cursor_completed_steps'] = [False, True]
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'cursor ACK/probe'):
            self.analyze_fixture(fixture)

    def test_setup_sample_index_is_checked(self):
        fixture = Fixture()
        fixture.requests[0]['sample_index'] = 1
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'setup sample index'):
            self.analyze_fixture(fixture)

    def test_setup_cannot_use_multiple_sessions_even_with_valid_batons(self):
        fixture = Fixture()
        fixture.requests[0]['session_id'] = fixture.requests[1]['session_id'] = 'other-setup'
        fixture.requests[2]['baton_in_sha256'] = None
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'setup crossed SDK sessions'):
            self.analyze_fixture(fixture)

    def test_persistent_cleanup_cannot_close_an_unrelated_session(self):
        fixture = Fixture()
        fixture.requests[-1].update(session_id='unrelated', baton_in_sha256=None)
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'persistent close belongs to another session'):
            self.analyze_fixture(fixture)

    def test_evidence_flush_must_fit_before_next_operation(self):
        fixture = Fixture()
        fixture.operations[0]['evidence_flush_ms'] = 1000
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'evidence flush lower bound'):
            self.analyze_fixture(fixture)

    def test_verification_cannot_precede_its_response_evidence(self):
        fixture = Fixture()
        fixture.requests[6]['clone_body_observed_ms'] = 1_000_000
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'before response evidence'):
            self.analyze_fixture(fixture)

    def test_wrong_business_sql_is_not_accepted_by_action_label(self):
        fixture = Fixture()
        fixture.requests[6]['sql'][0] = 'DELETE FROM perf_events'
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'fixed workload'):
            self.analyze_fixture(fixture)

    def test_orphan_operation_request_rejected(self):
        def mutate(fixture):
            request = copy.deepcopy(next(row for row in fixture.requests if row['phase'] == 'timed'))
            request['request_id'] = 'orphan'
            fixture.requests.insert(-1, request)
        self.rejected(mutate)

    def test_missing_request_rejected_even_when_manifest_rehashed(self):
        self.rejected(lambda fixture: fixture.requests.pop(6))

    def test_single_character_string_is_not_a_request_id_list(self):
        fixture = Fixture()
        operation = fixture.operations[0]
        fixture.requests[6]['request_id'] = 'X'
        operation['actions'][0]['request_ids'] = 'X'
        operation['request_ids'][0] = 'X'
        operation['write_effect']['request_id'] = 'X'
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'request IDs must be lists'):
            self.analyze_fixture(fixture)

    def test_wrong_run_id_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1].update(run_id='different-run'))

    def test_sample_gap_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1].update(sample_index=3))

    def test_missing_success_headers_rejected(self):
        self.rejected(lambda fixture: fixture.requests[6].pop('headers_ms'))

    def test_headers_after_sdk_return_rejected(self):
        self.rejected(lambda fixture: fixture.requests[6].update(headers_ms=1_000_000))

    def test_missing_action_time_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1]['actions'][0].pop('sdk_resolved_ms'))

    def test_wrong_readback_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1]['readback_effect'].update(value=-1))

    def test_replay_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1].update(retry_count=1))

    def test_baton_mismatch_rejected(self):
        self.rejected(lambda fixture: fixture.requests[1].update(baton_in_sha256=None))

    def test_close_without_explicit_null_rejected(self):
        self.rejected(lambda fixture: fixture.requests[-1].pop('close_baton_explicit_null'))

    def test_summary_count_disagreement_rejected(self):
        self.rejected(lambda fixture: fixture.summary.update(count=0))

    def test_boolean_is_not_a_numeric_count_or_schema(self):
        self.rejected(lambda fixture: fixture.operations[-1].update(schema=True))

    def test_stopped_status_does_not_allow_advancing_past_missing_prelude(self):
        fixture = Fixture(prelude=0, failure=('timed', 0, 'read'))
        # Identity, IDs, timings, batons, summary, and effects remain internally
        # consistent, but the declared prelude was skipped before warmup.
        fixture.run['prelude_writes'] = fixture.settings['prelude_writes'] = 1
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'advanced past incomplete phase: prelude'):
            self.analyze_fixture(fixture)

    def test_nonfinite_json_time_rejected(self):
        self.rejected(lambda fixture: fixture.operations[-1].update(issued_ms=float('nan')))

    def test_nonfinite_json_constant_and_duplicate_keys_rejected(self):
        for raw in ('{"x":NaN}', '{"x":1,"x":2}'):
            with self.subTest(raw=raw), self.assertRaises(MODULE.InvalidEvidence):
                MODULE.decode(raw, 'test')

    def test_raw_sha_tampering_rejected(self):
        fixture = Fixture()
        relative = fixture.write(self.root)
        with (self.root / relative / 'operations.jsonl').open('a') as stream:
            stream.write('{}\n')
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'raw SHA mismatch'):
            MODULE.analyze_client(MODULE.Inputs(self.root), relative, fixture.settings, 'A1', 'H4', defaultdict(set))

    def make_campaign(self, declared_complete=False):
        first = Fixture(prefix='A1H4')
        settings = first.settings
        helpers = self.root / 'helpers'
        helpers.mkdir()
        (helpers / 'issue15-sdk-client.mjs').write_text('client')
        (helpers / 'goal-observe.mjs').write_text('observer')
        (self.root / 'abba-settings.json').write_text(json.dumps(settings))
        for scenario in ('H4', 'H6'):
            Fixture(scenario, prefix='A1' + scenario).write(self.root, Path('R1/A1') / scenario / 'client')
        progress = {'schema': 2, 'arms': [{'round': 1, 'label': 'A1', 'returncode': 0}]}
        if declared_complete:
            progress['status'] = 'COMPLETED'
        (self.root / 'abba-progress.json').write_text(json.dumps(progress))

    def test_missing_arms_are_explicit_and_pool_remains_incomplete(self):
        self.make_campaign()
        report, _ = MODULE.analyze(self.root)
        self.assertEqual(report['campaign_status'], 'INCOMPLETE')
        self.assertEqual(report['runs']['R1/B1/H4']['status'], 'NOT_OBSERVED')
        group = report['pooled_timed']['R1/A/H4']
        self.assertFalse(group['members_complete'])
        self.assertEqual(group['metrics']['issued_operations'], 2)
        self.assertEqual(group['planned_timed_operations'], 4)
        self.assertEqual(report['ledger']['timed']['planned_operations'], 16)
        self.assertEqual(report['performance_acceptance'], 'NOT_EVALUATED')

    def test_completed_campaign_with_missing_arms_rejected(self):
        self.make_campaign(declared_complete=True)
        with self.assertRaisesRegex(MODULE.InvalidEvidence, 'declared complete'):
            MODULE.analyze(self.root)

    def test_cli_refuses_overwrite_and_input_overlap_without_writing(self):
        self.make_campaign()
        snapshot = {str(path): path.read_bytes() for path in self.root.rglob('*') if path.is_file()}
        with redirect_stderr(io.StringIO()):
            self.assertEqual(MODULE.main([str(self.root), '--out', str(self.root)]), 2)
            self.assertEqual(MODULE.main([str(self.root), '--out', str(self.root / 'analysis')]), 2)
        self.assertEqual(snapshot, {str(path): path.read_bytes() for path in self.root.rglob('*') if path.is_file()})

    def test_cli_valid_partial_report_preserves_inputs(self):
        self.make_campaign()
        snapshot = {str(path): path.read_bytes() for path in self.root.rglob('*') if path.is_file()}
        output = Path(self.temp.name) / 'output'
        with redirect_stdout(io.StringIO()):
            self.assertEqual(MODULE.main([str(self.root), '--out', str(output)]), 0)
        self.assertTrue((output / 'report.json').exists())
        self.assertFalse((output / 'invalid-input.json').exists())
        self.assertEqual(snapshot, {str(path): path.read_bytes() for path in self.root.rglob('*') if path.is_file()})

    def test_cli_invalid_evidence_never_writes_success_report(self):
        self.make_campaign(declared_complete=True)
        output = Path(self.temp.name) / 'invalid-output'
        with redirect_stderr(io.StringIO()):
            self.assertEqual(MODULE.main([str(self.root), '--out', str(output)]), 2)
        self.assertTrue((output / 'invalid-input.json').exists())
        self.assertFalse((output / 'report.json').exists())


if __name__ == '__main__':
    unittest.main()
