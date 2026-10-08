import asyncio
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from guard_profiles import parameters

SPEC = importlib.util.spec_from_file_location('run_guarded', Path(__file__).with_name('run_guarded.py'))
guarded = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guarded)


class FakeJournal:
    def __init__(self):
        self.events = []

    def emit(self, event, **fields):
        record = {'event': event, 'wall_ns': 100, 'monotonic_ns': 100, **fields}
        self.events.append(record)
        return record


class FakeChild:
    def __init__(self, pid=123456):
        self.pid, self.returncode, self.signals = pid, None, []
        self.joined = asyncio.Event()

    def send_signal(self, selected):
        self.signals.append(selected)

    async def wait(self):
        await self.joined.wait()
        return self.returncode


def owner(pid=123456):
    return {'pid': pid, 'ppid': os.getpid(), 'uid': os.getuid(), 'start': 'Wed Oct 7 23:08:03 2026', 'argv': '/private/fake-cli -cases mixed_oltp'}


class GuardTests(unittest.IsolatedAsyncioTestCase):
    def controller(self, **values):
        spec = {'probe_timeout_seconds': .1, 'interval_seconds': .01, 'hard_max_seconds': 100, 'max_process_rss_bytes': 536870912, 'max_output_bytes': 67108864, 'output': '/private/fake-output', 'launcher': owner(123455), 'services': [], **values}
        controller = guarded.Controller(FakeJournal(), spec, {})
        controller.child, controller.owner = FakeChild(), owner()
        return controller

    async def test_exact_proof_soft_signal_and_sticky_stop(self):
        controller = self.controller()
        controller.stop('first_builder')
        original = controller.first_stop.copy()
        controller.stop('later_probe_error')
        with patch.object(guarded, 'identity', AsyncMock(return_value=owner())):
            self.assertTrue(await controller.send(controller.child, controller.owner, signal.SIGUSR1))
        self.assertEqual(controller.first_stop, original)
        self.assertEqual(controller.child.signals, [signal.SIGUSR1])
        self.assertFalse(controller.journal.events[-1]['may_have_unknown'])

    async def test_operator_signals_are_soft_callbacks_not_task_cancellation(self):
        controller = self.controller()
        controller.run = AsyncMock(return_value=(1, {'status': 'HOLD'}))
        loop = Mock()
        with patch.object(guarded.asyncio, 'get_running_loop', return_value=loop), patch.object(guarded.signal, 'getsignal', return_value='fake-handler'), patch.object(guarded.signal, 'signal') as restore:
            result = await guarded.execute(controller)
        self.assertEqual(result[0], 1)
        self.assertEqual(loop.add_signal_handler.call_count, 2)
        callback = loop.add_signal_handler.call_args_list[0].args
        callback[1](*callback[2:])
        self.assertEqual(controller.first_stop['reason'], 'operator_SIGINT')
        self.assertEqual(restore.call_count, 2)
        self.assertEqual(controller.child.signals, [])

    async def test_reused_pid_refuses_signal(self):
        controller = self.controller()
        observed = {**owner(), 'start': 'different start'}
        with patch.object(guarded, 'identity', AsyncMock(return_value=observed)):
            self.assertFalse(await controller.send(controller.child, controller.owner, signal.SIGUSR1))
        self.assertEqual(controller.child.signals, [])
        self.assertIsNotNone(controller.first_stop)

    async def test_hard_term_records_unknown(self):
        controller = self.controller()
        controller.stop('hard_safety_budget')
        with patch.object(guarded, 'identity', AsyncMock(return_value=owner())):
            self.assertTrue(await controller.send(controller.child, controller.owner, signal.SIGTERM))
        self.assertTrue(controller.journal.events[-1]['may_have_unknown'])

    async def test_final_unknown_risk_nonzero_child_without_spawn_inference(self):
        for child_exit, spawned, expected_unknown in [(1, True, True), (0, True, False), (None, False, False)]:
            with self.subTest(child_exit=child_exit, spawned=spawned):
                controller = self.controller()
                child = controller.child
                child.returncode = child_exit
                if not spawned:
                    controller.child = controller.owner = None
                exit_code, result = controller.finish()
                self.assertEqual(result['may_have_unknown'], expected_unknown)
                self.assertEqual(controller.journal.events[-1]['may_have_unknown'], expected_unknown)
                self.assertEqual(exit_code, 0 if spawned and child_exit == 0 else 1)
                self.assertEqual(result['status'], 'CHILD_COMPLETE_PENDING_VERIFIER' if spawned and child_exit == 0 else 'HOLD')
                self.assertIsNone(result['first_stop'])
                self.assertFalse(result['soft_stop_sent'])
                self.assertFalse(result['hard_stop_sent'])
                self.assertFalse(result['child_alive'])
                self.assertEqual(child.signals, [])

    async def test_monitor_does_not_exit_after_sticky_error(self):
        controller = self.controller()
        calls = []

        async def tick():
            calls.append(len(calls))
            if len(calls) == 1:
                controller.stop('probe_timeout')
            if len(calls) == 4:
                controller.child.returncode = 0
                controller.child.joined.set()

        controller.tick = tick
        await asyncio.wait_for(controller.monitor(), .5)
        self.assertGreaterEqual(len(calls), 5)
        self.assertEqual(controller.child.returncode, 0)
        self.assertEqual(controller.first_stop['reason'], 'probe_timeout')
        self.assertEqual(controller.child.signals, [])

    async def test_timeout_tick_soft_stops_without_cancel(self):
        controller = self.controller()
        with patch.object(guarded, 'probe', AsyncMock(side_effect=asyncio.TimeoutError)), patch.object(guarded, 'identity', AsyncMock(return_value=owner())):
            await controller.tick()
            await controller.tick()
        self.assertEqual(controller.child.signals, [signal.SIGUSR1])
        self.assertFalse(controller.hard_sent)
        self.assertIsNone(controller.child.returncode)

    async def test_hard_deadline_survives_probe_failure(self):
        controller = self.controller(hard_max_seconds=1)
        controller.origin = (0, 0)
        with patch.object(guarded, 'probe', AsyncMock(side_effect=asyncio.TimeoutError)), patch.object(guarded, 'identity', AsyncMock(return_value=owner())), patch.object(guarded.time, 'monotonic_ns', return_value=2_000_000_000):
            await controller.tick()
        self.assertEqual(controller.child.signals, [signal.SIGTERM])
        self.assertTrue(controller.hard_sent)

    async def test_uncooperative_TERM_returns_bounded_retained_HOLD(self):
        controller = self.controller(hard_max_seconds=1, hard_grace_seconds=5)
        controller.origin = (0, 0)
        controller.stop('original_guard_error')
        clock = itertools.count(2_000_000_000, 1_000_000_000)
        with patch.object(guarded, 'probe', AsyncMock(side_effect=asyncio.TimeoutError)), patch.object(guarded, 'identity', AsyncMock(return_value=owner())), patch.object(guarded.time, 'monotonic_ns', side_effect=lambda: next(clock)), patch.object(guarded.time, 'time_ns', return_value=2_000_000_000):
            await asyncio.wait_for(controller.monitor(), .15)
        exit_code, result = controller.finish()
        self.assertEqual(controller.child.signals, [signal.SIGTERM])
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['retained_pid'], controller.child.pid)
        self.assertTrue(result['child_alive'])
        self.assertTrue(result['may_have_unknown'])
        self.assertFalse(result['clean_join'])
        self.assertEqual(result['first_stop']['reason'], 'original_guard_error')
        self.assertGreater(len([event for event in controller.journal.events if event['event'] == 'sample_error']), 1)

    async def test_identity_refusal_returns_bounded_without_any_signal(self):
        controller = self.controller(hard_max_seconds=1000, hard_grace_seconds=5)
        controller.origin = (0, 0)
        controller.stop('original_guard_error')
        changed = {**owner(), 'start': 'reused PID start'}
        clock = itertools.count(2_000_000_000, 1_000_000_000)
        with patch.object(guarded, 'probe', AsyncMock(side_effect=asyncio.TimeoutError)), patch.object(guarded, 'identity', AsyncMock(return_value=changed)), patch.object(guarded.time, 'monotonic_ns', side_effect=lambda: next(clock)), patch.object(guarded.time, 'time_ns', return_value=2_000_000_000):
            await asyncio.wait_for(controller.monitor(), .15)
        exit_code, result = controller.finish()
        self.assertEqual(controller.child.signals, [])
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['retained_pid'], controller.child.pid)
        self.assertTrue(result['child_alive'])
        self.assertFalse(result['clean_join'])
        self.assertTrue(result['may_have_unknown'])
        self.assertEqual(result['signal_refusal_proof']['observed']['start'], 'reused PID start')
        self.assertEqual(result['first_stop']['reason'], 'original_guard_error')

    async def test_probe_timeout_only_kills_own_probe(self):
        fake = FakeChild()
        fake.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        def kill():
            fake.signals.append('probe_kill')
            fake.returncode = -9
            fake.joined.set()

        fake.kill = kill
        with patch.object(guarded.asyncio, 'create_subprocess_exec', AsyncMock(return_value=fake)):
            with self.assertRaises(asyncio.TimeoutError):
                await guarded.probe(['/bin/ps'], .01)
        self.assertEqual(fake.signals, ['probe_kill'])

    async def test_foreign_parent_cannot_receive_owned_signal(self):
        controller = self.controller()
        controller.owner['ppid'] = 1
        with patch.object(guarded, 'identity', AsyncMock(return_value=controller.owner)) as read:
            self.assertFalse(await controller.send(controller.child, controller.owner, signal.SIGTERM))
        read.assert_not_awaited()
        self.assertEqual(controller.child.signals, [])

    async def test_child_zero_does_not_erase_stop(self):
        controller = self.controller()
        controller.child.returncode = 0
        controller.stop('external_builder')
        exit_code, result = controller.finish()
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['status'], 'HOLD')
        self.assertEqual(result['first_stop']['reason'], 'external_builder')
        self.assertFalse(result['performance_pass'])

    async def test_functional_confound_is_never_performance_pass(self):
        controller = self.controller()
        controller.child.returncode = 0
        controller.confounded = True
        exit_code, result = controller.finish()
        self.assertEqual(exit_code, 0)
        self.assertEqual(result['status'], 'FUNCTIONAL_ONLY_CONFOUNDED')
        self.assertFalse(result['performance_pass'])

    async def test_audit_failure_requires_hard_safety_even_if_probe_fails(self):
        controller = self.controller()
        controller.journal.failed = True
        with patch.object(guarded, 'probe', AsyncMock(side_effect=asyncio.TimeoutError)), patch.object(guarded, 'identity', AsyncMock(return_value=owner())):
            await controller.tick()
        self.assertEqual(controller.child.signals, [signal.SIGTERM])
        self.assertTrue(controller.hard_sent)

    async def test_builder_policy_and_after_stop_samples(self):
        for policy, owned, expected_signal in [('strict-stop', False, signal.SIGUSR1), ('record-confound', False, None), ('record-confound', True, signal.SIGUSR1)]:
            with self.subTest(policy=policy, owned=owned):
                with tempfile.TemporaryDirectory() as directory:
                    controller = self.controller(output=directory, external_builder_policy=policy, functional_only=policy == 'record-confound')
                    parent = controller.child.pid if owned else 1
                    raw = '\n'.join(f'{pid} {ppid} {os.getuid()} Wed Oct 7 23:08:03 2026 0.0 1024 {name}' for pid, ppid, name in [(123455, os.getpid(), '/private/launcher'), (123456, os.getpid(), '/private/fake-cli'), (123457, parent, '/private/cargo')])

                    async def probe(argv, timeout):
                        return raw if argv[0] == '/bin/ps' else ''

                    async def read_identity(pid, timeout):
                        return owner(pid)

                    with patch.object(guarded, 'probe', probe), patch.object(guarded, 'identity', read_identity):
                        await controller.tick()
                        await controller.tick()
                    self.assertEqual(controller.child.signals, [] if expected_signal is None else [expected_signal])
                    self.assertEqual(controller.confounded, expected_signal is None)
                    samples = [event for event in controller.journal.events if event['event'] == 'sample']
                    self.assertEqual(len(samples), 2)
                    self.assertEqual(samples[-1]['after_stop'], expected_signal is not None)

    async def test_partial_host_sample_survives_awake_probe_timeout(self):
        controller = self.controller()
        raw = '\n'.join(f'{pid} {os.getpid()} {os.getuid()} Wed Oct 7 23:08:03 2026 0.0 1024 /private/fake-cli' for pid in [123455, 123456])

        async def probe(argv, timeout):
            if argv[0] == '/bin/ps':
                return raw
            raise asyncio.TimeoutError

        async def read_identity(pid, timeout):
            return owner(pid)

        with patch.object(guarded, 'probe', probe), patch.object(guarded, 'identity', read_identity):
            await controller.tick()
        partial = [event for event in controller.journal.events if event['event'] == 'sample_error'][-1]
        self.assertEqual(partial['host_process_count'], 2)
        self.assertTrue(partial['after_stop'])
        self.assertEqual(controller.child.signals, [signal.SIGUSR1])


class ValidationTests(unittest.TestCase):
    def profile_spec(self, name):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve()
        root.chmod(0o700)
        binary, connection, gate = [root / name for name in ['fake-cli', 'connection.json', 'gate.json']]
        for path in [binary, connection, gate]:
            path.write_bytes(b'not executed or decoded')
            path.chmod(0o600)
        binary.chmod(0o500)
        argv = [str(binary)]
        selected = parameters(name) if name in ('mixed', 'architecture', 'common') else parameters()
        for key, value in {**selected, 'connection': str(connection), 'output': str(root), 'joint-pass': str(gate)}.items():
            argv.extend(['-' + key, value])
        return {'phase_profile': name, 'command': argv, 'output': str(root), 'artifacts': {str(binary): hashlib.sha256(binary.read_bytes()).hexdigest()}, 'launcher': owner(123455), 'services': [], 'soft_stop_capable': True, 'hard_max_seconds': 100}

    def test_unknown_phase_profile_rejected(self):
        for name in ('unknown', 'all', 'arch', ''):
            with self.subTest(name=name), patch.object(guarded.os.path, 'lexists', return_value=False):
                with self.assertRaises(ValueError):
                    guarded.validate(self.profile_spec(name))

    def test_architecture_exact_profile_accepted(self):
        with patch.object(guarded.os.path, 'lexists', return_value=False):
            self.assertTrue(guarded.validate(self.profile_spec('architecture')))

    def test_common_exact_profile_accepted(self):
        with patch.object(guarded.os.path, 'lexists', return_value=False):
            self.assertTrue(guarded.validate(self.profile_spec('common')))

    def test_profile_budget_tampering_rejected(self):
        changes = {'rows': '2000', 'seed': '0', 'request-timeout': '11s', 'max-process-rss-bytes': '536870913', 'max-generated-bytes': '268435457', 'max-artifact-bytes': '67108865'}
        for name in ('mixed', 'architecture', 'common'):
            spec = self.profile_spec(name)
            with patch.object(guarded.os.path, 'lexists', return_value=False):
                guarded.validate(spec)
                for key, value in changes.items():
                    with self.subTest(name=name, budget=key):
                        command = list(spec['command'])
                        command[command.index('-' + key) + 1] = value
                        with self.assertRaisesRegex(ValueError, 'unexpected_phase_parameters'):
                            guarded.validate({**spec, 'command': command})

    def test_journal_write_failure_is_sticky_and_does_not_raise(self):
        journal = guarded.Journal.__new__(guarded.Journal)
        journal.failed = False

        class BrokenStream:
            def write(self, value):
                raise OSError('fake_full_disk')

        journal.stream = BrokenStream()
        with patch.object(guarded.sys, 'stderr'):
            first = journal.emit('STOP', reason='first')
            second = journal.emit('sample_error')
        self.assertTrue(journal.failed)
        self.assertEqual(first['reason'], 'first')
        self.assertEqual(second['event'], 'sample_error')

    def test_environment_allowlist_does_not_pass_keys(self):
        with patch.dict(os.environ, {'S3_SECRET_ACCESS_KEY': 'fake-no-real-key', 'PERI_TOKEN': 'fake', 'PATH': '/private/fake'}, clear=True):
            self.assertEqual(guarded.safe_environment(), {'PATH': '/private/fake'})

    def test_ancestry_classifies_owned_builder(self):
        table = {10: {'ppid': 1}, 11: {'ppid': 10}, 12: {'ppid': 11}, 15: {'ppid': 1}}
        self.assertTrue(guarded.is_owned(12, table, {10}))
        self.assertFalse(guarded.is_owned(15, table, {10}))
        self.assertFalse(guarded.is_owned(12, {12: {'ppid': 12}}, {10}))

    def test_process_sample_schema(self):
        data = guarded.processes('123456 123455 501 Wed Oct 7 23:08:03 2026 14.0 1024 /private/cargo\n')
        self.assertEqual(data[123456]['rss_bytes'], 1048576)
        self.assertEqual(data[123456]['start'], 'Wed Oct 7 23:08:03 2026')
        with self.assertRaises(ValueError):
            guarded.processes('bad metadata row')

    def test_spec_command_and_private_artifact_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            root.chmod(0o700)
            binary, connection, gate = [root / name for name in ['fake-cli', 'connection.json', 'gate.json']]
            for path in [binary, connection, gate]:
                path.write_bytes(b'not executed or decoded')
                path.chmod(0o600)
            binary.chmod(0o500)
            argv = [str(binary)]
            for key, value in {**guarded.MIXED, 'connection': str(connection), 'output': str(root), 'joint-pass': str(gate)}.items():
                argv.extend(['-' + key, value])
            spec = {'command': argv, 'output': str(root), 'artifacts': {str(binary): hashlib.sha256(binary.read_bytes()).hexdigest()}, 'launcher': owner(123455), 'services': [], 'soft_stop_capable': True, 'hard_max_seconds': 100}
            with patch.object(guarded.os.path, 'lexists', return_value=False):
                self.assertIn(str(binary), guarded.validate(spec))
                invalid = {**spec, 'external_builder_policy': 'record-confound'}
                with self.assertRaisesRegex(ValueError, 'functional_only'):
                    guarded.validate(invalid)
                invalid['functional_only'] = True
                guarded.validate(invalid)
                invalid = {**spec, 'command': [*argv, '-cases', 'all']}
                with self.assertRaises(ValueError):
                    guarded.validate(invalid)
                binary.chmod(0o700)
                with self.assertRaisesRegex(ValueError, 'immutable_CLI'):
                    guarded.validate(spec)

    def test_spec_above_legacy_limit_accepts_bounded_read(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory).resolve() / 'spec.json'
            expected = {'sources': {'synthetic': 'x' * (65 << 10)}}
            filename.write_text(json.dumps(expected))
            filename.chmod(0o600)
            self.assertGreater(filename.stat().st_size, 64 << 10)
            self.assertEqual(guarded.read_spec(filename), expected)

    def test_spec_above_new_limit_rejects_before_open(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory).resolve() / 'spec.json'
            filename.write_bytes(b' ' * (guarded.MAX_SPEC_BYTES + 1))
            filename.chmod(0o600)
            with patch.object(Path, 'open') as opened:
                with self.assertRaisesRegex(ValueError, 'oversized_spec'):
                    guarded.read_spec(filename)
                opened.assert_not_called()

    def test_spec_growth_after_stat_still_has_bounded_read(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory).resolve() / 'spec.json'
            filename.write_bytes(b'{}')
            filename.chmod(0o600)
            with patch.object(Path, 'open') as opened:
                reader = opened.return_value.__enter__.return_value
                reader.read.return_value = b' ' * (guarded.MAX_SPEC_BYTES + 1)
                with self.assertRaisesRegex(ValueError, 'oversized_spec'):
                    guarded.read_spec(filename)
                reader.read.assert_called_once_with(guarded.MAX_SPEC_BYTES + 1)


if __name__ == '__main__':
    unittest.main()
