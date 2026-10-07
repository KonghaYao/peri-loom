"""Python >=3.12/macOS: python3 run_guarded.py --spec /private/operator.json; no API/keys/env dumps.
0600 spec: exact mixed command, artifacts/sources={absolute:sha256}, output0700, launcher/services/owner_roots={pid,ppid,uid,start,argv}, soft_stop_capable=true.
hard_max_seconds<=3600; hard_grace_seconds=5 (0.1..30); interval_seconds=2/probe_timeout_seconds=2; RSS<=512MiB/output<=64MiB. strict-stop default; record-confound requires functional_only=true.
Journal: wall/mono, owner proof, sticky STOP, host/awake, retainedPID/mayunknown/no-clean; no implicit retained-child kill, never performance_pass.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
FIELDS = ('pid', 'ppid', 'uid', 'start', 'argv')
BUILDERS = {'cargo', 'rustc', 'clippy-driver', 'go', 'compile', 'link', 'clang', 'clang++', 'cc', 'gcc', 'make', 'ninja', 'esbuild'}
MIXED = {'cases': 'mixed_oltp', 'concurrency': '1,4', 'warmup': '2s', 'smoke': '10s', 'measure': '30s', 'offered-rate': '50', 'rows': '1000', 'seed': '20261007', 'request-timeout': '10s', 'max-process-rss-bytes': '536870912', 'max-generated-bytes': '268435456', 'max-artifact-bytes': '67108864'}
def require(condition, reason):
    if not condition:
        raise ValueError(reason)
def private(path, mode, directory=False):
    metadata = path.lstat()
    require(path.is_absolute() and path.resolve() == path and metadata.st_uid == os.getuid(), 'unsafe_path_owner')
    require(stat.S_IMODE(metadata.st_mode) == mode and (stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode)), 'unsafe_private_mode')
def proof(observed, expected):
    return all(observed.get(key) == expected.get(key) for key in FIELDS)
def metadata(path):
    info = Path(path).lstat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_mode]
def validate(spec):
    require(sys.version_info >= (3, 12), 'Python_3.12_or_newer_required')
    require(not set(spec) - {'output', 'command', 'artifacts', 'launcher', 'services', 'soft_stop_capable', 'hard_max_seconds', 'hard_grace_seconds', 'sources', 'owner_roots', 'interval_seconds', 'probe_timeout_seconds', 'max_process_rss_bytes', 'max_output_bytes', 'external_builder_policy', 'functional_only'}, 'unknown_spec_fields')
    output = Path(spec['output'])
    private(output, 0o700, True)
    require(spec.get('soft_stop_capable') is True, 'matching_SIGUSR1_CLI_attestation_required')
    policy = spec.get('external_builder_policy', 'strict-stop')
    require(policy in ('strict-stop', 'record-confound'), 'invalid_builder_policy')
    require(policy != 'record-confound' or spec.get('functional_only') is True, 'confound_requires_functional_only')
    for key, default, minimum, maximum in [('hard_max_seconds', None, 1, 3600), ('hard_grace_seconds', 5, .1, 30), ('interval_seconds', 2, .1, 10), ('probe_timeout_seconds', 2, .1, 5), ('max_process_rss_bytes', 536870912, 1, 536870912), ('max_output_bytes', 67108864, 1024, 67108864)]:
        value = spec.get(key, default)
        require(isinstance(value, (int, float)) and not isinstance(value, bool) and minimum <= value <= maximum, 'invalid_' + key)
        spec[key] = value
    argv = spec['command']
    require(isinstance(argv, list) and len(argv) == 1 + 2 * (len(MIXED) + 3), 'unexpected_phase_command')
    require(all(isinstance(item, str) and re.fullmatch(r'[!-~]+', item) for item in argv), 'unsafe_argv')
    options = {}
    for offset in range(1, len(argv), 2):
        flag, value = argv[offset:offset + 2]
        require(flag.startswith('-') and not flag.startswith('--') and flag[1:] not in options, 'duplicate_or_invalid_flag')
        options[flag[1:]] = value
    require(set(options) == set(MIXED) | {'connection', 'output', 'joint-pass'} and all(options[key] == value for key, value in MIXED.items()), 'unexpected_phase_parameters')
    require(options['output'] == str(output), 'output_command_mismatch')
    for key in ['connection', 'joint-pass']:
        private(Path(options[key]), 0o600)
        private(Path(options[key]).parent, 0o700, True)
    require(argv[0] in spec['artifacts'] and Path(argv[0]).is_absolute(), 'frozen_CLI_hash_required')
    require(stat.S_IMODE(Path(argv[0]).lstat().st_mode) == 0o500, 'immutable_CLI_mode_required')
    for item in [spec['launcher'], *spec['services'], *spec.get('owner_roots', [])]:
        require(set(FIELDS) <= set(item) and type(item['pid']) is int and item['pid'] > 1 and item['uid'] == os.getuid(), 'invalid_owned_identity')
    require(len({item['pid'] for item in spec['services']}) == len(spec['services']), 'duplicate_service_identity')
    for name in [output / '.guarded.lock', output / '.stress.lock', Path('/tmp/peri-loom-stress.global.lock')]:
        require(not os.path.lexists(name), 'existing_lock_HOLD')
    snapshots = {}
    require(len(spec['artifacts']) + len(spec.get('sources', {})) <= 2048, 'bounded_freeze_required')
    for filename, expected in {**spec.get('sources', {}), **spec['artifacts']}.items():
        path = Path(filename)
        require(path.is_absolute() and path.resolve() == path and path.is_file() and filename != options['connection'], 'invalid_frozen_path')
        require('keys' not in path.parts and path.suffix != '.key' and path.name not in ('connection.json', 'launch.json', 'platform.json', '.env') and path.stat().st_size <= 64 << 20, 'secret_or_oversized_frozen_path')
        require(re.fullmatch(r'[0-9a-f]{64}', expected) is not None, 'invalid_sha256')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == expected, 'frozen_input_changed')
        snapshots[filename] = metadata(path)
    return snapshots
async def probe(argv, timeout):
    child = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, env=safe_environment())
    try:
        stdout, _ = await asyncio.wait_for(child.communicate(), timeout)
        require(child.returncode == 0 and len(stdout) <= 2 * 1024 * 1024, 'probe_failed_or_oversized')
        return stdout.decode('utf-8', errors='strict')
    except BaseException:
        if child.returncode is None:
            child.kill()
            await child.wait()
        raise
async def identity(pid, timeout):
    raw = await probe(['/bin/ps', '-p', str(pid), '-o', 'pid=,ppid=,uid=,lstart=,command='], timeout)
    values = raw.strip().split(None, 8)
    require(len(values) == 9, 'missing_identity')
    return {'pid': int(values[0]), 'ppid': int(values[1]), 'uid': int(values[2]), 'start': ' '.join(values[3:8]), 'argv': values[8]}
def processes(raw):
    result = {}
    for line in raw.splitlines():
        fields = line.strip().split(None, 10)
        require(len(fields) == 11, 'invalid_process_sample')
        result[int(fields[0])] = {'pid': int(fields[0]), 'ppid': int(fields[1]), 'uid': int(fields[2]), 'start': ' '.join(fields[3:8]), 'cpu_percent': float(fields[8]), 'rss_bytes': int(fields[9]) * 1024, 'executable': fields[10]}
    return result
def is_owned(pid, table, roots):
    visited = set()
    while pid not in visited:
        if pid in roots:
            return True
        if pid not in table:
            return False
        visited.add(pid)
        pid = table[pid]['ppid']
    return False
def safe_environment():
    return {name: os.environ[name] for name in ('PATH', 'HOME', 'TMPDIR', 'LANG', 'TZ') if name in os.environ}

class Journal:
    def __init__(self, output):
        self.failed = False
        self.stream = os.fdopen(os.open(output / 'guarded-events.jsonl', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w')
    def emit(self, event, **fields):
        record = {'event': event, 'wall_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(), **fields}
        try:
            self.stream.write(json.dumps(record, separators=(',', ':')) + '\n')
            self.stream.flush()
            os.fsync(self.stream.fileno())
        except OSError:
            if not self.failed:
                print('guard audit unavailable; HOLD/hard safety', file=sys.stderr)
            self.failed = True
        return record

class OwnedProcess:
    def __init__(self, argv, stdout, stderr):
        self.process = subprocess.Popen(argv, stdout=stdout, stderr=stderr, env=safe_environment(), start_new_session=True)
        self.pid = self.process.pid
    @property
    def returncode(self):
        return self.process.poll()
    def send_signal(self, selected):
        self.process.send_signal(selected)
    async def wait(self):
        while self.returncode is None:
            await asyncio.sleep(.05)
        return self.returncode

class Controller:
    def __init__(self, journal, spec, frozen):
        self.journal, self.spec, self.frozen = journal, spec, frozen
        self.child = self.owner = self.caff = self.caff_owner = None
        self.first_stop = None
        self.soft_sent = self.hard_sent = self.confounded = False
        self.join_deadline_ns = self.signal_refusal_proof = None
        self.signal_refused = False
        self.origin = (time.time_ns(), time.monotonic_ns())
    def stop(self, reason):
        if self.first_stop is None:
            self.first_stop = self.journal.emit('STOP', reason=reason, status='HOLD')
        else:
            self.journal.emit('guard_error_after_STOP', reason=reason)
    def bound_join(self, reason):
        if self.join_deadline_ns is None:
            self.join_deadline_ns = time.monotonic_ns() + int(self.spec.get('hard_grace_seconds', 5) * 1e9)
            self.journal.emit('join_grace_started', reason=reason, deadline_monotonic_ns=self.join_deadline_ns)
    async def send(self, target, owner, selected):
        observed = None
        try:
            require((target is self.child or target is self.caff) and owner is not None and owner['pid'] == target.pid and owner['uid'] == os.getuid() and owner['ppid'] == os.getpid(), 'not_exact_owned_spawn')
            observed = await identity(target.pid, self.spec['probe_timeout_seconds'])
            require(proof(observed, owner), 'signal_identity_mismatch')
            target.send_signal(selected)
            self.journal.emit('owned_signal', signal=selected.name, proof=observed, may_have_unknown=selected == signal.SIGTERM and target is self.child, first_stop=self.first_stop)
            return True
        except Exception as error:
            self.stop('signal_refused_' + type(error).__name__)
            if target is self.child:
                self.signal_refused = True
                self.signal_refusal_proof = {'expected': owner, 'observed': observed, 'error_type': type(error).__name__}
                self.journal.emit('signal_refused', proof=self.signal_refusal_proof)
                self.bound_join('signal_refused')
            return False
    async def tick(self):
        wall, mono = time.time_ns(), time.monotonic_ns()
        drift = (wall - self.origin[0]) - (mono - self.origin[1])
        hard = (mono - self.origin[1]) / 1e9 > self.spec['hard_max_seconds']
        table = {}
        try:
            if self.child is not None and self.child.returncode is None and self.owner is None:
                observed = await identity(self.child.pid, self.spec['probe_timeout_seconds'])
                require(observed['uid'] == os.getuid() and observed['ppid'] == os.getpid() and observed['argv'] == ' '.join(self.spec['command']), 'CLI_spawn_identity')
                self.owner = observed
                self.journal.emit('CLI_identity_recovered', proof=observed)
            raw, assertions = await asyncio.gather(probe(['/bin/ps', '-axo', 'pid=,ppid=,uid=,lstart=,pcpu=,rss=,comm='], self.spec['probe_timeout_seconds']), probe(['/usr/bin/pmset', '-g', 'assertions'], self.spec['probe_timeout_seconds']), return_exceptions=True)
            require(not isinstance(raw, BaseException), 'host_probe_unavailable')
            table = processes(raw)
            expected = [self.spec['launcher'], *self.spec['services'], *self.spec.get('owner_roots', [])]
            if self.child is not None and self.child.returncode is None:
                expected.append(self.owner)
            observed_owners = await asyncio.gather(*(identity(owner['pid'], self.spec['probe_timeout_seconds']) for owner in expected))
            for owner, observed in zip(expected, observed_owners):
                require(proof(observed, owner), 'owned_identity_changed')
                hard = hard or table[owner['pid']]['rss_bytes'] > self.spec['max_process_rss_bytes']
            require(not isinstance(assertions, BaseException), 'awake_probe_unavailable')
            if self.caff is not None:
                require(proof(await identity(self.caff.pid, self.spec['probe_timeout_seconds']), self.caff_owner), 'awake_identity_changed')
                require(any(re.search(r'pid\s+' + str(self.caff.pid) + r'\(caffeinate\)', line) and 'PreventUserIdleSystemSleep' in line for line in assertions.splitlines()), 'owned_awake_assertion_missing')
            require(wall >= self.origin[0] and abs(drift) <= 1_000_000_000, 'clock_jump')
            for filename, expected_metadata in self.frozen.items():
                require(metadata(filename) == expected_metadata, 'frozen_metadata_changed')
            roots = {os.getpid(), self.spec['launcher']['pid'], *(item['pid'] for item in self.spec.get('owner_roots', []))}
            if self.child is not None:
                roots.add(self.child.pid)
            builders = [{**entry, 'owned': is_owned(pid, table, roots)} for pid, entry in table.items() if Path(entry['executable']).name in BUILDERS]
            output_bytes = 0
            for count, path in enumerate(Path(self.spec['output']).glob('**/*')):
                require(count < 4096, 'hard_output_inventory_unbounded')
                if path.is_file() and not path.is_symlink():
                    output_bytes += path.stat().st_size
            hard = hard or output_bytes > self.spec['max_output_bytes']
            self.journal.emit('sample', observed_wall_ns=wall, observed_monotonic_ns=mono, processes=[table[owner['pid']] for owner in expected], host_process_count=len(table), host_ps_cpu_sum=sum(entry['cpu_percent'] for entry in table.values()), builders=builders, awake_owner=self.caff_owner, awake_effective=self.caff is not None, clock_delta_ns=drift, output_bytes=output_bytes, after_stop=self.first_stop is not None)
            for builder in builders:
                if builder['owned'] or self.spec.get('external_builder_policy', 'strict-stop') == 'strict-stop':
                    self.stop('owned_builder' if builder['owned'] else 'external_builder')
                else:
                    self.confounded = True
                    self.journal.emit('external_builder_confound', builder=builder, functional_only=True, performance_pass=False)
        except Exception as error:
            self.stop('monitor_' + type(error).__name__ + ('_' + str(error) if isinstance(error, ValueError) else ''))
            hard = hard or isinstance(error, ValueError) and str(error).startswith('hard_')
            self.journal.emit('sample_error', observed_wall_ns=wall, observed_monotonic_ns=mono, error_type=type(error).__name__, clock_delta_ns=drift, host_process_count=len(table) if table else None, host_ps_cpu_sum=sum(entry['cpu_percent'] for entry in table.values()) if table else None, after_stop=True)
        if getattr(self.journal, 'failed', False):
            self.stop('audit_write_failed')
            hard = True
        if hard:
            self.stop('hard_safety_budget')
            if self.child is not None and self.child.returncode is None:
                self.bound_join('hard_safety_budget')
        if self.child is not None and self.child.returncode is None:
            if hard and not self.hard_sent and not self.signal_refused:
                self.hard_sent = await self.send(self.child, self.owner, signal.SIGTERM)
            elif self.first_stop is not None and not self.soft_sent and not self.hard_sent and not self.signal_refused:
                self.soft_sent = await self.send(self.child, self.owner, signal.SIGUSR1)
    async def run(self):
        try:
            await self.tick()
            require(self.first_stop is None, 'preflight_STOP_no_CLI')
            argv = ['/usr/bin/caffeinate', '-i', '-t', str(int(self.spec['hard_max_seconds']) + 60), '-w', str(self.spec['launcher']['pid'])]
            self.caff = OwnedProcess(argv, subprocess.DEVNULL, subprocess.DEVNULL)
            self.caff_owner = await identity(self.caff.pid, self.spec['probe_timeout_seconds'])
            require(self.caff_owner['uid'] == os.getuid() and self.caff_owner['ppid'] == os.getpid() and self.caff_owner['argv'] == ' '.join(argv), 'caffeinate_spawn_identity')
            self.journal.emit('caffeinate_started', proof=self.caff_owner)
            await self.tick()
            require(self.first_stop is None, 'awake_preflight_STOP_no_CLI')
            output = Path(self.spec['output'])
            with os.fdopen(os.open(output / 'guarded-cli.stdout', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stdout, os.fdopen(os.open(output / 'guarded-cli.stderr', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stderr:
                self.child = OwnedProcess(self.spec['command'], stdout, stderr)
                self.owner = await identity(self.child.pid, self.spec['probe_timeout_seconds'])
                require(self.owner['uid'] == os.getuid() and self.owner['ppid'] == os.getpid() and self.owner['argv'] == ' '.join(self.spec['command']), 'CLI_spawn_identity')
                self.journal.emit('CLI_started', proof=self.owner, command=self.spec['command'])
                await self.monitor()
        except (Exception, asyncio.CancelledError) as error:
            self.stop('operator_' + type(error).__name__ + ('_' + str(error) if isinstance(error, ValueError) else ''))
            if self.child is not None and self.child.returncode is None:
                await self.monitor()
        finally:
            if self.caff is not None and self.caff.returncode is None:
                await self.send(self.caff, self.caff_owner, signal.SIGTERM)
                try:
                    await asyncio.wait_for(self.caff.wait(), 5)
                except asyncio.TimeoutError:
                    self.stop('owned_caffeinate_cleanup_unresolved')
            try:
                if self.child is not None and self.child.returncode is None:
                    self.journal.emit('after_checks_deferred', reason='retained_CLI_not_clean')
                else:
                    validate_after = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in self.frozen}
                    require(validate_after == {**self.spec.get('sources', {}), **self.spec['artifacts']}, 'final_frozen_input_changed')
                    self.journal.emit('readonly_artifacts_after', sha256=validate_after)
            except Exception as error:
                self.stop('after_' + type(error).__name__)
        return self.finish()
    def finish(self):
        if getattr(self.journal, 'failed', False):
            self.stop('audit_write_failed')
        alive = self.child is not None and self.child.returncode is None
        result = {'status': 'HOLD' if self.first_stop is not None or self.child is None or self.child.returncode != 0 else 'FUNCTIONAL_ONLY_CONFOUNDED' if self.confounded else 'CHILD_COMPLETE_PENDING_VERIFIER', 'child_exit': None if self.child is None else self.child.returncode, 'child_alive': alive, 'retained_pid': self.child.pid if alive else None, 'owned_spawn_proof': self.owner, 'signal_refusal_proof': self.signal_refusal_proof, 'clean_join': not alive and self.child is not None and self.child.returncode == 0 and self.first_stop is None and not self.hard_sent, 'first_stop': self.first_stop, 'soft_stop_sent': self.soft_sent, 'hard_stop_sent': self.hard_sent, 'may_have_unknown': self.hard_sent or alive, 'confounded': self.confounded, 'performance_pass': False, 'caffeinate_exit': None if self.caff is None else self.caff.returncode, 'retained_caffeinate_pid': self.caff.pid if self.caff is not None and self.caff.returncode is None else None}
        self.journal.emit('final', **result)
        return (1 if result['status'] == 'HOLD' else 0), result
    async def monitor(self):
        joined = asyncio.create_task(self.child.wait())
        try:
            while self.child.returncode is None:
                remaining = None if self.join_deadline_ns is None else (self.join_deadline_ns - time.monotonic_ns()) / 1e9
                if remaining is not None and remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self.tick(), remaining)
                    remaining = None if self.join_deadline_ns is None else (self.join_deadline_ns - time.monotonic_ns()) / 1e9
                    if remaining is not None and remaining <= 0:
                        break
                    await asyncio.wait_for(asyncio.shield(joined), min(self.spec['interval_seconds'], remaining) if remaining is not None else self.spec['interval_seconds'])
                except asyncio.TimeoutError:
                    continue
            if self.child.returncode is None:
                self.stop('join_grace_expired_CLI_retained')
                self.journal.emit('CLI_retained', pid=self.child.pid, proof=self.owner, may_have_unknown=True, clean_join=False)
            else:
                await self.tick()
                self.journal.emit('CLI_exit', exit=self.child.returncode, natural_drain=not self.hard_sent)
        finally:
            if not joined.done():
                joined.cancel()
            await asyncio.gather(joined, return_exceptions=True)
async def execute(controller):
    loop = asyncio.get_running_loop()
    previous = {selected: signal.getsignal(selected) for selected in (signal.SIGINT, signal.SIGTERM)}
    for selected in previous:
        loop.add_signal_handler(selected, controller.stop, 'operator_' + selected.name)
    try:
        return await controller.run()
    finally:
        for selected, handler in previous.items():
            loop.remove_signal_handler(selected)
            signal.signal(selected, handler)
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--spec', required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    journal = None
    lock = None
    try:
        private(args.spec, 0o600)
        require(args.spec.stat().st_size <= 64 << 10, 'oversized_spec')
        spec = json.loads(args.spec.read_text())
        frozen = validate(spec)
        output = Path(spec['output'])
        lock = os.open(output / '.guarded.lock', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        journal = Journal(output)
        journal.emit('readonly_artifacts_before', metadata=frozen, spec_sha256=hashlib.sha256(args.spec.read_bytes()).hexdigest(), external_builder_policy=spec.get('external_builder_policy', 'strict-stop'))
        exit_code, result = asyncio.run(execute(Controller(journal, spec, frozen)))
        print(json.dumps(result))
        return exit_code
    except Exception as error:
        print(json.dumps({'status': 'HOLD', 'error_type': type(error).__name__, 'reason': str(error) if isinstance(error, ValueError) else 'private_error_details_withheld'}))
        return 1
    finally:
        if journal is not None:
            journal.stream.close()
        if lock is not None:
            if os.path.lexists(output / '.guarded.lock') and metadata(output / '.guarded.lock')[:2] == [os.fstat(lock).st_dev, os.fstat(lock).st_ino]:
                (output / '.guarded.lock').unlink()
            os.close(lock)
if __name__ == '__main__':
    sys.exit(main())
