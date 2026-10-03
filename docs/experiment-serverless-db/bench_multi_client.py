#!/usr/bin/env python3
"""Isolated Simple point-read benchmark with 1/2/4 independent load generators.

Each phase keeps 64 in-flight requests in total. CPU usage is process CPU time
divided by wall time, so 1.0 means one fully utilized CPU core.
"""

import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import multiprocessing as mp
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import time

import aiohttp
import psutil


CONCURRENCY = 64
LOADGENS = (1, 2, 4, 2, 1)
STAGE_RE = re.compile(r'^simple_stage_micros_(sum|count)\{stage="([^"]+)"\} ([\d.eE+\-]+)$')


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * fraction + 0.999999))]


def cpu_seconds(pid):
    usage = psutil.Process(pid).cpu_times()
    return usage.user + usage.system


def stage_totals(prometheus_text):
    totals = {}
    for line in prometheus_text.splitlines():
        match = STAGE_RE.match(line)
        if match:
            kind, stage, value = match.groups()
            totals.setdefault(stage, {})[kind] = float(value)
    return totals


def stage_deltas(before, after):
    result = {}
    for stage, current in stage_totals(after).items():
        previous = stage_totals(before).get(stage, {})
        count = int(current.get('count', 0) - previous.get('count', 0))
        total = current.get('sum', 0) - previous.get('sum', 0)
        if count > 0:
            result[stage] = {'count': count, 'sum_us': total, 'avg_us': total / count}
    return result


async def loadgen_async(base, path, token, concurrency, duration, ready, start, output):
    rows = []
    headers = {'Authorization': f'Bearer {token}'}
    payload = {'sql': 'SELECT v FROM bench WHERE id = 1'}
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=0)) as session:
        ready.set()
        # multiprocessing.Event.wait blocks this process only before request tasks exist.
        start.wait()
        cpu_before = cpu_seconds(os.getpid())
        phase_start = time.perf_counter()
        deadline = phase_start + duration

        async def requester(worker_id):
            index = 0
            while time.perf_counter() < deadline:
                index += 1
                begun = time.perf_counter_ns()
                try:
                    async with session.post(base + path, json=payload, headers=headers) as response:
                        body = await response.json(content_type=None)
                        status = response.status
                    ok = status == 200 and body.get('rows') == [[42]] and isinstance(body.get('elapsed_micros'), int)
                    error = '' if ok else 'bad_response'
                except Exception as exc:
                    status, ok, error = 0, False, type(exc).__name__
                rows.append((worker_id, index, (time.perf_counter_ns() - begun) / 1000, status, int(ok), error))

        await asyncio.gather(*(requester(i) for i in range(concurrency)))
        phase_end = time.perf_counter()
        client_cpu_seconds = cpu_seconds(os.getpid()) - cpu_before

    with gzip.open(output, 'wt', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(('worker', 'index', 'client_us', 'status', 'ok', 'error'))
        writer.writerows(rows)
    return {'start': phase_start, 'end': phase_end, 'cpu_seconds': client_cpu_seconds, 'samples': len(rows)}


def loadgen_main(base, path, token, concurrency, duration, ready, start, output, answers, index):
    try:
        answer = asyncio.run(loadgen_async(base, path, token, concurrency, duration, ready, start, output))
        answers.put((index, answer, None))
    except BaseException as exc:
        ready.set()
        answers.put((index, None, repr(exc)))


async def request(session, base, method, path, payload=None, token=None):
    headers = {'Authorization': f'Bearer {token}'} if token else {}
    async with session.request(method, base + path, json=payload, headers=headers) as response:
        body = await response.json(content_type=None)
        return response.status, body


async def metrics_snapshot(session, base, token):
    async with session.get(base + '/metrics', headers={'Authorization': f'Bearer {token}'}) as response:
        if response.status != 200:
            raise RuntimeError(f'/metrics returned {response.status}')
        return await response.text()


async def setup(session, base, data_dir):
    for _ in range(300):
        try:
            if (await request(session, base, 'GET', '/readyz'))[0] == 200:
                break
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        await asyncio.sleep(.1)
    else:
        raise RuntimeError('readiness timeout')
    initial = json.loads((data_dir / 'secrets/initial-admin.json').read_text())
    status, login = await request(session, base, 'POST', '/api/v1/auth/login', initial)
    assert status == 200
    token = login['access_token']
    status, created = await request(session, base, 'POST', '/api/v1/databases', {'name': 'multi-client'}, token)
    assert status == 202
    for _ in range(300):
        status, operation = await request(session, base, 'GET', '/api/v1/operations/' + created['operation_id'], token=token)
        assert status == 200
        if operation['state'] == 'SUCCEEDED':
            break
        assert operation['state'] not in ('FAILED', 'CANCELLED')
        await asyncio.sleep(.1)
    else:
        raise RuntimeError('create timeout')
    path = '/data/v1/databases/' + created['database_id'] + '/query'
    for sql in ('CREATE TABLE bench (id INTEGER PRIMARY KEY, v INTEGER)', 'INSERT INTO bench VALUES (1, 42)'):
        status, body = await request(session, base, 'POST', path, {'sql': sql}, token)
        assert status == 200 and 'elapsed_micros' in body
    for _ in range(100):
        status, body = await request(session, base, 'POST', path, {'sql': 'SELECT v FROM bench WHERE id = 1'}, token)
        assert status == 200 and body['rows'] == [[42]]
    return path, token


def summarize_samples(files):
    latencies, errors = [], {}
    requests = success = 0
    for file in files:
        with gzip.open(file, 'rt', newline='') as handle:
            for row in csv.DictReader(handle):
                requests += 1
                if row['ok'] == '1':
                    success += 1
                    latencies.append(float(row['client_us']) / 1000)
                else:
                    errors[row['error']] = errors.get(row['error'], 0) + 1
    return requests, success, latencies, errors


async def main(args):
    binary = pathlib.Path(args.binary).resolve(strict=True)
    output = pathlib.Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    summaries = []
    with tempfile.TemporaryDirectory(prefix='peri-multi-client-') as tmp:
        data_dir = pathlib.Path(tmp) / 'data'
        env = os.environ.copy()
        if args.stage_metrics:
            env['PERI_LOOM_SIMPLE_STAGE_METRICS'] = '1'
        else:
            env.pop('PERI_LOOM_SIMPLE_STAGE_METRICS', None)
        with (output / 'server.log').open('w') as log:
            server = subprocess.Popen([str(binary), 'serve', '--mode', 'simple', '--data-dir', str(data_dir), '--listen', f'127.0.0.1:{port}', '--log-level', 'error'], stdout=log, stderr=log, env=env)
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    path, token = await setup(session, base, data_dir)
                    for phase_index, process_count in enumerate(args.loadgens, 1):
                        await asyncio.sleep(args.rest)
                        before_metrics = await metrics_snapshot(session, base, token) if args.stage_metrics else None
                        ctx = mp.get_context('spawn')
                        start = ctx.Event()
                        answers = ctx.Queue()
                        processes, ready_events, files = [], [], []
                        for index in range(process_count):
                            ready = ctx.Event()
                            file = output / f'phase{phase_index}_clients{process_count}_loadgen{index}.csv.gz'
                            process = ctx.Process(target=loadgen_main, args=(base, path, token, CONCURRENCY // process_count, args.duration, ready, start, str(file), answers, index))
                            process.start()
                            processes.append(process)
                            ready_events.append(ready)
                            files.append(file)
                        try:
                            for ready in ready_events:
                                if not await asyncio.to_thread(ready.wait, 30):
                                    raise RuntimeError('loadgen readiness timeout')
                            server_cpu_before = cpu_seconds(server.pid)
                            start.set()
                            received = [await asyncio.to_thread(answers.get, True, args.duration + 30) for _ in processes]
                            server_cpu_after = cpu_seconds(server.pid)
                            for process in processes:
                                await asyncio.to_thread(process.join, 10)
                                if process.exitcode != 0:
                                    raise RuntimeError(f'loadgen exited {process.exitcode}')
                            for _, _, error in received:
                                if error:
                                    raise RuntimeError(f'loadgen failed: {error}')
                            results = [item[1] for item in received]
                            wall = max(item['end'] for item in results) - min(item['start'] for item in results)
                            requests, success, latencies, errors = summarize_samples(files)
                            summary = {
                                'phase': phase_index, 'loadgen_processes': process_count, 'total_concurrency': CONCURRENCY,
                                'duration_s': wall, 'requests': requests, 'success': success,
                                'errors': requests - success, 'error_kinds': errors,
                                'success_rps': success / wall,
                                'http_p50_ms': percentile(latencies, .5) if latencies else None,
                                'http_p95_ms': percentile(latencies, .95) if latencies else None,
                                'http_p99_ms': percentile(latencies, .99) if latencies else None,
                                'server_cpu_cores': (server_cpu_after - server_cpu_before) / wall,
                                'client_cpu_cores': sum(item['cpu_seconds'] for item in results) / wall,
                                'sample_files': [file.name for file in files],
                            }
                            if args.stage_metrics:
                                after_metrics = await metrics_snapshot(session, base, token)
                                (output / f'phase{phase_index}_clients{process_count}_metrics_before.prom').write_text(before_metrics)
                                (output / f'phase{phase_index}_clients{process_count}_metrics_after.prom').write_text(after_metrics)
                                summary['stage_deltas'] = stage_deltas(before_metrics, after_metrics)
                            summaries.append(summary)
                            print(json.dumps(summary), flush=True)
                        finally:
                            start.set()
                            for process in processes:
                                if process.is_alive():
                                    process.terminate()
                                process.join(timeout=5)
            finally:
                server.terminate()
                try:
                    server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
    result = {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'mode': 'simple', 'workload': 'SELECT v FROM bench WHERE id = 1; rows [[42]]',
              'loadgen_processes': args.loadgens, 'total_concurrency': CONCURRENCY,
              'duration_per_phase_s': args.duration, 'rest_s': args.rest,
              'stage_metrics': args.stage_metrics,
              'cpu_cores_definition': '(process user + system CPU seconds) / phase wall seconds',
              'summary': summaries}
    (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', default='target/release/peri-loom')
    parser.add_argument('--output', default='docs/experiment-serverless-db/raw/multi_client')
    parser.add_argument('--duration', type=float, default=8)
    parser.add_argument('--rest', type=float, default=2)
    parser.add_argument('--loadgens', nargs='+', type=int, choices=(1, 2, 4), default=LOADGENS)
    parser.add_argument('--stage-metrics', action='store_true')
    asyncio.run(main(parser.parse_args()))
