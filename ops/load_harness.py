"""Reproducible HTTP load and OFFLINE analysis, standard library only.

Network timing ends after reading bytes, before JSON decoding, hashing, evidence
writes and validation. Failed requests, scheduled drops and incomplete workers
remain in the denominator. No request retry, oracle, node counting or result
validation is performed on the measured server's query path.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import http.client
import importlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import random
import re
import threading
import time
from urllib.parse import urlsplit


SECRET = re.compile(r'password|passwd|secret|token|authorization|cookie|api.?key', re.I)
TEMPLATE = re.compile(r'\{\{(client_id|request_index|run_id)\}\}')


def redact(value):
    if isinstance(value, dict):
        return {k: '[REDACTED]' if SECRET.search(k) else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def redact_known_values(value, credentials):
    """Also remove request credentials if a server accidentally echoes them."""
    value = redact(value)
    if isinstance(value, dict):
        return {key: redact_known_values(item, credentials) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_known_values(item, credentials) for item in value]
    if isinstance(value, str):
        for secret in credentials:
            if secret:
                value = value.replace(secret, '[REDACTED]')
    return value


def substitute(value, variables):
    if isinstance(value, dict):
        return {k: substitute(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, variables) for v in value]
    if not isinstance(value, str):
        return value
    match = TEMPLATE.fullmatch(value)
    if match:
        return variables[match.group(1)]
    return TEMPLATE.sub(lambda m: str(variables[m.group(1)]), value)


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for part in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(part)
    return digest.hexdigest()


def origin(value):
    url = urlsplit(value)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username
            or url.password or url.query or url.fragment or url.path not in ('', '/')):
        raise ValueError('base URL must be an HTTP(S) origin without credentials or path')
    port = url.port or (443 if url.scheme == 'https' else 80)
    return url.scheme, url.hostname, port


def read_workload(path):
    value = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    cases = value.get('cases')
    if value.get('version') != 'warehouse-load-v1' or not isinstance(cases, list) or not cases:
        raise ValueError('warehouse-load-v1 with nonempty cases required')
    names = set()
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get('name'), str) or case['name'] in names:
            raise ValueError('unique case names required')
        names.add(case['name'])
        if case.get('method', 'POST') not in ('GET', 'POST'):
            raise ValueError('only GET/POST workloads supported')
        if (not isinstance(case.get('path'), str) or not case['path'].startswith('/')
                or case['path'].startswith('//') or '#' in case['path']):
            raise ValueError('relative absolute-path request required')
        if case.get('kind', 'read') not in ('read', 'write'):
            raise ValueError('kind must be read or write')
        if case['path'].split('?', 1)[0] == '/api/control':
            raise ValueError('controller changes are not a query workload; use the separate owned-run controller')
        if (case.get('method', 'POST') == 'POST' and case['path'].split('?', 1)[0]
                not in ('/api/knowledge/query', '/api/agent/query') and case.get('kind') != 'write'):
            raise ValueError('unknown POST or observation endpoint must explicitly declare kind=write')
        if not isinstance(case.get('weight', 1), int) or case.get('weight', 1) <= 0:
            raise ValueError('positive integer weight required')
        if case.get('headers'):
            raise ValueError('literal headers disallowed; use headers_env to keep secrets out of evidence')
        if not isinstance(case.get('headers_env', {}), dict):
            raise ValueError('headers_env mapping required')
        for header, env in case.get('headers_env', {}).items():
            if not isinstance(env, str) or not re.fullmatch(r'[A-Za-z0-9_{}]+', env) or not re.fullmatch(r'[A-Za-z0-9-]+', header):
                raise ValueError('invalid header or environment variable name')
    return value


def _number_header(response, key):
    try:
        value = float(response.getheader(key))
        return value if math.isfinite(value) and value >= 0 else None
    except (TypeError, ValueError):
        return None


def request_once(connection, case, variables, request_id, scheduled_ns=None):
    body = substitute(case.get('body'), variables)
    path = substitute(case['path'], variables)
    headers = {'Content-Type': 'application/json', 'X-Probe-ID': request_id}
    credentials = []
    for key, env in case.get('headers_env', {}).items():
        env = str(substitute(re.sub(r'(?<!\{)\{client_id\}(?!\})', '{{client_id}}', env), variables))
        if env not in os.environ:
            raise ValueError('required header environment variable absent: ' + env)
        headers[key] = os.environ[env]
        if SECRET.search(key):
            credentials.append(headers[key])
            if headers[key].startswith('Bearer '):
                credentials.append(headers[key][7:])
    data = None if body is None else json.dumps(body, allow_nan=False).encode('utf-8')
    started = time.perf_counter_ns()
    status = None
    raw = b''
    error = None
    phases = {'start_ns': started}
    service = queue_ms = upstream = None
    try:
        connection.request(case.get('method', 'POST'), path, body=data, headers=headers)
        phases['send_complete_ns'] = time.perf_counter_ns()
        response = connection.getresponse()
        phases['headers_complete_ns'] = time.perf_counter_ns()
        status = response.status
        service = _number_header(response, 'X-Warehouse-Service-Ms')
        queue_ms = _number_header(response, 'X-Warehouse-Queue-Ms')
        upstream = _number_header(response, 'X-Warehouse-Upstream-Ms')
        raw = response.read()
    except Exception as exc:
        # Exception text can include credentials or a server-returned string.
        error = type(exc).__name__
        connection.close()
    ended = time.perf_counter_ns()
    phases['body_complete_or_error_ns'] = ended
    parsing = time.perf_counter_ns()
    parsed = None
    parse_error = None
    if raw:
        try:
            parsed = json.loads(raw)
        except (UnicodeDecodeError, ValueError):
            parse_error = 'invalid_json'
    elif error is None:
        parse_error = 'empty_response'
    parse_end = time.perf_counter_ns()
    outcome = 'transport_error' if error else 'http_error' if status != case.get('expected_status', 200) else 'parse_error' if parse_error else 'ok'
    return {'kind': 'request', 'request_id': request_id, 'client_id': variables['client_id'],
            'pid': os.getpid(), 'index': variables['request_index'], 'case': case['name'],
            'query_class': case.get('query_class', 'unspecified'), 'operation': case.get('kind', 'read'),
            'scheduled_ns': scheduled_ns, 'start_ns': started, 'end_ns': ended,
            'elapsed_ms': (ended - started) / 1e6, 'scheduler_lag_ms': max(0, (started - scheduled_ns) / 1e6) if scheduled_ns else 0,
            'end_to_end_json_ms': (parse_end-started)/1e6,
            'status': status, 'outcome': outcome, 'error': error, 'parse_error': parse_error,
            'parse_ms': (parse_end - parsing) / 1e6, 'service_ms': service, 'queue_ms': queue_ms,
            'upstream_ms': upstream, 'response_bytes': len(raw),
            'raw_response_sha256': hashlib.sha256(raw).hexdigest(), 'http_phases': phases,
            'request': redact_known_values(body, credentials),
            'response': redact_known_values(parsed, credentials)}


def _worker(index, clients, options, workload, ready, start, stop, shared_start):
    output = Path(options['output'])
    scheme, host, port = origin(options['base_url'])
    connection_type = http.client.HTTPSConnection if scheme == 'https' else http.client.HTTPConnection
    ready.put({'index': index, 'pid': os.getpid(), 'clients': clients})
    if not start.wait(options['startup_timeout'] + 10):
        raise TimeoutError('parent start barrier timeout')
    begun = shared_start.value
    duration_ns = int(options['duration'] * 1e9)
    deadline = begun + duration_ns

    def client(client_id):
        rng = random.Random(options['seed'] + client_id)
        cases = workload['cases']
        weights = [case.get('weight', 1) for case in cases]
        count = 0
        connection = connection_type(host, port, timeout=options['timeout'])
        if options['mode'] == 'open':
            period = int(options['clients'] / options['rate'] * 1e9)
            next_at = begun + int((client_id - 1) / options['rate'] * 1e9)
        elif options['mode'] == 'periodic':
            period = int(options['interval'] * 1e9)
            next_at = begun + int((client_id - 1) / options['clients'] * period)
        else:
            period, next_at = None, begun
        path = output / f'client-{client_id:04}.jsonl.gz'
        try:
            with gzip.open(path, 'xt', encoding='utf-8', compresslevel=1) as stream:
                while not stop.is_set() and time.perf_counter_ns() < deadline:
                    if options['requests'] and count >= options['requests']:
                        break
                    if next_at >= deadline:
                        break
                    if stop.wait(max(0., (next_at - time.perf_counter_ns()) / 1e9)):
                        break
                    count += 1
                    case = rng.choices(cases, weights=weights, k=1)[0]
                    variables = {'client_id': client_id, 'request_index': count, 'run_id': options['run_id']}
                    rid = f"{options['session']}-c{client_id:04}-n{count:08}"
                    try:
                        row = request_once(connection, case, variables, rid, next_at if period else None)
                    except Exception as exc:
                        row = {'kind': 'request', 'request_id': rid, 'client_id': client_id,
                               'pid': os.getpid(), 'case': case['name'], 'index': count,
                               'query_class': case.get('query_class', 'unspecified'),
                               'operation': case.get('kind', 'read'), 'outcome': 'generator_error',
                               'error': type(exc).__name__, 'elapsed_ms': None}
                    stream.write(json.dumps(row, allow_nan=False, separators=(',', ':')) + '\n')
                    if period:
                        next_at += period
                        # Fixed offered arrivals. A busy one-inflight robot drops arrivals;
                        # it does not replay missed arrivals as an unbounded catchup burst.
                        busy_until = time.perf_counter_ns()
                        while next_at < min(busy_until, deadline) and (not options['requests'] or count < options['requests']):
                            count += 1
                            dropped = {'kind': 'request', 'request_id': f"{options['session']}-c{client_id:04}-n{count:08}",
                                       'client_id': client_id, 'pid': os.getpid(), 'index': count,
                                       'case': 'not_dispatched', 'query_class': 'unspecified',
                                       'outcome': 'load_generator_busy', 'scheduled_ns': next_at,
                                       'elapsed_ms': None}
                            stream.write(json.dumps(dropped, separators=(',', ':')) + '\n')
                            next_at += period
                    else:
                        next_at = time.perf_counter_ns() + int(options['think'] * 1e9)
        finally:
            connection.close()
        return {'client_id': client_id, 'planned_requests': count}

    counts = []
    try:
        with ThreadPoolExecutor(max_workers=len(clients)) as executor:
            counts = list(executor.map(client, clients))
    finally:
        (output / f'process-{index:04}.json').write_text(json.dumps(
            {'index': index, 'pid': os.getpid(), 'clients': clients, 'counts': counts,
             'ended_ns': time.perf_counter_ns()}, indent=2), encoding='utf-8')


def run(args):
    options = vars(args).copy()
    options.pop('func', None)
    options.pop('command', None)
    options['output'] = str(args.output.resolve())
    options['workload'] = str(args.workload.resolve())
    options['identity'] = str(args.identity.resolve()) if args.identity else None
    origin(args.base_url)
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', args.session):
        raise ValueError('session must be an identifier of at most 64 characters')
    if not 1 <= args.processes <= args.clients <= 1000:
        raise ValueError('require 1 <= processes <= clients <= 1000')
    for name in ('duration', 'timeout', 'startup_timeout'):
        if not math.isfinite(options[name]) or options[name] <= 0:
            raise ValueError(name + ' must be positive finite')
    if args.duration > 86400 or args.timeout > 300 or args.requests < 0:
        raise ValueError('duration <= 86400, timeout <= 300 and nonnegative requests required')
    if args.mode == 'open' and (not math.isfinite(args.rate) or args.rate <= 0 or args.rate > 1e6):
        raise ValueError('open mode requires positive --rate <= 1000000 requests/second')
    if args.mode == 'periodic' and (not math.isfinite(args.interval) or args.interval <= 0):
        raise ValueError('periodic mode requires positive --interval')
    if not math.isfinite(args.think) or args.think < 0:
        raise ValueError('nonnegative finite think required')
    workload = read_workload(args.workload)
    supplied_identity = None
    if args.identity:
        supplied_identity = json.loads(args.identity.read_text(encoding='utf-8-sig'))
        if not isinstance(supplied_identity, dict):
            raise ValueError('identity evidence must be a JSON object')
    if any(c.get('kind') == 'write' for c in workload['cases']) and not args.allow_writes:
        raise ValueError('write workloads require --allow-writes; use an isolated run/database')
    args.output.mkdir(parents=True, exist_ok=False)
    identity = {'version': 'warehouse-load-run-v1', 'options': redact(options),
                'workload': redact(workload), 'workload_sha256': sha_file(args.workload),
                'script_sha256': sha_file(__file__), 'started_unix_ns': time.time_ns(),
                'clock': 'perf_counter_ns_same_host', 'qualification': 'not_evaluated',
                'phase': args.phase, 'cold_claim': 'operator_declared_no_server_restart_or_cache_flush_by_harness'}
    identity['supplied_evidence'] = redact(supplied_identity)
    identity['supplied_evidence_sha256'] = sha_file(args.identity) if args.identity else None
    identity['supplied_evidence_verification'] = 'captured_input_only; external source/schema/watermark proofs require independent audit'
    (args.output / 'identity.json').write_text(json.dumps(identity, indent=2), encoding='utf-8')
    ctx = mp.get_context('spawn')
    ready, start, stop = ctx.Queue(), ctx.Event(), ctx.Event()
    shared_start = ctx.Value('q', 0)
    children, ready_rows = [], []
    failure = None
    interrupted = False
    try:
        for index in range(args.processes):
            clients = list(range(index + 1, args.clients + 1, args.processes))
            child = ctx.Process(target=_worker, args=(index, clients, options, workload, ready, start, stop, shared_start))
            child.start()
            children.append(child)
        startup_deadline = time.monotonic() + args.startup_timeout
        while len(ready_rows) < len(children):
            remaining = startup_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('client startup deadline exceeded')
            ready_rows.append(ready.get(timeout=remaining))
        shared_start.value = time.perf_counter_ns() + int(0.5e9)
        start.set()
        deadline = time.monotonic() + args.duration + args.timeout + 15
        while any(child.is_alive() for child in children):
            if time.monotonic() > deadline:
                raise TimeoutError('owned clients exceeded drain deadline')
            if any(child.exitcode not in (None, 0) for child in children):
                raise RuntimeError('client process failed')
            time.sleep(.05)
    except KeyboardInterrupt:
        failure, interrupted = 'KeyboardInterrupt', True
    except Exception as exc:
        failure = type(exc).__name__
    finally:
        stop.set()
        start.set()
        drain = time.monotonic() + args.timeout + 5
        for child in children:
            child.join(max(0, drain - time.monotonic()))
        terminated = []
        for child in children:
            if child.is_alive():
                terminated.append(child.pid)
                child.terminate()
                child.join(5)
        result = {'ready_processes': ready_rows, 'configured_clients': args.clients,
                  'unique_client_pids': sorted({row['pid'] for row in ready_rows}),
                  'process_exitcodes': [child.exitcode for child in children],
                  'failure': failure, 'terminated_owned_pids': terminated,
                  'interrupted': interrupted, 'start_ns': shared_start.value,
                  'end_ns': time.perf_counter_ns(), 'finished_unix_ns': time.time_ns()}
        result['complete'] = failure is None and len(children) == args.processes and all(child.exitcode == 0 for child in children)
        (args.output / 'run.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(args.output), 'complete': result['complete'], 'processes': len(ready_rows)}))
    return 0 if result['complete'] else 2


def percentile(values, quantile):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def metrics(values, threshold=500.):
    values = [v for v in values if isinstance(v, (float, int)) and math.isfinite(v)]
    return {'count': len(values), 'mean_ms': sum(values) / len(values) if values else None,
            'p50_ms': percentile(values, .50), 'p95_ms': percentile(values, .95),
            'p99_ms': percentile(values, .99), 'max_ms': max(values) if values else None,
            'over_500ms': sum(v > threshold for v in values)}


def peak(intervals):
    edges = []
    for start, end in intervals:
        if isinstance(start, int) and isinstance(end, int) and end > start:
            edges.extend(((start, 1), (end, -1)))
    active = best = 0
    for _, delta in sorted(edges):  # end precedes start at equal timestamps
        active += delta
        best = max(best, active)
    return best


def records(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as stream:
        for line in stream:
            yield json.loads(line)


def analyze(args):
    folder = args.input
    identity = json.loads((folder / 'identity.json').read_text(encoding='utf-8'))
    run_info = json.loads((folder / 'run.json').read_text(encoding='utf-8'))
    validator = None
    if args.validator:
        module, function = args.validator.rsplit(':', 1)
        validator = getattr(importlib.import_module(module), function)
    rows, parse_errors, artifacts = [], [], {}
    validation_errors = []
    verify_started = time.perf_counter_ns()
    for path in sorted(folder.glob('client-*.jsonl.gz')):
        artifacts[path.name] = sha_file(path)
        try:
            for row in records(path):
                if row['outcome'] == 'ok':
                    response = row.get('response')
                    if not isinstance(response, dict):
                        validation_errors.append({'request_id': row['request_id'], 'error': 'JSON object required'})
                    elif args.require_run:
                        reported = response.get('run_id', response.get('runId'))
                        if reported is None and isinstance(response.get('provenance'), dict):
                            reported = response['provenance'].get('run_id')
                        if reported != args.require_run:
                            validation_errors.append({'request_id': row['request_id'], 'error': 'run identity absent or mismatched'})
                    if validator:
                        try:
                            result = validator(row)
                            if result not in (None, True):
                                validation_errors.append({'request_id': row['request_id'], 'error': 'external validator rejected'})
                        except Exception as exc:
                            validation_errors.append({'request_id': row['request_id'], 'error': 'validator ' + type(exc).__name__})
                # Full JSON stays in original gzip. Analysis stores only lightweight
                # request metrics, not millions of potentially large response objects.
                rows.append({k: v for k, v in row.items() if k not in ('response', 'request', 'http_phases')})
        except Exception as exc:
            parse_errors.append({'file': path.name, 'error': type(exc).__name__})
    groups = {}
    verification_ms = (time.perf_counter_ns() - verify_started) / 1e6
    failures, ids, duplicates = {}, set(), []
    client_counts = {}
    for row in rows:
        ident = row.get('request_id')
        if ident in ids:
            duplicates.append(ident)
        ids.add(ident)
        client_counts[row['client_id']] = client_counts.get(row['client_id'], 0) + 1
        failures[row['outcome']] = failures.get(row['outcome'], 0) + 1
        groups.setdefault(row.get('query_class', 'unspecified'), []).append(row)
    missing_clients = sorted(set(range(1, run_info['configured_clients'] + 1)) - set(client_counts))
    server, server_status, probe_errors = [], [], []
    for path in args.server_trace or []:
        artifacts[str(path)] = sha_file(path)
        try:
            for row in records(path):
                if row.get('kind') == 'api' and row.get('request_id') in ids:
                    server.append(row)
            status_path = Path(str(path) + '.status.json')
            server_status.append(json.loads(status_path.read_text(encoding='utf-8')) if status_path.exists() else {'complete': False, 'reason': 'probe close/status absent'})
        except Exception as exc:
            probe_errors.append(type(exc).__name__)
    server_ids = [r['request_id'] for r in server]
    observed = {r['request_id'] for r in rows if r['outcome'] != 'load_generator_busy'}
    probe_complete = (bool(args.server_trace) and all(s.get('complete') for s in server_status)
                      and not probe_errors and len(server_ids) == len(set(server_ids))
                      and set(server_ids) == observed
                      and all(r.get('body_send_completed') is True and isinstance(r.get('start_ns'), int)
                              and isinstance(r.get('end_ns'), int) and r['end_ns'] > r['start_ns'] for r in server))
    report = {'version': 'warehouse-load-analysis-v1', 'run_identity': identity,
              'run_complete': run_info['complete'], 'planned_arrivals': len(rows),
              'requests_by_client': client_counts, 'clients_without_requests': missing_clients,
              'outcomes': failures, 'parse_errors': parse_errors, 'duplicate_ids': duplicates,
              'offline_validation': {'scope': 'JSON object and optional run identity only; full domain oracle separate',
                                     'validator': args.validator, 'elapsed_ms': verification_ms,
                                     'errors': validation_errors},
              'client_observed_http_peak': peak((r.get('start_ns'), r.get('end_ns')) for r in rows),
              'actual_server_api_peak': peak((r['start_ns'], r['end_ns']) for r in server) if server else None,
              'server_peak_qualified': probe_complete,
              'server_probe_status': server_status, 'server_probe_errors': probe_errors,
              'server_intervals': len(server), 'groups': {},
              'million_prefix_qualified': False,
              'million_prefix_reason': 'requires independent same-run per-request prefix-size proof; never inferred from final count',
              'all_requests_under_500ms': False, 'artifact_sha256': artifacts}
    report['slow_examples'] = sorted(
        (r for r in rows if isinstance(r.get('elapsed_ms'), (int, float)) and r['elapsed_ms'] > 500),
        key=lambda r: r['elapsed_ms'], reverse=True)[:10]
    for name, values in groups.items():
        report['groups'][name] = {'arrivals': len(values),
                                 'errors_or_drops': sum(r['outcome'] != 'ok' for r in values),
                                 'network_all_observed': metrics([r.get('elapsed_ms') for r in values]),
                                 'network_success_only': metrics([r.get('elapsed_ms') for r in values if r['outcome'] == 'ok']),
                                 'complete_http_json': metrics([r.get('end_to_end_json_ms',r.get('elapsed_ms')) for r in values]),
                                 'server_service_header': metrics([r.get('service_ms') for r in values]),
                                 'server_queue_header': metrics([r.get('queue_ms') for r in values]),
                                 'upstream_header': metrics([r.get('upstream_ms') for r in values]),
                                 'json_parse_outside_network': metrics([r.get('parse_ms') for r in values])}
    report['all_requests_under_500ms'] = (bool(rows) and run_info['complete'] and not parse_errors and not duplicates and not missing_clients
        and not validation_errors and all(r['outcome'] == 'ok' and r.get('end_to_end_json_ms',r.get('elapsed_ms', math.inf)) <= 500 for r in rows))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    print(json.dumps({'output': str(args.output), 'outcomes': failures,
                      'actual_server_api_peak': report['actual_server_api_peak'],
                      'server_peak_qualified': probe_complete,
                      'all_requests_under_500ms': report['all_requests_under_500ms']}))
    return 0 if report['all_requests_under_500ms'] else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    load = commands.add_parser('run')
    load.add_argument('--base-url', required=True)
    load.add_argument('--workload', type=Path, required=True)
    load.add_argument('--output', type=Path, required=True)
    load.add_argument('--session', required=True)
    load.add_argument('--run-id', default='')
    load.add_argument('--identity', type=Path, help='capture source/data/schema/producer proof JSON and SHA without promoting it to qualification')
    load.add_argument('--clients', type=int, default=100)
    load.add_argument('--processes', type=int, default=100)
    load.add_argument('--mode', choices=('closed', 'periodic', 'open'), default='closed')
    load.add_argument('--duration', type=float, default=30)
    load.add_argument('--timeout', type=float, default=15)
    load.add_argument('--startup-timeout', type=float, default=120)
    load.add_argument('--requests', type=int, default=0, help='maximum planned arrivals per client; 0 uses duration')
    load.add_argument('--think', type=float, default=0)
    load.add_argument('--interval', type=float, default=5)
    load.add_argument('--rate', type=float, default=20, help='global offered requests/second in open mode')
    load.add_argument('--seed', type=int, default=20261029)
    load.add_argument('--phase', choices=('cold', 'warm', 'read-write', 'static'), default='warm')
    load.add_argument('--allow-writes', action='store_true')
    load.set_defaults(func=run)
    analysis = commands.add_parser('analyze')
    analysis.add_argument('--input', type=Path, required=True)
    analysis.add_argument('--output', type=Path, required=True)
    analysis.add_argument('--server-trace', type=Path, nargs='*')
    analysis.add_argument('--require-run')
    analysis.add_argument('--validator', help='trusted offline module:function(row), return True/None or raise/reject')
    analysis.set_defaults(func=analyze)
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    raise SystemExit(main())
