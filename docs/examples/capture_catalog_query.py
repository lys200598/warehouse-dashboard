"""Save ONE real self query in verify_catalog_responses' input format.

Teaching/format bridge only, not a load generator or a source verifier.
Use an owned, paused/stable live run; never fabricates or fills missing facts.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def save(path, value):
    with path.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--output', type=Path, required=True, help='new directory')
    args = parser.parse_args()
    parsed = urlsplit(args.base_url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
            or parsed.password or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        parser.error('base URL must be an HTTP(S) origin')
    base = args.base_url.rstrip('/')
    with urlopen(base+'/api/health', timeout=15) as response:
        health = json.load(response)
    run = health['run_id']
    seq = health['runtime']['knowledge']['mirrored_seq']
    if type(seq) is not int or seq < 1:
        parser.error('no mirrored query point')
    args.output.mkdir(parents=True, exist_ok=False)
    body = {'query_id': 'self', 'run_id': run, 'seq': seq, 'robot_id': 1}
    request_id = 'capture-'+str(time.time_ns())
    request = Request(base+'/api/knowledge/query', data=json.dumps(body).encode(),
                      headers={'Content-Type': 'application/json', 'X-Probe-ID': request_id})
    start = time.perf_counter_ns()
    try:
        response = urlopen(request, timeout=15)
    except HTTPError as exc:
        response = exc
    with response:
        status, raw = response.status, response.read()
    end = time.perf_counter_ns()
    # Keep bytes even if decoding/verification fails.
    (args.output/'response-body.bin').write_bytes(raw)
    value = json.loads(raw)
    save(args.output/'requests.jsonl', {
        'request_id': request_id, 'run_id': run, 'seq': seq, 'request': body,
        'start_ns': start, 'end_ns': end, 'ok': status == 200,
        'response_sha256': digest(value), 'http_status': status})
    save(args.output/'responses.jsonl', {'request_id': request_id, 'result': value})
    source = health.get('runtime', {}).get('metadata', {}).get('output')
    save(args.output/'identity.json', {
        'scope': 'one real self query; source must be verified separately',
        'base_url': base, 'run_id': run, 'seq': seq,
        'journal': str(Path(source)/'journal.sqlite') if source else None})
    print(json.dumps({'output': str(args.output), 'run_id': run, 'seq': seq, 'status': status}))
    return 0 if status == 200 else 2


if __name__ == '__main__':
    raise SystemExit(main())
