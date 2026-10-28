"""Launch an unchanged reader benchmark with immutable provenance and full logs.

The child owns evidence-dir/benchmark. This wrapper never edits schema or source.
Run with the same Python/Rust environment intended for the benchmark child.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback


READERS = ('__init__.py', 'oracle.py', 'prepare.py', 'store.py', 'benchmark.py', 'profile.py')
ALLOWED_READERS = {*READERS, 'loadgen.py'}


def hashes(root, names=READERS):
    return {name: hashlib.sha256((root/'backend/real_scale'/name).read_bytes()).hexdigest()
            for name in names}


def save(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def run(args):
    root, output = args.root.resolve(), args.evidence_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    expected = json.loads(args.reader_manifest.read_text(encoding='utf-8'))['overlay']
    if (not set(READERS) <= set(expected) <= ALLOWED_READERS or
            args.worker_processes > 1 and 'loadgen.py' not in expected):
        save(output/'failure.json', {'error': 'manifest must cover all six readers and every selected worker module'})
        return 2
    before = hashes(root, sorted(expected))
    if expected != before:
        save(output/'failure.json', {'error': 'reader source differs from selected six-file manifest'})
        return 2
    command = [sys.executable, '-m', 'backend.real_scale.benchmark',
        '--oracle-index', str(args.oracle_index.resolve()), '--output-dir', str(output/'benchmark'),
        '--mode', args.mode, '--clients', str(args.clients), '--rounds', str(args.rounds),
        '--samples', str(args.samples), '--min-states', str(args.min_states),
        '--stability-seconds', str(args.stability_seconds), '--recall-samples', str(args.recall_samples),
        '--max-wall-seconds', str(args.max_wall_seconds)]
    if args.observe_writer:
        command.append('--observe-writer')
    if args.worker_processes != 1:
        command.extend(['--worker-processes', str(args.worker_processes)])
    invocation = {'version': 'real-query-launch-v1', 'command': command, 'root': str(root),
        'start_ns': time.time_ns(), 'reader_pre_sha256': before,
        'reader_manifest_path': str(args.reader_manifest.resolve()),
        'reader_manifest_sha256': hashlib.sha256(args.reader_manifest.read_bytes()).hexdigest(),
        'python': sys.executable, 'hostname': platform.node(),
        'scope': 'unchanged full Python/Bolt/JSON/oracle API, not HTTP/browser/edge-device'}
    save(output/'invocation.json', invocation)
    exit_code, error = 1, None
    try:
        with (output/'stdout.log').open('xb') as stdout, (output/'stderr.log').open('xb') as stderr:
            exit_code = subprocess.run(command, cwd=root, stdout=stdout, stderr=stderr, check=False).returncode
    except Exception as exc:
        error = type(exc).__name__+': '+str(exc)
        with (output/'launch-exception.log').open('x', encoding='utf-8') as stream:
            traceback.print_exc(file=stream)
    after = hashes(root, sorted(expected))
    save(output/'finish.json', {'end_ns': time.time_ns(), 'exit_code': exit_code,
        'error': error, 'reader_post_sha256': after, 'reader_source_unchanged': before == after})
    return exit_code if before == after else 3


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path.cwd())
    p.add_argument('--reader-manifest', type=Path, required=True)
    p.add_argument('--oracle-index', type=Path, required=True)
    p.add_argument('--evidence-dir', type=Path, required=True)
    p.add_argument('--mode', choices=['baseline', 'ordered'], default='ordered')
    p.add_argument('--clients', type=int, default=100)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--samples', type=int, default=10)
    p.add_argument('--min-states', type=int, default=1000000)
    p.add_argument('--stability-seconds', type=float, default=20)
    p.add_argument('--recall-samples', type=int, default=2)
    p.add_argument('--max-wall-seconds', type=float, default=600)
    p.add_argument('--observe-writer', action='store_true')
    p.add_argument('--worker-processes', choices=[1, 4, 8], type=int, default=1)
    return run(p.parse_args(argv))


if __name__ == '__main__':
    raise SystemExit(main())
