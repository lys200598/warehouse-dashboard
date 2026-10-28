"""Version-checked playback; unchanged wide knowledge APIs are not configured."""
import argparse
from pathlib import Path

from ..checkpoint import read_checkpoint
from ..wide.web import serve
from . import RELEASE
from .sim import execution_identity


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--scheduler', choices=['optimized', 'baseline'], required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--dist', required=True)
    args = parser.parse_args()
    cp = read_checkpoint(Path(args.output) / 'checkpoint.json')
    if cp.get('scheduler_v4', {}).get('identity') != execution_identity(args.scheduler):
        raise ValueError('viewer refused a different release, mode or source snapshot')
    args.neo4j = False  # Existing knowledge service and its query logic stay untouched.
    serve(args)


if __name__ == '__main__':
    main()
