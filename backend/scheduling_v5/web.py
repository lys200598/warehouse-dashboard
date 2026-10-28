"""Read-only playback of a source- and policy-identified v5 run."""
import argparse
from pathlib import Path

from ..checkpoint import read_checkpoint
from ..wide.web import serve
from .sim import execution_identity


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True)
    p.add_argument('--dist', default='dist/v5')
    p.add_argument('--port', type=int, default=14182)
    args = p.parse_args()
    state = read_checkpoint(Path(args.output)/'checkpoint.json')
    identity = state.get('scheduler_v5', {}).get('identity', {})
    expected = execution_identity(identity.get('policy'), identity.get('parameters'), identity.get('planner'))
    if identity != expected:
        raise ValueError('v5 playback source/policy identity mismatch')
    args.neo4j = False
    serve(args)


if __name__ == '__main__':
    main()
