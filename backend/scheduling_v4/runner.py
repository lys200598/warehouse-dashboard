"""Release entry point: --scheduler optimized (default) or baseline."""
from functools import partial
from pathlib import Path

from ..checkpoint import read_checkpoint
from ..scheduling_v3.runner import build_parser, run
from ..wide.graph import WideMirror, audit_wide
from .sim import Simulation, Config, execution_identity


def main(argv=None):
    parser = build_parser()
    parser.add_argument('--scheduler', choices=['optimized', 'baseline'], default='optimized')
    args = parser.parse_args(argv)
    if args.resume:
        state = read_checkpoint(Path(args.output) / 'checkpoint.json')
        if state.get('scheduler_v4', {}).get('identity') != execution_identity(args.scheduler):
            raise ValueError('refusing cross-mode/version resume before opening the journal')
    return run(args, simulation_class=partial(Simulation, scheduler=args.scheduler),
               config_class=Config, mirror_class=WideMirror,
               graph_auditor=lambda store, sim: audit_wide(store, sim, expected_handoffs=sim.metrics['handoffs']))


if __name__ == '__main__':
    raise SystemExit(main())
