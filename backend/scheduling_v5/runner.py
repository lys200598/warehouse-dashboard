"""Isolated candidate runner; same inputs, checks and durable v3 journal."""
from functools import partial
import json
from pathlib import Path

from ..checkpoint import read_checkpoint
from ..scheduling_v3.runner import build_parser, run
from ..wide.graph import WideMirror, audit_wide
from .sim import CandidateSimulation, Config, DEFAULTS, PLANNERS, execution_identity


def main(argv=None):
    parser = build_parser()
    parser.add_argument('--policy', choices=sorted(DEFAULTS), default='guard')
    parser.add_argument('--planner', choices=PLANNERS, default='goal-window')
    parser.add_argument('--params-json', '--policy-json', dest='params_json')
    args = parser.parse_args(argv)
    parameters = json.loads(Path(args.params_json).read_text(encoding='utf-8')) if args.params_json else {}
    identity = execution_identity(args.policy, parameters, args.planner)
    if args.resume:
        state = read_checkpoint(Path(args.output) / 'checkpoint.json')
        if state.get('scheduler_v5', {}).get('identity') != identity:
            raise ValueError('refusing cross-policy/parameters/version resume before opening journal')
    return run(args, simulation_class=partial(CandidateSimulation, policy=args.policy,
                   policy_parameters=parameters, planner=args.planner), config_class=Config, mirror_class=WideMirror,
               graph_auditor=lambda store, sim: audit_wide(store, sim,
                   expected_handoffs=sim.metrics['handoffs']))


if __name__ == '__main__':
    raise SystemExit(main())
