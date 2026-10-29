"""Independent opt-in runner retaining V5 traffic and warehouse auditing."""
from functools import partial
import json
from pathlib import Path
from ..scheduling_v3.runner import build_parser, run
from ..scheduling_v5.sim import DEFAULTS, PLANNERS
from ..wide.graph import WideMirror, audit_wide
from .sim import WheeledSimulation, WheeledConfig


def main(argv=None):
    parser = build_parser()
    parser.add_argument('--policy', choices=sorted(DEFAULTS), default='port-guard')
    parser.add_argument('--planner', choices=PLANNERS, default='original-sipp')
    parser.add_argument('--params-json')
    args = parser.parse_args(argv)
    parameters = json.loads(Path(args.params_json).read_text(encoding='utf-8')) if args.params_json else {}
    return run(args, simulation_class=partial(WheeledSimulation, policy=args.policy,
        policy_parameters=parameters, planner=args.planner), config_class=WheeledConfig,
        mirror_class=WideMirror, graph_auditor=lambda store, sim: audit_wide(store, sim,
            expected_handoffs=sim.metrics['handoffs']))


if __name__ == '__main__':
    raise SystemExit(main())
