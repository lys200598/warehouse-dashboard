"""Wide scene adapter; reuses the bounded, immutable-output v3 experiment runner."""
from ..scheduling_v3.runner import run, build_parser
from .sim import Simulation, Config
from .graph import WideMirror, audit_wide


if __name__=='__main__':
    raise SystemExit(run(build_parser().parse_args(),simulation_class=Simulation,
                         config_class=Config,mirror_class=WideMirror,
                         graph_auditor=lambda store,sim: audit_wide(store,sim,expected_handoffs=sim.metrics['handoffs'])))
