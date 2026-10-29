"""Existing knowledge runner + independent robot processes, explicitly enabled.

Usage: python -m ops.run_agent_simulation --scene ... --dataset ... --output ...
  --run-id unique --knowledge-mode rules --knowledge-backend neo4j --commit-seconds 30
"""
from functools import partial
from pathlib import Path

from backend.agent_runtime.integration import agent_mirror_class
from backend.knowledge_runtime.runner import build_parser, run
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.real_scale.writer import Config, RealMirror, audit_real


def main(argv=None):
    parser = build_parser()
    parser.set_defaults(knowledge_backend='neo4j', commit_seconds=30)
    parser.add_argument('--agent-query-seconds', type=int, default=30)
    parser.add_argument('--agent-observation-seconds', type=int, default=30)
    parser.add_argument('--agent-timeout', type=float, default=30.)
    parser.add_argument('--agent-startup-timeout', type=float, default=120.)
    args = parser.parse_args(argv)
    if args.knowledge_backend != 'neo4j':
        parser.error('the integrated agent runner requires real Neo4j; journal-only is a protocol-test backend')
    mirror = agent_mirror_class(RealMirror, output_dir=Path(args.output)/'agents',
        query_interval=args.agent_query_seconds, observation_interval=args.agent_observation_seconds,
        timeout=args.agent_timeout, startup_timeout=args.agent_startup_timeout)
    factory = partial(KnowledgeSimulation, commit_seconds=args.commit_seconds, knowledge_mode=args.knowledge_mode)
    return run(args, simulation_class=factory, config_class=Config, mirror_class=mirror, graph_auditor=audit_real)


if __name__ == '__main__':
    raise SystemExit(main())
