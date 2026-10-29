"""Explicit composition of existing knowledge control, wheel transport and I/O.

No existing run or default core is replaced. New options require a new run.
"""
from pathlib import Path


def physical_classes(mobility='legacy'):
    from ..knowledge_runtime.sim import KnowledgeSimulation
    from ..real_scale.writer import Config
    if mobility == 'legacy':
        return KnowledgeSimulation, Config
    if mobility != 'wheeled':
        raise ValueError('unknown mobility model')
    from ..wheeled.sim import WheeledMixin, WheeledConfig

    class WheeledKnowledgeSimulation(WheeledMixin, KnowledgeSimulation):
        pass

    return WheeledKnowledgeSimulation, WheeledConfig


def with_agents(mirror_class, *, output, enabled=False, options=None):
    if not enabled:
        return mirror_class
    from ..agent_runtime.integration import agent_mirror_class
    return agent_mirror_class(mirror_class, output_dir=Path(output)/'agents', **(options or {}))


def add_options(parser):
    parser.add_argument('--mobility', choices=('legacy', 'wheeled'), default='legacy',
                        help='new run: opt-in wheeled transport + finite pack exchange')
    parser.add_argument('--robot-agents', action='store_true',
                        help='100 separate robot HTTP I/O processes; requires Neo4j')
    parser.add_argument('--agent-query-seconds', type=int, default=30)
    parser.add_argument('--agent-observation-seconds', type=int, default=30)
    parser.add_argument('--agent-timeout', type=float, default=30.)
    parser.add_argument('--agent-startup-timeout', type=float, default=120.)


def agent_options(args):
    return dict(query_interval=args.agent_query_seconds,
                observation_interval=args.agent_observation_seconds,
                timeout=args.agent_timeout, startup_timeout=args.agent_startup_timeout)
