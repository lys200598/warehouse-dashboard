"""Explicit composition of existing knowledge control, wheel transport and I/O.

No existing run or default core is replaced. New options require a new run.
"""
from pathlib import Path


def _base_physical_classes(mobility='legacy'):
    from ..knowledge_runtime.sim import KnowledgeSimulation
    from ..real_scale.writer import Config
    if mobility == 'legacy':
        return KnowledgeSimulation, Config
    if mobility == 'wheeled':
        from ..wheeled.sim import WheeledMixin, WheeledConfig

        class WheeledKnowledgeSimulation(WheeledMixin, KnowledgeSimulation):
            pass

        return WheeledKnowledgeSimulation, WheeledConfig
    if mobility == 'wheeled_ray':
        from ..ray_medium.sim import RayWheeledKnowledgeSimulation
        from ..wheeled.sim import WheeledConfig
        return RayWheeledKnowledgeSimulation, WheeledConfig
    raise ValueError('unknown mobility model')

def physical_classes(mobility='legacy', robot_processes='off', ray_address=None, ray_actor_cpus=0.1):
    """Simulation and config classes for a mobility model.

    robot_processes: 'off' keeps the sequential engine; 'local' or 'ray' lets each
    robot advance in its own robot process (backend/robot_actor), in-process or as
    one Ray actor per robot. The dispatch results are the same in all three.
    """
    simulation_class, config_class = _base_physical_classes(mobility)
    if robot_processes == 'off':
        return simulation_class, config_class
    if robot_processes == 'local':
        from ..robot_actor.local import RobotProcessSplit as split
        attrs = {}
    elif robot_processes == 'ray':
        from ..robot_actor.ray_actor import RayRobotProcessSplit as split
        attrs = {'ray_address': ray_address, 'ray_actor_cpus': ray_actor_cpus}
    else:
        raise ValueError('robot_processes must be off, local or ray')
    return type(f'{robot_processes.title()}RobotProcess{simulation_class.__name__}', (split, simulation_class), attrs), config_class


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
