"""Opt-in versioned integration, never runtime patches of another scheduler."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from ..wide.sim import Simulation as WideSimulation, Config
from ..wide.motion import WideRoadmap, WideMotionTable
from ..scheduling_v2.motion import SafeIntervalPlanner
from . import RELEASE
from .geometry import (OptimizedMotionTable, OptimizedRoadmap, geometry_cache_info,
                       CELL_CACHE_LIMIT, DISTANCE_CACHE_LIMIT)
from .matching import priority_assignment
from .planner import GoalWindowPlanner


def execution_identity(mode):
    if mode not in {'optimized', 'baseline'}:
        raise ValueError('scheduler must be optimized or baseline')
    backend = Path(__file__).parents[1]
    sources = {p.relative_to(backend).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
               for p in sorted(backend.rglob('*.py'))}
    source_sha = hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    identity = {'release': RELEASE, 'mode': mode, 'source_sha256': source_sha,
                'features': ['priority-subset', 'goal-window-sipp', 'exact-cells', 'coordinate-distance-cache']
                            if mode == 'optimized' else [],
                'cell_cache_limit': CELL_CACHE_LIMIT if mode == 'optimized' else 0,
                'distance_cache_limit': DISTANCE_CACHE_LIMIT if mode == 'optimized' else 128}
    identity['execution_sha256'] = hashlib.sha256(json.dumps(identity, sort_keys=True,
                                                            separators=(',', ':')).encode()).hexdigest()
    return identity


class Simulation(WideSimulation):
    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None, *, scheduler='optimized'):
        self.scheduler = scheduler
        self.execution_identity = execution_identity(scheduler)
        self.reservation_type = OptimizedMotionTable if scheduler == 'optimized' else WideMotionTable
        self.roadmap_type = OptimizedRoadmap if scheduler == 'optimized' else WideRoadmap
        self.planner_type = GoalWindowPlanner if scheduler == 'optimized' else SafeIntervalPlanner
        super().__init__(scene_path, dataset, run_id, config or Config(), checkpoint=None)
        self.run_metadata.update(backend=RELEASE + '-' + scheduler,
            scheduler_identity=deepcopy(self.execution_identity),
            execution_code_sha256=self.execution_identity['execution_sha256'])
        if checkpoint is not None:
            self.restore(checkpoint)

    def _assignment(self, costs):
        return priority_assignment(costs) if self.scheduler == 'optimized' else super()._assignment(costs)

    def execution_status(self):
        return {'identity': deepcopy(self.execution_identity),
                'search': {'calls': self.planner.calls, 'failures': self.planner.failures,
                           'expansions': self.planner.expansions},
                'static_cache': self.roadmap.cache_stats() if self.scheduler == 'optimized' else None,
                'geometry_cache': geometry_cache_info() if self.scheduler == 'optimized' else None}

    def checkpoint(self):
        state = super().checkpoint()
        # Cache warmth is not durable business state and is deliberately absent.
        state['scheduler_v4'] = {'identity': deepcopy(self.execution_identity),
            'search': {'calls': self.planner.calls, 'failures': self.planner.failures,
                       'expansions': self.planner.expansions}}
        return state

    def restore(self, state):
        header = state.get('scheduler_v4')
        if not isinstance(header, dict) or header.get('identity') != self.execution_identity:
            raise ValueError('scheduler release/mode/source identity mismatch; no implicit checkpoint migration')
        search = header.get('search', {})
        if set(search) != {'calls', 'failures', 'expansions'} or any(
                isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in search.values()):
            raise ValueError('invalid v4 search counters')
        if search['failures'] > search['calls']:
            raise ValueError('invalid v4 failure count')
        super().restore(state)
        self.planner.calls, self.planner.failures, self.planner.expansions = (
            search['calls'], search['failures'], search['expansions'])

    def frame(self):
        value = super().frame()
        value['backend'] = self.run_metadata['backend']
        value['scheduler'] = {'release': RELEASE, 'mode': self.scheduler,
                              'execution_sha256': self.execution_identity['execution_sha256']}
        return value
