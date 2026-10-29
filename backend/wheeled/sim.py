"""Opt-in V5 transport: 1.2 m/s and finite station-local battery exchange.

The same SIPP swept-body reservations cover approach, stationary exchange and
return. Charging remains a fallback if no ready pack or safe swap trip exists.
"""
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path

from ..scheduling_v5.sim import CandidateSimulation
from ..wide.sim import Config as BaseConfig
from .batteries import BatteryPool


@dataclass
class WheeledConfig(BaseConfig):
    speed: float = 1.2
    replenishment_policy: str = 'swap-first'
    swap_stations: int = 12
    swap_spares_per_station: int = 4
    swap_offline_chargers_per_station: int = 2
    swap_seconds: int = 180

    def __post_init__(self):
        super().__post_init__()
        if self.replenishment_policy not in {'charge-only', 'swap-first'}:
            raise ValueError('unknown replenishment policy')
        for key, lower, upper in [('swap_stations', 1, self.chargers),
                                 ('swap_spares_per_station', 1, 100),
                                 ('swap_offline_chargers_per_station', 1, 100),
                                 ('swap_seconds', 1, 3600)]:
            n = getattr(self, key)
            if isinstance(n, bool) or not isinstance(n, int) or not lower <= n <= upper:
                raise ValueError(f'invalid {key}')


class WheeledMixin:
    """Composable before CandidateSimulation or platform KnowledgeSimulation."""
    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None, **kwargs):
        self.battery_pool = None
        super().__init__(scene_path, dataset, run_id, config or WheeledConfig(), checkpoint=None, **kwargs)
        ids = sorted(self.docks)
        stations = [ids[i * len(ids) // self.config.swap_stations] for i in range(self.config.swap_stations)]
        self.battery_pool = BatteryPool(self.robots, stations, self.config.swap_spares_per_station,
            self.config.swap_offline_chargers_per_station, self.energy)
        self.run_metadata['wheeled_replenishment'] = {
            'version': 1, 'policy': self.config.replenishment_policy, 'station_ids': stations,
            'seconds': self.config.swap_seconds, 'spares_per_station': self.config.swap_spares_per_station,
            'offline_chargers_per_station': self.config.swap_offline_chargers_per_station,
            'battery_target_soc': 100, 'energy_hardware_calibrated': False,
            'pack_movement': 'station-local inventory; no cross-station battery teleportation',
            'physical_bay': 'reuse selected charge/standby pad; one physical body per reserved pad'}
        sources = [Path(__file__), Path(__file__).with_name('batteries.py'),
                   Path(__file__).parents[1] / 'scheduling_v3' / 'energy.py']
        source_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
        self.run_metadata['wheeled_replenishment']['source_sha256'] = hashlib.sha256(
            json.dumps(source_hashes, sort_keys=True).encode()).hexdigest()
        self.metrics.update(swap_reserved=0, swap_completed=0, swap_cancelled=0,
                            swap_fallbacks=0, max_swapping=0)
        if checkpoint is not None:
            self.restore(checkpoint)

    def _sync_pack_reservations(self):
        # Recovery may replace a swap plan with conventional charge/return.
        # A reservation authorises exchange only while its actual marker exists.
        for token, p in list(self.battery_pool.packs.items()):
            if p['status'] != 'reserved':
                continue
            owner = p['owner']
            job = self.jobs.get(owner, {})
            if job.get('swap_pack_id') != token or not any(
                    q.get('marker') == 'swap_complete' for q in job.get('parts', [])):
                self.battery_pool.cancel(token, owner)
                self.metrics['swap_cancelled'] += 1
                self.event('battery_swap_cancelled', owner, pack_id=token, reason='safe_plan_replaced')

    def schedule_charge(self, robot):
        if self.config.replenishment_policy == 'swap-first' and not robot['carrying']:
            self._sync_pack_reservations()
            if self._schedule_swap(robot):
                return True
            self.metrics['swap_fallbacks'] += 1
        return super().schedule_charge(robot)

    def _schedule_swap(self, robot):
        if any(p['owner'] == robot['robot_id'] for p in self.battery_pool.packs.values()):
            return False
        choices = [self.docks[s] for s in self.battery_pool.chargers if self.battery_pool.ready(s)]
        choices.sort(key=lambda d: (max(self.elapsed, d['available_at']),
            math.dist((robot['x'], robot['y']), (d['point']['x'], d['point']['y'])), d['id']))
        for dock in choices:
            if dock['available_at'] > self.elapsed + self.config.route_horizon:
                continue  # bounded search; regular charging remains a safe fallback
            out = self._route(self._location(robot), dock['id'], self.elapsed, robot['robot_id'],
                'to_charge', self.config.swap_seconds, 'swapping', 'swap_complete',
                earliest_goal=max(self.elapsed, dock['available_at']))
            if not out:
                continue
            parts, end = out
            if robot['battery'] < self.energy.consume(parts) + self.config.emergency_reserve_soc:
                continue
            back = self._route(dock['id'], robot['home_id'], end, robot['robot_id'], 'returning')
            if not back or self.energy.consume(back[0]) + self.config.emergency_reserve_soc > 100:
                continue
            parts = parts + back[0]
            token = self.battery_pool.reserve(dock['id'], robot['robot_id'])
            if token is None:
                continue
            try:
                self._commit(robot, parts, {'task_id': None, 'dock_id': dock['id'],
                    'swap_pack_id': token, 'swap_started': False, 'replenishment': 'swap',
                    'request_time': self.elapsed})
            except BaseException:
                self.battery_pool.cancel(token, robot['robot_id'])
                raise
            robot.update(stage='charge_queue', dock_id=dock['id'], task_id=None)
            self._update_docks()
            self.metrics['swap_reserved'] += 1
            self.event('battery_swap_reserved', robot['robot_id'], dock_id=dock['id'],
                       pack_id=token, service_seconds=self.config.swap_seconds)
            return True
        return False

    def _update_docks(self):
        super()._update_docks()
        for job in self.jobs.values():
            if job.get('replenishment') != 'swap':
                continue
            serviced = False
            for p in job['parts']:
                if p['phase'] == 'swapping':
                    serviced = True
                    self.docks[job['dock_id']]['available_at'] = max(
                        self.docks[job['dock_id']]['available_at'], job['start'] + p['end'])
                elif serviced and p['a'] != p['b']:
                    self.docks[job['dock_id']]['available_at'] = max(
                        self.docks[job['dock_id']]['available_at'], job['start'] + p['start'] + 3)
                    break

    def marker(self, robot, job, marker, task_id=None):
        if marker != 'swap_complete':
            return super().marker(robot, job, marker, task_id)
        token = job.get('swap_pack_id')
        if token is None:
            raise AssertionError('swap completion without finite pack')
        # Settle elapsed offline charging BEFORE accepting this returned pack.
        # KnowledgeSimulation can reject a step before any robot moves, so do
        # not advance the pool to a future clock at the beginning of step().
        self.battery_pool.advance(self.elapsed)
        transfer = self.battery_pool.complete(token, robot['robot_id'], robot['battery'])
        robot['battery'] = transfer['installed_soc']
        robot['location_id'] = job['dock_id']
        job.pop('swap_pack_id')
        self.metrics['swap_completed'] += 1
        self.event('battery_swap_completed', robot['robot_id'], dock_id=job['dock_id'], **transfer)
        if self.config.task_chaining:
            self.handoff_ready.add(robot['robot_id'])

    def step(self):
        self._sync_pack_reservations()
        for owner, job in self.jobs.items():
            if job.get('swap_pack_id') and not job.get('swap_started') and any(
                    p['phase'] == 'swapping' and job['start'] + p['start'] <= self.elapsed
                    for p in job['parts']):
                job['swap_started'] = True
                self.event('battery_swap_started', owner, dock_id=job['dock_id'], pack_id=job['swap_pack_id'])
        super().step()
        self.battery_pool.advance(self.elapsed)
        self._sync_pack_reservations()
        self.metrics['max_swapping'] = max(self.metrics['max_swapping'],
            sum(r['stage'] == 'swapping' for r in self.robots.values()))

    def checkpoint(self):
        state = super().checkpoint()
        if self.battery_pool is not None:
            state['wheeled_batteries'] = self.battery_pool.snapshot()
        return state

    def restore(self, state):
        if 'wheeled_batteries' not in state:
            raise ValueError('wheeled battery checkpoint required; old checkpoints remain on old engine')
        self.battery_pool.restore(state['wheeled_batteries'])
        super().restore(state)
        if self.battery_pool.updated_at != self.elapsed:
            raise ValueError('battery pool and simulation clocks disagree')
        self._sync_pack_reservations()

    def frame(self):
        value = super().frame()
        if self.battery_pool is not None:
            value['batteryService'] = {'swapping': sum(r['stage'] == 'swapping' for r in self.robots.values()),
                'completed': self.metrics['swap_completed'], 'packCounts': {
                    status: sum(p['status'] == status for p in self.battery_pool.packs.values())
                    for status in ('ready', 'reserved', 'waiting', 'charging')}}
        return value


class WheeledSimulation(WheeledMixin, CandidateSimulation):
    pass
