from copy import deepcopy
from pathlib import Path
import unittest

from backend.scheduling_v3.energy import EnergyProfile
from backend.wheeled.batteries import BatteryPool
from backend.wheeled.sim import WheeledConfig, WheeledSimulation
from backend.wheeled.sim import WheeledMixin
from backend.wide.sim import Config as OldConfig
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.knowledge_runtime.policy import DecisionRejected
from tests.test_knowledge_runtime import LocalTestProvider

ROOT = Path(__file__).resolve().parents[1]


class WheeledKnowledgeSimulation(WheeledMixin, KnowledgeSimulation):
    pass


class PackInventoryTests(unittest.TestCase):
    def pool(self):
        return BatteryPool([1, 2], ['C-A'], 2, 1, EnergyProfile())

    def test_unique_ids_and_finite_ready_inventory(self):
        p = self.pool()
        a, b = p.reserve('C-A', 1), p.reserve('C-A', 2)
        self.assertNotEqual(a, b)
        self.assertEqual([], p.ready('C-A'))
        self.assertEqual(4, len(p.all_ids))
        with self.assertRaises(ValueError):
            p.reserve('C-A', 1)
        p.cancel(a, 1)
        self.assertEqual([a], p.ready('C-A'))

    def test_exchange_no_creation_and_offline_charger_limit(self):
        p = self.pool()
        a, b = p.reserve('C-A', 1), p.reserve('C-A', 2)
        before = set(p.all_ids)
        p.complete(a, 1, 20.)
        p.complete(b, 2, 30.)
        self.assertEqual(0, len(p.ready('C-A')))
        p.advance(3600)
        # Only one of the two depleted packs may charge in this hour.
        self.assertEqual(1, sum(v['status'] == 'charging' for v in p.packs.values()))
        self.assertEqual(1, sum(v['status'] == 'waiting' for v in p.packs.values()))
        self.assertAlmostEqual(80., max(v['soc'] for v in p.packs.values()), places=6)
        self.assertEqual(before, set(p.installed.values()) | set(p.packs))
        p.advance(6000)
        self.assertEqual(1, len(p.ready('C-A')))
        with self.assertRaises(ValueError):
            p.advance(1)

    def test_recovery_preserves_partial_charge_and_reservation(self):
        p = self.pool()
        a = p.reserve('C-A', 1)
        p.complete(a, 1, 40.)
        b = p.reserve('C-A', 2)
        p.advance(123)
        state = p.snapshot()
        other = self.pool()
        other.restore(state)
        self.assertEqual(state, other.snapshot())
        p.advance(7800)
        other.advance(7800)
        self.assertEqual(p.snapshot(), other.snapshot())
        broken = deepcopy(state)
        broken['packs']['invented'] = deepcopy(next(iter(broken['packs'].values())))
        with self.assertRaises(AssertionError):
            self.pool().restore(broken)


class WheeledIntegrationTests(unittest.TestCase):
    def sim(self, **kwargs):
        return WheeledSimulation(ROOT/'data/layouts/warehouse100-wide-v2.json',
            ROOT/'data/examples/warehouse-v5', 'wheel-tests',
            WheeledConfig(charging_policy='none', **kwargs), policy='port-guard')

    def test_optin_keeps_old_speed_and_checks_parameters(self):
        self.assertEqual(.8, OldConfig().speed)
        self.assertEqual(1.2, WheeledConfig().speed)
        for kw in ({'swap_stations': 101}, {'swap_seconds': 0}, {'swap_spares_per_station': True},
                   {'replenishment_policy': 'infinite'}):
            with self.assertRaises(ValueError):
                WheeledConfig(**kw)

    def test_actual_motion_swap_marker_and_checkpoint_restore(self):
        sim = self.sim()
        # Isolate the battery-service path while retaining the real 100-body
        # planner and swept-motion audits. These tasks are still untouched.
        sim.release_and_dispatch = lambda: None
        robot = sim.robots[1]
        robot['battery'] = 34.
        self.assertTrue(sim.schedule_charge(robot))
        job = sim.jobs[1]
        self.assertIn('swap_pack_id', job)
        token = job['swap_pack_id']
        swap = next(q for q in job['parts'] if q['phase'] == 'swapping')
        complete_at = job['start'] + swap['end']
        for _ in range(50):
            sim.step()
        self.assertLess(robot['battery'], 34.)  # no gradual in-place charging during exchange
        state = sim.checkpoint()
        restored = self.sim()
        restored.restore(state)
        restored.release_and_dispatch = lambda: None
        for _ in range(complete_at - sim.elapsed):
            sim.step()
            restored.step()
        self.assertEqual(1, sim.metrics['swap_completed'])
        self.assertEqual(100., robot['battery'])
        self.assertEqual(token, sim.battery_pool.installed['1'])
        self.assertEqual(sim.battery_pool.snapshot(), restored.battery_pool.snapshot())
        self.assertEqual(148, len(sim.battery_pool.all_ids))
        old = sim.battery_pool.packs['installed-1']
        self.assertEqual('waiting', old['status'])  # returned at this boundary, not charged retroactively
        self.assertLess(old['soc'], 34.)
        self.assertEqual('battery_swap_completed', sim.pending_events[-1]['kind'])

    def test_cancelled_actual_plan_releases_unchanged_ready_pack(self):
        sim = self.sim()
        sim.robots[1]['battery'] = 34.
        self.assertTrue(sim.schedule_charge(sim.robots[1]))
        token = sim.jobs[1]['swap_pack_id']
        sim.jobs[1].pop('swap_pack_id')  # recovery selected a conventional safe suffix
        sim._sync_pack_reservations()
        self.assertEqual('ready', sim.battery_pool.packs[token]['status'])
        self.assertEqual('installed-1', sim.battery_pool.installed['1'])
        self.assertEqual(34., sim.robots[1]['battery'])

    def test_charge_only_uses_existing_replenishment(self):
        sim = self.sim(replenishment_policy='charge-only')
        sim.robots[1]['battery'] = 34.
        self.assertTrue(sim.schedule_charge(sim.robots[1]))
        self.assertNotIn('swap_pack_id', sim.jobs[1])
        self.assertTrue(any(q['phase'] == 'charging' for q in sim.jobs[1]['parts']))

    def test_actual_delay_during_swap_keeps_pack_until_physical_completion(self):
        sim = self.sim()
        sim.release_and_dispatch = lambda: None
        sim.robots[1]['battery'] = 34.
        self.assertTrue(sim.schedule_charge(sim.robots[1]))
        token = sim.jobs[1]['swap_pack_id']
        for _ in range(50):
            sim.step()
        self.assertTrue(sim.inject_delay(1, 10))
        # Re-timed service must retain, not hand back, its selected pack.
        self.assertEqual('reserved', sim.battery_pool.packs[token]['status'])
        while sim.metrics['swap_completed'] == 0:
            self.assertEqual('installed-1', sim.battery_pool.installed['1'])
            sim.step()
        self.assertEqual(token, sim.battery_pool.installed['1'])
        self.assertEqual(0, sim.metrics['swap_cancelled'])
        self.assertEqual(148, len(sim.battery_pool.all_ids))

    def combined(self):
        sim = WheeledKnowledgeSimulation(ROOT/'data/layouts/warehouse100-wide-v2.json',
            ROOT/'data/examples/warehouse-v5', 'wheel-knowledge-tests',
            WheeledConfig(charging_policy='none'), knowledge_mode='required',
            resource_graph=True, commit_seconds=5)
        sim.history_provider = LocalTestProvider()
        return sim

    def test_knowledge_chain_initial_and_mid_swap_restore_preserved(self):
        sim = self.combined()
        sim.release_and_dispatch = lambda: None
        sim.robots[1]['battery'] = 34.
        self.assertTrue(sim.schedule_charge(sim.robots[1]))
        for _ in range(80):
            sim.step()
        state = sim.checkpoint()
        self.assertIn('knowledge_runtime', state)
        self.assertIn('resource_graph', state['knowledge_runtime']['identity'])
        other = self.combined()
        other.restore(state)
        other.release_and_dispatch = lambda: None
        while sim.metrics['swap_completed'] == 0:
            sim.step()
            other.step()
        self.assertEqual(sim.battery_pool.snapshot(), other.battery_pool.snapshot())
        self.assertEqual(sim.knowledge_stats, other.knowledge_stats)
        self.assertIn(1, sim.handoff_ready)

    def test_required_rejection_before_motion_does_not_advance_offline_charge(self):
        sim = self.combined()
        owner = None
        for _ in range(1800):
            sim.step()
            owner = next((i for i, r in sim.robots.items() if r['carrying'] and i not in sim.assists), None)
            if owner is not None:
                break
        self.assertIsNotNone(owner)
        self.assertTrue(sim.request_assistance(owner))
        at = sim.assists[owner]['handoff_at']
        while sim.elapsed < at - 1:
            sim.step()
        sim.history_provider.bad = True
        before = deepcopy((sim.elapsed, sim.robots, sim.jobs, sim.goods, sim.battery_pool.snapshot()))
        with self.assertRaises(DecisionRejected):
            sim.step()
        self.assertEqual(before, (sim.elapsed, sim.robots, sim.jobs, sim.goods, sim.battery_pool.snapshot()))
        self.assertEqual(sim.elapsed, sim.battery_pool.updated_at)
        # Rejected history still yields a recoverable stable checkpoint.
        other = self.combined()
        other.restore(sim.checkpoint())
        self.assertEqual(sim.battery_pool.snapshot(), other.battery_pool.snapshot())


if __name__ == '__main__':
    unittest.main()
