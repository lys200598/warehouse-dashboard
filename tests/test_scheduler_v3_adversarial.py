"""Integration counterexamples for delayed execution and durable recovery."""
import copy
import json
from pathlib import Path
import tempfile
import time
import unittest

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.day1.data import generate
from backend.day1.nav import hold
from backend.scheduling_v2.motion import continuous_clear
from backend.scheduling_v3.sim import Config, Simulation


class AdversarialSimulationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Use an explicitly writable workspace directory, not Windows AppContainer TEMP.
        cls.root = Path(__file__).resolve().parents[2] / 'research' / 'scheduler-v3-adversarial'
        cls.root.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(dir=cls.root)
        cls.scene = Path(__file__).resolve().parents[1] / 'data/layouts/warehouse100-v1.json'
        cls.dataset = Path(cls.temp.name) / 'baseline'
        generate(cls.scene, cls.dataset, slot_interval_seconds=21600)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def simulation(self, suffix, **config):
        return Simulation(self.scene, self.dataset, 'v3-adversarial-' + suffix, Config(**config))

    def advance(self, sim, until):
        while sim.elapsed < until:
            sim.step()

    def active_business(self, sim):
        for _ in range(300):
            for owner, job in sim.jobs.items():
                if job.get('task_id') and sim.tasks[job['task_id']]['status'] != 'completed':
                    return owner, job['task_id']
            sim.step()
        self.fail('no active business task within initial 300 simulated seconds')

    def test_default_420_seconds_advances_real_tasks_with_inventory_and_clearance(self):
        sim = self.simulation('smoke')
        started = time.perf_counter()
        self.advance(sim, 420)
        wall = time.perf_counter() - started
        self.assertGreater(sim.metrics['completed'], 0)
        self.assertGreater(sim.metrics['motion_audit_pairs'], 0)
        self.assertGreaterEqual(sim.metrics['min_separation_m'], 1.1 - 1e-6)
        sim.assert_inventory()
        # Evidence, not a timing assertion on a shared workstation.
        self.smoke_evidence = {'wall420': wall, 'completed': sim.metrics['completed']}

    def test_delay_keeps_pick_delivery_markers_and_real_custody(self):
        sim = self.simulation('markers', task_chaining=False, charging_policy='critical-only')
        owner, task_id = self.active_business(sim)
        before_goods = copy.deepcopy(sim.goods)
        before_markers = [p['marker'] for p in sim._suffix(owner) if p.get('marker')]
        self.assertTrue(sim.inject_delay(owner, 30))
        self.assertEqual(sim.goods, before_goods)
        self.assertEqual([p['marker'] for p in sim._suffix(owner) if p.get('marker')], before_markers)
        self.advance(sim, 700)
        task = sim.tasks[task_id]
        self.assertEqual(task['status'], 'completed')
        relevant = [event for event in sim.pending_events if event.get('task_id') == task_id]
        self.assertEqual(sum(event['kind'] == 'picked' for event in relevant), 1)
        self.assertEqual(sum(event['kind'] in {'placed', 'shipped'} for event in relevant), 1)
        sim.assert_inventory()

    def test_delay_on_charger_preserves_reachable_charge_target(self):
        sim = self.simulation('charger', task_chaining=False)
        self.advance(sim, 100)
        owner = next(i for i, robot in sim.robots.items() if robot['stage'] == 'charging')
        dock_id = sim.jobs[owner]['dock_id']
        before = sim.robots[owner]['battery']
        self.assertTrue(sim.inject_delay(owner, 60))
        self.advance(sim, sim.elapsed + 60)
        self.assertGreaterEqual(sim.robots[owner]['battery'], before)
        self.advance(sim, 3000)
        done = [e for e in sim.pending_events if e['kind'] == 'charge_complete' and e['robot_id'] == owner]
        self.assertTrue(done)
        self.assertEqual(done[0]['dock_id'], dock_id)
        self.assertGreaterEqual(done[0]['battery'], sim.config.charge_target_soc - 1e-6)
        sim.assert_inventory()

    def test_delay_without_energy_margin_does_not_replace_old_trajectory(self):
        sim = self.simulation('energy-reject', task_chaining=False, charging_policy='critical-only')
        owner, _ = self.active_business(sim)
        sim.robots[owner]['battery'] = sim.config.emergency_reserve_soc + .001
        jobs = copy.deepcopy(sim.jobs)
        table = copy.deepcopy(sim.reservations.__dict__)
        self.assertFalse(sim.inject_delay(owner, 600))
        self.assertEqual(sim.jobs, jobs)
        self.assertEqual(sim.reservations.__dict__, table)

    def test_durable_checkpoint_after_retime_replays_identical_physics(self):
        sim = self.simulation('restore', task_chaining=False, charging_policy='critical-only')
        owner, _ = self.active_business(sim)
        self.assertTrue(sim.inject_delay(owner, 35))
        self.advance(sim, sim.elapsed + 10)
        path = Path(self.temp.name) / 'retimed-checkpoint.json'
        write_checkpoint(path, sim.checkpoint())
        restored = Simulation(self.scene, self.dataset, sim.run_id, sim.config, checkpoint=read_checkpoint(path))
        for _ in range(90):
            sim.step()
            restored.step()
        left, right = sim.checkpoint(), restored.checkpoint()
        for key in ['elapsed', 'tasks', 'robots', 'racks', 'goods', 'inside', 'ports', 'jobs', 'scheduler_v3']:
            self.assertEqual(left[key], right[key], key)
        self.assertEqual(left['metrics']['completed'], right['metrics']['completed'])
        self.assertEqual(left['metrics']['min_soc'], right['metrics']['min_soc'])
        self.assertEqual(left['metrics']['min_separation_m'], right['metrics']['min_separation_m'])

    def stopped_carrier(self, suffix, assistance=False):
        sim = self.simulation(suffix, charging_policy='critical-only')
        self.advance(sim, 160)
        # Fixed seed: carrier 76 blocks the future path of robot 77 here.
        self.assertTrue(sim.robots[76]['carrying'])
        self.assertTrue(sim.inject_stop(76, 120))
        if assistance:
            sim.robots[76]['battery'] = 24.
            self.assertTrue(sim.request_assistance(76))
        return sim, 76, sim.robots[76]['task_id']

    def test_finite_stop_preserves_body_and_other_robot_reroutes(self):
        sim = self.simulation('finite-stop', charging_policy='critical-only')
        donor, affected = None, []
        # Select an actual carried parcel whose new stop obstructs another
        # committed trajectory, rather than hardcoding an assignment choice.
        for _ in range(550):
            sim.step()
            for owner, robot in sim.robots.items():
                if not robot['carrying']:
                    continue
                obstacle = hold(sim._point(robot), 'fault_wait', sim.elapsed, 120)
                affected = [i for i in sim.jobs if i != owner and any(
                    not continuous_clear(p['a'], p['b'], p['start'], p['end'], obstacle)
                    for p in sim._suffix(i))]
                if affected:
                    donor = owner
                    break
            if donor is not None:
                break
        self.assertIsNotNone(donor)
        task_id = sim.robots[donor]['task_id']
        point = sim._point(sim.robots[donor])
        distance_before = {i: sim.robots[i]['distance'] for i in affected}
        self.assertTrue(sim.inject_stop(donor, 120))
        reroutes = [e for e in sim.pending_events if e['kind'] == 'traffic_rerouted']
        self.assertTrue(any(donor in e['affected_robots'] and set(affected) & set(e['affected_robots']) for e in reroutes))
        until = sim.faults[donor]['until']
        while sim.elapsed < until:
            sim.step()
            self.assertEqual(sim._point(sim.robots[donor]), point)
        self.assertNotIn(donor, sim.faults)
        self.assertTrue(any(sim.robots[i]['distance'] > distance_before[i] for i in affected))
        self.advance(sim, until + 500)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertGreaterEqual(sim.metrics['min_separation_m'], 1.1 - 1e-6)
        sim.assert_inventory()

    def test_handoff_does_not_repair_a_faulted_donor_before_declared_end(self):
        sim, donor, task_id = self.stopped_carrier('fault-handoff', assistance=True)
        point = sim._point(sim.robots[donor])
        until = sim.faults[donor]['until']
        handoff_at = sim.assists[donor]['handoff_at']
        self.assertLess(handoff_at, until)
        while sim.elapsed < until:
            sim.step()
            self.assertEqual(sim._point(sim.robots[donor]), point)
        self.assertEqual(sim.metrics['handoffs'], 1)
        self.advance(sim, until + 500)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        events = [e for e in sim.pending_events if e.get('task_id') == task_id]
        self.assertEqual(sum(e['kind'] == 'cargo_handoff' for e in events), 1)
        self.assertEqual(sum(e['kind'] in {'placed', 'shipped'} for e in events), 1)
        sim.assert_inventory()

    def test_checkpoint_mid_handoff_preserves_fault_and_custody(self):
        sim, donor, _ = self.stopped_carrier('handoff-restore', assistance=True)
        self.advance(sim, sim.elapsed + 10)
        self.assertTrue(sim.assists)
        path = Path(self.temp.name) / 'handoff-checkpoint.json'
        write_checkpoint(path, sim.checkpoint())
        restored = Simulation(self.scene, self.dataset, sim.run_id, sim.config, checkpoint=read_checkpoint(path))
        for _ in range(160):
            sim.step()
            restored.step()
        left, right = sim.checkpoint(), restored.checkpoint()
        for key in ['elapsed', 'tasks', 'robots', 'racks', 'goods', 'inside', 'ports', 'jobs', 'scheduler_v3']:
            self.assertEqual(left[key], right[key], key)
        self.assertEqual(left['metrics']['handoffs'], 1)
        self.assertNotIn(donor, sim.faults)
        self.assertEqual(left['metrics']['min_soc'], right['metrics']['min_soc'])

    def test_receiver_delay_keeps_handoff_pair_and_fault_clock_synchronized(self):
        sim, donor, task_id = self.stopped_carrier('handoff-pair-delay', assistance=True)
        receiver = sim.assists[donor]['receiver']
        original_handoff = sim.assists[donor]['handoff_at']
        original_until = sim.faults[donor]['until']
        point = sim._point(sim.robots[donor])
        self.assertTrue(sim.inject_delay(receiver, 100))
        self.assertEqual(sim.assists[donor]['handoff_at'], original_handoff + 100)
        self.assertEqual(sim.faults[donor]['until'], original_until + 100)
        while sim.elapsed < sim.faults.get(donor, {}).get('until', sim.elapsed):
            sim.step()
            self.assertEqual(sim._point(sim.robots[donor]), point)
        self.advance(sim, original_until + 700)
        self.assertEqual(sim.metrics['handoffs'], 1)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertEqual(sim.tasks[task_id]['handoffs'], 1)
        sim.assert_inventory()

    def test_general_replanner_cannot_drop_fault_or_handoff_required_prefix(self):
        sim, donor, _ = self.stopped_carrier('protected-prefix', assistance=True)
        receiver = sim.assists[donor]['receiver']
        old_jobs = copy.deepcopy(sim.jobs)
        old_goods = copy.deepcopy(sim.goods)
        old_table = copy.deepcopy(sim.reservations.__dict__)
        self.assertFalse(sim._coordinated_replan({}, preferred=[donor]))
        self.assertFalse(sim._coordinated_replan({}, preferred=[receiver]))
        self.assertEqual(sim.jobs, old_jobs)
        self.assertEqual(sim.goods, old_goods)
        self.assertEqual(sim.reservations.__dict__, old_table)

    def test_unrecoverably_low_measured_soc_pauses_before_any_physical_step(self):
        sim = self.simulation('fail-closed-energy', charging_policy='critical-only')
        self.advance(sim, 160)
        donor = next(i for i, robot in sim.robots.items() if robot['carrying'])
        goods_id = sim.robots[donor]['cargo_id']
        positions = {i: sim._point(robot) for i, robot in sim.robots.items()}
        now = sim.elapsed
        self.assertFalse(sim.report_soc(donor, sim.config.emergency_reserve_soc + .001))
        self.assertIsNotNone(sim.energy_unresolved)
        with self.assertRaisesRegex(RuntimeError, 'safely paused'):
            sim.step()
        self.assertEqual(sim.elapsed, now)
        self.assertEqual({i: sim._point(robot) for i, robot in sim.robots.items()}, positions)
        self.assertEqual(sim.goods[goods_id], f'ROBOT-{donor}')
        sim.assert_inventory()

    def test_second_stop_on_receiver_cannot_silently_break_active_handoff(self):
        sim, donor, task_id = self.stopped_carrier('nested-helper-stop', assistance=True)
        receiver = sim.assists[donor]['receiver']
        old_jobs = copy.deepcopy(sim.jobs)
        old_table = copy.deepcopy(sim.reservations.__dict__)
        accepted = sim.inject_stop(receiver, 100)
        if not accepted:
            # Explicitly unsupported nested faults may fail closed, but must
            # preserve the already safe handoff rather than partially commit.
            self.assertEqual(sim.jobs, old_jobs)
            self.assertEqual(sim.reservations.__dict__, old_table)
        self.advance(sim, 900)
        self.assertEqual(sim.metrics['handoffs'], 1)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        sim.assert_inventory()

    def test_handoff_while_donor_is_already_at_delivery_point_can_complete(self):
        sim = self.simulation('handoff-at-delivery', charging_policy='critical-only')
        donor = None
        for _ in range(700):
            sim.step()
            donor = next((i for i, robot in sim.robots.items()
                          if robot['carrying'] and robot['stage'] == 'placing'), None)
            if donor is not None:
                break
        self.assertIsNotNone(donor)
        task_id = sim.robots[donor]['task_id']
        goods_id = sim.robots[donor]['cargo_id']
        self.assertEqual(sim._point(sim.robots[donor]), sim.nav.locations[sim.tasks[task_id]['target_id']])
        sim.robots[donor]['battery'] = 24.
        self.assertTrue(sim.request_assistance(donor))
        self.assertEqual(sim.goods[goods_id], f'ROBOT-{donor}')
        self.advance(sim, sim.elapsed + 900)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertEqual(sim.tasks[task_id]['handoffs'], 1)
        events = [e for e in sim.pending_events if e.get('task_id') == task_id]
        self.assertEqual(sum(e['kind'] == 'cargo_handoff' for e in events), 1)
        self.assertEqual(sum(e['kind'] in {'placed', 'shipped'} for e in events), 1)
        sim.assert_inventory()


if __name__ == '__main__':
    unittest.main()
