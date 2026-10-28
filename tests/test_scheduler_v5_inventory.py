"""Independent physical-ledger negative cases and real runtime boundaries."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.scheduling_v4.sim import Simulation as V4Simulation
from backend.scheduling_v5.invariants import assert_warehouse_inventory
from backend.wide.scenarios import generate_scenario


def fixture():
    tasks = {
        'old-out': dict(task_id='old-out', task_type='OUTBOUND', goods_id='old',
                        source_id='S0', target_id='OUT-1', status='scheduled', robot_id=None),
        'in': dict(task_id='in', task_type='INBOUND', goods_id='new',
                   source_id='IN-1', target_id='S1', status='scheduled', robot_id=None),
        'new-out': dict(task_id='new-out', task_type='OUTBOUND', goods_id='new',
                        source_id='S1', target_id='OUT-1', status='scheduled', robot_id=None),
    }
    return SimpleNamespace(
        slots={'S0': dict(slot_id='S0', occupied=True, cargo_id='old', reservedBy=None),
               'S1': dict(slot_id='S1', occupied=False, cargo_id=None, reservedBy=None)},
        goods={'old': 'S0'}, robots={i: dict(robot_id=i, carrying=False, cargo_id=None, task_id=None)
                                    for i in [1, 2]},
        tasks=tasks, initial=[{'goods_id': 'old', 'location_id': 'S0'}],
        scene={'stations': [{'id': 'IN-1', 'kind': 'inbound'}, {'id': 'OUT-1', 'kind': 'outbound'}]},
        ports=Counter(), inside=1, metrics={'received': 0, 'shipped': 0, 'completed': 0})


def receive(sim):
    sim.tasks['in'].update(status='accepted')
    sim.slots['S1']['reservedBy'] = 'in'
    sim.goods['new'] = 'IN-1'
    sim.ports['IN-1'] += 1
    sim.metrics['received'] += 1
    sim.inside += 1
    return sim


def assigned(sim, task_id='in', owner=1):
    task = sim.tasks[task_id]
    task.update(status='assigned', robot_id=owner)
    sim.robots[owner]['task_id'] = task_id
    if task['task_type'] == 'OUTBOUND':
        sim.slots[task['source_id']]['reservedBy'] = task_id
    return sim


def picked(sim, task_id='in', owner=1):
    task = sim.tasks[task_id]
    task.update(status='carrying', robot_id=owner)
    sim.robots[owner].update(task_id=task_id, carrying=True, cargo_id=task['goods_id'])
    sim.goods[task['goods_id']] = f'ROBOT-{owner}'
    if task['task_type'] == 'INBOUND':
        sim.ports[task['source_id']] -= 1
    else:
        sim.slots[task['source_id']].update(occupied=False, cargo_id=None, reservedBy=None)
    return sim


def delivered(sim, task_id='in', owner=1):
    task = sim.tasks[task_id]
    task.update(status='completed')
    sim.robots[owner].update(carrying=False, cargo_id=None)  # returning may still name completed task
    if task['task_type'] == 'INBOUND':
        sim.slots[task['target_id']].update(occupied=True, cargo_id=task['goods_id'], reservedBy=None)
        sim.goods[task['goods_id']] = task['target_id']
    else:
        sim.goods[task['goods_id']] = 'SHIPPED'
        sim.metrics['shipped'] += 1
        sim.inside -= 1
    sim.metrics['completed'] += 1
    return sim


class InventoryContractTests(unittest.TestCase):
    def test_all_normal_lifecycle_boundaries_and_later_outbound(self):
        sim = fixture()
        for action in [lambda s: s, receive, assigned, picked, delivered,
                       lambda s: assigned(s, 'new-out', 2),
                       lambda s: picked(s, 'new-out', 2),
                       lambda s: delivered(s, 'new-out', 2)]:
            action(sim)
            before = deepcopy(vars(sim))
            assert_warehouse_inventory(sim)
            self.assertEqual(vars(sim), before, 'validator must never repair or mutate state')

    def test_waiting_future_outbound_and_empty_approaching_helper_are_legal(self):
        sim = picked(assigned(receive(fixture())))
        sim.tasks['new-out']['status'] = 'waiting'  # not yet placed, so not ready
        sim.robots[2]['task_id'] = 'in'  # empty helper approaching donor, not a duplicate holder
        assert_warehouse_inventory(sim)
        sim.robots[1].update(carrying=False, cargo_id=None, task_id=None)
        sim.robots[2].update(carrying=True, cargo_id='new')
        sim.tasks['in']['robot_id'] = 2
        sim.goods['new'] = 'ROBOT-2'
        self.assertEqual(assert_warehouse_inventory(sim)['carried'], 1)

    def test_no_physical_robot_payload_is_rejected_in_both_directions(self):
        sim = fixture()
        sim.slots['S0'].update(occupied=False, cargo_id=None)
        sim.goods['old'] = 'ROBOT-1'
        with self.assertRaisesRegex(AssertionError, 'no physical payload'):
            assert_warehouse_inventory(sim)
        sim = picked(assigned(receive(fixture())))
        sim.goods['new'] = 'ROBOT-2'
        with self.assertRaisesRegex(AssertionError, 'carrying task'):
            assert_warehouse_inventory(sim)

    def test_negative_custody_port_reservation_and_count_cases(self):
        changes = [
            lambda s: s.ports.update({'IN-1': 1}),
            lambda s: s.ports.update({'UNKNOWN': 0}),
            lambda s: s.slots['S1'].update(reservedBy=None),
            lambda s: s.slots['S0'].update(reservedBy='UNKNOWN'),
            lambda s: s.slots['S1'].update(occupied=True, cargo_id='new'),
            lambda s: s.goods.update(new='SHIPPED'),
            lambda s: s.goods.update(new='UNKNOWN'),
            lambda s: s.goods.update(phantom='S1'),
            lambda s: s.goods.pop('old'),
            lambda s: s.metrics.update(received=0),
            lambda s: s.metrics.update(shipped=1),
            lambda s: s.metrics.update(completed=1),
            lambda s: setattr(s, 'inside', 1),
            lambda s: s.robots[2].update(cargo_id='new'),
            lambda s: s.tasks['in'].update(target_id='UNKNOWN'),
        ]
        for change in changes:
            with self.subTest(change=change):
                sim = assigned(receive(fixture()))
                change(sim)
                with self.assertRaises(AssertionError):
                    assert_warehouse_inventory(sim)

    def test_outbound_picking_releases_slot_not_total_inside_capacity(self):
        sim = fixture()
        assigned(sim, 'old-out', 1)
        assert_warehouse_inventory(sim)
        picked(sim, 'old-out', 1)
        self.assertEqual(sim.inside, 1)
        assert_warehouse_inventory(sim)
        delivered(sim, 'old-out', 1)
        self.assertEqual(assert_warehouse_inventory(sim)['inside'], 0)


class RuntimeInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        (cls.root/'data/local-check').mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(dir=cls.root/'data/local-check')
        cls.folder = Path(cls.temp.name)
        cls.scene = cls.root/'data/layouts/warehouse100-wide-v2.json'
        cls.dataset = cls.folder/'input'
        generate_scenario(cls.scene, cls.dataset, active_slots=64,
                          duration_seconds=1800, slot_interval_seconds=3600)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self, name):
        return V4Simulation(self.scene, self.dataset, 'inventory-v5-contract-'+name)

    def carrier(self, sim):
        assert_warehouse_inventory(sim)
        for _ in range(800):
            sim.step()
            assert_warehouse_inventory(sim)
            owner = next((i for i, r in sim.robots.items() if r['carrying']), None)
            if owner is not None:
                return owner
        self.fail('fixture never produced a real carrying robot')

    def test_real_ticks_handoff_delay_and_disk_restore_are_legal(self):
        sim = self.make('handoff')
        owner = self.carrier(sim)
        self.assertTrue(sim.report_soc(owner, 24.))
        assert_warehouse_inventory(sim)
        helper = sim.assists[owner]['receiver']
        self.assertTrue(sim.inject_delay(helper, 20))
        assert_warehouse_inventory(sim)
        for _ in range(5):
            sim.step()
            assert_warehouse_inventory(sim)
        path = self.folder/'handoff-checkpoint.json'
        write_checkpoint(path, sim.checkpoint())
        restored = V4Simulation(self.scene, self.dataset, sim.run_id, checkpoint=read_checkpoint(path))
        assert_warehouse_inventory(restored)
        for _ in range(700):
            sim.step()
            assert_warehouse_inventory(sim)
            restored.step()
            assert_warehouse_inventory(restored)
        self.assertGreater(sim.metrics['handoffs'], 0)
        self.assertEqual(sim.goods, restored.goods)
        self.assertEqual(sim.tasks, restored.tasks)

    def test_real_persistent_stop_transfer_and_explicit_clear_are_legal(self):
        sim = self.make('persistent')
        owner = self.carrier(sim)
        self.assertTrue(sim.inject_stop(owner, None))
        assert_warehouse_inventory(sim)
        self.assertTrue(sim.request_assistance(owner, reason='persistent_contract_test'))
        assert_warehouse_inventory(sim)
        for _ in range(700):
            sim.step()
            assert_warehouse_inventory(sim)
            if owner not in sim.assists and sim.metrics['handoffs']:
                break
        self.assertGreater(sim.metrics['handoffs'], 0)
        self.assertTrue(sim.clear_fault(owner))
        assert_warehouse_inventory(sim)
        for _ in range(100):
            sim.step()
            assert_warehouse_inventory(sim)


if __name__ == '__main__':
    unittest.main()
