import ast
import os
from copy import deepcopy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.scheduling_v2.matching import minimum_assignment
from backend.scheduling_v2.motion import SafeIntervalPlanner
from backend.scheduling_v4.geometry import OptimizedMotionTable
from backend.scheduling_v4.planner import GoalWindowPlanner
from backend.scheduling_v4.sim import Simulation as EngineSimulation
from backend.scheduling_v5.sim import CandidateSimulation, Config, execution_identity, resolve_parameters
from backend.scheduling_v5.matching import guarded_assignment
from backend.wide.scenarios import generate_scenario
from tests.test_scheduler_v4 import physical


CODE_ROOT = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get('WAREHOUSE_FIXTURE_ROOT', CODE_ROOT))


_PORT_RELIEF_OVERRIDE = ast.parse("""if self.config.port_relief_policy == 'deadline':
    pool = self._port_relief_pool(ready)
""").body[0]
_ACTIVATION_FILTER = ast.parse(
    "candidates = [r for r in candidates if self.activation_available(r)]").body[0]
_ACTIVATION_SKIP = ast.parse(
    "if not self.activation_available(r):\n    continue\n").body[0]


def _is_exact_port_relief_override(node):
    # Compare the WHOLE subtree: condition, single assignment/call and no else.
    # Extra logic, a changed RHS/condition, or any safety statements remain visible.
    return isinstance(node, ast.If) and ast.dump(node, include_attributes=False) == ast.dump(
        _PORT_RELIEF_OVERRIDE, include_attributes=False)


def _is_exact_activation_filter(node):
    return isinstance(node, ast.Assign) and ast.dump(node, include_attributes=False) == ast.dump(
        _ACTIVATION_FILTER, include_attributes=False)


def _is_exact_activation_skip(node):
    return isinstance(node, ast.If) and ast.dump(node, include_attributes=False) == ast.dump(
        _ACTIVATION_SKIP, include_attributes=False)


class PolicyContractTests(unittest.TestCase):
    def test_static_dispatch_copy_has_only_the_documented_policy_seam(self):
        def method(path, owner):
            module = ast.parse(path.read_text(encoding='utf-8'))
            cls = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == owner)
            return next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == 'release_and_dispatch')

        class Normalize(ast.NodeTransformer):
            def __init__(self):
                self.port_relief_overrides = 0
                self.activation_filters = 0
                self.activation_skips = 0

            def visit_If(self, node):
                if _is_exact_activation_skip(node):
                    self.activation_skips += 1
                    return None
                if _is_exact_port_relief_override(node):
                    self.port_relief_overrides += 1
                    return None
                return self.generic_visit(node)

            def visit_Assign(self, node):
                if _is_exact_activation_filter(node):
                    self.activation_filters += 1
                    return None
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Tuple):
                    names = [v.id for v in node.targets[0].elts]
                    if names in [['costs', 'pair_times'], ['row', 'travel_row']]:
                        node.targets = [ast.Name(id=names[0], ctx=ast.Store())]
                        node.value = ast.List(elts=[], ctx=ast.Load())
                return self.generic_visit(node)

            def visit_Expr(self, node):
                call = node.value
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'append' and isinstance(call.func.value, ast.Name) and call.func.value.id in {'pair_times', 'travel_row'}:
                    return None
                return self.generic_visit(node)

            def visit_Call(self, node):
                if isinstance(node.func, ast.Attribute) and node.func.attr == '_select_assignment':
                    node.func.attr = '_assignment'
                    node.args = node.args[:1]
                return self.generic_visit(node)

        old = method(CODE_ROOT/'backend/scheduling_v3/sim.py', 'Simulation')
        normalizer = Normalize()
        new = normalizer.visit(method(CODE_ROOT/'backend/scheduling_v5/sim.py', 'CandidateSimulation'))
        self.assertEqual(normalizer.port_relief_overrides, 1)
        self.assertEqual(normalizer.activation_filters, 1)
        self.assertEqual(normalizer.activation_skips, 1)
        self.assertEqual(ast.dump(old, include_attributes=False), ast.dump(new, include_attributes=False))

    def test_port_relief_ast_exception_matches_only_the_exact_documented_override(self):
        source = "if self.config.port_relief_policy == 'deadline':\n    pool = self._port_relief_pool(ready)\n"
        self.assertTrue(_is_exact_port_relief_override(ast.parse(source).body[0]))
        mutations = [
            source.replace("== 'deadline'", "!= 'deadline'"),
            source.replace('ready)', 'all_tasks)'),
            source.replace('pool =', 'costs ='),
            source.replace('_port_relief_pool', '_unsafe_pool'),
            source + 'else:\n    self.schedule_charge(robot)\n',
            source + '    self.assert_inventory()\n',
        ]
        for changed in mutations:
            with self.subTest(changed=changed):
                self.assertFalse(_is_exact_port_relief_override(ast.parse(changed).body[0]))

    def select(self, policy, rows, times, elapsed=300, parameters=None):
        context = SimpleNamespace(config=Config(), policy=policy, policy_parameters=resolve_parameters(policy, parameters),
            elapsed=elapsed, metrics={'v5_matching_calls': 0, 'v5_guard_rows': 0,
                'v5_guard_rows_selected': 0, 'v5_rewarded_rows': 0})
        costs = [[value, 1e8, 1e8] for value in rows]
        pool = [{'ready_at': ready, 'due_second': due} for ready, due in times]
        return CandidateSimulation._select_assignment(context, costs, pool, [[r] for r in rows])

    def test_guard_preserves_ordinary_cost_choice_until_wait_or_deadline_guard(self):
        # At elapsed300, neither ordinary job is urgent: the nearer one wins.
        tasks = [(200, 2000), (200, 2000)]
        self.assertIn((1, 0), self.select('guard', [200, 20], tasks))
        self.assertIn((0, 0), self.select('guard', [200, 20], [(0, 2000), (200, 2000)]))
        # Slack120 is protected even though task age is only100.
        self.assertIn((0, 0), self.select('guard', [200, 20], [(200, 620), (200, 2000)]))

    def test_pressure_is_gradual_but_does_not_modify_original_cost_control(self):
        tasks = [(50, 2000), (300, 2000)]
        self.assertIn((0, 0), self.select('pressure', [100, 20], tasks))
        self.assertIn((1, 0), self.select('cost', [100, 20], tasks))

    def test_parameters_and_identity_are_explicit_and_validated(self):
        self.assertNotEqual(execution_identity('guard'), execution_identity('pressure'))
        self.assertNotEqual(execution_identity('guard'), execution_identity('guard', {'wait_limit': 240}))
        self.assertNotEqual(execution_identity('guard'), execution_identity('guard', planner='original-sipp'))
        for policy, params in [('unknown', {}), ('cost', {'wait_limit': 300}),
                              ('guard', {'wait_limit': 0}), ('guard', {'wait_limit': True}),
                              ('guard', {'wait_limit': 2.5}), ('pressure', {'age_weight': float('nan')}),
                              ('guard', {'deadline_guard': -1}), ('guard', {'wait_limit': 10**1000}),
                              ('port-guard', {'port_queue_cap': 7}), ('port-guard', {'port_queue_cap': 6.0})]:
            with self.assertRaises(ValueError):
                execution_identity(policy, params)
        with self.assertRaises(ValueError):
            execution_identity('guard', planner='unknown')

    def port_select(self, *, full=True, queue=6, occupied=False, reserved=False, capacity=False,
                    critical_out=False, weight=1):
        pool = [dict(task_id='in', task_type='INBOUND', status='accepted', source_id='IN-1',
                     ready_at=300, due_second=2000),
                dict(task_id='out', task_type='OUTBOUND', status='waiting', source_id='S-old',
                     ready_at=0 if critical_out else 300, due_second=2000)]
        tasks = {f'q{i}': dict(task_id=f'q{i}', task_type='INBOUND', status='waiting',
                              source_id='IN-1', target_id=f'S{i}') for i in range(queue)}
        slots = {f'S{i}': dict(occupied=occupied, reservedBy='other' if reserved else None) for i in range(max(1, queue))}
        metrics = {'v5_matching_calls': 0, 'v5_guard_rows': 0, 'v5_guard_rows_selected': 0,
                   'v5_rewarded_rows': 0, 'v5_port_rewarded_rows': 0}
        context = SimpleNamespace(policy='port-guard', elapsed=400,
            policy_parameters=resolve_parameters('port-guard', {'port_pressure_weight': weight}),
            config=SimpleNamespace(dispatch_seconds=10), ports={'IN-1': 6 if full else 5},
            pending=list(tasks), tasks=tasks, slots=slots, inside=len(slots) if capacity else 0,
            metrics=metrics)
        with patch('backend.scheduling_v5.sim.guarded_assignment', wraps=guarded_assignment) as matcher:
            result = CandidateSimulation._select_assignment(context, [[70, 1e8, 1e8], [20, 1e8, 1e8]], pool, [[70], [20]])
            metrics['probe_rewards'] = matcher.call_args.args[2]
        return result, metrics

    def test_port_reward_requires_actual_full_buffer_and_admissible_waiting_in(self):
        self.assertIn((0, 0), self.port_select()[0])
        for options in [dict(full=False), dict(queue=0), dict(occupied=True), dict(reserved=True),
                        dict(capacity=True), dict(weight=0)]:
            result, metrics = self.port_select(**options)
            self.assertIn((1, 0), result)
            self.assertEqual(metrics['v5_port_rewarded_rows'], 0)

    def test_port_reward_does_not_override_critical_outbound_guard(self):
        self.assertIn((1, 0), self.port_select(critical_out=True)[0])
        result, metrics = self.port_select(queue=60)
        self.assertIn((0, 0), result)
        self.assertEqual(metrics['v5_port_rewarded_rows'], 1)
        self.assertEqual(metrics['probe_rewards'], [60., 0.])


class RuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (ROOT/'data/local-check').mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        cls.folder = Path(cls.temp.name)
        cls.scene = ROOT/'data/layouts/warehouse100-wide-v2.json'
        cls.dataset = cls.folder/'input'
        generate_scenario(cls.scene, cls.dataset, duration_seconds=1800,
                          slot_interval_seconds=3600, active_slots=48)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self, policy='guard', name='candidate-test', **kw):
        return CandidateSimulation(self.scene, self.dataset, name, Config(), policy=policy, **kw)

    def test_cost_control_copy_preserves_engine_dispatch_physics(self):
        class Control(EngineSimulation):
            _assignment = staticmethod(minimum_assignment)
        control = Control(self.scene, self.dataset, 'cost-copy', Config())
        candidate = self.make('cost', 'cost-copy')
        for _ in range(360):
            control.step()
            candidate.step()
        self.assertEqual(physical(control), physical(candidate))
        self.assertEqual(control.metrics['completed'], candidate.metrics['completed'])
        self.assertEqual(control.metrics['planning_calls'], candidate.metrics['planning_calls'])

    def test_explicit_off_fifo_matches_default_physics(self):
        default = self.make('port-guard', 'off-control', planner='original-sipp')
        explicit = CandidateSimulation(self.scene, self.dataset, 'off-control',
            Config(inbound_admission_policy='fifo', port_relief_policy='off'),
            policy='port-guard', planner='original-sipp')
        for _ in range(120):
            default.step()
            explicit.step()
        self.assertEqual(physical(default), physical(explicit))
        self.assertEqual(default.metrics['planning_calls'], explicit.metrics['planning_calls'])
        self.assertEqual(default.config.inbound_admission_policy, 'fifo')
        self.assertEqual(default.config.port_relief_policy, 'off')

    def test_guard_low_soc_handoff_restore_and_future_remain_exact(self):
        sim = self.make()
        for _ in range(800):
            sim.step()
            donor = next((i for i, r in sim.robots.items() if r['carrying']), None)
            if donor is not None:
                break
        self.assertIsNotNone(donor)
        self.assertTrue(sim.report_soc(donor, 24.))
        self.assertIn(donor, sim.assists)
        path = self.folder/'v5-handoff.json'
        write_checkpoint(path, sim.checkpoint())
        restored = self.make(checkpoint=read_checkpoint(path))
        self.assertIsInstance(restored.planner, GoalWindowPlanner)
        self.assertIsInstance(restored.reservations, OptimizedMotionTable)
        self.assertIs(restored.planner.table, restored.reservations)
        for _ in range(180):
            sim.step()
            restored.step()
        self.assertEqual(physical(sim), physical(restored))
        self.assertEqual(sim.planner.expansions, restored.planner.expansions)
        self.assertEqual(sim.metrics['v5_matching_calls'], restored.metrics['v5_matching_calls'])
        restored.assert_inventory()

    def test_cross_policy_parameters_source_and_v4_restore_are_refused(self):
        sim = self.make()
        state = sim.checkpoint()
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.make('pressure', checkpoint=state)
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.make(policy_parameters={'wait_limit': 240}, checkpoint=state)
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.make(planner='original-sipp', checkpoint=state)
        bad = deepcopy(state)
        bad['scheduler_v5']['identity']['source_sha256'] = 'tampered'
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.make(checkpoint=bad)
        engine = EngineSimulation(self.scene, self.dataset, sim.run_id, Config())
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.make(checkpoint=engine.checkpoint())
        self.assertEqual(sim.frame()['scheduler']['policy'], 'guard')
        self.assertEqual(sim.execution_status()['identity'], state['scheduler_v5']['identity'])

    def test_original_sipp_keeps_exact_caches_and_restores_explicit_identity(self):
        sim = self.make('port-guard', planner='original-sipp')
        self.assertIs(type(sim.planner), SafeIntervalPlanner)
        self.assertIsInstance(sim.reservations, OptimizedMotionTable)
        self.assertEqual(sim.execution_status()['identity']['planner'], 'original-sipp')
        self.assertNotIn('goal-window-sipp', sim.execution_status()['engine_identity']['features'])
        for _ in range(180):
            sim.step()
        restored = self.make('port-guard', planner='original-sipp', checkpoint=sim.checkpoint())
        for _ in range(120):
            sim.step()
            restored.step()
        self.assertEqual(physical(sim), physical(restored))
        self.assertEqual(sim.planner.expansions, restored.planner.expansions)


if __name__ == '__main__':
    unittest.main()
