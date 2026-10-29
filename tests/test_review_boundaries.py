"""Negative cases for the four P2 boundaries found in the 2026-10-29 review."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backend.agent_runtime import AgentKnowledgeService, JournalBackend
from backend.agent_runtime import fleet as fleet_module
from backend.wide.data import load_scene

ROOT = Path(__file__).resolve().parents[1]
MEDIUM = ROOT / 'data/layouts/expansion-candidates/medium/layout.json'


def observation(robot=1, seq=1, sim_time=0, canonical_seq=1):
    return {'run_id': 'test-run', 'robot_id': robot, 'seq': seq, 'sim_time': sim_time,
            'committed_seq': canonical_seq, 'state': {'robot_id': robot, 'stage': 'idle', 'soc': 80},
            'events': [{'kind': 'observation_started', 'sim_time': sim_time}]}


class AuthoritativeTimes(JournalBackend):
    """Journal backend that knows the committed sim_time of each robot state."""
    def __init__(self, times):
        super().__init__()
        self.times = times

    def source_times(self, rows):
        return dict(self.times)


class RobotRadiusTests(unittest.TestCase):
    def test_default_radius_loads(self):
        load_scene(MEDIUM)

    def test_larger_radius_is_rejected_instead_of_under_checked(self):
        scene = json.loads(MEDIUM.read_text(encoding='utf-8'))
        scene['spec']['robotRadius'] = 0.7
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'layout.json'
            path.write_text(json.dumps(scene), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'robotRadius 0.7 m is not supported'):
                load_scene(path)


class ObservationTimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        backend = AuthoritativeTimes({'test-run:1:1': 5, 'test-run:1:2': 5})
        self.service = AgentKnowledgeService(run_id='test-run', robot_ids=[1, 2], backend=backend,
            journal_path=Path(self.tmp.name) / 'journal.sqlite', batch_wait=.01)

    def tearDown(self):
        self.service.close(); self.tmp.cleanup()

    def test_future_time_rejected_before_journal_then_correct_time_succeeds(self):
        with self.assertRaisesRegex(ValueError, 'authoritative committed state'):
            self.service.submit(1, observation(sim_time=999999))
        self.assertEqual(self.service.head(1), 0)
        self.service.submit(1, observation(sim_time=5))
        self.assertEqual(self.service.head(1), 1)

    def test_one_wrong_robot_does_not_fail_others(self):
        with self.assertRaises(ValueError):
            self.service.submit(1, observation(robot=1, sim_time=6))
        self.service.submit(2, observation(robot=2, sim_time=5))
        self.assertEqual(self.service.head(2), 1)


class FleetConstructionTests(unittest.TestCase):
    def test_http_construction_failure_closes_writer_and_journal(self):
        created = []
        real = fleet_module.AgentKnowledgeService

        def make(*args, **kwargs):
            service = real(*args, **kwargs); created.append(service); return service
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(fleet_module, 'AgentKnowledgeService', side_effect=make), \
                patch.object(fleet_module, 'AgentHttpServer', side_effect=OSError('address in use')):
            with self.assertRaises(OSError):
                fleet_module.AgentFleet(run_id='test-run', robot_ids=[1], backend=JournalBackend(), output_dir=tmp)
            self.assertEqual(len(created), 1)
            created[0].writer.join(timeout=5)
            self.assertFalse(created[0].writer.is_alive())

    def test_service_construction_failure_has_nothing_to_leak(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(fleet_module, 'AgentKnowledgeService', side_effect=OSError('journal unavailable')):
            with self.assertRaises(OSError):
                fleet_module.AgentFleet(run_id='test-run', robot_ids=[1], backend=JournalBackend(), output_dir=tmp)


class CapacityAuditExitTests(unittest.TestCase):
    def run_main(self, plan, *extra):
        from ops import audit_wheeled_capacity as audit
        with tempfile.TemporaryDirectory() as tmp, patch('builtins.print'):
            return audit.main(['--matrix', tmp, '--source', str(ROOT), '--plan-json', json.dumps(plan), *extra])

    def test_pending_case_is_not_a_green_exit(self):
        self.assertEqual(self.run_main([{'name': 'peak'}]), 2)

    def test_empty_plan_is_not_a_green_exit(self):
        self.assertEqual(self.run_main([]), 2)

    def test_allow_pending_is_explicit_report_only_mode(self):
        self.assertEqual(self.run_main([{'name': 'peak'}], '--allow-pending'), 0)


if __name__ == '__main__':
    unittest.main()
