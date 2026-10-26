import tempfile
import unittest
from pathlib import Path

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator


class SimulationCoordinatorTest(unittest.TestCase):
    def setUp(self):
        self.config = SimulationConfig(robot_count=12, step_seconds=0.5, flow_rate=3600)
        self.simulation = SimulationCoordinator(LocalRobotRuntime(), self.config)

    def tearDown(self):
        self.simulation.close()

    def test_initial_tasks_have_unique_reserved_slots(self):
        reserved = [
            slot["slot_id"]
            for rack in self.simulation.racks
            for slot in rack["slots"]
            if slot["reserved_by"] is not None
        ]
        self.assertEqual(len(reserved), self.config.robot_count)
        self.assertEqual(len(set(reserved)), len(reserved))

    def test_step_advances_all_robots_through_common_barrier(self):
        snapshot = self.simulation.step_once()
        self.assertEqual(snapshot["tick"], 1)
        self.assertEqual(snapshot["elapsed"], 0.5)
        self.assertEqual(len(snapshot["robots"]), self.config.robot_count)
        revisions = {
            payload["state"]["revision"] for payload in self.simulation.robot_snapshots
        }
        self.assertEqual(len(revisions), 1)

    def test_checkpoint_round_trip_and_deterministic_next_step(self):
        for _ in range(8):
            self.simulation.step_once()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "checkpoint.json"
            write_checkpoint(path, self.simulation.checkpoint_payload())
            restored = SimulationCoordinator(LocalRobotRuntime(), self.config)
            try:
                restored.restore_checkpoint(read_checkpoint(path))
                self.assertEqual(restored.tick, self.simulation.tick)
                self.assertEqual(restored.run_id, self.simulation.run_id)
                original_next = self.simulation.step_once()
                restored_next = restored.step_once()
                self.assertEqual(original_next["tick"], restored_next["tick"])
                self.assertEqual(original_next["robots"], restored_next["robots"])
                self.assertEqual(original_next["racks"], restored_next["racks"])
            finally:
                restored.close()

    def test_priority_task_is_queued_when_all_robots_are_busy(self):
        result = self.simulation.inject_priority_task()
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["pending_priority"], 1)

    def test_scheduler_limits_active_robots(self):
        simulation = SimulationCoordinator(
            LocalRobotRuntime(),
            SimulationConfig(robot_count=40, max_active_robots=8),
        )
        try:
            snapshot = simulation.snapshot()
            active = sum(robot["taskId"] is not None for robot in snapshot["robots"])
            self.assertEqual(active, 8)
            self.assertEqual(snapshot["maxActiveRobots"], 8)
        finally:
            simulation.close()

    def test_scheduler_and_traffic_continue_to_make_progress(self):
        simulation = SimulationCoordinator(
            LocalRobotRuntime(),
            SimulationConfig(
                robot_count=40,
                max_active_robots=8,
                step_seconds=0.25,
                flow_rate=3600,
            ),
        )
        try:
            for _ in range(400):
                snapshot = simulation.step_once()
            self.assertGreaterEqual(snapshot["completed"], 1)
            self.assertLess(len(snapshot["blockedRobotIds"]), 8)
        finally:
            simulation.close()


if __name__ == "__main__":
    unittest.main()
