import unittest
from types import SimpleNamespace

from backend.domain import RobotAgent
from backend.runtime import LocalRobotRuntime, _RayRobotActor


class RobotActorGraphTests(unittest.TestCase):
    def test_actor_writes_assignment_and_commit_from_its_own_receipt_path(self):
        actor = _RayRobotActor(RobotAgent.create(1, 20261013).snapshot())
        writes = []
        actor._write_graph = lambda tick, events: writes.append((tick, events))  # type: ignore[method-assign]
        assignment = {
            "graph_tick": 7, "task_id": "TK-000001", "cargo_id": "BX-004001",
            "priority": "normal", "pickup": {"x": 1.0, "y": 2.0},
            "rack_id": 1, "slot_id": "R01-L1-C1-D1", "zone": "A",
            "rack_x": 8.0, "rack_y": 4.3, "target_x": 8.5, "target_y": 5.0,
            "level": 1, "bay": 1, "depth_row": 1,
        }
        assigned = actor.assign(assignment)
        self.assertTrue(assigned["accepted"])
        self.assertEqual(writes[0][0], 7)
        self.assertEqual(writes[0][1][0]["type"], "task_assigned")

        proposal = actor.propose_step(0.25, 0.25, 8)
        committed = actor.commit_step(proposal, True)
        self.assertIsNone(committed["graph_error"])
        self.assertEqual(writes[1][0], 8)

    def test_actor_graph_snapshot_carries_fleet_robot_count(self):
        actor = _RayRobotActor(RobotAgent.create(1, 20261013).snapshot())
        captured = []
        actor.graph_writer = SimpleNamespace(record_snapshot=lambda snapshot: captured.append(snapshot))
        actor.graph_config = {"run_id": "run-a", "max_active_robots": 100,
                              "robot_count": 100, "state_interval_ticks": 4}
        actor._write_graph(1, [])
        self.assertEqual(captured[0]["robotCount"], 100)
        self.assertEqual(captured[0]["robots"][0]["id"], 1)

    def test_local_runtime_cannot_claim_actor_owned_graph_writing(self):
        with self.assertRaisesRegex(RuntimeError, "Ray backend"):
            LocalRobotRuntime().configure_graph_writer({})


if __name__ == "__main__":
    unittest.main()
