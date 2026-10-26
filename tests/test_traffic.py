import unittest

from backend.traffic import TrafficManager


def proposal(robot_id, start, target, priority="normal"):
    return {
        "robot_id": robot_id,
        "moving": start != target,
        "from": {"x": start[0], "y": start[1]},
        "to": {"x": target[0], "y": target[1]},
        "priority": priority,
    }


class TrafficManagerTest(unittest.TestCase):
    def test_only_one_robot_wins_same_target_cell(self):
        manager = TrafficManager(cell_size=1.0)
        result = manager.resolve(
            [
                proposal(2, (3.1, 0.1), (1.1, 0.1)),
                proposal(1, (0.1, 0.1), (1.2, 0.1), priority="high"),
            ]
        )
        self.assertTrue(result.accepted[1])
        self.assertFalse(result.accepted[2])

    def test_head_on_swap_is_rejected(self):
        manager = TrafficManager(cell_size=1.0)
        result = manager.resolve(
            [
                proposal(1, (0.1, 0.1), (1.1, 0.1)),
                proposal(2, (1.1, 0.1), (0.1, 0.1)),
            ]
        )
        self.assertEqual(result.blocked_robot_ids, [1, 2])


if __name__ == "__main__":
    unittest.main()
