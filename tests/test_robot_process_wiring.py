"""--robot-processes wiring: class composition and runtime option checks."""
import unittest

from backend.warehouse.composition import physical_classes


class RobotProcessWiringTests(unittest.TestCase):
    def test_off_keeps_the_sequential_engine(self):
        base, _ = physical_classes('wheeled')
        same, _ = physical_classes('wheeled', robot_processes='off')
        self.assertIs(base, same)

    def test_local_and_ray_wrap_the_same_engine(self):
        from backend.robot_actor.local import RobotProcessSplit
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        base, _ = physical_classes('wheeled')
        local, _ = physical_classes('wheeled', robot_processes='local')
        ray, _ = physical_classes('wheeled', robot_processes='ray', ray_address='127.0.0.1:27380', ray_actor_cpus=0.02)
        self.assertTrue(issubclass(local, RobotProcessSplit) and issubclass(local, base))
        self.assertTrue(issubclass(ray, RayRobotProcessSplit) and issubclass(ray, base))
        self.assertEqual((ray.ray_address, ray.ray_actor_cpus), ('127.0.0.1:27380', 0.02))

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            physical_classes('wheeled', robot_processes='threads')


if __name__ == '__main__':
    unittest.main()
