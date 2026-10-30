"""--robot-processes wiring: class composition and option checks.

physical_classes builds a fresh engine class on each call, so the checks compare
class names and the method resolution order rather than class identity.
"""
import unittest

from backend.warehouse.composition import physical_classes


def names(cls):
    return [c.__name__ for c in cls.__mro__]


class RobotProcessWiringTests(unittest.TestCase):
    def test_off_keeps_the_sequential_engine(self):
        base, _ = physical_classes('wheeled')
        off, _ = physical_classes('wheeled', robot_processes='off')
        self.assertEqual(names(off), names(base))
        self.assertNotIn('RobotProcessSplit', names(off))

    def test_local_and_ray_put_the_split_in_front_of_the_same_engine(self):
        base, _ = physical_classes('wheeled')
        local, _ = physical_classes('wheeled', robot_processes='local')
        ray, _ = physical_classes('wheeled', robot_processes='ray', ray_address='127.0.0.1:27380', ray_actor_cpus=0.02)
        self.assertEqual(names(local)[1:], ['RobotProcessSplit'] + names(base))
        self.assertEqual(names(ray)[1:], ['RayRobotProcessSplit', 'RobotProcessSplit'] + names(base))
        self.assertEqual((ray.ray_address, ray.ray_actor_cpus), ('127.0.0.1:27380', 0.02))

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            physical_classes('wheeled', robot_processes='threads')


if __name__ == '__main__':
    unittest.main()
