from copy import deepcopy
import unittest

from ops.audit_wheeled_capacity import spatial_snapshot


class SnapshotAuditTests(unittest.TestCase):
    def fixture(self):
        scene = {'spec': {'robotRadius': .55, 'width': 40, 'depth': 40}, 'racks': []}
        robots = [{'robot_id': i+1, 'x': 2+(i % 10)*2, 'y': 2+(i//10)*2,
                   'battery': 50.} for i in range(100)]
        return scene, robots

    def test_100_unique_positions_and_soc(self):
        s, r = self.fixture()
        out = spatial_snapshot(r, s)
        self.assertEqual(2., out['min_pair_separation_m'])
        self.assertEqual(50., out['minimum_soc'])

    def test_overlap_and_shelf_intrusion_rejected(self):
        s, r = self.fixture()
        r[1]['x'] = r[0]['x']+.5
        with self.assertRaisesRegex(AssertionError, 'body overlap'):
            spatial_snapshot(r, s)
        s, r = self.fixture()
        s['racks'] = [{'id': 'R1', 'x': 1.5, 'y': 1.5, 'w': 1., 'd': 1.}]
        with self.assertRaisesRegex(AssertionError, 'rack overlap'):
            spatial_snapshot(r, s)

    def test_nonfinite_out_of_bounds_low_soc_and_duplicates_rejected(self):
        s, base = self.fixture()
        for key, value in [('x', -1.), ('x', float('nan')), ('battery', 9.99), ('robot_id', 100)]:
            r = deepcopy(base)
            r[0][key] = value
            with self.assertRaises(AssertionError):
                spatial_snapshot(r, s)


if __name__ == '__main__':
    unittest.main()
