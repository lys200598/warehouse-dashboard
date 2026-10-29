import unittest

from ops.battery_supply_sensitivity import SupplyModel


class SupplySensitivityTests(unittest.TestCase):
    def test_finite_pack_identity_and_time_conservation(self):
        r = SupplyModel(robot_count=3, stations=1, spares_per_station=1,
            channels_per_station=1, fallback=False, initial='staggered', measure_hours=12).run()
        self.assertEqual(4, r['physical_pack_count'])
        self.assertTrue(r['pack_identity_conserved'])
        self.assertEqual(3*12*3600, sum(r['robot_seconds'].values()))
        self.assertGreater(r['counts']['swaps_completed'], 0)
        self.assertFalse(r['warehouse_business_acceptance'])
        self.assertFalse(r['traffic_included'])

    def test_fixed_seed_reproduces(self):
        kw = dict(initial='staggered', measure_hours=3)
        self.assertEqual(SupplyModel(**kw).run(), SupplyModel(**kw).run())

    def test_steady_supply_improves_with_finite_extra_channels(self):
        a = SupplyModel(initial='staggered', fallback=False, channels_per_station=2,
            warmup_hours=48, measure_hours=12).run()
        b = SupplyModel(initial='staggered', fallback=False, channels_per_station=4,
            warmup_hours=48, measure_hours=12).run()
        self.assertGreater(b['productive_fraction'], a['productive_fraction'])
        self.assertEqual(a['physical_pack_count'], b['physical_pack_count'])
        self.assertGreater(a['max_waiting_robots'], b['max_waiting_robots'])

    def test_validation(self):
        for kw in ({'measure_hours': 0}, {'channels_per_station': True}, {'initial': 'unknown'}):
            with self.assertRaises(ValueError):
                SupplyModel(**kw)


if __name__ == '__main__':
    unittest.main()
