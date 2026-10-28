import importlib.util
import math
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location('browser_source_profile',
    Path(__file__).resolve().parents[1] / 'ops/browser_query_source_profile.py')
MOD = importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)


class BrowserNumberTests(unittest.TestCase):
    def test_exact_finite_number_representation_is_recorded(self):
        result = MOD.browser_number_compare({'distance': 0}, {'distance': 0.0})
        self.assertTrue(result['passed'])
        self.assertEqual(result['conversions'][0], {'path': '$.distance', 'actual_type': 'int', 'actual_value': 0,
            'expected_type': 'float', 'expected_value': 0.0,
            'rule': 'finite exactly-equal JSON number representation; no rounding'})

    def test_boolean_is_not_number(self):
        for left, right in ((False, 0), (True, 1.), (0, False)):
            with self.subTest(values=(left, right)):
                self.assertFalse(MOD.browser_number_compare(left, right)['passed'])

    def test_small_value_difference_is_not_tolerated(self):
        self.assertFalse(MOD.browser_number_compare(.1, .1001)['passed'])

    def test_large_integer_precision_loss_is_not_hidden(self):
        value = 2 ** 53 + 1
        self.assertFalse(MOD.browser_number_compare(float(value), value)['passed'])
        self.assertFalse(MOD.browser_number_compare(int(float(value)), value)['passed'])
        self.assertTrue(MOD.browser_number_compare(10 ** 500, 10 ** 500)['passed'])

    def test_nonfinite_numbers_are_rejected(self):
        for number in (math.nan, math.inf, -math.inf):
            self.assertFalse(MOD.browser_number_compare(number, number)['passed'])

    def test_fields_and_array_order_remain_exact(self):
        self.assertFalse(MOD.browser_number_compare({'a': 1, 'extra': 2}, {'a': 1})['passed'])
        self.assertFalse(MOD.browser_number_compare([1, 2], [2, 1])['passed'])
        self.assertFalse(MOD.browser_number_compare('0', 0)['passed'])

    def test_score_approx_keeps_original_explicit_contract(self):
        spec = importlib.util.spec_from_file_location('strict_score_verifier',
            Path(__file__).resolve().parents[1] / 'ops/verify_catalog_responses.py')
        verifier = importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
        point = verifier.Approx(.5, 1e-12)
        options = {'approx_type': verifier.Approx, 'approx_compare': verifier.compare}
        self.assertTrue(MOD.browser_number_compare(.5 + 5e-13, point, **options)['passed'])
        self.assertFalse(MOD.browser_number_compare(.5001, point, **options)['passed'])


if __name__ == '__main__':
    unittest.main()
