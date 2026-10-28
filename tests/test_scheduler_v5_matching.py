import itertools
import math
import random
import struct
import unittest
from unittest.mock import patch

from backend.scheduling_v5.matching import guarded_assignment
from backend.scheduling_v2.matching import minimum_assignment


class GuardedMatchingTests(unittest.TestCase):
    def test_360_small_graphs_against_exhaustive_lexicographic_oracle(self):
        rng = random.Random(2026102805)
        for _ in range(360):
            rows, columns = rng.randint(1, 5), rng.randint(0, 4)
            costs = [[1e12 if rng.random() < .35 else rng.randint(-10, 500)
                      for _ in range(columns)] + [1e8] * rows for _ in range(rows)]
            protected = rng.sample(range(rows), rng.randint(0, rows))
            rewards = [rng.randint(0, 100) * .5 for _ in range(rows)]
            scores = []
            for selected in itertools.product(range(-1, columns), repeat=rows):
                real = [c for c in selected if c >= 0]
                if len(real) != len(set(real)) or any(c >= 0 and costs[r][c] >= 1e8
                                                    for r, c in enumerate(selected)):
                    continue
                scores.append((-len(real), tuple(-int(selected[r] >= 0) for r in protected),
                    sum(costs[r][c] - rewards[r] for r, c in enumerate(selected) if c >= 0)))
            result = guarded_assignment(costs, protected, rewards)
            self.assertEqual(len(result), rows)
            self.assertEqual(len({r for r, _ in result}), rows)
            self.assertEqual(len({c for _, c in result}), rows)
            chosen = {r: c for r, c in result if c < columns}
            actual = (-len(chosen), tuple(-int(r in chosen) for r in protected),
                      sum(costs[r][c] - rewards[r] for r, c in chosen.items()))
            self.assertEqual(actual, min(scores))

    def test_ordinary_rows_are_not_implicitly_locked_by_input_order(self):
        costs = [[600, 1e8, 1e8], [50, 1e8, 1e8]]
        self.assertEqual([(r, c) for r, c in guarded_assignment(costs) if c == 0], [(1, 0)])
        self.assertEqual([(r, c) for r, c in guarded_assignment(costs, [0]) if c == 0], [(0, 0)])

    def test_guard_can_be_extended_without_destroying_global_cardinality(self):
        # Giving guard row 0 its cheapest edge would strand row 1. Joint pairing
        # must move row 0 onto robot 1 and keep BOTH tasks, not hard-fix the edge.
        costs = [[1, 10, 1e8, 1e8], [2, 1e12, 1e8, 1e8]]
        self.assertEqual(guarded_assignment(costs, [0]), [(0, 1), (1, 0)])

    def test_infeasible_guard_does_not_hide_independent_work(self):
        costs = [[1e12, 1e8, 1e8], [5, 1e8, 1e8]]
        real = [(r, c) for r, c in guarded_assignment(costs, [0, 1]) if c == 0]
        self.assertEqual(real, [(1, 0)])

    def test_reward_cannot_reduce_cardinality_or_override_guard(self):
        costs = [[400, 1e8, 1e8], [1, 1e8, 1e8]]
        self.assertEqual([(r, c) for r, c in guarded_assignment(costs, [0], [0, 1e9]) if c == 0], [(0, 0)])
        self.assertEqual([(r, c) for r, c in guarded_assignment(costs, [], [500, 0]) if c == 0], [(0, 0)])

    def test_invalid_contracts_fail_explicitly(self):
        for matrix in [None, [[1]], [[1, math.nan]], [[True, 1e8]], [[math.inf, 1e8]],
                       [[1, 0]], [[-1e15, 1e8]], [[10**1000, 1e8]], [[1e8]] * 101]:
            with self.assertRaises(ValueError):
                guarded_assignment(matrix)
        for protected in [[0, 0], [True], [1], '0']:
            with self.assertRaises(ValueError):
                guarded_assignment([[10, 1e8]], protected)
        for rewards in [[-1], [math.nan], [True], [], [1e15], [10**1000]]:
            with self.assertRaises(ValueError):
                guarded_assignment([[10, 1e8]], [], rewards)
        self.assertEqual(guarded_assignment([]), [])

    def test_common_large_reward_cannot_erase_small_cost_difference(self):
        costs = [[100.00001, 1e8, 1e8], [100., 1e8, 1e8]]
        self.assertIn((1, 0), guarded_assignment(costs, [], [1e12, 1e12]))
        self.assertEqual(guarded_assignment([], [], []), [])
        self.assertEqual(guarded_assignment([[1e8, 1e8], [1e8, 1e8]], [], [1e12, 1e12]), [(0, 0), (1, 1)])

    def test_zero_reward_r1_cost_domain_keeps_identical_hungarian_matrix(self):
        rng = random.Random(51)
        for _ in range(80):
            rows, columns = rng.randint(1, 12), rng.randint(1, 16)
            costs = [[1e12 if rng.random() < .2 else
                      rng.randrange(300) + rng.randrange(500) + 4*rng.randrange(600) + .05*rng.randrange(100)
                      for _ in range(columns)] + [1e8]*rows for _ in range(rows)]
            protected = rng.sample(range(rows), rng.randrange(rows+1))
            captures = []
            def capture(matrix):
                captures.append(matrix)
                return minimum_assignment(matrix)
            with patch('backend.scheduling_v5.matching.minimum_assignment', capture):
                result = guarded_assignment(costs, protected, [0.]*rows)
            chosen = {r for r, c in result if c < columns}
            required = chosen & set(protected)
            values = [costs[r][c] for r in range(rows) for c in range(columns) if costs[r][c] < 1e8]
            offset = min(values, default=0.)
            ceiling = max((v-offset for v in values), default=0.)
            penalty = (ceiling+1)*(rows+1)
            old_matrix = [[costs[r][c]-0.-offset if costs[r][c] < 1e8 else penalty
                           for c in range(columns)] + [penalty if r in required else 0.]*(rows-len(chosen))
                          for r in range(rows)]
            self.assertEqual([[struct.pack('d', v) for v in row] for row in captures[0]],
                             [[struct.pack('d', v) for v in row] for row in old_matrix])
            self.assertEqual(result, minimum_assignment(old_matrix))


if __name__ == '__main__':
    unittest.main()
