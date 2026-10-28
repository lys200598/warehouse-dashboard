"""Maximum-cardinality matching with limited priority guards, then cost.

The task rows protected by a guard are a matchable independent set. Extending
that set to a maximum matching does not require fixing ALL other task rows.
Only selected guard rows are forbidden from taking a dummy column; ordinary
rows remain free to trade task selection and robot pairing by total cost.
"""
import math

from ..scheduling_v2.matching import minimum_assignment


FORBIDDEN = 1e8


def _validate(costs):
    if not isinstance(costs, (list, tuple)):
        raise ValueError('assignment matrix must be a sequence')
    if not costs:
        return 0, 0
    rows = len(costs)
    if rows > 100 or not isinstance(costs[0], (list, tuple)):
        raise ValueError('bounded rectangular assignment matrix required')
    columns = len(costs[0]) - rows
    if not 0 <= columns <= 100 or any(not isinstance(row, (list, tuple)) or
            len(row) != rows + columns for row in costs):
        raise ValueError('one explicit dummy per task required')
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or
            abs(v) > 1e12 or not math.isfinite(v) for row in costs for v in row):
        raise ValueError('bounded finite numeric costs required')
    if any(v < FORBIDDEN for row in costs for v in row[columns:]):
        raise ValueError('dummy columns must be explicit unassigned placeholders')
    return rows, columns


def guarded_assignment(costs, protected=(), rewards=None):
    """Lexicographic objectives: cardinality, protected rows, adjusted cost.

``protected`` is a priority-ordered list of distinct row indices. A row without
any feasible static robot pair is not fabricated as feasible. ``rewards`` is a
finite, nonnegative defer-avoidance value per row; it changes only the final
cost objective, never cardinality, guard ordering, or forbidden edges.

Returned real columns refer to the original robot order. Dummy columns may be
fewer than the input because their exact count fixes the maximum cardinality.
All dynamic route, SOC, capacity and reservation checks remain the caller's job.
    """
    rows, columns = _validate(costs)
    if not isinstance(protected, (list, tuple)) or any(isinstance(r, bool) or
            not isinstance(r, int) or not 0 <= r < rows for r in protected) or len(set(protected)) != len(protected):
        raise ValueError('protected rows must be distinct valid indices')
    if rewards is None:
        rewards = [0.] * rows
    if not isinstance(rewards, (list, tuple)) or len(rewards) != rows or any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or
            not 0 <= v <= 1e12 or not math.isfinite(v) for v in rewards):
        raise ValueError('one bounded finite nonnegative reward per task required')
    if not rows:
        return []
    edges = [sorted((c for c in range(columns) if costs[r][c] < FORBIDDEN),
                    key=lambda c: (costs[r][c], c)) for r in range(rows)]

    def selected_rows(order):
        owners, selected = {}, []

        def augment(row, seen):
            for col in edges[row]:
                if col in seen:
                    continue
                seen.add(col)
                if col not in owners or augment(owners[col], seen):
                    owners[col] = row
                    return True
            return False

        for row in order:
            if augment(row, set()):
                selected.append(row)
        return selected

    count = len(selected_rows(range(rows)))
    required = set(selected_rows(protected))
    dummy_count = rows - count
    # Exactly count rewards are taken. Remove a common reward BEFORE subtracting
    # from small costs, so a large shared reward cannot erase meaningful ties.
    reward_origin = min(rewards, default=0.)
    adjusted_rewards = [value - reward_origin for value in rewards]
    values = [costs[r][c] - adjusted_rewards[r] for r in range(rows) for c in edges[r]]
    # Exactly count real assignments are enforced by the dummy count. Shifting
    # every feasible real edge by the same offset therefore preserves the sum.
    offset = min(values, default=0.)
    ceiling = max((value - offset for value in values), default=0.)
    penalty = (ceiling + 1.) * (rows + 1)
    matrix = [[costs[r][c] - adjusted_rewards[r] - offset if c in edges[r] else penalty
               for c in range(columns)] +
              [penalty if r in required else 0.] * dummy_count for r in range(rows)]
    result = minimum_assignment(matrix)
    real = {r: c for r, c in result if c < columns}
    if len(real) != count or not required <= set(real) or any(c not in edges[r] for r, c in real.items()):
        raise AssertionError('guarded assignment lost feasibility or cardinality')
    return result
