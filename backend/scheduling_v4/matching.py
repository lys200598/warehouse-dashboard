"""Priority-preserving maximum feasible task subset, then minimum-cost pairing."""
import math

from ..scheduling_v2.matching import minimum_assignment


FORBIDDEN = 1e8


def priority_assignment(costs):
    """Use the existing row order as priority, not just as pool admission order.

    Inputs retain v3's real robot columns followed by one dummy per task.
    Finite edges here are only static assignment candidates; the coordinator
    still validates full dynamic routes, battery and reservations afterwards.
    """
    if not isinstance(costs, (list, tuple)):
        raise ValueError('assignment matrix must be a sequence')
    if not costs:
        return []
    rows = len(costs)
    if rows > 100 or not isinstance(costs[0], (list, tuple)):
        raise ValueError('bounded rectangular assignment matrix required')
    columns = len(costs[0]) - rows
    if not 0 <= columns <= 100 or any(not isinstance(row, (list, tuple)) or
            len(row) != columns + rows for row in costs):
        raise ValueError('explicit dummy-column contract required')
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or
           not math.isfinite(value) for row in costs for value in row):
        raise ValueError('finite numeric assignment costs required')
    if any(value < FORBIDDEN for row in costs for value in row[columns:]):
        raise ValueError('dummy columns must be explicit unassigned placeholders')
    edges = [sorted((c for c in range(columns) if costs[r][c] < FORBIDDEN),
                    key=lambda c: (costs[r][c], c)) for r in range(rows)]
    owners = {}

    def augment(row, visited):
        for column in edges[row]:
            if column in visited:
                continue
            visited.add(column)
            if column not in owners or augment(owners[column], visited):
                owners[column] = row
                return True
        return False

    selected = []
    for row in range(rows):
        if augment(row, set()):
            selected.append(row)
    pairs = minimum_assignment([costs[row][:columns] for row in selected]) if selected else []
    result = []
    for selected_row, column in pairs:
        row = selected[selected_row]
        if costs[row][column] >= FORBIDDEN:
            raise AssertionError('selected subset lost finite matching')
        result.append((row, column))
    omitted = [row for row in range(rows) if row not in selected]
    result.extend((row, columns + index) for index, row in enumerate(omitted))
    return sorted(result)
