"""Independent implementation of classical minimum-cost rectangular assignment.

Method reference: H. W. Kuhn (1955), The Hungarian method for the assignment
problem, doi:10.1002/nav.3800020109. No SciPy or third-party solver source is
copied or vendored. Dummy columns represent genuinely unassigned tasks.
"""
import math


def minimum_assignment(costs):
    """Return (row, column) pairs minimizing a finite matrix, rows <= columns.

    This solves only the supplied linear assignment, not joint multi-robot
    routing. Callers must add dummy columns and exclude forbidden pairs.
    """
    if not costs:
        return []
    rows, cols = len(costs), len(costs[0])
    if cols < rows or any(len(row) != cols for row in costs):
        raise ValueError('assignment requires a rectangular matrix, rows <= columns')
    if any(not math.isfinite(v) for row in costs for v in row):
        raise ValueError('costs must be finite; use explicit dummy assignments')
    # Primal-dual augmentations. Column zero is the temporary unmatched root.
    row_price, col_price = [0.] * (rows + 1), [0.] * (cols + 1)
    matched_row = [0] * (cols + 1)
    for root in range(1, rows + 1):
        matched_row[0] = root
        best = [math.inf] * (cols + 1)
        previous = [0] * (cols + 1)
        visited = [False] * (cols + 1)
        column = 0
        while True:
            visited[column] = True
            row = matched_row[column]
            shift, next_column = math.inf, 0
            for candidate in range(1, cols + 1):
                if visited[candidate]:
                    continue
                reduced = costs[row - 1][candidate - 1] - row_price[row] - col_price[candidate]
                if reduced < best[candidate]:
                    best[candidate], previous[candidate] = reduced, column
                if best[candidate] < shift:
                    shift, next_column = best[candidate], candidate
            for candidate in range(cols + 1):
                if visited[candidate]:
                    row_price[matched_row[candidate]] += shift
                    col_price[candidate] -= shift
                else:
                    best[candidate] -= shift
            column = next_column
            if matched_row[column] == 0:
                break
        while column:
            prior = previous[column]
            matched_row[column] = matched_row[prior]
            column = prior
    return sorted((row - 1, column - 1) for column, row in enumerate(matched_row) if column and row)
