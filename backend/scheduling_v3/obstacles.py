"""Persistent stopped bodies without expanding days of identical time buckets."""
from copy import deepcopy
from ..scheduling_v2.motion import MotionTable as BaseTable


class MotionTable(BaseTable):
    def __init__(self):
        self.fixed = {}
        super().__init__()

    def _erase(self, owner):
        self.fixed.pop(owner, None)
        super()._erase(owner)

    def reserve(self, parts, offset=0, owner=None):
        parts = list(parts)
        fixed = [deepcopy(p) for p in parts if p.get('permanent_obstacle')]
        ordinary = [p for p in parts if not p.get('permanent_obstacle')]
        for part in fixed:
            if part['a'] != part['b'] or part['end'] <= part['start']:
                raise ValueError('persistent obstacle must be a real stationary body')
            part['start'] += offset
            part['end'] += offset
        super().reserve(ordinary, offset, owner)
        if fixed:
            self.fixed[owner] = fixed

    def nearby(self, a, b, ignore, start=None, end=None):
        # A permanent candidate spans far more time buckets than the complete
        # committed table. Query its spatial index, then let the exact kernel
        # intersect real span times. Never enumerate the sentinel's buckets.
        if start is not None and end - start > 3600:
            result = super().nearby(a, b, ignore)
        else:
            result = super().nearby(a, b, ignore, start, end)
        result.extend((owner, p) for owner, parts in self.fixed.items() if owner != ignore
                      for p in parts if start is None or p['end'] >= start and p['start'] <= end)
        return result

    def prune(self, now):
        fixed = deepcopy(self.fixed)
        super().prune(now)
        self.fixed = fixed
