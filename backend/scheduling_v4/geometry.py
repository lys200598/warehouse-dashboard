"""Bounded immutable geometry memoization, never cached traffic occupancy."""
from collections import OrderedDict
from functools import lru_cache
import math

from ..day1.nav import Reservations
from ..wide.motion import WideMotionTable, WideRoadmap


CELL_CACHE_LIMIT = 60000
DISTANCE_CACHE_LIMIT = 512


@lru_cache(maxsize=CELL_CACHE_LIMIT)
def _cells(ax, ay, bx, by, padding):
    if not all(math.isfinite(value) for value in (ax, ay, bx, by, padding)) or padding < 0:
        raise ValueError('finite geometry and nonnegative padding required')
    return tuple(Reservations.cells({'x': ax, 'y': ay}, {'x': bx, 'y': by}, padding))


def cached_cells(a, b, padding=1.1):
    return _cells(a['x'], a['y'], b['x'], b['y'], padding)


def geometry_cache_info():
    return _cells.cache_info()._asdict()


class OptimizedMotionTable(WideMotionTable):
    cells = staticmethod(cached_cells)


class OptimizedRoadmap(WideRoadmap):
    def __init__(self, navigator):
        super().__init__(navigator)
        if navigator.private:
            raise ValueError('v4 geometry alias cache requires the wide public-bay model')
        self._distance_cache = OrderedDict()
        self._distance_hits = self._distance_misses = 0

    def distances(self, target, speed):
        point = self.locations[target]
        signature = (point['x'], point['y'], tuple(self.connections(target)), speed)
        if signature in self._distance_cache:
            self._distance_hits += 1
            self._distance_cache.move_to_end(signature)
            return self._distance_cache[signature]
        self._distance_misses += 1
        value = self._compute_distances(target, speed)
        self._distance_cache[signature] = value
        if len(self._distance_cache) > DISTANCE_CACHE_LIMIT:
            self._distance_cache.popitem(last=False)
        return value

    def cache_stats(self):
        return {'hits': self._distance_hits, 'misses': self._distance_misses,
                'maxsize': DISTANCE_CACHE_LIMIT, 'currsize': len(self._distance_cache)}
