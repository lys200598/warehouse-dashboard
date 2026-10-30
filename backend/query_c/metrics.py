"""Summaries of measured actor/independent-process context request samples."""
from collections import defaultdict
import statistics

from .catalog import PACKS


def _distribution(values):
    values = sorted(values)
    if not values:
        return {'n': 0, 'mean': None, 'p50': None, 'p95': None, 'p99': None, 'max': None, 'over_500': 0}
    def percentile(q):
        return values[min(len(values)-1, int(q*(len(values)-1)))]
    return {'n': len(values), 'mean': statistics.fmean(values), 'p50': percentile(.50),
            'p95': percentile(.95), 'p99': percentile(.99), 'max': values[-1],
            'over_500': sum(value > 500 for value in values)}


def context_distributions(samples):
    """Flat query() results -> aggregate and per-robot five-pack distributions.

    pack_ms is the sum of actual DB calls supplying each pack (shared reads
    appear in more than one pack). total_ms is the full measured request.
    Failed request samples are counted, never converted to zero latency.
    """
    samples = list(samples)
    good = [sample for sample in samples if 'total_ms' in sample and not sample.get('error')]
    by_robot = defaultdict(list)
    for sample in good:
        by_robot[sample['robot_id']].append(sample)

    def summarize(items):
        return {'requests': len(items), 'total_ms': _distribution([item['total_ms'] for item in items]),
                'packs': {name: {'query_ms': _distribution([item['pack_ms'][name] for item in items]),
                                 'rows': _distribution([item['pack_rows'][name] for item in items])}
                          for name in PACKS}}

    return {**summarize(good), 'errors': len(samples) - len(good), 'robots': len(by_robot),
            'processes': len({sample['pid'] for sample in good}),
            'pack_timing_scope': 'sum of shared database queries; total_ms is end-to-end',
            'per_robot': {str(robot): summarize(items) for robot, items in sorted(by_robot.items())}}
