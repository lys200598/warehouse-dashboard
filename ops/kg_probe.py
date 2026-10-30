"""Bounded real-service development probe and five-phase timing sample."""
import argparse
import json
import statistics
from pathlib import Path
from unittest.mock import patch

from tests import test_dispatch_golden as golden
from tests.test_robot_process_kg import engine_class, audit, cleanup


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticks', type=int, default=30)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scenario', choices=('relay', 'ordinary'), default='relay')
    args = parser.parse_args()
    instances, timings = [], []
    try:
        if args.scenario == 'ordinary':
            with patch.object(golden, 'MAX_TICKS', args.ticks):
                actual = golden.record(sim_class=engine_class(instances, timings))
            expected = json.loads(golden.FIXTURE.read_text())
        else:
            actual = golden.record_relay(sim_class=engine_class(instances, timings), max_ticks=args.ticks)
            expected = json.loads(golden.RELAY_FIXTURE.read_text())
        assert actual['assignments'] == [a for a in expected['assignments'] if a['tick'] <= args.ticks]
        assert actual.get('handoffs', []) == [h for h in expected.get('handoffs', []) if h['tick'] <= args.ticks]
        evidence = audit(instances[0])
        import ray
        result = {'ticks': args.ticks, 'scenario': args.scenario, 'cpus': ray.cluster_resources()['CPU'],
                  'mean_ms': {k: statistics.fmean(r.get(k, 0.) for r in timings[1:]) for k in timings[0]}, **evidence}
        result['speedup'] = 1000 / result['mean_ms']['total_ms']
        from backend.query_c import context_distributions
        result['robot_context'] = context_distributions(list(instances[0].context_samples))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({k: v for k, v in result.items() if k != 'robot_context'}), flush=True)
        print('[context] ' + json.dumps({k: v for k, v in result['robot_context'].items() if k != 'per_robot'}), flush=True)
    finally:
        for sim in instances:
            cleanup(sim)


if __name__ == '__main__':
    main()
