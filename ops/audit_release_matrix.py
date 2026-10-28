"""Read-only completed-run verification; no graph connection or state changes."""
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.checkpoint import read_checkpoint
from backend.scheduling_v4.sim import execution_identity


def physical(state):
    excluded = {'run_id', 'run_metadata', 'scheduler_v4', 'robots', 'metrics'}
    timing = {'planning_seconds', 'dispatch_wall_seconds', 'recovery_wall_seconds'}
    return {**{k: v for k, v in state.items() if k not in excluded},
            'robots': [r['state'] for r in state['robots']],
            'metrics': {k: v for k, v in state['metrics'].items() if k not in timing}}


def main():
    cases = {}
    source = Path('/data/workspace/wz/warehouse-validation-v2-20261027/data')
    for case, old in [('peak', 'pair-saturation-hour'), ('day', 'pair-uniform-day')]:
        for mode in ['baseline', 'optimized']:
            folder = ROOT / f'data/regression/{mode}-{case}'
            result = json.loads((folder/'results.json').read_text())
            state = read_checkpoint(folder/'checkpoint.json')
            assert result['status'] == 'complete' and result['error'] is None
            assert result['unfinished'] == 0 and result['acceptance']['all_input_completed']
            assert result['registered_robots'] == 100
            assert 0 < result['robots_completed_tasks'] <= 100
            if case == 'peak':
                assert result['robots_completed_tasks'] == 100
            assert result['scheduler']['identity'] == execution_identity(mode)
            assert result['graph_audit'] is None
            assert result['acceptance']['all_disturbances_exercised']
            assert result['metrics']['unsafe_events'] == 0
            assert result['metrics']['min_separation_m'] >= 1.1 - 1e-8
            with sqlite3.connect(f'file:{folder / "frames.sqlite"}?mode=ro', uri=True) as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                count, lo, hi = db.execute('SELECT COUNT(*), MIN(second), MAX(second) FROM frames').fetchone()
                samples = []
                for second in [lo, 3000 if case == 'peak' else 43200, hi]:
                    payload = db.execute('SELECT payload FROM frames WHERE second<=? ORDER BY second DESC LIMIT 1', (second,)).fetchone()[0]
                    frame = json.loads(zlib.decompress(payload))
                    assert len(frame['robots']) == 100 and frame['scheduler']['mode'] == mode
                    samples.append(frame['elapsed'])
            assert lo == 0 and hi == result['elapsed']
            m = result['metrics']
            row = {k: result[k] for k in ['run_id', 'elapsed', 'wall_seconds', 'input_identity',
                'code_identity', 'tasks_total', 'registered_robots', 'robots_completed_tasks', 'flow_seconds', 'scheduler']}
            row.update({k: m[k] for k in ['completed', 'late', 'planning_seconds', 'planning_calls',
                'planning_failures', 'max_active_business', 'min_soc', 'min_separation_m', 'unsafe_events', 'handoffs']})
            row['recorded_frames'] = count
            row['frame_samples'] = samples
            if mode == 'baseline':
                original = read_checkpoint(source/old/'run-wide/checkpoint.json')
                row['physical_state_identical_to_original_wide'] = physical(state) == physical(original)
                assert row['physical_state_identical_to_original_wide'], 'baseline drift from preserved original'
            cases[f'{mode}-{case}'] = row
    for case in ['peak', 'day']:
        assert cases[f'baseline-{case}']['input_identity'] == cases[f'optimized-{case}']['input_identity']
    report = {'status': 'passed', 'cases': cases, 'knowledge_query_changed': False,
              'neo4j_integration_exercised': False,
              'timing_method': 'paired one-run comparison; two low-priority single-thread workers; frame recording both modes'}
    (ROOT/'logs/final-verification.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
