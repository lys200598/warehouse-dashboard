"""Evidence summaries: no interpolation of uncompleted tasks or failed runs."""
from __future__ import annotations

from collections import defaultdict
import csv
from datetime import datetime
import json
from pathlib import Path

from ..checkpoint import read_checkpoint


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'total': 0, 'mean': None, 'p95': None, 'p99': None, 'max': None}
    def quantile(q):
        return values[min(len(values) - 1, int((len(values) - 1) * q))]
    return {'count': len(values), 'total': sum(values), 'mean': sum(values) / len(values),
            'p95': quantile(.95), 'p99': quantile(.99), 'max': values[-1]}


def task_timeline(tasks, end_second, window_seconds):
    """Exact release/completion backlog from final per-task timestamps.

    Only evaluate through observed end; a timeout must not invent progress to
    the requested release-window end. At equal timestamps releases/completions
    are grouped. The simulator releases at the start of the step at t and
    stores the resulting boundary at t+1, so a release at t is first visible
    in a boundary snapshot at t+1. This is unfinished released work, not a
    traffic queue length.
    """
    events = defaultdict(lambda: [0, 0])
    for task in tasks:
        released = task['release_second']
        if released < end_second:
            events[released + 1][0] += 1
        completed = task.get('completed_at') if task.get('status') == 'completed' else None
        if completed is not None and completed <= end_second:
            if completed < released:
                raise ValueError('completion before release')
            events[completed][1] += 1
    released_count = completed_count = peak = backlog_area = 0
    previous = 0
    hourly = []
    window_end = min(window_seconds, end_second)
    released_at_window = completed_at_window = 0
    # Include hour boundaries and the exact cutoff; do not fabricate events.
    times = set(events) | set(range(0, end_second + 1, 3600)) | {end_second, window_end}
    for second in sorted(times):
        if second < 0 or second > end_second:
            continue
        backlog_area += (second - previous) * (released_count - completed_count)
        released_count += events[second][0]
        completed_count += events[second][1]
        if completed_count > released_count:
            raise ValueError('negative unfinished backlog')
        peak = max(peak, released_count - completed_count)
        if second <= window_end:
            released_at_window, completed_at_window = released_count, completed_count
        if second % 3600 == 0 or second == end_second:
            hourly.append({'second': second, 'released': released_count,
                           'completed': completed_count,
                           'released_unfinished': released_count - completed_count})
        previous = second
    return {'observed_until_second': end_second, 'requested_release_window_seconds': window_seconds,
            'release_window_observed': end_second >= window_seconds,
            'release_window_evaluation_second': window_end,
            'peak_released_unfinished': peak,
            'mean_released_unfinished': backlog_area / max(1, end_second),
            'released_unfinished_at_release_window_or_observed_end': released_at_window - completed_at_window,
            'completed_at_release_window_or_observed_end': completed_at_window,
            'completed_per_simulated_hour_at_release_window_or_observed_end': completed_at_window * 3600 / max(1, window_end),
            'released_unfinished_at_observed_end': released_count - completed_count,
            'cumulative_hourly': hourly}


def input_and_checkpoint_tasks(output):
    """Checkpoint excludes scheduled input; overlay it on immutable full CSV."""
    output = Path(output)
    checkpoint = read_checkpoint(output / 'checkpoint.json')
    variant = 'wide' if output.name.endswith('wide') else 'old'
    source = output.parent / ('input-' + variant)
    manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    start = datetime.fromisoformat(manifest['start'])
    with (source / 'tasks.csv').open(encoding='utf-8', newline='') as stream:
        tasks = {row['task_id']: dict(row,
                 release_second=int((datetime.fromisoformat(row['release_time']) - start).total_seconds()),
                 due_second=int((datetime.fromisoformat(row['due_time']) - start).total_seconds()),
                 status='scheduled') for row in csv.DictReader(stream)}
    tasks.update(checkpoint['tasks'])
    return manifest, list(tasks.values()), checkpoint


def summarize_run(output):
    output = Path(output)
    result = json.loads((output / 'results.json').read_text(encoding='utf-8'))
    manifest, tasks, checkpoint = input_and_checkpoint_tasks(output)
    if checkpoint['run_id'] != result['run_id'] or checkpoint['elapsed'] != result['elapsed']:
        raise ValueError('result/checkpoint identity or cutoff mismatch')
    metrics = result['metrics']
    window = manifest['duration_seconds']
    completed = [task for task in tasks if task['status'] == 'completed']
    late = [task for task in completed if task['completed_at'] > task['due_second']]
    if len(completed) != metrics['completed']:
        raise ValueError('completed tasks disagree with metrics')
    if len(tasks) != result['tasks_total']:
        raise ValueError('immutable input task count disagrees with result')
    waits = {key: distribution(metrics.get(key, [])) for key in
             ('departure_wait_seconds', 'charge_wait_seconds', 'dispatch_wait_seconds')}
    return {
        'run_id': result['run_id'], 'status': result['status'], 'error': result['error'],
        'elapsed': result['elapsed'], 'wall_seconds': result['wall_seconds'],
        'input_identity': result['input_identity'], 'code_identity': result['code_identity'],
        'config': result['config'], 'disturbances': result['disturbances'],
        'registered_robots': result['registered_robots'],
        'robots_completed_tasks': result['robots_completed_tasks'],
        'tasks_total': len(tasks), 'completed': len(completed), 'late_completed': len(late),
        'unreleased': sum(task['status'] == 'scheduled' for task in tasks),
        'released_unfinished': result['unfinished'], 'late_unfinished': result['late_unfinished'],
        'all_input_completed': result['acceptance']['all_input_completed'],
        'flow_seconds': distribution([task['completed_at'] - task['release_second'] for task in completed]),
        'waits': waits, 'backlog': task_timeline(tasks, result['elapsed'], window),
        'resource_metrics': {key: metrics.get(key) for key in (
            'max_active_business', 'max_working', 'max_charging', 'max_queued', 'min_soc',
            'traffic_wait_seconds', 'charged_but_held_seconds', 'charge_sessions',
            'planning_calls', 'planning_failures', 'planning_seconds',
            'dispatch_wall_seconds', 'recovery_wall_seconds', 'matching_retries',
            'energy_rejections', 'handoffs', 'assistance_failures', 'progress_warnings',
            'empty_distance_m', 'loaded_distance_m', 'productive_robot_seconds',
            'unsafe_events', 'min_separation_m')},
        'planning_failure_reasons': {key: value for key, value in metrics.items()
                                     if key.startswith('planning_fail_')},
        'knowledge': result['knowledge'], 'graph_audit': result['graph_audit'],
        'metric_caveats': [
            'backlog means all released unfinished tasks, not robots waiting in traffic',
            'departure waits are committed mission departures; not total robot idle time',
            'charge_wait_seconds records traffic_wait on committed charge trips; not all rejected charge requests',
            'dispatch_wait is measured from task ready_at if present, not necessarily raw release',
            'planning_seconds includes unsuccessful candidate search; wall includes persistence/audits/other work',
            'completed-only flow distributions exclude uncompleted tasks; backlog/late_unfinished remain visible',
            'peak active business is a per-second runtime metric, not hundred processes or hundred queries'],
    }


def summarize_common_cutoff(output, cutoff):
    manifest, tasks, checkpoint = input_and_checkpoint_tasks(output)
    if cutoff > checkpoint['elapsed']:
        raise ValueError('common cutoff beyond observed execution')
    completed = [task for task in tasks if task.get('status') == 'completed'
                 and task['completed_at'] <= cutoff]
    released = [task for task in tasks if task['release_second'] < cutoff]
    completed_ids = {task['task_id'] for task in completed}
    return {'cutoff_second': cutoff,
            'released': len(released), 'completed': len(completed),
            'released_unfinished': len([task for task in released if task['task_id'] not in completed_ids]),
            'late_completed': sum(task['completed_at'] > task['due_second'] for task in completed),
            'late_unfinished': sum(task['due_second'] < cutoff and task['task_id'] not in completed_ids
                                   for task in released),
            'completed_flow_seconds': distribution([task['completed_at'] - task['release_second']
                                                     for task in completed]),
            'timeline': task_timeline(tasks, cutoff, manifest['duration_seconds']),
            'scope': 'task release/completion timestamps only; final cumulative motion/planning metrics are not truncated'}


def compare_runs(old, wide):
    csv_keys = ('tasks_sha256', 'initial_inventory_sha256')
    inputs_equal = all(old['input_identity'][key] == wide['input_identity'][key] for key in csv_keys)
    differing = {key: {'old': old['config'].get(key), 'wide': wide['config'].get(key)}
                 for key in sorted(set(old['config']) | set(wide['config']))
                 if old['config'].get(key) != wide['config'].get(key)}
    charge_difference_ok = ('chargers' not in differing or differing['chargers'] == {'old': 24, 'wide': 100})
    disturbance_schedule = lambda run: [(e['index'], e['requested_at'], e['kind']) for e in run.get('disturbances', [])]
    requested_schedule_equal = disturbance_schedule(old) == disturbance_schedule(wide)
    controlled = inputs_equal and set(differing) <= {'chargers'} and charge_difference_ok and requested_schedule_equal
    both_complete = all(run['status'] == 'complete' and run['all_input_completed']
                        for run in (old, wide))
    deltas = {}
    for key in ('wall_seconds', 'completed', 'late_completed', 'released_unfinished'):
        deltas[key] = wide[key] - old[key]
    for key in ('traffic_wait_seconds', 'charged_but_held_seconds', 'planning_seconds'):
        a, b = old['resource_metrics'].get(key), wide['resource_metrics'].get(key)
        deltas[key] = b - a if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None
    return {'schema': 'warehouse-layout-comparison-v1', 'input_csv_bytes_identical': inputs_equal,
            'config_differences': differing, 'controlled_except_layout_resources': controlled,
            'requested_disturbance_schedule_equal': requested_schedule_equal,
            'actual_disturbance_selection': {'old': old.get('disturbances', []), 'wide': wide.get('disturbances', [])},
            'pair_complete': both_complete,
            'no_late_on_this_input': both_complete and all(run['late_completed'] == 0 for run in (old, wide)),
            'wide_minus_old': deltas, 'old': old, 'wide': wide,
            'causal_scope': 'whole layout/bay model only; not a pure aisle width or charger count causal ablation',
            'bottleneck_eliminated_generally': False,
            'million_scale_joint_qualified': False,
            'notes': ['carrying selectors are state-dependent: same requested disturbance schedule may choose different robots/times',
                      'wall timeout results must be compared at a common simulated cutoff separately',
                      'success on one input does not establish all-load stability or formal edge latency']}
