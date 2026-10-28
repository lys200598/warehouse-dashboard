"""Source-derived event-driven knowledge demand and read-only handoff advice.

The 100-per-commit comparison is an EXPLICIT HYPOTHETICAL polling policy, not
the previous synthetic concurrency benchmark or a measured production policy.
This module never changes the scheduler, source journal or graph.
"""
import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from ..knowledge import digest
from ..real_scale.benchmark import typed_json_equal
from ..real_scale.oracle import RealOracle
from ..wide.queries import COOPERATION_KINDS
from .history_decision import advise_from_history


POLICY_VERSION = 'warehouse-event-driven-shadow-demand-v2'
# Literal phases from scheduling_v3.sim / energy, including the actual full-day
# fault_wait / handoff_give observations (not invented "faulted"/"handoff_send").
SPECIAL_STAGES = {'charge_queue', 'charging', 'to_charge', 'to_handoff', 'handoff',
                  'handoff_receive', 'handoff_give', 'fault_wait', 'recovery_wait',
                  'traffic_wait', 'blocked'}
ALERT_KINDS = frozenset({'task_progress_warning', 'persistent_wait_cycle',
    'traffic_recovery_rejected', 'fault_injection_unresolved', 'energy_recovery_unresolved'})


def _save(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def _sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            value.update(block)
    return value.hexdigest()


def _policy(identity):
    threshold = identity['config'].get('assistance_soc')
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not 0 < threshold < 100:
        raise ValueError('source config must provide assistance_soc; do not invent an energy threshold')
    return {'version': POLICY_VERSION, 'assistance_soc': threshold,
            'threshold_origin': 'unchanged source config.assistance_soc',
            'normal_motion_ticks_query_graph': False,
            'robot_refresh_triggers': ['initial bootstrap', 'task_id change', 'cargo/carrying change',
                'entry/exit of charge/fault/handoff stage', 'entry into low-SOC while carrying',
                'observed assignment/custody/energy/cooperation event',
                'warning/unresolved/recovery-rejected event with explicit affected robot identities'],
            'alert_refresh_triggers': sorted(ALERT_KINDS),
            'cooperation_triggers': ['task_released', *COOPERATION_KINDS, *sorted(ALERT_KINDS)],
            'alerts_without_robot_identity': 'query cooperation context when task_id is explicit; otherwise reject unroutable alert',
            'input_events_not_redundantly_queried': {'initial_stock': 'initialize local custody from the input message',
                'received': 'update local arrival/custody from the input message; assignment/custody changes still trigger refresh'},
            'batching': 'one robot_context request for all affected robots in the same committed point',
            'immutable_publication': 'one run identity/config version, out-of-band initialization',
            'comparison': 'hypothetical each robot requests full context after every source commit',
            'scope': 'offline source-derived demand model; not an executed scheduler workload',
            'not_replacement_for_100_concurrent_full_query_benchmark': True}


def _state_reasons(before, task, cargo, stage, carrying, low):
    reasons = set()
    if before is None:
        reasons.add('initial_bootstrap')
    else:
        if task != before['task']:
            reasons.add('task_changed')
        if (cargo, carrying) != (before['cargo'], before['carrying']):
            reasons.add('custody_changed')
        if stage != before['stage'] and (stage in SPECIAL_STAGES or before['stage'] in SPECIAL_STAGES):
            reasons.add('control_stage_changed')
        if low and not before['low']:
            reasons.add('low_soc_while_carrying')
    return reasons


def iter_plan(oracle):
    """Stream deterministic requests using at most one fleet and task registry.

    State rows are the observations available at each committed point. This is
    not free production graph polling: a real adapter consumes local snapshots
    and events already emitted by the simulation, just as this replay does.
    """
    identity = oracle.identity
    policy = _policy(identity)
    db = sqlite3.connect(oracle.index_path.as_uri()+'?mode=ro&immutable=1', uri=True)
    try:
        batches = iter(db.execute('SELECT seq,checksum,sim_time FROM batches ORDER BY seq'))
        states = iter(db.execute('SELECT seq,robot,task_id,cargo_id,stage,carrying,payload FROM states ORDER BY seq,robot'))
        tasks = iter(db.execute('SELECT seq,task_id,goods_id FROM tasks ORDER BY seq,task_id'))
        events = iter(db.execute('SELECT seq,event_seq,payload FROM events ORDER BY seq,event_seq'))
        state_row, task_row, event_row = next(states, None), next(tasks, None), next(events, None)
        previous, task_goods = {}, {}
        for seq, checksum, sim_time in batches:
            if not 1 <= seq <= oracle.prefix:
                raise ValueError('source batch outside verified prefix')
            while task_row is not None and task_row[0] == seq:
                task_goods[task_row[1]] = task_row[2]
                task_row = next(tasks, None)
            affected, cooperation = defaultdict(set), {}
            observed = 0
            while state_row is not None and state_row[0] == seq:
                _, robot, task, cargo, stage, carrying, payload = state_row
                state = json.loads(payload)
                battery = state['battery']
                low = bool(carrying and battery <= policy['assistance_soc'])
                before = previous.get(robot)
                why = _state_reasons(before, task, cargo, stage, carrying, low)
                if why:
                    affected[robot].update(why)
                previous[robot] = {'task': task, 'cargo': cargo, 'carrying': carrying, 'stage': stage, 'low': low}
                observed += 1
                state_row = next(states, None)
            if observed != 100 or set(previous) != set(range(1, 101)):
                raise ValueError('demand replay requires complete 100-robot source points')
            while event_row is not None and event_row[0] == seq:
                _, number, payload = event_row
                event = json.loads(payload)
                kind, task_id = event['kind'], event.get('task_id')
                relevant = kind in COOPERATION_KINDS or kind in ALERT_KINDS or kind in {
                    'task_assigned', 'picked', 'placed', 'shipped', 'charge_reserved',
                    'charge_started', 'charge_complete', 'task_released'}
                if relevant:
                    participants = {event.get(key) for key in ('robot_id', 'affected_robot', 'donor_robot_id', 'donor_id',
                                    'receiver_robot_id', 'receiver_id') if event.get(key) is not None}
                    participants.update(event.get('affected_robots', []))
                    for robot in participants:
                        if type(robot) is not int or robot not in previous:
                            raise ValueError('source event refers to invalid robot')
                        affected[robot].add('event:' + kind)
                    if kind in ALERT_KINDS and not participants and not task_id:
                        raise ValueError('unroutable alert: explicit robot or task identity required')
                if task_id and (kind in COOPERATION_KINDS or kind in ALERT_KINDS or kind == 'task_released'):
                    goods_id = task_goods.get(task_id)
                    if not goods_id:
                        raise ValueError('source event references unavailable task/cargo')
                    case = cooperation.setdefault(task_id, {'goods_id': goods_id, 'reasons': set(), 'event_seqs': []})
                    case['reasons'].add('event:' + kind)
                    case['event_seqs'].append(number)
                event_row = next(events, None)
            common = {'run_id': oracle.run_id, 'seq': seq, 'sim_time': sim_time, 'batch_checksum': checksum}
            if affected:
                yield {**common, 'kind': 'robot_context', 'robot_ids': sorted(affected),
                       'reasons_by_robot': {str(robot): sorted(affected[robot]) for robot in sorted(affected)}}
            for task_id, case in sorted(cooperation.items()):
                yield {**common, 'kind': 'cooperation_context', 'task_id': task_id,
                       'goods_id': case['goods_id'], 'reasons': sorted(case['reasons']),
                       'trigger_event_seqs': case['event_seqs']}
        if state_row is not None or task_row is not None or event_row is not None:
            raise ValueError('source rows fall outside verified batch sequence')
    finally:
        db.close()


def build_plan(oracle, output_dir):
    output = Path(output_dir)
    identity = oracle.identity
    policy = _policy(identity)
    _save(output/'demand-policy.json', policy)
    kinds, reasons = Counter(), Counter()
    robot_reads = largest = requests = 0
    path = output/'demand-plan.jsonl'
    with path.open('x', encoding='utf-8') as stream:
        for request in iter_plan(oracle):
            requests += 1
            request['request_id'] = requests
            kinds[request['kind']] += 1
            if request['kind'] == 'robot_context':
                robot_reads += len(request['robot_ids'])
                largest = max(largest, len(request['robot_ids']))
                for values in request['reasons_by_robot'].values():
                    reasons.update(values)
            else:
                reasons.update(request['reasons'])
            stream.write(json.dumps(request, ensure_ascii=False, separators=(',', ':'), allow_nan=False)+'\n')
    polling = 100*oracle.prefix
    summary = {'scope': policy['scope'], 'policy_version': POLICY_VERSION,
        'run_id': oracle.run_id, 'prefix': oracle.prefix, 'source_prefix_sha256': identity['prefix_sha256'],
        'last_sim_time': identity['last_sim_time'], 'source_state_rows': identity['states'],
        'plan_requests': requests, 'requests_by_kind': dict(kinds), 'robot_snapshots_requested': robot_reads,
        'largest_robot_batch': largest, 'trigger_reason_counts_not_unique_requests': dict(reasons),
        'immutable_identity_publications_outside_graph_requests': 1,
        'hypothetical_polling_policy': '100 complete context requests per actual source commit',
        'hypothetical_polling_requests': polling,
        'model_request_reduction_fraction': 1-requests/polling,
        'model_robot_snapshot_reduction_fraction': 1-robot_reads/polling,
        'event_policy_without_robot_batching_requests': robot_reads+kinds['cooperation_context'],
        'not_measured_old_production_workload': True, 'not_measured_performance_speedup': True,
        'all_plan_requests_executed': False, 'plan_file_sha256': _sha(path)}
    _save(output/'demand-summary.json', summary)
    return summary


def validate_robot_context(value, oracle, case):
    if value.get('run_id') != oracle.run_id or type(value.get('seq')) is not int or value['seq'] != case['seq']:
        raise ValueError('robot API source point mismatch')
    if type(value.get('graph_committed_seq')) is not int or value['graph_committed_seq'] < case['seq']:
        raise ValueError('robot API uncommitted point')
    if value.get('batch_checksum') != oracle.batch_checksum(case['seq']):
        raise ValueError('robot API source checksum mismatch')
    rows = value.get('robots')
    if not isinstance(rows, list) or any(type(r.get('robot_id')) is not int for r in rows) or (
            [r.get('robot_id') for r in rows] != sorted(case['robot_ids'])):
        raise ValueError('robot API missing/duplicate/reordered identities')
    for row in rows:
        robot = row['robot_id']
        expected = oracle.state(case['seq'], robot)
        if digest(row.get('robot')) != digest(expected) or row.get('robot_sha256') != digest(expected):
            raise ValueError('robot API source payload mismatch')
        if digest(row.get('task')) != digest(oracle.task(case['seq'], expected.get('task_id'))):
            raise ValueError('robot API source task mismatch')


def validate_cooperation_context(value, oracle, case):
    seq, task, goods = case['seq'], case['task_id'], case['goods_id']
    if (value.get('run_id'), value.get('seq'), value.get('task_id'), value.get('goods_id')) != (oracle.run_id, seq, task, goods):
        raise ValueError('cooperation API source point mismatch')
    if type(value.get('seq')) is not int or type(value.get('graph_committed_seq')) is not int or value['graph_committed_seq'] < seq:
        raise ValueError('cooperation API uncommitted point')
    if value.get('batch_checksum') != oracle.batch_checksum(seq) or digest(value.get('focus_task')) != digest(oracle.task(seq, task)):
        raise ValueError('cooperation API source checksum/task mismatch')
    cursors = value.get('request_cursors')
    if not isinstance(cursors, dict) or set(cursors) != {'goods_after', 'cooperation_after'} or any(
            type(cursor) is not int or cursor != 0 for cursor in cursors.values()):
        raise ValueError('cooperation API complete first-page cursors required')
    for field, expected in [('goods_history', oracle.goods_history(seq, goods)),
                            ('cooperation', oracle.cooperation(seq, task))]:
        actual = deepcopy(value[field]); expected = deepcopy(expected)
        if actual.pop('graph_committed_seq') != value['graph_committed_seq']:
            raise ValueError('cooperation API mixed watermark')
        expected.pop('graph_committed_seq')
        if not typed_json_equal(actual, expected):
            raise ValueError('cooperation API source history mismatch')


def execute_plan_sample(store, oracle, plan_path, output_dir, max_cases=128):
    if type(max_cases) is not int or not 1 <= max_cases <= 1000:
        raise ValueError('bounded business request sample required')
    # Count first, select uniformly across the entire plan, not just startup.
    with Path(plan_path).open(encoding='utf-8') as stream:
        count = sum(1 for _ in stream)
    selected = {round(i*(count-1)/max(1, min(max_cases, count)-1)) for i in range(min(max_cases, count))}
    rows = []
    with gzip.open(Path(output_dir)/'business-sample-payloads.jsonl.gz', 'xt', encoding='utf-8') as payloads:
        with Path(plan_path).open(encoding='utf-8') as stream:
            for number, raw in enumerate(stream):
                if number not in selected:
                    continue
                case = json.loads(raw)
                row = {'request_id': case['request_id'], 'kind': case['kind'], 'seq': case['seq'], 'ok': False}
                started = time.perf_counter_ns()
                try:
                    if case['kind'] == 'robot_context':
                        value = store.robot_context(oracle.run_id, case['seq'], case['robot_ids'])
                    else:
                        value = store.cooperation_context(oracle.run_id, case['seq'], case['task_id'], case['goods_id'])
                    returned = time.perf_counter_ns()
                    if case['kind'] == 'robot_context':
                        validate_robot_context(value, oracle, case)
                    else:
                        validate_cooperation_context(value, oracle, case)
                    checked = time.perf_counter_ns()
                    row.update(ok=True, service_ms=(returned-started)/1e6, validation_ms=(checked-returned)/1e6,
                               result_sha256=digest(value))
                    payloads.write(json.dumps({'case': case, 'result': value}, ensure_ascii=False, allow_nan=False)+'\n')
                except Exception as exc:
                    row.update(error=type(exc).__name__+': '+str(exc), elapsed_ms=(time.perf_counter_ns()-started)/1e6)
                rows.append(row)
    with (Path(output_dir)/'business-sample-requests.jsonl').open('x', encoding='utf-8') as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False)+'\n')
    summary = {'scope': 'bounded sequential business API sample, NOT 100 concurrent acceptance',
               'planned_requests': count, 'executed_requests': len(rows),
               'all_plan_requests_executed': len(rows) == count, 'failures': sum(not row['ok'] for row in rows),
               'requests_by_kind': dict(Counter(row['kind'] for row in rows)),
               'service_mean_ms': sum(row['service_ms'] for row in rows if row['ok'])/max(1, sum(row['ok'] for row in rows))}
    _save(Path(output_dir)/'business-sample-summary.json', summary)
    return summary


def handoff_shadow_demo(store, oracle, output_dir):
    rows = []
    for point in oracle.handoff_points(limit=10000):
        for phase, seq in [('before', point['before_seq']), ('at', point['seq']), ('after', point['seq']+1)]:
            if not 1 <= seq <= oracle.prefix:
                raise ValueError('handoff demo needs before/at/after points inside verified prefix')
            case = {'seq': seq, 'task_id': point['task_id'], 'goods_id': point['goods_id']}
            robots = {'seq': seq, 'robot_ids': sorted({point['donor_robot_id'], point['receiver_robot_id']})}
            row = {'source_handoff': point, 'phase': phase, 'seq': seq, 'ok': False}
            try:
                cooperation = store.cooperation_context(oracle.run_id, seq, point['task_id'], point['goods_id'])
                states = store.robot_context(oracle.run_id, seq, robots['robot_ids'])
                validate_cooperation_context(cooperation, oracle, case)
                validate_robot_context(states, oracle, robots)
                # Different advancing heads are valid for the same immutable
                # cutoff/batch. Preserve every actual head; do not rewrite one
                # to fabricate a common latest read.
                observed_heads = [cooperation['graph_committed_seq'], states['graph_committed_seq']]
                head = min(observed_heads)
                if head < seq:
                    raise ValueError('handoff point uncommitted')
                decision = advise_from_history(cooperation, states)
                numbers = decision['historical_event_seqs_used']
                if phase == 'before' and point['event_seq'] in numbers:
                    raise ValueError('future handoff leaked into earlier decision')
                if phase != 'before' and point['event_seq'] not in numbers:
                    raise ValueError('completed handoff not used in advice')
                row.update(ok=True, observed_graph_heads=observed_heads, coherent_cutoff_head=head,
                           cooperation_context=cooperation, robot_context=states, decision=decision,
                           oracle_checks='complete task/robot payload and typed cargo/cooperation histories')
            except Exception as exc:
                row['error'] = type(exc).__name__+': '+str(exc)
            rows.append(row)
    _save(Path(output_dir)/'handoff-shadow-decisions.json', rows)
    summary = {'scope': 'same real source handoff history used for read-only advice; NOT scheduler control',
        'source_handoffs': len(rows)//3, 'decision_points': len(rows), 'failures': sum(not row['ok'] for row in rows),
        'history_used_points': sum(bool(row.get('decision', {}).get('historical_event_seqs_used')) for row in rows),
        'all_real_actions_require_atomic_current_custody_recheck': True,
        'no_claim_of_improved_completion_or_live_closed_loop': True}
    _save(Path(output_dir)/'handoff-shadow-summary.json', summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-index', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--max-cases', type=int, default=128)
    args = parser.parse_args(argv)
    if not 1 <= args.max_cases <= 1000:
        parser.error('--max-cases must be 1..1000')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    store = None
    with RealOracle.open(args.oracle_index) as oracle:
        identity_before = _sha(args.oracle_index)
        summary = build_plan(oracle, args.output_dir)
        if args.execute:
            from .service import QueryService
            store = QueryService.from_env()
            try:
                summary['execution'] = execute_plan_sample(store, oracle, args.output_dir/'demand-plan.jsonl',
                                                          args.output_dir, args.max_cases)
                summary['history_shadow'] = handoff_shadow_demo(store, oracle, args.output_dir)
            finally:
                store.close()
        if _sha(args.oracle_index) != identity_before:
            raise ValueError('read-only source oracle changed')
        summary['oracle_file_sha256'] = identity_before
        _save(args.output_dir/'summary.json', summary)
        print(json.dumps(summary, ensure_ascii=False, allow_nan=False))
        return 1 if any(summary.get(key, {}).get('failures', 0) for key in ('execution', 'history_shadow')) else 0


if __name__ == '__main__':
    raise SystemExit(main())
