"""Read-only, explainable use of temporal knowledge for present-point advice.

This is a shadow decision function, NOT the running scheduler or a custody
transaction.  Its caller must provide a coherent committed point, and any real
action must recheck/reserve ownership atomically against the current head.
"""
import json


def _integer(value, label, minimum=1):
    if type(value) is not int or value < minimum:
        raise ValueError('invalid ' + label)
    return value


def _same_point(value, run, seq, head=None):
    if value.get('run_id') != run or type(value.get('seq')) is not int or value['seq'] != seq:
        raise ValueError('mixed run/cutoff in decision context')
    if type(value.get('graph_committed_seq')) is not int or value['graph_committed_seq'] < seq or (
            head is not None and value['graph_committed_seq'] != head):
        raise ValueError('mixed graph watermark in decision context')


def _participant(event, name):
    long, short = name + '_robot_id', name + '_id'
    value = event.get(long, event.get(short))
    _integer(value, name)
    for field in (long, short):
        if field in event:
            _integer(event[field], name)
    if value > 100 or long in event and short in event and event[long] != event[short]:
        raise ValueError('invalid/conflicting handoff ' + name)
    return value


def advise_from_history(context, robot_context):
    """Return non-executing advice using current custody AND observed history.

The historical cutoff is the "present" of this replayed decision.  No claim is
made that it is the live database head or that this code controls a robot.
Complete, first pages are required: silently truncated histories are unsafe
inputs for counting or reasoning about previous transfers.
"""
    run, seq = context.get('run_id'), _integer(context.get('seq'), 'cutoff')
    head = _integer(context.get('graph_committed_seq'), 'head')
    if not isinstance(run, str) or not run or head < seq:
        raise ValueError('uncommitted decision point')
    # Two read-only statements may observe different advancing heads. Their
    # immutable cutoff and source batch checksum, not equal mutable heads, make
    # a coherent historical point. Preserve both actual observations below.
    _same_point(robot_context, run, seq)
    if context.get('batch_checksum') != robot_context.get('batch_checksum') or not context.get('batch_checksum'):
        raise ValueError('decision batch checksum mismatch')
    task_id, goods_id = context.get('task_id'), context.get('goods_id')
    task = context.get('focus_task')
    if not isinstance(task, dict) or task.get('task_id') != task_id or task.get('goods_id') != goods_id:
        raise ValueError('decision task/cargo mismatch')
    cursors = context.get('request_cursors')
    if not isinstance(cursors, dict) or set(cursors) != {'goods_after', 'cooperation_after'} or any(
            type(cursor) is not int or cursor != 0 for cursor in cursors.values()):
        raise ValueError('complete first-page decision request cursors required')
    goods, cooperation = context.get('goods_history'), context.get('cooperation')
    if not isinstance(goods, dict) or not isinstance(cooperation, dict):
        raise ValueError('missing decision histories')
    _same_point(goods, run, seq, head); _same_point(cooperation, run, seq, head)
    if goods.get('goods_id') != goods_id or cooperation.get('task_id') != task_id:
        raise ValueError('cross-task/cargo decision history')
    for history in (goods, cooperation):
        if history.get('has_more') is not False or history.get('next_after_event_seq') is not None:
            raise ValueError('complete first-page decision histories required')

    goods_rows, previous, previous_location, prior_locations = {}, 0, None, {}
    for row in goods.get('rows', []):
        number = _integer(row.get('event_seq'), 'cargo event')
        if number <= previous or not 1 <= _integer(row.get('seq'), 'cargo commit') <= seq:
            raise ValueError('future/unordered cargo history')
        prior_locations[number] = previous_location
        previous, goods_rows[number], previous_location = number, row, row.get('location_id')
    current = goods.get('current_as_of')
    if current is not None:
        if not isinstance(current, dict) or not goods_rows:
            raise ValueError('cargo current state lacks complete provenance')
        last = goods_rows[max(goods_rows)]
        if type(current.get('event_seq')) is not int or any(
                current.get(key) != last.get(key) for key in ('event_seq', 'location_id', 'sim_time')):
            raise ValueError('cargo history does not reach current state')
    elif goods_rows:
        raise ValueError('cargo current state missing despite history')

    transfers, pending, evidence = [], None, []
    previous = 0
    for row in cooperation.get('rows', []):
        number = _integer(row.get('event_seq'), 'cooperation event')
        commit = _integer(row.get('seq'), 'cooperation commit')
        event = row.get('event')
        if isinstance(event, str):
            event = json.loads(event)
        if number <= previous or commit > seq or not isinstance(event, dict):
            raise ValueError('future/unordered cooperation history')
        if type(event.get('event_seq')) is not int or event['event_seq'] != number or event.get('task_id') != task_id:
            raise ValueError('cooperation payload identity mismatch')
        if event.get('goods_id') not in (None, goods_id):
            raise ValueError('cooperation references another cargo')
        previous = number
        kind = event.get('kind')
        if kind == 'cargo_handoff':
            donor, receiver = _participant(event, 'donor'), _participant(event, 'receiver')
            cargo = goods_rows.get(number)
            if donor == receiver or not cargo or cargo.get('kind') != 'cargo_handoff' or (
                    cargo.get('seq') != commit or cargo.get('location_id') != f'ROBOT-{receiver}' or
                    event.get('location_id') != f'ROBOT-{receiver}' or
                    prior_locations[number] != f'ROBOT-{donor}'):
                raise ValueError('handoff history contradicts cargo transition')
            transfers.append({'event_seq': number, 'seq': commit, 'donor_robot_id': donor,
                              'receiver_robot_id': receiver})
            pending = None
            evidence.append(number)
        elif kind in {'assistance_requested', 'handoff_reserved', 'assistance_deferred'}:
            pending = {'event_seq': number, 'kind': kind,
                       'receiver_robot_id': event.get('receiver_robot_id')}
            evidence.append(number)
        elif kind == 'task_requeued':
            pending = None
            evidence.append(number)

    robots = {}
    for item in robot_context.get('robots', []):
        owner = _integer(item.get('robot_id'), 'context robot')
        state = item.get('robot')
        if owner > 100 or owner in robots or not isinstance(state, dict) or state.get('robot_id') != owner:
            raise ValueError('invalid/duplicate decision robot')
        robots[owner] = state
    if not robots:
        raise ValueError('decision requires current robot states')
    location = current.get('location_id') if current else None
    holder = None
    if isinstance(location, str) and location.startswith('ROBOT-'):
        try:
            holder = int(location[6:])
        except ValueError as exc:
            raise ValueError('invalid current cargo holder') from exc
        if not 1 <= holder <= 100:
            raise ValueError('invalid current cargo holder')
    status = task.get('status')
    if status not in {'scheduled', 'waiting', 'accepted', 'assigned', 'carrying', 'completed'}:
        raise ValueError('unknown current task status')
    if task.get('robot_id') is not None and (type(task['robot_id']) is not int or not 1 <= task['robot_id'] <= 100):
        raise ValueError('invalid task robot identity')
    if status == 'carrying':
        if holder is None or type(task.get('robot_id')) is not int or task['robot_id'] != holder or holder not in robots:
            raise ValueError('current task/cargo/robot holder disagree')
        owner = robots[holder]
        if owner.get('task_id') != task_id or owner.get('cargo_id') != goods_id or owner.get('carrying') is not True:
            raise ValueError('holder robot does not carry the task cargo')
    for owner, state in robots.items():
        if state.get('cargo_id') == goods_id and (state.get('carrying') is not True or owner != holder):
            raise ValueError('duplicate/inconsistent current robot cargo')
    latest_transfer = transfers[-1] if transfers else None
    actions = []
    for owner, state in sorted(robots.items()):
        related_transfer = next((row for row in reversed(transfers) if row['donor_robot_id'] == owner), None)
        reason, used = 'current task and custody', []
        if status == 'completed':
            action = 'do_not_repeat_completed_task'
            if latest_transfer:
                reason, used = 'task is complete; preserve prior transfer provenance', [latest_transfer['event_seq']]
        elif status == 'carrying' and owner == holder:
            if pending:
                action, reason, used = ('retain_custody_until_atomic_handoff',
                    'assistance observed but no subsequent custody transfer', [pending['event_seq']])
            elif state.get('stage') == 'fault_wait' or state.get('power_isolated') is True:
                action, reason = ('request_assistance_while_retaining_custody',
                                  'current carrier is stopped; no completed custody transfer permits another carrier')
            else:
                action = 'continue_current_task'
                if latest_transfer and latest_transfer['receiver_robot_id'] == owner:
                    reason, used = 'current holder continues after recorded handoff', [latest_transfer['event_seq']]
        elif related_transfer:
            action, reason, used = ('do_not_repeat_delivery_after_transfer',
                'historical donor is not the current cargo holder', [related_transfer['event_seq']])
        elif pending and pending.get('receiver_robot_id') == owner:
            action, reason, used = ('wait_for_custody_transfer',
                'reserved receiver must not act as carrier before transfer', [pending['event_seq']])
        elif status == 'assigned' and task.get('robot_id') == owner and state.get('task_id') == task_id:
            action = 'execute_assigned_pickup'
        else:
            action = 'do_not_execute_unowned_task'
        actions.append({'robot_id': owner, 'action': action, 'reason': reason,
                        'history_evidence_event_seqs': used, 'history_changed_explanation': bool(used)})
    return {'scope': 'read_only_shadow_advice_not_scheduler_control', 'run_id': run, 'seq': seq,
            'graph_committed_seq': head, 'task_id': task_id, 'goods_id': goods_id,
            'current_task_status': status, 'current_cargo_location': location,
            'current_holder_robot_id': holder, 'observed_handoff_count': len(transfers),
            'historical_donor_robot_ids': sorted({row['donor_robot_id'] for row in transfers}),
            'historical_event_seqs_used': sorted(set(evidence)), 'pending_assistance': pending,
            'actions': actions, 'robot_graph_committed_seq': robot_context['graph_committed_seq'],
            'observed_graph_heads': [head, robot_context['graph_committed_seq']],
            'coherent_minimum_observed_head': min(head, robot_context['graph_committed_seq']),
            'historical_point_not_live_latest': seq != head or seq != robot_context['graph_committed_seq'],
            'real_action_requires_current_head_and_atomic_custody_recheck': True}
