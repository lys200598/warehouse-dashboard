"""Pure temporal-history gates consumed by the running coordinator."""
from ..knowledge import digest


class DecisionRejected(RuntimeError):
    pass


def authorize(context, *, run, donor, task, goods, receiver=None):
    """Authorize planning or transfer using observed reservation/custody history.

    Current physical ownership is independently checked by the coordinator;
    this gate additionally requires matching committed shared knowledge.
    """
    def require(value, message):
        if not value:
            raise DecisionRejected(message)
    require(context.get('run_id') == run, 'foreign knowledge run')
    require(type(context.get('seq')) is int and type(context.get('graph_committed_seq')) is int
            and 0 < context['seq'] <= context['graph_committed_seq'], 'uncommitted knowledge')
    require(context.get('batch_checksum') and context.get('goods_id') == goods
            and context.get('task_id') == task['task_id'], 'knowledge task/cargo mismatch')
    past = context.get('focus_task', {})
    require(past.get('task_id') == task['task_id'] and past.get('goods_id') == goods
            and past.get('status') == 'carrying' and type(past.get('robot_id')) is int
            and past['robot_id'] == donor, 'shared task owner differs from live owner')
    custody, cooperation = context['goods_history'], context['cooperation']
    require(custody.get('goods_id')==goods and cooperation.get('task_id')==task['task_id'],
            'foreign custody/cooperation context')
    require(context.get('request_cursors') == {'goods_after':0,'cooperation_after':0}
            and all(type(v) is int for v in context['request_cursors'].values()), 'history cursor incomplete')
    for history in (custody, cooperation):
        require(history.get('run_id') == run and type(history.get('seq')) is int
                and history['seq'] == context['seq'] and history.get('graph_committed_seq') == context['graph_committed_seq']
                and history.get('has_more') is False and history.get('next_after_event_seq') is None,
                'mixed or truncated history')
    current = custody.get('current_as_of') or {}
    require(current.get('location_id') == f'ROBOT-{donor}', 'shared cargo owner differs from live owner')
    rows = custody.get('rows', [])
    previous=0
    for row in rows:
        require(type(row.get('seq')) is int and 0 < row['seq'] <= context['seq']
                and type(row.get('event_seq')) is int and row['event_seq'] > previous,
                'future or unordered custody history')
        previous=row['event_seq']
    require(rows and rows[-1].get('event_seq') == current.get('event_seq')
            and rows[-1].get('location_id') == current['location_id'], 'custody has no provenance')
    events, previous = [], 0
    for row in cooperation.get('rows', []):
        event = row.get('event', {})
        require(type(row.get('seq')) is int and 0 < row['seq'] <= context['seq']
                and type(row.get('event_seq')) is int and row['event_seq'] > previous
                and event.get('task_id') == task['task_id'] and event.get('event_seq') == row['event_seq'],
                'future or foreign cooperation event')
        require(event.get('goods_id') in (None,goods), 'foreign cooperation cargo')
        for name in ('donor','receiver'):
            long,short=name+'_robot_id',name+'_id'
            values=[event[k] for k in (long,short) if k in event]
            require(all(type(v) is int and 1<=v<=100 for v in values)
                    and (len(values)<2 or values[0]==values[1]),'invalid participant aliases')
        previous = row['event_seq']
        events.append(event)
    former_donors = {e.get('donor_robot_id', e.get('donor_id')) for e in events if e.get('kind')=='cargo_handoff'}
    evidence = [r['event_seq'] for r in rows]
    if receiver is not None:
        reservations = [e for e in events if e.get('kind') == 'handoff_reserved']
        require(reservations and type(reservations[-1].get('robot_id')) is int and reservations[-1]['robot_id'] == donor
                and type(reservations[-1].get('receiver_robot_id')) is int and reservations[-1]['receiver_robot_id'] == receiver,
                'handoff lacks matching committed reservation')
        evidence.append(reservations[-1]['event_seq'])
    return {'action':'authorize_transfer' if receiver is not None else 'authorize_assistance_planning',
            'run_id':run, 'seq':context['seq'], 'batch_checksum':context['batch_checksum'],
            'task_id':task['task_id'], 'goods_id':goods, 'donor_robot_id':donor,
            'receiver_robot_id':receiver, 'historical_donor_robot_ids':sorted(x for x in former_donors if type(x) is int),
            'history_event_seqs':sorted(set(evidence)), 'context_sha256':digest(context)}
