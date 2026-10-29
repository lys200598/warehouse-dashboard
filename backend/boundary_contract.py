"""Pure plan/physical-boundary contract shared by graph writers and auditors.

Logical groups describe immutable intent. Cargo custody and executed routes
always use a physical station. No file, graph, or future-plan reads occur here.
"""
VERSION = 'warehouse-logical-boundaries-v1'
LOGICAL = {'source': 'INBOUND', 'target': 'OUTBOUND'}


def declared(metadata):
    value = metadata.get('boundary_routing')
    if value is None:
        return False
    if not isinstance(value, dict) or value.get('version') != VERSION:
        raise ValueError('unknown boundary routing declaration')
    for key, expected in (('logical_inbound', 'INBOUND'), ('logical_outbound', 'OUTBOUND')):
        if value.get(key, expected) != expected:
            raise ValueError('conflicting logical boundary marker')
    return True


def plan_endpoints(task, metadata):
    enabled = declared(metadata)
    if not enabled:
        if any(k in task for k in ('plan_source_id', 'plan_target_id')):
            raise ValueError('plan endpoints require a boundary routing declaration')
        return task['source_id'], task['target_id']
    values = tuple(task.get('plan_' + side + '_id') for side in ('source', 'target'))
    if any(not isinstance(x, str) or not x or x.strip() != x for x in values):
        raise ValueError('declared boundary task lacks immutable plan endpoints')
    kind = task.get('task_type')
    if values[0] == 'OUTBOUND' or values[1] == 'INBOUND' or (
            values[0] == 'INBOUND' and kind != 'INBOUND') or (
            values[1] == 'OUTBOUND' and kind != 'OUTBOUND'):
        raise ValueError('logical boundary has wrong task role')
    return values


def location_registry(scene):
    spec = scene['spec']
    slots = {f"{rack['id']}-L{level}-C{bay}-F{face}"
             for rack in scene['racks'] for level in range(1, spec['levels'] + 1)
             for face in range(1, spec['faces'] + 1) for bay in range(1, spec['baysPerFace'] + 1)}
    stations = {p['id']: p['kind'] for p in scene['stations']}
    return slots, stations


def validate_task(task, metadata, *, cutoff=None, registry=None):
    """Validate the public task version without silently rewriting its plan."""
    source, target = plan_endpoints(task, metadata)
    if not declared(metadata):
        return source, target
    slots, stations = registry or location_registry(metadata['scene'])
    kind = task.get('task_type')
    if kind not in ('INBOUND', 'OUTBOUND'):
        raise ValueError('unsupported boundary task type')
    bindings = task.get('boundary_bindings', {})
    if not isinstance(bindings, dict) or set(bindings) - {'source', 'target'}:
        raise ValueError('invalid boundary binding registry')
    if (kind == 'INBOUND' and (target not in slots or source != 'INBOUND' and stations.get(source) != 'inbound')
            or kind == 'OUTBOUND' and (source not in slots or target != 'OUTBOUND' and stations.get(target) != 'outbound')):
        raise ValueError('planned boundary task has invalid source/target roles')
    if cutoff is not None and task.get('status') != 'scheduled' and task.get('release_second', cutoff + 1) > cutoff:
        raise ValueError('unpublished future task appeared in boundary facts')
    expected = {'source': source, 'target': target}
    for side, plan in expected.items():
        actual = task.get(side + '_id')
        logical = plan == LOGICAL[side]
        if not logical:
            if actual != plan or actual not in slots | set(stations):
                raise ValueError('fixed plan endpoint changed or is foreign')
            if task.get('resolved_' + side + '_id', actual) != actual:
                raise ValueError('fixed resolved endpoint mismatch')
            if side in task.get('boundary_bindings', {}):
                raise ValueError('fixed plan endpoint cannot acquire a boundary binding')
            continue
        if actual == plan:
            allowed = {'scheduled', 'waiting'} if side == 'source' else {'scheduled'}
            if task.get('status') not in allowed or task.get('resolved_' + side + '_id') is not None or task.get(side + '_bound_at') is not None:
                raise ValueError('unbound logical endpoint has physical execution state')
            if side in task.get('boundary_bindings', {}):
                raise ValueError('unbound endpoint has a binding record')
            continue
        role = 'inbound' if side == 'source' else 'outbound'
        if stations.get(actual) != role or task.get('resolved_' + side + '_id') != actual:
            raise ValueError('resolved boundary is not a physical station of its role')
        at = task.get(side + '_bound_at')
        release = task.get('release_second')
        if type(at) is not int or type(release) is not int or at < release or cutoff is not None and at > cutoff:
            raise ValueError('boundary binding timestamp is invalid/future')
        binding = task.get('boundary_bindings', {}).get(side)
        phase = 'admission' if side == 'source' else 'release'
        if not isinstance(binding, dict) or any(binding.get(k) != v for k, v in
                (('logical_id', plan), ('actual_id', actual), ('at', at), ('phase', phase))):
            raise ValueError('boundary binding record differs from task endpoint')
        sequence, event_id = binding.get('event_seq'), binding.get('event_id')
        if type(sequence) is not int or sequence < 1 or not isinstance(event_id, str) or not event_id.endswith(':event:' + str(sequence)):
            raise ValueError('boundary binding lacks its source event identity')
        if side == 'source' and task.get('received_at') != at:
            raise ValueError('inbound binding must coincide with actual admission')
    return source, target


def validate_binding_history(tasks, events, metadata, *, cutoff=None):
    """Check every declared final binding against one actual source event."""
    if not declared(metadata):
        if any(e.get('kind') == 'boundary_port_bound' for e in events):
            raise ValueError('binding event lacks declared boundary contract')
        return {'bindings': 0, 'logical_tasks': 0}
    registry = location_registry(metadata['scene'])
    seen = {}
    lifecycle = {}
    for event in events:
        if event.get('kind') in ('task_released', 'received'):
            lifecycle.setdefault((event.get('task_id'), event['kind']), []).append(event)
    for event in events:
        if event.get('kind') != 'boundary_port_bound':
            continue
        if 'goods_id' in event or 'location_id' in event:
            raise ValueError('port selection must not fabricate cargo custody')
        task = tasks.get(event.get('task_id')); side = event.get('endpoint')
        if task is None or side not in LOGICAL:
            raise ValueError('binding event has foreign task/endpoint')
        key = (task['task_id'], side)
        if key in seen:
            raise ValueError('boundary endpoint was bound more than once')
        plan = plan_endpoints(task, metadata)[0 if side == 'source' else 1]
        actual = task.get(side + '_id'); at = task.get(side + '_bound_at')
        phase = 'admission' if side == 'source' else 'release'
        if plan != LOGICAL[side] or any(event.get(k) != v for k, v in
                (('planned_location_id', plan), ('actual_location_id', actual),
                 ('logical_id', plan), ('actual_id', actual), ('binding_phase', phase), ('sim_time', at))):
            raise ValueError('boundary event disagrees with immutable plan or actual binding')
        record = task['boundary_bindings'][side]
        if record.get('event_seq') != event['event_seq'] or not record.get('event_id', '').endswith(':event:' + str(event['event_seq'])):
            raise ValueError('binding record references a different source event')
        following = lifecycle.get((task['task_id'], 'received' if side == 'source' else 'task_released'), [])
        if (len(following) != 1 or following[0]['sim_time'] != at
                or following[0]['event_seq'] <= event['event_seq']):
            raise ValueError('binding must precede its same-tick admission/release')
        seen[key] = event['event_seq']
    logical_count = 0
    for task in tasks.values():
        plans = validate_task(task, metadata, cutoff=cutoff, registry=registry)
        for side, plan in zip(('source', 'target'), plans):
            if plan != LOGICAL[side]:
                continue
            logical_count += 1
            bound = task.get(side + '_id') != plan
            if bound != ((task['task_id'], side) in seen):
                raise ValueError('task binding is missing its unique source event')
    return {'bindings': len(seen), 'logical_tasks': logical_count}
