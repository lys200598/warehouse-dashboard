"""Late binding of declared logical boundaries to existing physical stations.

Plan endpoints stay in plan_*; runtime endpoints become physical only at the
documented release/admission boundary. No cargo moves during this decision.
"""
from collections import Counter
import math

VERSION = 'warehouse-logical-boundaries-v1'
MARKERS = {'INBOUND': 'inbound', 'OUTBOUND': 'outbound'}
DECLARATION = {'version': VERSION, 'logical_inbound': 'INBOUND', 'logical_outbound': 'OUTBOUND'}


def plan_endpoint(task, endpoint):
    return task.get('plan_' + endpoint + '_id', task[endpoint + '_id'])


def prepare_tasks(sim):
    policy = sim.config.port_selection_policy
    sim._boundary_plans = {}
    for key, task in sim.tasks.items():
        source, target = task['source_id'], task['target_id']
        logical = source == 'INBOUND' or target == 'OUTBOUND'
        if logical and policy != 'balanced':
            raise ValueError('logical boundary tasks require port_selection_policy=balanced')
        if policy != 'balanced':
            continue
        sim._boundary_plans[key] = (source, target)
        task.update(plan_source_id=source, plan_target_id=target,
                    resolved_source_id=None if source == 'INBOUND' else source,
                    resolved_target_id=None if target == 'OUTBOUND' else target,
                    boundary_bindings={})
    if policy == 'balanced':
        sim.run_metadata['boundary_routing'] = {
            **DECLARATION, 'policy': policy,
            'inbound_binding': 'actual admission before received',
            'outbound_binding': 'actual release before task_released',
            'source_kind': 'published physical state only',
        }


def _stations(sim, kind):
    return sorted(p['id'] for p in sim.scene['stations'] if p['kind'] == kind)


def outbound_loads(sim):
    # Check publication/status before inspecting any future plan endpoint.
    return Counter(task['target_id'] for task in sim.tasks.values()
        if task['status'] not in {'scheduled', 'completed'}
        and task['task_type'] == 'OUTBOUND' and task['target_id'] != 'OUTBOUND')


def choose_inbound(sim, task):
    ports = [port for port in _stations(sim, 'inbound') if sim.ports.get(port, 0) < 6]
    if not ports:
        return None
    available = [robot for owner, robot in sim.robots.items()
        if owner not in sim.faults
        and (owner not in sim.jobs or sim.config.task_chaining and owner in sim.handoff_ready)
        and robot['battery'] > sim.config.charge_request_soc
        and sim.task_capability_reason(robot, task) is None]
    rows = []
    for port in ports:
        count = sim.ports.get(port, 0)
        if count >= 6:
            continue
        delivery = sim.roadmap.travel_time(port, task['target_id'], sim.config.speed)
        pickup = min((sim.roadmap.travel_time(robot['location_id'], port, sim.config.speed)
                      for robot in available), default=0.)
        score = delivery + pickup + 2 * sim.config.pick_seconds * count
        if math.isfinite(score):
            rows.append((score, count, port))
    return min(rows)[2] if rows else None


def choose_outbound(sim, task, loads):
    rows = []
    for port in _stations(sim, 'outbound'):
        score = sim.roadmap.travel_time(task['source_id'], port, sim.config.speed)
        score += sim.config.place_seconds * loads[port]
        if math.isfinite(score):
            rows.append((score, loads[port], port))
    return min(rows)[2] if rows else None


def bind(sim, task, endpoint, actual, phase):
    field = endpoint + '_id'; logical = plan_endpoint(task, endpoint)
    expected = 'INBOUND' if endpoint == 'source' else 'OUTBOUND'
    if logical != expected or task[field] != logical or endpoint in task['boundary_bindings']:
        raise AssertionError('logical boundary may be bound exactly once')
    kind = MARKERS[logical]
    if actual not in _stations(sim, kind):
        raise AssertionError('bound port has wrong physical role')
    task[field] = actual
    task['resolved_' + field] = actual
    task[endpoint + '_bound_at'] = sim.elapsed
    task['boundary_bindings'][endpoint] = {'logical_id': logical, 'actual_id': actual,
        'at': sim.elapsed, 'phase': phase, 'policy': VERSION}
    # This is a selection event, not a cargo custody transition. In particular
    # it does not carry goods_id/location_id, which graph writers use for custody.
    sim.event('boundary_port_bound', task_id=task['task_id'], endpoint=endpoint,
              logical_id=logical, actual_id=actual, planned_location_id=logical,
              actual_location_id=actual, binding_phase=phase, bound_at=sim.elapsed)
    task['boundary_bindings'][endpoint].update(event_seq=sim.event_sequence,
        event_id=f'{sim.run_id}:event:{sim.event_sequence}')


def release(sim):
    # No complete task-history scan on idle ticks or inbound-only releases.
    loads = None
    while sim.cursor < len(sim.release_order):
        key = sim.release_order[sim.cursor]; task = sim.tasks[key]
        if task['release_second'] > sim.elapsed:
            break
        if task['target_id'] == 'OUTBOUND':
            if loads is None:
                loads = outbound_loads(sim)
            port = choose_outbound(sim, task, loads)
            if port is None:
                raise RuntimeError('logical outbound has no reachable physical station')
            bind(sim, task, 'target', port, 'release')
        if task['task_type'] == 'OUTBOUND' and loads is not None:
            loads[task['target_id']] += 1
        sim.cursor += 1
        task['status'] = 'waiting'; sim.pending.append(key)
        sim.event('task_released', task_id=key)
    for key in sim._inbound_admission_order():
        task = sim.tasks[key]
        if task['task_type'] != 'INBOUND' or task['status'] != 'waiting':
            continue
        slot = sim.slots[task['target_id']]
        if sim.inside >= len(sim.slots) or slot['occupied'] or slot['reservedBy']:
            continue
        port = choose_inbound(sim, task) if task['source_id'] == 'INBOUND' else task['source_id']
        if port is None or sim.ports.get(port, 0) >= 6:
            continue
        if task['source_id'] == 'INBOUND':
            bind(sim, task, 'source', port, 'admission')
        task.update(status='accepted', received_at=sim.elapsed)
        slot['reservedBy'] = key
        sim.goods[task['goods_id']] = port
        sim.inside += 1; sim.ports[port] += 1; sim.metrics['received'] += 1
        sim.event('received', task_id=key, goods_id=task['goods_id'], location_id=port)


def endpoints_valid(task, inbound_ports, outbound_ports, slots):
    """Unbound markers are permitted only before their binding boundary."""
    status, source, target = task['status'], task['source_id'], task['target_id']
    if task['task_type'] == 'INBOUND':
        source_ok = source in inbound_ports or (source == 'INBOUND'
            and status in {'scheduled', 'waiting'}
            and task.get('plan_source_id') == 'INBOUND'
            and task.get('resolved_source_id') is None)
        return source_ok and target in slots
    target_ok = target in outbound_ports or (target == 'OUTBOUND' and status == 'scheduled'
        and task.get('plan_target_id') == 'OUTBOUND' and task.get('resolved_target_id') is None)
    return source in slots and target_ok


def assert_bindings(sim):
    for key, planned in sim._boundary_plans.items():
        task = sim.tasks[key]
        if (task.get('plan_source_id'), task.get('plan_target_id')) != planned:
            raise AssertionError('runtime task changed immutable logical plan')
        for endpoint, logical in zip(('source', 'target'), planned):
            field = endpoint + '_id'; actual = task[field]
            if logical not in MARKERS:
                if actual != logical or task.get('resolved_' + field) != logical:
                    raise AssertionError('runtime task changed its fixed plan endpoint')
                continue
            record = task.get('boundary_bindings', {}).get(endpoint)
            if actual == logical:
                if record is not None or task.get('resolved_' + field) is not None:
                    raise AssertionError('unbound logical task contains a physical binding')
                if task['status'] not in ({'scheduled', 'waiting'} if endpoint == 'source' else {'scheduled'}):
                    raise AssertionError('task passed boundary without a physical binding')
            else:
                if not isinstance(record, dict) or record.get('logical_id') != logical or record.get('actual_id') != actual:
                    raise AssertionError('runtime physical endpoint changed after binding')
                if actual != task.get('resolved_' + field) or actual not in _stations(sim, MARKERS[logical]):
                    raise AssertionError('bound endpoint is not a physical port of the required role')
                at = task.get(endpoint + '_bound_at')
                if type(at) is not int or at != record.get('at') or not task['release_second'] <= at <= sim.elapsed:
                    raise AssertionError('binding time is not a released physical fact')
                if endpoint == 'source' and at != task.get('received_at'):
                    raise AssertionError('inbound binding must coincide with actual admission')
                sequence = record.get('event_seq')
                if (type(sequence) is not int or not 1 <= sequence <= sim.event_sequence or
                        record.get('event_id') != f'{sim.run_id}:event:{sequence}'):
                    raise AssertionError('binding lacks its original source event identity')
