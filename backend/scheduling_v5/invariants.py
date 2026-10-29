"""Read-only, bidirectional cargo conservation for stable simulator boundaries.

Call after a complete release/assignment/marker/fault transaction, never while
individual fields of that transaction are being updated. A waiting future OUT
may legitimately refer to an IN which has not arrived. A completed IN may have
subsequently shipped. Empty helpers may already name the donor's task.
"""
from collections import Counter
from ..wide.free_ports import endpoints_valid


def _require(condition, message):
    if not condition:
        raise AssertionError('warehouse inventory: ' + message)


def assert_warehouse_inventory(sim):
    """Check physical holders, task custody, port counts and slot reservations.

    This is not a traffic/energy validator or a claim of fault recoverability.
    It never changes state, makes repairs, or relaxes the original assertions.
    All scans are linear in existing goods/tasks/slots/robots.
    """
    slots, goods, robots, tasks = sim.slots, sim.goods, sim.robots, sim.tasks
    initial_goods = {row['goods_id'] for row in sim.initial}
    _require(len(initial_goods) == len(sim.initial), 'duplicate initial goods')
    inbound_ports = {p['id'] for p in sim.scene['stations'] if p['kind'] == 'inbound'}
    outbound_ports = {p['id'] for p in sim.scene['stations'] if p['kind'] == 'outbound'}
    expected_goods = set(initial_goods)
    staged, expected_ports, expected_reservations = {}, Counter(), {}
    completed, received, shipped = 0, 0, 0
    shipped_goods = set()

    for task_id, task in tasks.items():
        _require(task.get('task_id') == task_id, 'task identity disagrees with registry')
        status, kind, cargo = task.get('status'), task.get('task_type'), task.get('goods_id')
        _require(status in {'scheduled', 'waiting', 'accepted', 'assigned', 'carrying', 'completed'},
                 f'{task_id} has unknown status')
        _require(kind in {'INBOUND', 'OUTBOUND'}, f'{task_id} has unknown type')
        _require(task.get('cargo_id', cargo) == cargo, f'{task_id} cargo identity mismatch')
        _require(endpoints_valid(task, inbound_ports, outbound_ports, slots),
                 f'{task_id} endpoints disagree with warehouse boundary roles')
        if kind == 'INBOUND' and status not in {'scheduled', 'waiting'}:
            received += 1
            _require(cargo not in expected_goods, f'{task_id} duplicates an already received cargo')
            expected_goods.add(cargo)
            if status in {'accepted', 'assigned', 'carrying'}:
                target = task['target_id']
                _require(target not in expected_reservations, f'{target} has duplicate inbound reservation')
                expected_reservations[target] = task_id
            if status in {'accepted', 'assigned'}:
                source = task['source_id']
                _require(source in inbound_ports, f'{task_id} has unknown receiving port')
                _require(cargo not in staged, f'{cargo} appears twice at receiving ports')
                staged[cargo] = source
                expected_ports[source] += 1
                _require(goods.get(cargo) == source, f'{task_id} unpicked cargo left its source')
        if status == 'accepted':
            _require(kind == 'INBOUND' and task.get('robot_id') is None,
                     f'{task_id} invalid accepted state')
        if status == 'assigned':
            owner = task.get('robot_id')
            _require(owner in robots, f'{task_id} assigned to absent robot')
            robot = robots[owner]
            _require(robot.get('task_id') == task_id and not robot.get('carrying'),
                     f'{task_id} assignment disagrees with executing robot')
            if kind == 'OUTBOUND':
                source = task['source_id']
                _require(source not in expected_reservations, f'{source} has duplicate task reservation')
                expected_reservations[source] = task_id
                _require(goods.get(cargo) == source, f'{task_id} unpicked outbound left its source')
        if status == 'carrying':
            owner = task.get('robot_id')
            _require(owner in robots, f'{task_id} carried by absent robot')
            robot = robots[owner]
            _require(robot.get('carrying') and robot.get('cargo_id') == cargo and
                     robot.get('task_id') == task_id and goods.get(cargo) == f'ROBOT-{owner}',
                     f'{task_id} carrying task disagrees with unique robot holder')
        if status == 'completed':
            completed += 1
            if kind == 'OUTBOUND':
                _require(cargo not in shipped_goods, f'{cargo} shipped more than once')
                shipped_goods.add(cargo)
                shipped += 1
                _require(goods.get(cargo) == 'SHIPPED', f'{task_id} completed outbound not shipped')

    _require(set(goods) == expected_goods, 'ledger omits received cargo or invents cargo')
    _require(sim.metrics['received'] == received, 'received metric disagrees with tasks')
    _require(sim.metrics['shipped'] == shipped, 'shipped metric disagrees with tasks')
    _require(sim.metrics['completed'] == completed, 'completed metric disagrees with tasks')
    _require(sim.inside == len(initial_goods) + received - shipped, 'inside flow conservation mismatch')
    _require(sim.inside == sum(holder != 'SHIPPED' for holder in goods.values()), 'inside ledger count mismatch')
    _require(0 <= sim.inside <= len(slots), 'total warehouse capacity exceeded')

    for port, number in sim.ports.items():
        _require(port in inbound_ports, f'unknown inbound counter {port}')
        _require(not isinstance(number, bool) and isinstance(number, int) and 0 <= number <= 6,
                 f'{port} invalid or over-capacity counter')
    _require(all(sim.ports.get(port, 0) == expected_ports[port] for port in inbound_ports),
             'port counts disagree with accepted/assigned inbound custody')

    stored = {}
    for slot_id, slot in slots.items():
        _require(slot.get('slot_id') == slot_id, 'slot identity disagrees with registry')
        cargo = slot.get('cargo_id')
        if slot.get('occupied'):
            _require(cargo is not None and goods.get(cargo) == slot_id, f'{slot_id} occupied cargo ledger mismatch')
            _require(cargo not in stored, f'{cargo} occupies multiple slots')
            stored[cargo] = slot_id
        else:
            _require(cargo is None, f'{slot_id} empty slot still names cargo')
        reservation = slot.get('reservedBy')
        _require(reservation == expected_reservations.get(slot_id), f'{slot_id} reservation disagrees with task lifecycle')
        if reservation:
            task = tasks[reservation]
            _require(task['task_type'] != 'INBOUND' or not slot.get('occupied'),
                     f'{slot_id} occupied plus inbound reserved')
            _require(task['task_type'] != 'OUTBOUND' or
                     (slot.get('occupied') and cargo == task['goods_id']),
                     f'{slot_id} outbound reservation lacks its stored cargo')

    held = {}
    robot_locations = {}
    for owner, robot in robots.items():
        _require(robot.get('robot_id') == owner, 'robot identity disagrees with registry')
        location, cargo = f'ROBOT-{owner}', robot.get('cargo_id')
        robot_locations[location] = robot
        if robot.get('carrying'):
            _require(cargo is not None and goods.get(cargo) == location, f'{location} cargo ledger mismatch')
            _require(cargo not in held and cargo not in stored, f'{cargo} has multiple physical holders')
            task = tasks.get(robot.get('task_id'))
            _require(task is not None and task.get('status') == 'carrying' and
                     task.get('robot_id') == owner and task.get('goods_id') == cargo,
                     f'{location} payload lacks matching carrying task')
            held[cargo] = location
        else:
            _require(cargo is None, f'{location} empty robot still names cargo')

    for cargo, holder in goods.items():
        if holder in slots:
            _require(stored.get(cargo) == holder, f'{cargo} ledger shelf has no physical cargo')
        elif holder in robot_locations:
            _require(held.get(cargo) == holder, f'{cargo} ledger robot has no physical payload')
        elif holder in inbound_ports:
            _require(staged.get(cargo) == holder, f'{cargo} ledger port has no accepted inbound task')
        elif holder == 'SHIPPED':
            _require(cargo in shipped_goods, f'{cargo} ledger shipped before actual outbound completion')
        else:
            raise AssertionError(f'warehouse inventory: {cargo} has unknown holder {holder}')
    return {'inside': sim.inside, 'stored': len(stored), 'carried': len(held),
            'at_inbound': len(staged), 'shipped': shipped, 'received': received,
            'completed': completed}
