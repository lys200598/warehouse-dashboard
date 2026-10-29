"""Opt-in deadline donation from published waiting cargo to full-port pickups.

No task fields, capacity gates, future plan rows, routes or energy are changed.
Observed service cadence only ranks decisions; it is not a feasibility claim.
"""
import math
from statistics import median

VERSION = 'published-full-port-observed-cadence-v1'
PICKUP_EXTRA_WEIGHT = 3.  # Total empty approach weight becomes four, not speed.
MIN_REWARD_SECONDS = 300.
RISK_WINDOW_SECONDS = 120.


def deadline_admission_order(sim):
    waiting = [key for key in sim.pending
        if sim.tasks[key]['status'] == 'waiting'
        and sim.tasks[key]['task_type'] == 'INBOUND'
        and sim.tasks[key]['release_second'] <= sim.elapsed]
    return sorted(waiting, key=lambda key: (sim.tasks[key]['due_second'],
        sim.tasks[key]['release_second'], key))


def observed_pickup_cadences(sim, sources):
    """Forecast from past backlogged pickups; includes approach/traffic time.

    Reconstructed from actual task facts, so checkpoints need no new state.
    Missing/invalid receipt facts cannot assert a backlogged interval.
    """
    histories = {source: [] for source in sources}
    for task in sim.tasks.values():
        if task.get('status') == 'scheduled' or task.get('task_type') != 'INBOUND':
            continue
        source = task.get('source_id')
        if source not in histories:
            continue
        picked = task.get('picked_at')
        if (isinstance(picked, bool) or not isinstance(picked, (int, float))
                or not math.isfinite(picked) or not 0 <= picked <= sim.elapsed):
            continue
        received = task.get('received_at')
        if (isinstance(received, bool) or not isinstance(received, (int, float))
                or not math.isfinite(received) or not 0 <= received <= picked):
            received = None
        histories[source].append((picked, received, str(task.get('task_id', ''))))
    result = {}
    for source, history in histories.items():
        history.sort(key=lambda row: (row[0], row[2]))
        gaps = [current[0] - previous[0] for previous, current in zip(history, history[1:])
            if current[1] is not None and current[1] <= previous[0] and current[0] > previous[0]]
        estimate = median(gaps[-12:]) if gaps else sim.config.pick_seconds
        result[source] = max(sim.config.pick_seconds, min(60., estimate))
    return result


def context(sim):
    """Only demand blocked SOLELY by its full source port donates priority."""
    ports = {}
    if sim.inside >= len(sim.slots):
        return ports
    for key in sim.pending:
        task = sim.tasks[key]
        if (task['status'] != 'waiting' or task['task_type'] != 'INBOUND'
                or task['release_second'] > sim.elapsed
                or sim.ports.get(task['source_id'], 0) < 6):
            continue
        slot = sim.slots[task['target_id']]
        if slot['occupied'] or slot['reservedBy']:
            continue
        remaining = sim.roadmap.travel_time(task['source_id'], task['target_id'], sim.config.speed)
        remaining += sim.config.pick_seconds + sim.config.place_seconds
        if not math.isfinite(remaining):
            continue
        source = task['source_id']
        latest = task['due_second'] - remaining
        age = sim.elapsed - task['release_second']
        if source not in ports:
            ports[source] = {'latest_pickup': latest, 'max_wait_age': age, 'waiting': 1, '_deadlines': [latest]}
        else:
            ports[source]['latest_pickup'] = min(ports[source]['latest_pickup'], latest)
            ports[source]['max_wait_age'] = max(ports[source]['max_wait_age'], age)
            ports[source]['waiting'] += 1
            ports[source]['_deadlines'].append(latest)
    # Predict a deadline-density envelope from observed backlogged service.
    # The median gap includes approach/traffic; unlike the pick-only version,
    # this is a forecast, NOT a physical necessary or sufficient bound.
    cadences = observed_pickup_cadences(sim, ports)
    for source, donor in ports.items():
        deadlines = sorted(donor.pop('_deadlines'))
        donor['latest_pickup'] = min(deadline - rank * cadences[source]
            for rank, deadline in enumerate(deadlines))
    return ports


def recipient(task, ports):
    return (ports.get(task['source_id'])
        if task['task_type'] == 'INBOUND' and task['status'] == 'accepted' else None)


def priority(task, ports, elapsed):
    ordinary = task['due_second'] - .5 * (elapsed - task['ready_at'])
    donor = recipient(task, ports)
    return (min(ordinary, donor['latest_pickup'] - .5 * donor['max_wait_age'])
        if donor else ordinary)


def row_adjustment(sim, task, row, pair_times, ports, forbidden):
    """Keep real-column identities and forbidden pairs; prefer fast pickups."""
    donor = recipient(task, ports)
    if not donor:
        return list(row), None, 0.
    delivery = sim.roadmap.travel_time(task['source_id'], task['target_id'], sim.config.speed)
    delivery += sim.config.pick_seconds + sim.config.place_seconds
    result, pickup_finishes = list(row), []
    for col, total in enumerate(pair_times):
        if row[col] >= forbidden:
            continue
        approach = max(0., total - delivery)
        pickup_finishes.append(approach + sim.config.pick_seconds)
        # Saturation keeps a statically feasible edge feasible. Real routing,
        # capability, energy and continuous reservations still decide admission.
        result[col] = min(forbidden - 1., row[col] + PICKUP_EXTRA_WEIGHT * approach)
    if not pickup_finishes:
        return result, None, 0.
    slack = donor['latest_pickup'] - sim.elapsed - min(pickup_finishes)
    reward = MIN_REWARD_SECONDS + 4. * max(0., RISK_WINDOW_SECONDS - slack)
    return result, slack, min(reward, 1e7)
