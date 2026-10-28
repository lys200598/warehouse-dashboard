"""Continuous-stock, reproducible multi-day warehouse boundary task inputs.

One reference state machine spans every day: IN is counted at receipt, OUT
releases reference capacity on its estimated completion, and each slot keeps
its cargo identity and turnaround state across midnight. These are declared
synthetic workloads, not original LaDe/Jilin records or measured outcomes.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta
import heapq
import json
import math
from pathlib import Path
import random

from .data import FIELDS, load_scene, sha, validate_dataset


VERSION = 'warehouse-continuous-multiday-v1'
DAY = 86400
PROFILES = ('uniform', 'fixed_peak', 'local_hotspot', 'batch_pulses',
            'moving_hotspot', 'tidal', 'mixed_recovery')


def _integer(value, name, minimum=1, maximum=None):
    if type(value) is not int or value < minimum or maximum is not None and value > maximum:
        raise ValueError('invalid ' + name)
    return value


def _number(value, name, minimum=0., maximum=None):
    if type(value) not in (int, float) or not math.isfinite(value) or value < minimum or (
            maximum is not None and value > maximum):
        raise ValueError('invalid ' + name)
    return value


def _json(path, value):
    with Path(path).open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')


class RoadReference:
    """Static warehouse-road reference; no simulation outcomes are consulted.

    The roadmap is undirected. Querying slot->station for both task directions
    keeps reverse-distance caches limited to the sixteen stations. The common
    deadhead allowance is twice the maximum graph/named-location distance to
    IN-1, an explicit via-hub allowance for a prior robot position.
    """
    def __init__(self, scene, slots, speed):
        from .motion import WideNavigator, WideRoadmap
        self.speed = speed
        self.roadmap = WideRoadmap(WideNavigator(scene, slots))
        hub = 'IN-1'
        values = list(self.roadmap.distances(hub, speed).values())
        for location in self.roadmap.locations:
            value = self.roadmap.travel_time(location, hub, speed)
            if not math.isfinite(value):
                raise ValueError('reference road disconnected at ' + location)
            values.append(value)
        self.deadhead_seconds = 2*max(values, default=0)
        self._cache = {}

    def task_seconds(self, slot, station):
        key = (slot, station)
        if key not in self._cache:
            value = self.roadmap.travel_time(slot, station, self.speed)
            if not math.isfinite(value):
                raise ValueError('reference road disconnected task endpoints')
            self._cache[key] = value
        return self._cache[key]


def day_release_seconds(profile, count, rng, batch_size=48):
    """Fixed counts isolate temporal concentration without changing daily N."""
    if profile not in PROFILES:
        raise ValueError('unknown daily profile ' + str(profile))
    _integer(count, 'daily tasks'); _integer(batch_size, 'batch size', 1, 48)
    if profile == 'batch_pulses':
        # Three two-hour windows contain same-second batches. Every batch has
        # <=48 tasks, so a fully-IN batch can be spread over eight 6-box ports.
        batches = math.ceil(count/batch_size)
        if batches > 3*7200:
            raise ValueError('batch count exceeds distinct integer-second release slots in declared windows')
        result = []
        for number in range(batches):
            wave = min(2, number*3//batches)
            first = math.ceil(wave*batches/3)
            stop = math.ceil((wave+1)*batches/3)
            local = number-first
            release = (8+6*wave)*3600 + local*7200//max(1, stop-first)
            result.extend([release]*min(batch_size, count-len(result)))
        return result
    if profile == 'fixed_peak':
        weights = [3. if hour in {8, 9, 14, 15, 20, 21} else .25 for hour in range(24)]
    elif profile == 'mixed_recovery':
        weights = [.15/6 if hour < 6 else .70/6 if hour < 12 else
                   .05/6 if hour < 18 else .10/6 for hour in range(24)]
    else:
        weights = [1.]*24
    total = sum(weights)
    prefix, current = [], 0.
    for weight in weights:
        current += weight/total
        prefix.append(current)
    values = []
    for index in range(count):
        quantile = (index+rng.random())/count
        hour = next(i for i, upper in enumerate(prefix) if quantile < upper or i == 23)
        lower = prefix[hour-1] if hour else 0.
        portion = (quantile-lower)/(prefix[hour]-lower)
        values.append(min(DAY-1, hour*3600+int(portion*3600)))
    return values


def _hot_slots(keys, slots, fraction, reverse=False):
    centre = max(keys, key=lambda key: (slots[key]['x'], slots[key]['y'], key)) if reverse else min(
        keys, key=lambda key: (slots[key]['x'], slots[key]['y'], key))
    point = slots[centre]
    ordered = sorted(keys, key=lambda key: ((slots[key]['x']-point['x'])**2 +
                      (slots[key]['y']-point['y'])**2, key))
    return ordered[:max(1, math.ceil(len(keys)*fraction))], {'x': point['x'], 'y': point['y']}


def _snapshot(states, inside):
    counts = Counter(value['status'] for value in states.values())
    return {'inside': inside, 'stored': counts['stored'], 'inbound_pending': counts['inbound_pending'],
            'outbound_pending': counts['outbound_pending'], 'empty': counts['empty'],
            'inside_after_all_pending_out_complete': inside-counts['outbound_pending']}


def generate_multiday(scene_path, output, *, days=7, profiles=None, tasks_per_day=6912,
                      seed=20261028, date='2026-10-28', active_slots=None,
                      minimum_slot_gap_seconds=1800, clearance_seconds=60,
                      deadline_slack_seconds=900, reference_speed=.8,
                      pick_seconds=10, place_seconds=12, mixed_load_factor=1.25,
                      batch_size=48, max_tasks=250000):
    """Write a fresh continuous dataset and independent reference reports.

    ``days`` has no seven-day ceiling. Explicit ``max_tasks`` bounds this job's
    memory/output budget; the caller may raise it for a larger declared run.
    The simulation still applies its own actual admission, traffic and energy
    decisions. Reference inventory correctness is a separate data property.
    """
    _integer(days, 'days'); _integer(tasks_per_day, 'tasks per day')
    _integer(seed, 'seed', 0, 2**64-1); _integer(max_tasks, 'max tasks')
    _integer(minimum_slot_gap_seconds, 'minimum slot gap seconds')
    _integer(clearance_seconds, 'clearance seconds')
    _integer(deadline_slack_seconds, 'deadline slack seconds', 0)
    _number(reference_speed, 'reference speed', 1e-9)
    _integer(pick_seconds, 'pick seconds'); _integer(place_seconds, 'place seconds')
    _number(mixed_load_factor, 'mixed load factor', 1.)
    _integer(batch_size, 'batch size', 1, 48)
    if profiles is None:
        profiles = PROFILES
    if not isinstance(profiles, (list, tuple)) or not profiles or any(value not in PROFILES for value in profiles):
        raise ValueError('profiles must contain known daily profiles')
    # This lower bound rejects enormous requests before allocating profile/day
    # lists or building the roadmap. The cap is a caller-configured task budget.
    if days*tasks_per_day > max_tasks:
        raise ValueError('requested workload exceeds max_tasks; choose an explicit larger budget')
    selected_profiles = [profiles[day % len(profiles)] for day in range(days)]
    counts = [round(tasks_per_day*(mixed_load_factor if name == 'mixed_recovery' else 1.))
              for name in selected_profiles]
    if sum(counts) > max_tasks:
        raise ValueError('requested workload exceeds max_tasks including mixed load factor')
    try:
        start = datetime.fromisoformat(date+'T00:00:00+08:00')
        finish = start+timedelta(days=days)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('invalid date or calendar horizon') from exc
    output = Path(output)
    if output.exists():
        raise FileExistsError('dataset output exists; choose a new directory')
    scene, slots = load_scene(scene_path)
    keys = sorted(slots)
    slot_numbers = {key: index+1 for index, key in enumerate(keys)}
    if active_slots is not None:
        _integer(active_slots, 'active slots', 1, len(keys))
    rng = random.Random(seed)
    initial_order = list(keys); rng.shuffle(initial_order)
    initial_set = set(initial_order[:len(keys)//2])
    active_order = list(keys); rng.shuffle(active_order)
    active = sorted(active_order[:active_slots] if active_slots is not None else keys)
    active_set = set(active)
    passive_stock = len(initial_set-active_set)
    hot_a, centre_a = _hot_slots(active, slots, .2)
    hot_b, centre_b = _hot_slots(active, slots, .2, reverse=True)
    hot_mixed, centre_mixed = _hot_slots(active, slots, .3)
    spatial_sets = {'a': set(hot_a), 'b': set(hot_b), 'mixed': set(hot_mixed)}
    spatial_lists = {name: (sorted(value), [key for key in active if key not in value])
                     for name, value in spatial_sets.items()}
    reference = RoadReference(scene, slots, reference_speed)
    initial = [{'goods_id': f'MD-INITIAL-{index+1:04d}', 'location_id': key}
               for index, key in enumerate(keys) if key in initial_set]
    initial_goods = {row['location_id']: row['goods_id'] for row in initial}
    states = {key: {'status': 'stored' if key in initial_set else 'empty',
                   'goods': initial_goods.get(key), 'last_release': None, 'incoming': 0}
              for key in keys}
    available = {'INBOUND': set(active_set-initial_set), 'OUTBOUND': set(active_set & initial_set)}
    inside = len(initial)
    low = high = inside
    event_heap, event_serial = [], 0
    pending_out = 0
    reference_received = reference_shipped = 0
    tasks, per_day, hour_inventory = [], [], []
    capacity_checks = transitions = 0
    minimum_observed_gap = None
    time_cursor = 0
    next_hour = 0

    def observe_capacity():
        nonlocal low, high, capacity_checks
        if not 0 <= inside <= len(slots):
            raise AssertionError('reference accepted inventory exceeds physical capacity')
        low, high = min(low, inside), max(high, inside)
        capacity_checks += 1

    def enqueue(at, kind, key):
        nonlocal event_serial
        event_serial += 1
        heapq.heappush(event_heap, (at, event_serial, kind, key))

    def advance(clock):
        nonlocal inside, pending_out, time_cursor, transitions, reference_shipped
        if clock < time_cursor:
            raise AssertionError('reference clock moved backwards')
        while event_heap and event_heap[0][0] <= clock:
            at, _, kind, key = heapq.heappop(event_heap)
            value = states[key]
            if kind == 'complete_in':
                if value['status'] != 'inbound_pending':
                    raise AssertionError('reference inbound completion lacks reservation')
                value['status'] = 'stored'
                enqueue(max(value['last_release']+minimum_slot_gap_seconds, at+clearance_seconds), 'eligible', key)
            elif kind == 'complete_out':
                if value['status'] != 'outbound_pending':
                    raise AssertionError('reference outbound completion lacks unique goods')
                value.update(status='empty', goods=None)
                inside -= 1
                pending_out -= 1
                reference_shipped += 1
                enqueue(max(value['last_release']+minimum_slot_gap_seconds, at+clearance_seconds), 'eligible', key)
            elif kind == 'eligible':
                target = 'INBOUND' if value['status'] == 'empty' else 'OUTBOUND' if value['status'] == 'stored' else None
                if target is None:
                    raise AssertionError('pending reference slot became eligible early')
                available[target].add(key)
            else:
                raise AssertionError('unknown reference transition')
            observe_capacity()
            transitions += 1
        time_cursor = clock

    def record_hours_through(clock):
        nonlocal next_hour
        while next_hour <= clock and next_hour <= days*DAY:
            advance(next_hour)
            hour_inventory.append({'second': next_hour, **_snapshot(states, inside)})
            next_hour += 3600
        advance(clock)

    station_counters = Counter()
    for day, (name, count) in enumerate(zip(selected_profiles, counts)):
        day_start, day_end = day*DAY, (day+1)*DAY
        record_hours_through(day_start)
        opening = _snapshot(states, inside)
        opening_received, opening_shipped = reference_received, reference_shipped
        hourly, incoming_hourly, outgoing_hourly = [0]*24, [0]*24, [0]*24
        rack_counts, station_counts, hot_counts = Counter(), Counter(), Counter()
        fallbacks = Counter()
        first_task = len(tasks)
        releases = day_release_seconds(name, count, rng, batch_size)
        for local_second in releases:
            second = day_start+local_second
            record_hours_through(second)
            hour = local_second//3600
            if name == 'tidal':
                target_fraction = .25 if hour < 8 else .75 if hour < 16 else .5
                target_stock = passive_stock+round(len(active)*target_fraction)
                difference = target_stock-(inside-pending_out)
                p_in = .5 + max(-.35, min(.35, difference/max(1., len(active)*.3)))
            else:
                p_in = .5
            wanted = 'INBOUND' if rng.random() < p_in else 'OUTBOUND'
            other = 'OUTBOUND' if wanted == 'INBOUND' else 'INBOUND'
            spatial = ('a' if name == 'local_hotspot' or name == 'moving_hotspot' and hour < 12 else
                       'b' if name == 'moving_hotspot' else
                       'mixed' if name == 'mixed_recovery' and 6 <= hour < 12 else None)
            prefer_hot = spatial is not None and rng.random() < (.65 if spatial == 'mixed' else .70)
            preferred = spatial_lists[spatial][0 if prefer_hot else 1] if spatial else active
            selected = None
            for kind in (wanted, other):
                if kind == 'INBOUND' and inside >= len(slots):
                    continue
                candidates = [key for key in preferred if key in available[kind]]
                if not candidates:
                    candidates = [key for key in active if key in available[kind]]
                if candidates:
                    selected = kind, rng.choice(candidates)
                    break
            if selected is None:
                raise ValueError(f'declared turnover capacity exhausted on day {day+1} at second {local_second}; '
                                 'reduce tasks_per_day or adjust the explicit slot-gap policy')
            kind, key = selected
            value = states[key]
            available[kind].remove(key)
            if kind != wanted:
                fallbacks['direction_availability'] += 1
            if spatial and (key in spatial_sets[spatial]) != prefer_hot:
                fallbacks['spatial_availability'] += 1
            if value['last_release'] is not None:
                gap = second-value['last_release']
                minimum_observed_gap = gap if minimum_observed_gap is None else min(gap, minimum_observed_gap)
                if gap < minimum_slot_gap_seconds:
                    raise AssertionError('slot turnover gap violated')
            value['last_release'] = second
            station_counters[kind] += 1
            station = ('IN-' if kind == 'INBOUND' else 'OUT-') + str(1+(station_counters[kind]-1) % 8)
            if kind == 'INBOUND':
                if value['status'] != 'empty' or value['goods'] is not None:
                    raise AssertionError('inbound reference slot not empty')
                value['incoming'] += 1
                goods = f'MD-G-{slot_numbers[key]:04d}-{value["incoming"]:05d}'
                value.update(status='inbound_pending', goods=goods)
                inside += 1
                reference_received += 1
                source, target = station, key
                incoming_hourly[hour] += 1
            else:
                if value['status'] != 'stored' or not value['goods']:
                    raise AssertionError('outbound reference cargo not stored')
                goods = value['goods']
                value['status'] = 'outbound_pending'
                pending_out += 1
                source, target = key, station
                outgoing_hourly[hour] += 1
            travel = reference.task_seconds(key, station)
            service = math.ceil(reference.deadhead_seconds+travel+pick_seconds+place_seconds)
            enqueue(second+service, 'complete_in' if kind == 'INBOUND' else 'complete_out', key)
            due = second+service+deadline_slack_seconds
            serial = len(tasks)+1
            tasks.append({'task_id': f'MD-T-{serial:08d}', 'task_type': kind, 'goods_id': goods,
                'release_time': (start+timedelta(seconds=second)).isoformat(),
                'due_time': (start+timedelta(seconds=due)).isoformat(), 'source_id': source, 'target_id': target})
            hourly[hour] += 1
            rack_counts[slots[key]['rack_id']] += 1
            station_counts[station] += 1
            if spatial:
                hot_counts['evaluated_tasks'] += 1
                hot_counts['inside_current_hotspot'] += int(key in spatial_sets[spatial])
                hot_counts['morning_hotspot_a'] += int(hour < 12 and key in spatial_sets['a'])
                hot_counts['afternoon_hotspot_b'] += int(hour >= 12 and key in spatial_sets['b'])
            observe_capacity()
        record_hours_through(day_end)
        counts_for_day = Counter(row['task_type'] for row in tasks[first_task:])
        day_received, day_shipped = reference_received-opening_received, reference_shipped-opening_shipped
        if opening['inside']+day_received-day_shipped != inside:
            raise AssertionError('per-day reference inventory conservation failed')
        per_day.append({'day_number': day+1, 'date': (start+timedelta(days=day)).date().isoformat(),
            'profile': name, 'tasks': count, 'inbound': counts_for_day['INBOUND'], 'outbound': counts_for_day['OUTBOUND'],
            'hourly_tasks': hourly, 'hourly_inbound': incoming_hourly, 'hourly_outbound': outgoing_hourly,
            'opening_reference_inventory': opening, 'closing_reference_inventory': _snapshot(states, inside),
            'reference_received_during_day': day_received, 'reference_shipped_during_day': day_shipped,
            'reference_inventory_conserved': True,
            'rack_task_counts': dict(sorted(rack_counts.items())), 'station_task_counts': dict(sorted(station_counts.items())),
            'hotspot_statistics': dict(hot_counts), 'availability_fallbacks': dict(fallbacks),
            'release_distribution': 'same-second bounded batches in three windows' if name == 'batch_pulses' else
                                    'stratified random quantiles of declared hourly intensity',
            'profile_restarts_robot_or_inventory_state': False})
    end_inventory = _snapshot(states, inside)
    drain_at = max((entry[0] for entry in event_heap), default=days*DAY)
    while event_heap:
        drain_at = max(drain_at, event_heap[0][0])
        advance(drain_at)
    final_inventory = _snapshot(states, inside)
    if inside != len(initial)+sum(row['task_type'] == 'INBOUND' for row in tasks)-sum(row['task_type'] == 'OUTBOUND' for row in tasks):
        raise AssertionError('reference inventory conservation failed')
    tasks.sort(key=lambda row: (row['release_time'], row['task_id']))
    output.mkdir(parents=True, exist_ok=False)
    for filename, rows, fields in [('tasks.csv', tasks, FIELDS),
                                  ('initial_inventory.csv', initial, ['goods_id', 'location_id'])]:
        with (output/filename).open('x', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
            writer.writeheader(); writer.writerows(rows)
    spatial_metadata = {'hotspot_a': {'centre': centre_a, 'slot_ids': hot_a, 'fraction': .2},
                        'hotspot_b': {'centre': centre_b, 'slot_ids': hot_b, 'fraction': .2},
                        'mixed_hotspot': {'centre': centre_mixed, 'slot_ids': hot_mixed, 'fraction': .3}}
    profile_report = {'version': VERSION, 'days': per_day, 'spatial_definitions': spatial_metadata,
                      'fixed_daily_total_except_mixed': tasks_per_day,
                      'data_origin': 'synthetic warehouse boundary input; LaDe-like organization, not actual Jilin records'}
    inventory_report = {'version': VERSION, 'capacity': len(slots), 'initial_inventory': len(initial),
        'minimum_inside': low, 'maximum_inside': high, 'capacity_checks': capacity_checks,
        'reference_transitions': transitions, 'hourly_inventory': hour_inventory,
        'reference_received_total': reference_received, 'reference_shipped_total': reference_shipped,
        'inventory_at_horizon': end_inventory, 'inventory_after_reference_drain': final_inventory,
        'reference_drain_second': drain_at, 'minimum_observed_same_slot_release_gap': minimum_observed_gap,
        'counting': 'IN counted at reference receipt/release; OUT capacity released at reference completion',
        'slot_rule': 'one cargo/reservation per slot; next release after reference completion and declared turnaround',
        'reference_capacity_conserved': True, 'runtime_feasibility_claim': False}
    _json(output/'profiles-report.json', profile_report)
    _json(output/'reference-inventory.json', inventory_report)
    manifest = {'version': VERSION, 'scenario': 'continuous-multiday-seven-pattern-cycle', 'seed': seed,
        'start': start.isoformat(), 'end': finish.isoformat(), 'duration_seconds': days*DAY, 'days': days,
        'time_unit': 'ISO8601 +08:00 integer seconds', 'scene_sha256': sha(scene_path),
        'capacity': len(slots), 'initial_inventory': len(initial), 'tasks': len(tasks),
        'inbound': sum(row['task_type'] == 'INBOUND' for row in tasks),
        'outbound': sum(row['task_type'] == 'OUTBOUND' for row in tasks), 'active_slots': len(active),
        'active_slot_ids': active, 'day_profiles': selected_profiles,
        'generator_parameters': {'tasks_per_day': tasks_per_day, 'mixed_load_factor': mixed_load_factor,
            'minimum_slot_gap_seconds': minimum_slot_gap_seconds, 'clearance_seconds': clearance_seconds,
            'deadline_slack_seconds': deadline_slack_seconds, 'reference_speed_m_s': reference_speed,
            'reference_pick_seconds': pick_seconds, 'reference_place_seconds': place_seconds,
            'reference_deadhead_seconds': reference.deadhead_seconds, 'batch_size': batch_size,
            'max_tasks_job_budget': max_tasks},
        'assumptions': {'inventory': 'one continuous reference state across all days; pending work crosses midnight',
            'goods': 'one stable ID per item, at most one IN and one later OUT; initial cargo can start with OUT',
            'deadline': 'static undirected warehouse road task time + via-IN-1 graph deadhead allowance + handling + fixed slack',
            'reference_completion': 'static task/deadhead/handling reference, before fixed deadline slack; not a measured schedule',
            'capacity': 'IN reference admission requires inside<1728 and empty unreserved target; OUT releases total capacity on reference completion',
            'runtime': 'actual admission separately enforces six boxes per IN port, actual goods readiness, traffic and energy',
            'ports': 'round-robin within task direction; <=48 same-second tasks per declared pulse batch',
            'tidal': 'reference projected-stock targets 25%,75%,50% of active storage in three eight-hour phases plus passive stock',
            'mixed': '1.25 default daily volume; 70% arrivals 06-12, 5% arrivals 12-18 recovery window; hotspot active in high window',
            'algorithm_separation': 'robot assignments, SOC, faults, charge and forced handoff are absent from task CSV',
            'source': 'synthetic representative variations; no enterprise order/customer model or claim of original Jilin measurements'},
        'generator_sources': {'multiday.py': sha(Path(__file__)), 'wide_data.py': sha(Path(__file__).with_name('data.py'))},
        'daily_totals': [{'day': day['day_number'], 'profile': day['profile'], 'tasks': day['tasks'],
                          'inbound': day['inbound'], 'outbound': day['outbound']} for day in per_day],
        'files': {filename: sha(output/filename) for filename in ('tasks.csv', 'initial_inventory.csv')},
        'reports': {filename: sha(output/filename) for filename in ('profiles-report.json', 'reference-inventory.json')}}
    _json(output/'manifest.json', manifest)
    validation = validate_dataset(scene_path, output)
    if validation['planned_final_inventory'] != final_inventory['inside']:
        raise AssertionError('independent reference validator final inventory disagrees')
    validation.update(multiday_reference_capacity_conserved=True, continuous_inventory_across_days=True,
                      profiles_report_sha256=manifest['reports']['profiles-report.json'],
                      reference_inventory_sha256=manifest['reports']['reference-inventory.json'])
    _json(output/'validation.json', validation)
    return manifest, validation


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scene', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--profiles', help='comma-separated profile cycle; default is all seven in order')
    parser.add_argument('--tasks-per-day', type=int, default=6912)
    parser.add_argument('--seed', type=int, default=20261028)
    parser.add_argument('--date', default='2026-10-28')
    parser.add_argument('--active-slots', type=int)
    parser.add_argument('--minimum-slot-gap-seconds', type=int, default=1800)
    parser.add_argument('--deadline-slack-seconds', type=int, default=900)
    parser.add_argument('--mixed-load-factor', type=float, default=1.25)
    parser.add_argument('--batch-size', type=int, default=48)
    parser.add_argument('--max-tasks', type=int, default=250000)
    args = parser.parse_args(argv)
    manifest, validation = generate_multiday(args.scene, args.output, days=args.days,
        profiles=args.profiles.split(',') if args.profiles else None, tasks_per_day=args.tasks_per_day,
        seed=args.seed, date=args.date, active_slots=args.active_slots,
        minimum_slot_gap_seconds=args.minimum_slot_gap_seconds,
        deadline_slack_seconds=args.deadline_slack_seconds, mixed_load_factor=args.mixed_load_factor,
        batch_size=args.batch_size, max_tasks=args.max_tasks)
    print(json.dumps({'output': str(Path(args.output).resolve()), 'version': VERSION,
        'days': manifest['days'], 'tasks': manifest['tasks'], 'inbound': manifest['inbound'],
        'outbound': manifest['outbound'], 'daily_totals': manifest['daily_totals'],
        'independent_validation': validation['valid'], 'files': manifest['files'],
        'runtime_feasibility_claim': False}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
