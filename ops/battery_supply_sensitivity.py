"""Event-driven battery-supply sensitivity, NOT a warehouse business acceptance.

Uses the production SOC/charge curve, finite station-local spare packs and
charger channels. Travel, rack handling, collision avoidance, KG and workload
orders are deliberately absent. Report it as an energy duty-cycle upper bound.
"""
from __future__ import annotations
import argparse
from collections import Counter, deque
import heapq
import hashlib
import json
import math
from pathlib import Path
import random

from backend.scheduling_v3.energy import EnergyProfile


class SupplyModel:
    def __init__(self, *, channels_per_station=2, initial='full', seed=20261029,
                 fallback=True, warmup_hours=0, measure_hours=12, robot_count=100,
                 stations=12, spares_per_station=4, swap_seconds=180):
        if initial not in {'full', 'staggered'} or type(fallback) is not bool:
            raise ValueError('invalid initial/fallback')
        for n in (channels_per_station, robot_count, stations, spares_per_station, swap_seconds):
            if type(n) is not int or n < 1:
                raise ValueError('positive integer resource counts required')
        if min(warmup_hours, measure_hours) < 0 or measure_hours <= 0:
            raise ValueError('invalid measurement window')
        self.energy = EnergyProfile()
        self.start, self.end = int(warmup_hours*3600), int((warmup_hours+measure_hours)*3600)
        self.parameters = dict(channels_per_station=channels_per_station, initial=initial,
            seed=seed, fallback=fallback, warmup_hours=warmup_hours, measure_hours=measure_hours,
            robot_count=robot_count, stations=stations, spares_per_station=spares_per_station,
            swap_seconds=swap_seconds)
        self.swap_seconds = swap_seconds
        self.fallback = fallback
        self.events, self.event_seq = [], 0
        self.counts, self.seconds = Counter(), Counter()
        self.swap_bays = {s: 0 for s in range(stations)}
        self.channels = {s: [0]*channels_per_station for s in range(stations)}
        self.ready = {s: deque(f's{s}-spare{i}' for i in range(spares_per_station)) for s in range(stations)}
        self.waiting = {s: deque() for s in range(stations)}
        self.packs = {key: dict(station=s, status='ready', soc=100.) for s, keys in self.ready.items() for key in keys}
        rng = random.Random(seed)
        self.robots = {i: dict(pack=f'robot{i}', soc=100. if initial == 'full' else rng.uniform(40, 80),
            since=0, phase='working', station=i % stations) for i in range(robot_count)}
        self.all_pack_ids = set(self.packs) | {r['pack'] for r in self.robots.values()}
        self.min_soc = 100.
        self.max_queue = 0
        for i, r in self.robots.items():
            self.push(math.ceil((r['soc']-35)/self.energy.rate('to_target', True)), 'request', i)
        self.audit()

    def push(self, at, kind, owner):
        self.event_seq += 1
        heapq.heappush(self.events, (at, 0 if kind == 'pack_ready' else 1, self.event_seq, kind, owner))

    def tick_count(self, key, at):
        if self.start <= at < self.end:
            self.counts[key] += 1

    def span(self, robot, at, phase):
        r = self.robots[robot]
        duration = at - r['since']
        if duration < 0:
            raise AssertionError('robot clock reversed')
        clip = max(0, min(at, self.end)-max(r['since'], self.start))
        self.seconds[r['phase']] += clip
        if r['phase'] == 'working':
            r['soc'] -= duration*self.energy.rate('to_target', True)
        elif r['phase'] in {'queued', 'swapping'}:
            r['soc'] -= duration*self.energy.rate('idle')
        r.update(since=at, phase=phase)
        self.min_soc = min(self.min_soc, r['soc'])

    def request(self, i, at):
        r = self.robots[i]
        if r['phase'] != 'queued':
            self.span(i, at, 'queued')
            self.tick_count('replenishment_requests', at)
        s = r['station']
        if not self.ready[s]:
            self.tick_count('ready_pack_misses', at)
            if self.fallback:
                self.span(i, at, 'charging')
                self.tick_count('ordinary_charge_starts', at)
                self.push(at+self.energy.charge_seconds(r['soc'], 80), 'charge_complete', i)
            else:
                self.waiting[s].append(i)
                self.max_queue = max(self.max_queue, sum(map(len, self.waiting.values())))
            return
        token = self.ready[s].popleft()
        self.packs[token]['status'] = 'reserved'
        start = max(at, self.swap_bays[s])
        self.swap_bays[s] = start+self.swap_seconds
        r['reserved'] = token
        self.push(start, 'swap_start', i)
        self.push(start+self.swap_seconds, 'swap_complete', i)

    def audit(self):
        ids = list(self.packs)+[r['pack'] for r in self.robots.values()]
        if len(ids) != len(set(ids)) or set(ids) != self.all_pack_ids:
            raise AssertionError('pack identity/count conservation failed')
        if any(not math.isfinite(r['soc']) or r['soc'] < 10-1e-6 or r['soc'] > 100+1e-6 for r in self.robots.values()):
            raise AssertionError('hard 10% reserve violated in supply-only model')

    def run(self):
        while self.events:
            at, _, _, kind, owner = heapq.heappop(self.events)
            if at >= self.end:
                break
            if kind == 'request':
                self.request(owner, at)
            elif kind == 'swap_start':
                self.span(owner, at, 'swapping')
                self.tick_count('swap_starts', at)
            elif kind == 'swap_complete':
                self.span(owner, at, 'working')
                r = self.robots[owner]
                s = r['station']
                token = r.pop('reserved')
                old, old_soc = r['pack'], r['soc']
                if self.packs[token]['status'] != 'reserved':
                    raise AssertionError('unreserved exchange')
                del self.packs[token]
                r.update(pack=token, soc=100.)
                channel = heapq.heappop(self.channels[s])
                start = max(channel, at)
                finish = start+self.energy.charge_seconds(old_soc, 100)
                heapq.heappush(self.channels[s], finish)
                self.packs[old] = dict(station=s, status='offline', soc=old_soc, start=start, finish=finish)
                self.push(finish, 'pack_ready', old)
                self.push(at+math.ceil(65/self.energy.rate('to_target', True)), 'request', owner)
                self.tick_count('swaps_completed', at)
            elif kind == 'charge_complete':
                self.span(owner, at, 'working')
                self.robots[owner]['soc'] = 80.
                self.push(at+math.ceil(45/self.energy.rate('to_target', True)), 'request', owner)
                self.tick_count('ordinary_charge_completions', at)
            elif kind == 'pack_ready':
                p = self.packs[owner]
                p.update(status='ready', soc=100.)
                self.ready[p['station']].append(owner)
                self.tick_count('offline_charge_completions', at)
                if self.waiting[p['station']]:
                    i = self.waiting[p['station']].popleft()
                    self.request(i, at)
            self.audit()
        for i in self.robots:
            self.span(i, self.end, self.robots[i]['phase'])
        self.audit()
        total = (self.end-self.start)*len(self.robots)
        if sum(self.seconds.values()) != total:
            raise AssertionError('robot time accounting mismatch')
        return {'parameters': self.parameters, 'counts': dict(self.counts), 'robot_seconds': dict(self.seconds),
            'productive_fraction': self.seconds['working']/total, 'minimum_soc': self.min_soc,
            'pack_identity_conserved': True, 'physical_pack_count': len(self.all_pack_ids),
            'max_waiting_robots': self.max_queue, 'pack_counts_end': dict(Counter(p['status'] for p in self.packs.values())),
            'traffic_included': False, 'warehouse_business_acceptance': False,
            'ordinary_charge_capacity_assumption': '100 chargers, no travel/occupancy delay; optimistic supply upper bound'}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--measure-hours', type=float, default=12)
    p.add_argument('--warmup-hours', type=float, default=48)
    p.add_argument('--steady-window-hours', type=float, default=168,
                   help='additional long window after warmup; still no traffic/business acceptance')
    args = p.parse_args(argv)
    if args.output.exists():
        p.error('output exists; retain previous result')
    rows = []
    for channels in (2, 4):
        for initial, warmup in (('full', 0), ('staggered', 0), ('staggered', args.warmup_hours)):
            rows.append(SupplyModel(channels_per_station=channels, initial=initial,
                warmup_hours=warmup, measure_hours=args.measure_hours).run())
    rows += [SupplyModel(channels_per_station=c, initial='staggered', fallback=False,
                         warmup_hours=w, measure_hours=args.measure_hours).run()
             for c in (2, 4) for w in (0, args.warmup_hours)]
    if args.steady_window_hours > 0:
        rows += [SupplyModel(channels_per_station=c, initial='staggered', fallback=f,
                             warmup_hours=args.warmup_hours, measure_hours=args.steady_window_hours).run()
                 for c in (2, 4) for f in (False, True)]
    charge_hours = EnergyProfile().charge_seconds(35, 100)/3600
    result = {'scope': 'finite battery supply sensitivity; no task/traffic/KG acceptance',
              'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'cases': rows,
              'energy_parameters': EnergyProfile().as_dict(),
              'energy_source_sha256': hashlib.sha256((Path(__file__).resolve().parents[1]/
                  'backend/scheduling_v3/energy.py').read_bytes()).hexdigest(),
              'analytical': {'loaded_100_to_35_hours': 65/18, 'charge_35_to_100_minutes': charge_hours*60,
                  '100_robots_full_load_pack_demand_per_hour': 100/(65/18),
                  '24_channels_pack_supply_per_hour': 24/charge_hours,
                  '48_channels_pack_supply_per_hour': 48/charge_hours}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')
    print(json.dumps([{'channels':r['parameters']['channels_per_station']*12,
        'warmup':r['parameters']['warmup_hours'],'initial':r['parameters']['initial'],
        'fallback':r['parameters']['fallback'],'productive':r['productive_fraction'],
        'swaps':r['counts'].get('swaps_completed',0),'charges':r['counts'].get('ordinary_charge_starts',0)} for r in rows]))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
