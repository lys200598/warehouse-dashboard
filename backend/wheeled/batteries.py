"""Finite pack inventory; service duration and physical routing live in sim.py.

SOC is percentage points. No pack is created on exchange: the installed pack
joins the station inventory and the reserved ready pack becomes installed.
"""
from copy import deepcopy
import math


class BatteryPool:
    def __init__(self, owners, stations, spares_per_station, chargers_per_station, profile):
        self.profile = profile
        self.installed = {str(i): f'installed-{i}' for i in owners}
        self.owners = set(self.installed)
        self.chargers = {s: chargers_per_station for s in stations}
        self.packs = {f'{s}-spare-{i}': {'station': s, 'soc': 100., 'status': 'ready', 'owner': None}
                      for s in stations for i in range(spares_per_station)}
        self.all_ids = set(self.installed.values()) | set(self.packs)
        self.updated_at = 0
        self.audit()

    def audit(self):
        if set(self.installed) != self.owners:
            raise AssertionError('installed battery owner set changed')
        ids = list(self.installed.values()) + list(self.packs)
        if len(ids) != len(set(ids)) or set(ids) != self.all_ids:
            raise AssertionError('battery identity/count conservation violated')
        for p in self.packs.values():
            if (p['station'] not in self.chargers or p['status'] not in {'ready', 'reserved', 'waiting', 'charging'}
                    or not math.isfinite(p['soc']) or not 0 <= p['soc'] <= 100):
                raise AssertionError('invalid battery inventory')
            if (p['status'] == 'reserved') != (p['owner'] is not None):
                raise AssertionError('battery reservation ownership mismatch')
            if p['status'] in {'ready', 'reserved'} and abs(p['soc'] - 100) > 1e-7:
                raise AssertionError('non-full battery advertised as ready')
        for s, limit in self.chargers.items():
            if sum(p['station'] == s and p['status'] == 'charging' for p in self.packs.values()) > limit:
                raise AssertionError('offline charger capacity exceeded')

    def ready(self, station):
        return sorted(k for k, p in self.packs.items() if p['station'] == station and p['status'] == 'ready')

    def reserve(self, station, owner):
        if str(owner) not in self.installed:
            raise ValueError('unknown battery owner')
        if any(p['owner'] == owner for p in self.packs.values()):
            raise ValueError('owner already reserved a pack')
        choices = self.ready(station)
        if not choices:
            return None
        token = choices[0]
        self.packs[token].update(status='reserved', owner=owner)
        self.audit()
        return token

    def cancel(self, token, owner):
        p = self.packs[token]
        if p['status'] != 'reserved' or p['owner'] != owner:
            raise ValueError('invalid battery cancellation')
        p.update(status='ready', owner=None)
        self.audit()

    def complete(self, token, owner, removed_soc):
        if not math.isfinite(removed_soc) or not 0 <= removed_soc <= 100:
            raise ValueError('invalid removed SOC')
        p = self.packs[token]
        if p['status'] != 'reserved' or p['owner'] != owner:
            raise ValueError('missing battery reservation')
        station, installed_soc = p['station'], p['soc']
        old = self.installed[str(owner)]
        del self.packs[token]
        self.installed[str(owner)] = token
        self.packs[old] = {'station': station, 'soc': removed_soc,
                           'status': 'ready' if removed_soc >= 100 else 'waiting', 'owner': None}
        self.audit()
        return {'removed_pack': old, 'installed_pack': token, 'removed_soc': removed_soc,
                'installed_soc': installed_soc, 'station': station}

    def advance(self, now):
        if isinstance(now, bool) or not isinstance(now, int) or now < self.updated_at:
            raise ValueError('battery pool clock must be monotonic integer seconds')
        # Advance one second at a time so waiting packs may start on a channel
        # released by a full pack; do not grant an entire interval retroactively.
        while self.updated_at < now:
            for station, limit in self.chargers.items():
                active = sum(p['station'] == station and p['status'] == 'charging' for p in self.packs.values())
                for key in sorted(self.packs):
                    p = self.packs[key]
                    if active < limit and p['station'] == station and p['status'] == 'waiting':
                        p['status'] = 'charging'
                        active += 1
                for p in self.packs.values():
                    if p['station'] == station and p['status'] == 'charging':
                        p['soc'] = min(100., p['soc'] + self.profile.charge_gain(p['soc'], 1))
                        if p['soc'] >= 100 - 1e-9:
                            p.update(soc=100., status='ready')
            self.updated_at += 1
        self.audit()

    def snapshot(self):
        self.audit()
        return deepcopy({'installed': self.installed, 'chargers': self.chargers, 'packs': self.packs,
                         'all_ids': sorted(self.all_ids), 'updated_at': self.updated_at})

    def restore(self, state):
        if set(state['all_ids']) != self.all_ids or state['chargers'] != self.chargers:
            raise ValueError('battery pool configuration/identity mismatch')
        self.installed, self.packs = deepcopy(state['installed']), deepcopy(state['packs'])
        self.updated_at = state['updated_at']
        if isinstance(self.updated_at, bool) or not isinstance(self.updated_at, int) or self.updated_at < 0:
            raise ValueError('invalid battery pool clock')
        self.audit()
