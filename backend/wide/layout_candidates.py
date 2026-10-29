"""Independent larger warehouse candidates, never a replacement for V2.

Geometry and source identity only: this module does not tune robot speed,
deadlines, task volume or routing safety. Throughput acceptance is separate.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import csv
import hashlib
import io
import json
from pathlib import Path

from .data import load_scene


ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'data/layouts/warehouse100-wide-v2.json'
CANDIDATES = {
    'medium': {'width': 120., 'depth': 90., 'columns': 6, 'rows': 8, 'rack_count': 48},
    'large': {'width': 150., 'depth': 100., 'columns': 8, 'rows': 8, 'rack_count': 60},
}
LOCATION_FIELDS = ('location_id', 'kind', 'rack_id', 'level', 'face', 'bay', 'x', 'y')


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _axis_positions(count, size, ordinary=5., main=8.):
    """Two regular blocks separated by one explicit main cross aisle."""
    values, cursor = [], 0.
    for index in range(count):
        values.append(cursor)
        cursor += size
        if index < count-1:
            cursor += main if index == count//2-1 else ordinary
    return values, cursor


def build_candidate(name, *, source=BASE, rack_count=None):
    if name not in CANDIDATES:
        raise ValueError('unknown independently versioned candidate')
    base_raw = Path(source).read_bytes()
    scene = json.loads(base_raw)
    shape = CANDIDATES[name]
    count = shape['rack_count'] if rack_count is None else rack_count
    if type(count) is not int or not 36 <= count <= shape['columns']*shape['rows']:
        raise ValueError('rack_count must preserve R01..R36 and fit the candidate grid')
    width, depth = shape['width'], shape['depth']
    spec = scene['spec']
    spec.update(version=f'warehouse100-expanded-{name}-v1', width=width, depth=depth,
                ordinaryAisleWidth=5., mainAisleWidth=8.)
    if count != shape['rack_count']:
        spec['version'] += f'-r{count}'
    offsets_x, span_x = _axis_positions(shape['columns'], spec['rackWidth'])
    offsets_y, span_y = _axis_positions(shape['rows'], spec['rackDepth'])
    left, top = (width-span_x)/2, (depth-span_y)/2
    if left < 16 or top < 14:
        raise ValueError('candidate does not retain boundary circulation space')
    racks = []
    # Column-major IDs preserve every existing R01..R36 and its slot IDs.
    # The map version/hash and coordinates change explicitly.
    for column, dx in enumerate(offsets_x):
        for row, dy in enumerate(offsets_y):
            if len(racks) == count:
                break
            rid = f'R{len(racks)+1:02d}'
            zone = ('W' if column < shape['columns']//2 else 'E') + ('N' if row < shape['rows']//2 else 'S')
            racks.append({'id': rid, 'x': round(left+dx, 6), 'y': round(top+dy, 6),
                          'w': spec['rackWidth'], 'd': spec['rackDepth'], 'kind': 'rack',
                          'capacity': spec['slotsPerRack'], 'zone': zone, 'column': column, 'row': row})
    scene['racks'] = racks
    scene['capacity'] = len(racks)*spec['slotsPerRack']
    docks, homes, groups = [], [], []
    bank_width, bank_count, per_bank = 15., 5, 10
    bank_gap = (width-3-bank_count*bank_width)/(bank_count-1)
    for group, side in [('A', 'north'), ('B', 'south')]:
        y, gate_y = (1.5, 6.) if side == 'north' else (depth-3.3, depth-6.)
        for bank in range(bank_count):
            start_x = 1.5+bank*(bank_width+bank_gap)
            groups.append({'id': f'{group}-{bank+1}', 'x': start_x, 'y': y, 'w': bank_width,
                           'd': 1.8, 'kind': 'charging-bank', 'side': side, 'count': per_bank,
                           'staticCollision': False})
            for index in range(per_bank):
                local = bank*per_bank+index+1
                dock = {'id': f'C-{group}{local:02d}', 'x': start_x+index*1.5, 'y': y,
                        'w': 1.5, 'd': 1.8, 'kind': 'dock', 'group': group, 'side': side,
                        'point': {'x': start_x+index*1.5+.75, 'y': round(y+.9, 6)},
                        'gate': {'x': start_x+index*1.5+.75, 'y': gate_y},
                        'staticCollision': False, 'robotCollisionWhenOccupied': True, 'capacity': 1}
                docks.append(dock)
                home = deepcopy(dock)
                home.update(id=f'P{len(homes)+1:03d}', alias_of=dock['id'], kind='home')
                homes.append(home)
    scene.update(docks=docks, homes=homes, groups=groups)
    scene['stations'] = [
        {'id': f'{prefix}-{index+1}', 'kind': kind, 'x': x,
         'y': round(17+index*(depth-34)/7, 6)}
        for index in range(8)
        for prefix, kind, x in [('IN', 'inbound', 7.6), ('OUT', 'outbound', width-7.6)]
    ]
    scene['buffers'] = [
        {'id': 'IB', 'x': 1.5, 'y': 14., 'w': 4., 'd': depth-28, 'kind': 'buffer'},
        {'id': 'OB', 'x': width-5.5, 'y': 14., 'w': 4., 'd': depth-28, 'kind': 'buffer'},
    ]
    scene['zones'] = [
        {'id': 'inbound', 'x': .5, 'y': 13., 'w': 11.2, 'd': depth-26, 'kind': 'zone', 'name': '收货 / 8点 / 暂存48件'},
        {'id': 'outbound', 'x': width-11.7, 'y': 13., 'w': 11.2, 'd': depth-26, 'kind': 'zone', 'name': '发货 / 8点 / 边界确认离仓'},
        {'id': 'handover', 'x': 2.5, 'y': depth-10., 'w': 7., 'd': 5., 'kind': 'zone', 'name': '交接 / 维护预留'},
    ]
    middle_y = top+offsets_y[shape['rows']//2]-8.
    middle_x = left+offsets_x[shape['columns']//2]-8.
    scene['trunks'] = [
        {'id': key, 'x': left-8, 'y': y, 'w': span_x+16, 'd': 8., 'kind': 'trunk'}
        for key, y in [('north', top-8), ('middle', middle_y), ('south', top+span_y)]
    ] + [
        {'id': key, 'x': x, 'y': top-8, 'w': 8., 'd': span_y+16, 'kind': 'trunk'}
        for key, x in [('west', left-8), ('spine', middle_x), ('east', left+span_x)]
    ]
    scene['obstacles'] = deepcopy(racks+scene['buffers'])
    scene.update(status='candidate-geometric-validation-only', designOnly=True, notBackendCompatible=False)
    scene['migration'] = {'source_scene_sha256': sha_bytes(base_raw),
        'preserved_rack_ids': [f'R{i:02d}' for i in range(1, 37)],
        'old_task_location_ids_preserved': True, 'coordinates_changed': True,
        'capacity_changed': True, 'task_schema_unchanged': True,
        'requires': ['new dataset manifest scene hash', 'new location coordinate table',
                     'same-load deadline/backlog/charging and fleet-capability regression'],
        'accepted_for_production': False}
    scene['candidate'] = {'name': name, 'layout_only': True, 'base_source_sha256': sha_bytes(base_raw),
                          'rack_count': len(racks), 'throughput_accepted': False,
                          'preserves_physical_speed_and_safety': True}
    return scene


def location_rows(scene, slots):
    rows = [{'location_id': s['slot_id'], 'kind': 'slot', 'rack_id': s['rack_id'],
             'level': s['level'], 'face': s['face']+1, 'bay': s['bay'], 'x': s['x'], 'y': s['y']}
            for s in slots.values()]
    rows += [{'location_id': p['id'], 'kind': p['kind'], 'rack_id': '', 'level': '',
              'face': '', 'bay': '', 'x': p['x'], 'y': p['y']} for p in scene['stations']]
    return rows


def write_candidate(name, output, *, source=BASE, audit=False, rack_count=None):
    """Create a NEW candidate directory. Never overwrite any map or dataset."""
    output = Path(output)
    if output.exists():
        raise FileExistsError('candidate output already exists; preserve its evidence')
    scene = build_candidate(name, source=source, rack_count=rack_count)
    output.mkdir(parents=True)
    raw = (json.dumps(scene, ensure_ascii=False, indent=2)+'\n').encode()
    scene_path = output/'layout.json'
    scene_path.write_bytes(raw)
    normalized, slots = load_scene(scene_path)
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=LOCATION_FIELDS, lineterminator='\n')
    writer.writeheader(); writer.writerows(location_rows(normalized, slots))
    location_raw = stream.getvalue().encode()
    (output/'task-locations.csv').write_bytes(location_raw)
    report = None
    if audit:
        from .motion import WideNavigator, WideRoadmap
        from ..scheduling_v3.layout import audit_roadmap
        report = audit_roadmap(WideRoadmap(WideNavigator(normalized, slots)), circle_probes=False)
        (output/'geometry-audit.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
    manifest = {'version': 'warehouse-layout-candidate-v1', 'name': name,
                'scene_version': scene['spec']['version'], 'source_scene_sha256': sha_bytes(Path(source).read_bytes()),
                'scene_sha256': sha_bytes(raw), 'files': {'layout.json': sha_bytes(raw), 'task-locations.csv': sha_bytes(location_raw)},
                'width': scene['spec']['width'], 'depth': scene['spec']['depth'], 'racks': len(scene['racks']),
                'capacity': len(slots), 'task_locations': len(slots)+16, 'physical_bays': 100,
                'geometric_audit_passed': report['initial_roadmap_usable'] if report else None,
                'throughput_accepted': False, 'default_layout_changed': False}
    (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', choices=CANDIDATES, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=BASE)
    parser.add_argument('--rack-count', type=int, help='explicit 36..grid capacity; keeps default coordinates')
    parser.add_argument('--audit', action='store_true')
    args = parser.parse_args(argv)
    print(json.dumps(write_candidate(args.candidate, args.output, source=args.source,
                                    audit=args.audit, rack_count=args.rack_count), indent=2))


if __name__ == '__main__':
    main()
