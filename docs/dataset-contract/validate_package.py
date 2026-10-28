"""Stdlib-only, read-only contract validator; never a runtime feasibility proof.

--write-manifest is the only write operation and must be explicitly requested.
Run it after the owner fills the pending final acceptance metadata.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parent
TASK_FIELDS = ['task_id', 'task_type', 'goods_id', 'release_time', 'due_time', 'source_id', 'target_id']
SKIP_MANIFEST = {'package-manifest.json', 'SHA256SUMS.txt', 'package-validation.json'}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def rows(path, fields, max_rows=None):
    with Path(path).open(encoding='utf-8', newline='') as stream:
        reader = csv.reader(stream)
        require(next(reader, None) == fields, f'{path}: exact CSV header mismatch')
        result = []
        for number, row in enumerate(reader, 2):
            if not row:
                continue
            require(len(row) == len(fields), f'{path}:{number}: incorrect field count')
            require(max_rows is None or len(result) < max_rows,
                    f'{path}: exceeds configured row resource budget')
            result.append(dict(zip(fields, row)))
    return result


def finite(value):
    require(not isinstance(value, bool) and isinstance(value, (float, int)) and math.isfinite(value),
            'geometry requires finite numeric coordinates')
    return value


def identifier(value):
    require(isinstance(value, str) and 1 <= len(value) <= 128 and value.strip() == value and
            not any(ord(c) < 32 for c in value), 'empty/whitespace/control identifier')


def timestamp(value, offset=None):
    require(isinstance(value, str) and len(value) >= 6 and value[-6] in {'+', '-'} and value[-3] == ':',
            'use an explicit numeric timezone offset, e.g. +08:00 (portable to target Python)')
    result = datetime.fromisoformat(value)
    require(result.tzinfo is not None and result.utcoffset() is not None, 'timestamp requires explicit timezone')
    require(result.microsecond == 0, 'timestamp requires integer-second precision')
    require(offset is None or result.utcoffset() == offset, 'timestamp offset differs from manifest start')
    return result


def scene_contract(root=ROOT):
    contract = read_json(root/'contract/layout-contract.json')
    path = root/contract['scene_file']
    require(sha(path) == contract['scene_sha256'], 'scene SHA differs from layout contract')
    scene = read_json(path)
    spec = scene['spec']
    require((spec['unit'], spec['width'], spec['depth']) == ('m', 90, 76), 'wrong wide scene extent/unit')
    require(spec['robotCount'] == spec['chargerCount'] == 100, 'scene must have 100 robots/canonical bays')
    require((spec['levels'], spec['faces'], spec['baysPerFace'], spec['slotPitch']) == (3, 2, 8, 1.2),
            'wrong storage geometry')
    for key in ['width', 'depth', 'slotPitch', 'robotRadius', 'robotBodyWidth', 'robotBodyDepth']:
        require(finite(spec[key]) > 0, 'nonpositive geometry')
    for item in scene['racks'] + scene['docks'] + scene['homes'] + scene.get('buffers', []):
        x, y, width, depth = (finite(item[k]) for k in ['x', 'y', 'w', 'd'])
        require(x >= 0 and y >= 0 and width > 0 and depth > 0 and
                x+width <= spec['width'] and y+depth <= spec['depth'], 'rectangle outside map')
    radius = spec['robotRadius']
    require(radius + 1e-7 >= math.hypot(spec['robotBodyWidth']/2, spec['robotBodyDepth']/2),
            'planning circle does not cover body')
    docks = {d['id']: d for d in scene['docks']}
    homes = {h['id']: h for h in scene['homes']}
    require(len(docks) == len(homes) == len(scene['docks']) == len(scene['homes']) == 100,
            'duplicate or missing physical resource IDs')
    require(not set(docks) & set(homes), 'canonical/home names overlap')
    seen = set()
    for key, home in homes.items():
        identifier(key)
        target = home['alias_of']
        require(target in docks and target not in seen, 'home alias is not a bijection to canonical bays')
        seen.add(target)
        dock = docks[target]
        require(home['point'] == dock['point'] and all(home[k] == dock[k] for k in ['x', 'y', 'w', 'd']),
                'home alias changes physical pose/rectangle')
    for key, dock in docks.items():
        identifier(key)
        require(dock['capacity'] == 1 and not isinstance(dock['capacity'], bool), 'physical bay capacity must be one')
        require(dock['staticCollision'] is False, 'empty dock hardware must not become a wall')
        for point in [dock['point'], dock['gate']]:
            require(0 <= finite(point['x']) <= spec['width'] and 0 <= finite(point['y']) <= spec['depth'],
                    'resource point out of map')
        x, y = dock['point']['x'], dock['point']['y']
        require(dock['x'] + radius <= x <= dock['x'] + dock['w'] - radius and
                dock['y'] + radius <= y <= dock['y'] + dock['d'] - radius, 'body does not fit dock')
    for index, dock in enumerate(scene['docks']):
        for other in scene['docks'][index+1:]:
            require(math.dist(tuple(dock['point'][k] for k in ['x', 'y']),
                              tuple(other['point'][k] for k in ['x', 'y'])) >= 2*radius-1e-7,
                    'initial physical bay bodies overlap')
    slots = {}
    require(len(scene['racks']) == 36, 'expected 36 racks')
    for rack in scene['racks']:
        require(rack['capacity'] == spec['levels']*spec['faces']*spec['baysPerFace'], 'rack capacity mismatch')
        pitch = finite(rack['w']) / spec['baysPerFace']
        require(abs(pitch-spec['slotPitch']) < 1e-7, 'rack pitch mismatch')
        for level in range(1, spec['levels']+1):
            for face in range(1, spec['faces']+1):
                for bay in range(1, spec['baysPerFace']+1):
                    key = f"{rack['id']}-L{level}-C{bay}-F{face}"
                    require(key not in slots, 'duplicate storage ID')
                    slots[key] = dict(location_id=key, kind='slot', rack_id=rack['id'], level=level, face=face,
                                      bay=bay, x=rack['x']+(bay-.5)*pitch,
                                      y=rack['y']-.7 if face == 1 else rack['y']+rack['d']+.7)
                    require(0 <= slots[key]['x'] <= spec['width'] and 0 <= slots[key]['y'] <= spec['depth'],
                            'storage approach point outside map')
    require(len(slots) == scene['capacity'] == 1728, 'wrong physical storage capacity')
    stations = {s['id']: s for s in scene['stations']}
    require(len(stations) == len(scene['stations']) == 16 and
            set(stations) == {f'{prefix}-{i}' for prefix in ['IN', 'OUT'] for i in range(1, 9)},
            'wrong boundary station IDs')
    for key, station in stations.items():
        require(station['kind'] == ('inbound' if key.startswith('IN-') else 'outbound'), 'station role mismatch')
        require(0 <= finite(station['x']) <= spec['width'] and 0 <= finite(station['y']) <= spec['depth'],
                'boundary point outside map')
    fields = ['location_id', 'kind', 'rack_id', 'level', 'face', 'bay', 'x', 'y']
    locations = rows(root/'layout/task-locations.csv', fields)
    by_id = {p['location_id']: p for p in locations}
    require(len(by_id) == len(locations) == 1744 and set(by_id) == set(slots)|set(stations),
            'task location dictionary mismatch')
    for key, expected in {**slots, **stations}.items():
        row = by_id[key]
        require(row['kind'] == expected['kind'] and all(abs(float(row[k])-expected[k]) < 1e-7 for k in ['x', 'y']),
                f'task coordinate mismatch: {key}')
        if key in slots:
            require(row['rack_id'] == expected['rack_id'] and
                    all(int(row[k]) == expected[k] for k in ['level', 'face', 'bay']),
                    f'task storage metadata mismatch: {key}')
    all_fields = ['location_id', 'kind', 'canonical_location_id', 'independent_capacity_units',
                  'task_endpoint_allowed', 'x', 'y', 'rack_id', 'level', 'face', 'bay']
    all_rows = rows(root/'layout/all-locations.csv', all_fields)
    all_ids = {p['location_id']: p for p in all_rows}
    require(len(all_ids) == len(all_rows) == 1944 and set(all_ids) == set(by_id)|set(docks)|set(homes),
            'complete location registry mismatch')
    for key, record in all_ids.items():
        if key in by_id:
            require(record['canonical_location_id'] == key and record['task_endpoint_allowed'] == 'true',
                    'task point registry semantics mismatch')
            require(record['kind'] == by_id[key]['kind'] and
                    int(record['independent_capacity_units']) == (1 if key in slots else 0) and
                    all(abs(float(record[k])-float(by_id[key][k])) < 1e-7 for k in ['x', 'y']),
                    'complete task registry geometry/capacity mismatch')
        else:
            canonical = key if key in docks else homes[key]['alias_of']
            require(record['canonical_location_id'] == canonical and record['task_endpoint_allowed'] == 'false',
                    'resource alias/canonical registry mismatch')
            require(int(record['independent_capacity_units']) == (1 if key in docks else 0),
                    'home alias counted as extra physical capacity')
            require(all(abs(float(record[k])-docks[canonical]['point'][k]) < 1e-7 for k in ['x', 'y']),
                    'resource registry pose mismatch')
    bay_fields = ['physical_bay_id', 'home_alias', 'group', 'side', 'capacity', 'x', 'y', 'w', 'd',
                  'point_x', 'point_y', 'gate_x', 'gate_y']
    bay_rows = rows(root/'layout/physical-bays.csv', bay_fields)
    require(len(bay_rows) == 100 and {r['physical_bay_id'] for r in bay_rows} == set(docks),
            'physical bay table identity mismatch')
    for row in bay_rows:
        dock = docks[row['physical_bay_id']]
        require(row['home_alias'] in homes and homes[row['home_alias']]['alias_of'] == dock['id'] and
                int(row['capacity']) == 1 and row['group'] == dock['group'] and row['side'] == dock['side'],
                'physical bay table alias/capacity mismatch')
        require(all(abs(float(row[k])-dock[k]) < 1e-7 for k in ['x', 'y', 'w', 'd']) and
                all(abs(float(row[f'{part}_{k}'])-dock[part][k]) < 1e-7
                    for part in ['point', 'gate'] for k in ['x', 'y']), 'physical bay table geometry mismatch')
    return scene, slots, stations, contract


def time_bucket_report(duration, bucket_seconds, counts):
    """Dense short reports stay compatible; long empty periods cost no arrays."""
    bucket_count = (duration+bucket_seconds-1)//bucket_seconds
    if bucket_count <= 4096:
        return [counts.get(index, 0) for index in range(bucket_count)]
    return {'format': 'sparse-v1', 'bucket_seconds': bucket_seconds,
            'bucket_count': bucket_count, 'zero_buckets_omitted': True,
            'nonzero': [{'bucket': index, 'tasks': tasks}
                        for index, tasks in sorted(counts.items())]}


def validate_dataset(directory, scene, slots, stations, scene_sha, *, max_tasks=None):
    directory = Path(directory)
    manifest = read_json(directory/'manifest.json')
    duration = manifest['duration_seconds']
    require(not isinstance(duration, bool) and isinstance(duration, int) and duration >= 1,
            'duration must be a positive integer number of seconds; no fixed calendar-duration cap')
    if max_tasks is not None:
        require(type(max_tasks) is int and max_tasks >= 1, 'max_tasks must be a positive row resource budget')
    start = timestamp(manifest['start'])
    require(manifest['scene_sha256'] == scene_sha, 'dataset scene SHA mismatch')
    require(manifest['capacity'] == len(slots), 'dataset capacity mismatch')
    require(set(manifest['files']) == {'tasks.csv', 'initial_inventory.csv'}, 'unexpected manifest input files')
    for filename, expected in manifest['files'].items():
        require(sha(directory/filename) == expected, f'dataset file SHA mismatch: {filename}')
    require(max_tasks is None or (type(manifest['tasks']) is int and manifest['tasks'] <= max_tasks),
            'dataset exceeds configured max_tasks resource budget')
    initial = rows(directory/'initial_inventory.csv', ['goods_id', 'location_id'], max_rows=len(slots))
    tasks = rows(directory/'tasks.csv', TASK_FIELDS, max_rows=max_tasks)
    expected_counts = {'tasks': len(tasks), 'inbound': sum(t['task_type'] == 'INBOUND' for t in tasks),
                       'outbound': sum(t['task_type'] == 'OUTBOUND' for t in tasks), 'initial_inventory': len(initial)}
    require(all(manifest[k] == v and isinstance(manifest[k], int) and not isinstance(manifest[k], bool)
                for k, v in expected_counts.items()),
            'dataset counts mismatch')
    stock, where, ever_seen = {}, {}, set()
    for row in initial:
        goods, slot = row['goods_id'], row['location_id']
        identifier(goods); identifier(slot)
        require(slot in slots and slot not in stock and goods not in ever_seen, 'invalid initial inventory')
        stock[slot], where[goods] = goods, slot
        ever_seen.add(goods)
    require(len(stock) <= len(slots), 'initial capacity exceeded')
    low = high = len(stock)
    seen = set()
    timed = []
    for task in tasks:
        for key in ['task_id', 'goods_id', 'source_id', 'target_id']:
            identifier(task[key])
        require(task['task_id'] not in seen, 'duplicate task ID')
        seen.add(task['task_id'])
        released = timestamp(task['release_time'], start.utcoffset())
        due = timestamp(task['due_time'], start.utcoffset())
        second = (released-start).total_seconds()
        require(0 <= second < duration and due > released, 'invalid release/due timestamp range')
        timed.append((int(second), task))
    daily = Counter()
    for second, task in sorted(timed, key=lambda pair: (pair[0], pair[1]['task_id'])):
        daily[second//86400] += 1
        goods, source, target = (task[k] for k in ['goods_id', 'source_id', 'target_id'])
        if task['task_type'] == 'INBOUND':
            require(source in stations and stations[source]['kind'] == 'inbound' and target in slots,
                    'invalid inbound endpoint roles')
            require(target not in stock and goods not in ever_seen, 'inbound reference dependency violation')
            stock[target], where[goods] = goods, target
            ever_seen.add(goods)
        elif task['task_type'] == 'OUTBOUND':
            require(target in stations and stations[target]['kind'] == 'outbound' and source in slots,
                    'invalid outbound endpoint roles')
            require(stock.get(source) == goods and where.get(goods) == source, 'outbound reference dependency violation')
            del stock[source]; del where[goods]
        else:
            raise ValueError('unknown task type')
        low, high = min(low, len(stock)), max(high, len(stock))
        require(len(stock) <= len(slots), 'reference capacity exceeded')
    return {'path': str(directory), **expected_counts, 'duration_seconds': duration,
            'daily_tasks': time_bucket_report(duration, 86400, daily),
            'planned_min_inventory': low, 'planned_max_inventory': high, 'planned_final_inventory': len(stock),
            'reference_valid': True, 'runtime_feasibility_claim': False}


def package_files(root):
    return sorted(p for p in root.rglob('*') if p.is_file() and p.name not in SKIP_MANIFEST and
                  '__pycache__' not in p.parts and p.suffix != '.pyc')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, help='add an external colleague dataset to the reference validation')
    parser.add_argument('--write-manifest', action='store_true')
    parser.add_argument('--max-tasks', type=int,
                        help='optional validated task-row resource budget, independent of duration')
    args = parser.parse_args()
    if args.write_manifest:
        files = {p.relative_to(ROOT).as_posix(): sha(p) for p in package_files(ROOT)}
        (ROOT/'package-manifest.json').write_text(json.dumps({'format': 'warehouse-handoff-sha256-v1', 'files': files},
                                                         indent=2)+'\n', encoding='utf-8')
        (ROOT/'SHA256SUMS.txt').write_text(''.join(f'{value}  {name}\n' for name, value in files.items()), encoding='utf-8')
    manifest = read_json(ROOT/'package-manifest.json')['files']
    require(set(manifest) == {p.relative_to(ROOT).as_posix() for p in package_files(ROOT)}, 'package file list changed')
    for name, expected in manifest.items():
        path = (ROOT/name).resolve()
        require(path.is_relative_to(ROOT.resolve()), 'unsafe package path')
        require(sha(path) == expected, f'package SHA mismatch: {name}')
    scene, slots, stations, layout = scene_contract()
    datasets = [ROOT/'sample-input'] + sorted(p.parent for p in (ROOT/'supplement-inputs').glob('*/manifest.json'))
    if args.dataset:
        datasets.append(args.dataset.resolve())
    reports = [validate_dataset(p, scene, slots, stations, layout['scene_sha256'], max_tasks=args.max_tasks)
               for p in datasets]
    acceptance = read_json(ROOT/'contract/acceptance-status.json')
    print(json.dumps({'package_valid': True, 'hashed_files': len(manifest), 'physical_bays': len(scene['docks']),
                      'storage_slots': len(slots), 'task_locations': 1744, 'all_named_locations': 1944,
                      'datasets': reports, 'acceptance_metadata_state': acceptance['release_state'],
                      'runtime_acceptance_checked': False,
                      'scope': 'hashes, schema/coordinates/IDs and reference inventory only; no path/dispatch/energy/KG proof'},
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
