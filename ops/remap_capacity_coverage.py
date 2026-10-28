"""Offline capacity-coverage experiment; never a byte-identical CSV control.

Task identities, types, release/due times, counts and initial inventory count
are retained. Only storage endpoint references change through one injective
mapping. No simulation completion times, API calls or database reads are used.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import csv
import hashlib
import io
import json
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from backend.day1.data import FIELDS, sha
from backend.wide.data import load_scene, validate_dataset

VERSION = 'warehouse-capacity-coverage-remap-v1'
INVENTORY_FIELDS = ['goods_id', 'location_id']


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()


def csv_rows(path):
    with Path(path).open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))


def csv_bytes(rows, fields):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
    writer.writeheader(); writer.writerows(rows)
    return stream.getvalue().encode()


def mapping_for_coverage(old_slots, new_slots, task_slot_ids, referenced_ids, seed):
    """Task-bearing slots first: inventory-only coverage cannot mask weak load.

    All map racks have uniform level/face capabilities under load_scene. A
    least-used-rack first choice therefore covers min(referenced slots,racks)
    distinct racks whenever the required level/face signatures are available.
    Column preservation breaks coverage ties and is preferred, not mandatory.
    """
    if type(seed) is not int:
        raise ValueError('seed must be an integer, not bool')
    rng = random.Random(seed)
    task_order = sorted(task_slot_ids); rng.shuffle(task_order)
    inventory_only = sorted(set(referenced_ids)-set(task_slot_ids)); rng.shuffle(inventory_only)
    destination_ids = sorted(new_slots); rng.shuffle(destination_ids)
    rank = {key: i for i, key in enumerate(destination_ids)}
    available = defaultdict(list)
    for key, slot in new_slots.items():
        available[slot['level'], slot['face']].append(key)
    counts = Counter((old_slots[key]['level'], old_slots[key]['face']) for key in referenced_ids)
    if any(count > len(available[signature]) for signature, count in counts.items()):
        raise ValueError('target lacks injective storage capacity for a required level/face')
    used_racks, result = Counter(), {}
    for source in task_order+inventory_only:
        slot = old_slots[source]
        options = available[slot['level'], slot['face']]
        target = min(options, key=lambda key: (
            used_racks[new_slots[key]['rack_id']],
            new_slots[key]['bay'] != slot['bay'], rank[key]))
        result[source] = target
        used_racks[new_slots[target]['rack_id']] += 1
        options.remove(target)
    if len(set(result.values())) != len(result):
        raise AssertionError('internal mapping is not injective')
    return result


def remap(old_scene, dataset, new_scene, output, *, seed=20261028):
    old_scene, dataset, new_scene, output = map(Path, (old_scene, dataset, new_scene, output))
    if output.exists():
        raise FileExistsError('output exists; preserve previous evidence and choose a new directory')
    if output.resolve().is_relative_to(dataset.resolve()):
        raise ValueError('output cannot be inside the immutable source dataset')
    source_paths = {'old_scene': old_scene, 'new_scene': new_scene,
                    **{name: dataset/name for name in ('tasks.csv', 'initial_inventory.csv', 'manifest.json')}}
    source_hashes = {name: sha(path) for name, path in source_paths.items()}
    source_validation = validate_dataset(old_scene, dataset)
    _, old_slots = load_scene(old_scene)
    target_scene, new_slots = load_scene(new_scene)
    original_tasks = csv_rows(dataset/'tasks.csv')
    original_inventory = csv_rows(dataset/'initial_inventory.csv')
    task_slot_ids = {row[field] for row in original_tasks for field in ('source_id', 'target_id') if row[field] in old_slots}
    referenced = task_slot_ids | {row['location_id'] for row in original_inventory}
    mapping = mapping_for_coverage(old_slots, new_slots, task_slot_ids, referenced, seed)
    tasks, inventory = deepcopy(original_tasks), deepcopy(original_inventory)
    changed = Counter()
    for row in tasks:
        for field in ('source_id', 'target_id'):
            old = row[field]
            row[field] = mapping.get(old, old)
            changed[field] += row[field] != old
    for row in inventory:
        old = row['location_id']; row['location_id'] = mapping[old]
        changed['location_id'] += row['location_id'] != old
    # Independent immutable-field check; mapping cannot change counts/timing.
    for before, after in zip(original_tasks, tasks):
        if any(before[k] != after[k] for k in FIELDS if k not in ('source_id', 'target_id')):
            raise AssertionError('task identity/type/timestamp changed')
    if len(tasks) != len(original_tasks) or len(inventory) != len(original_inventory):
        raise AssertionError('task or inventory row count changed')
    if any(a['goods_id'] != b['goods_id'] for a, b in zip(original_inventory, inventory)):
        raise AssertionError('initial goods IDs changed')
    for source, target in mapping.items():
        if any(old_slots[source][key] != new_slots[target][key] for key in ('level', 'face')):
            raise AssertionError('required robot capability signature changed')
    all_racks = {s['rack_id'] for s in new_slots.values()}
    task_racks = {new_slots[mapping[key]]['rack_id'] for key in task_slot_ids}
    initial_racks = {new_slots[row['location_id']]['rack_id'] for row in inventory}
    coverage = {'target_rack_count': len(all_racks), 'referenced_source_slots': len(referenced),
                'task_source_slots': len(task_slot_ids), 'task_target_racks': sorted(task_racks),
                'task_rack_count': len(task_racks), 'task_rack_coverage_complete': task_racks == all_racks,
                'maximum_task_racks_possible': min(len(task_slot_ids), len(all_racks)),
                'initial_rack_count': len(initial_racks), 'all_referenced_rack_count': len(task_racks | initial_racks),
                'all_referenced_rack_coverage_complete': task_racks | initial_racks == all_racks,
                'uncovered_task_racks': sorted(all_racks-task_racks)}
    map_payload = {'version': VERSION, 'seed': seed, 'source_sha256': source_hashes,
                   'mapping_scope': 'every storage ID appearing in tasks or initial inventory; tasks assigned first',
                   'mapping': dict(sorted(mapping.items())),
                   'signature_preserved': ['level', 'face'],
                   'column_preserved_count': sum(old_slots[a]['bay'] == new_slots[b]['bay'] for a, b in mapping.items()),
                   'injective': True, 'coverage': coverage}
    map_raw = canonical(map_payload)
    output.mkdir(parents=True, exist_ok=False)
    (output/'slot-map.json').write_bytes(map_raw)
    (output/'tasks.csv').write_bytes(csv_bytes(tasks, FIELDS))
    (output/'initial_inventory.csv').write_bytes(csv_bytes(inventory, INVENTORY_FIELDS))
    manifest = json.loads((dataset/'manifest.json').read_text(encoding='utf-8'))
    spatial_metadata = {key: deepcopy(manifest[key]) for key in ('active_slot_ids', 'hotspot_slot_ids', 'hotspot_centre') if key in manifest}
    manifest.update(version=VERSION, scene_sha256=source_hashes['new_scene'],
                    scene_version=target_scene['spec']['version'], capacity=len(new_slots),
                    active_slots=len(task_slot_ids), active_slot_ids=sorted(mapping[k] for k in task_slot_ids),
                    files={name: sha(output/name) for name in ('tasks.csv', 'initial_inventory.csv')})
    if 'hotspot_centre' in manifest:
        manifest['hotspot_centre'] = None
    if 'hotspot_slot_ids' in manifest:
        manifest['hotspot_slot_ids'] = sorted(mapping[k] for k in spatial_metadata.get('hotspot_slot_ids', []) if k in mapping)
    manifest['coverage_remap'] = {
        'version': VERSION, 'seed': seed, 'source_sha256': source_hashes,
        'mapping_file': 'slot-map.json', 'mapping_sha256': hashlib.sha256(map_raw).hexdigest(),
        'changed_fields': ['source_id', 'target_id', 'location_id'],
        'changed_value_counts': dict(changed), 'deadline_changes': 0,
        'preserved_task_fields': [k for k in FIELDS if k not in ('source_id', 'target_id')],
        'task_count_unchanged': True, 'initial_inventory_count_unchanged': True,
        'initial_goods_ids_unchanged': True, 'csv_byte_identical_control': False,
        'experiment_role': 'separate coverage workload, not original-CSV same-input control',
        'uses_measured_completion_times': False, 'spatial_reclassification_required': True,
        'source_spatial_metadata': spatial_metadata, 'coverage': coverage}
    (output/'manifest.json').write_bytes(canonical(manifest))
    validation = validate_dataset(new_scene, output)
    (output/'validation.json').write_bytes(canonical(validation))
    if any(sha(path) != source_hashes[name] for name, path in source_paths.items()):
        raise AssertionError('original scene/dataset changed during remap')
    report = {'version': VERSION, 'valid': validation['valid'], 'source_validation': source_validation,
              'source_sha256': source_hashes, 'target_sha256': {
                  'scene': sha(new_scene), **{n: sha(output/n) for n in ('tasks.csv', 'initial_inventory.csv', 'manifest.json', 'slot-map.json')}},
              'seed': seed, 'mapping_count': len(mapping), 'coverage': coverage,
              'deadline_changes': 0, 'source_files_unchanged': True,
              'changed_fields': ['source_id', 'target_id', 'location_id'],
              'csv_byte_identical_control': False, 'runtime_throughput_claim': False}
    (output/'coverage-remap-report.json').write_bytes(canonical(report))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-scene', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--new-scene', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seed', type=int, default=20261028)
    args = parser.parse_args(argv)
    print(json.dumps(remap(args.old_scene, args.dataset, args.new_scene, args.output, seed=args.seed), indent=2))


if __name__ == '__main__':
    main()
