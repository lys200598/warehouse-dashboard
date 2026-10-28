"""Pure immutable records; no file/network IO or driver import.

Resource relations follow lys200598's Warehouse/Rack/Location design. IDs are
content-addressed and business IDs are separate properties; later ** unpacking
can never overwrite the qualified ID. A dataset also binds the actual plan and
initial stock, so different seeds on one date cannot overwrite one another.
"""
import math

from ..knowledge import digest
from ..boundary_contract import declared as boundary_declared, validate_task as boundary_task, location_registry

VERSION = 'warehouse-resource-graph-v1'


def ident(value, name='identity'):
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 512:
        raise ValueError('invalid resource ' + name)
    return value


def integer(value, name, low=0):
    if type(value) is not int or value < low:
        raise ValueError('invalid resource ' + name)
    return value


def scalar_properties(value):
    """Only Neo4j scalar properties, never maps or nested lists."""
    result = {}
    for key, item in value.items():
        if item is None or type(item) in (str, bool, int):
            result[key] = item
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError('nonfinite resource property')
            result[key] = item
    return result


def descriptor(metadata):
    """Identity of immutable layout/input declarations, independent of run ID."""
    scene = metadata.get('scene')
    if not isinstance(scene, dict):
        raise ValueError('resource graph needs a committed scene')
    values = {'scene_content_sha256': digest(scene)}
    for key in ('scene_sha256', 'tasks_sha256', 'initial_inventory_sha256', 'manifest_sha256'):
        value = metadata.get(key)
        if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('missing or invalid resource ' + key)
        values[key] = value
    return values


def build_package(batch):
    """Construct static resources from the full first real commit only."""
    if batch.get('seq') != 1 or type(batch.get('seq')) is not int:
        raise ValueError('resource bootstrap requires source seq 1')
    run = ident(batch.get('run_id'), 'run')
    metadata = batch.get('metadata', {})
    source = descriptor(metadata)
    scene = metadata['scene']; spec = scene['spec']
    warehouse_id = 'warehouse:sha256:' + source['scene_content_sha256']
    warehouse = {**scalar_properties(spec), 'id': warehouse_id,
                 'layout_version': spec.get('version'),
                 'scene_content_sha256': source['scene_content_sha256']}
    rack_records = scene.get('racks', [])
    racks, rack_ids = [], set()
    levels = integer(spec.get('levels'), 'levels', 1)
    faces = integer(spec.get('faces'), 'faces', 1)
    bays = integer(spec.get('baysPerFace'), 'bays', 1)
    expected_slots = {}
    for rack in rack_records:
        rack_id = ident(rack.get('id'), 'rack')
        if rack_id in rack_ids:
            raise ValueError('duplicate resource rack')
        rack_ids.add(rack_id)
        capacity = levels * faces * bays
        if rack.get('capacity') != capacity:
            raise ValueError('resource rack capacity mismatch')
        # Set scoped ID LAST, preserving R01 as the separate business rack_id.
        qualified = f'{warehouse_id}:rack:{rack_id}'
        racks.append({**scalar_properties(rack), 'id': qualified, 'rack_id': rack_id,
                      'warehouse_id': warehouse_id})
        for level in range(1, levels + 1):
            for face in range(1, faces + 1):
                for bay in range(1, bays + 1):
                    location = f'{rack_id}-L{level}-C{bay}-F{face}'
                    expected_slots[location] = (qualified, rack_id, level, face, bay)
    if not racks or len(expected_slots) != scene.get('capacity'):
        raise ValueError('resource warehouse capacity mismatch')
    slots = batch.get('slots', [])
    actual = [slot.get('slot_id') for slot in slots]
    if len(set(actual)) != len(actual) or set(actual) != set(expected_slots):
        raise ValueError('resource bootstrap must contain every real storage slot exactly once')
    locations = []
    for slot in slots:
        location = slot['slot_id']; qualified, rack_id, level, face, bay = expected_slots[location]
        locations.append({'id': f'{warehouse_id}:location:{location}', 'location_id': location,
            'warehouse_id': warehouse_id, 'rack_node_id': qualified, 'rack_id': rack_id,
            'kind': 'slot', 'level': level, 'face': face, 'bay': bay,
            'x': slot['x'], 'y': slot['y'], 'coordinate_kind': 'robot_approach'})
    for kind, items in (('station', scene.get('stations', [])), ('charger', scene.get('docks', [])),
                        ('home-alias', scene.get('homes', []))):
        for item in items:
            location = ident(item.get('id'), 'location')
            point = item.get('point', item)
            locations.append({'id': f'{warehouse_id}:location:{location}', 'location_id': location,
                'warehouse_id': warehouse_id, 'kind': item.get('kind', kind),
                'alias_of': item.get('alias_of'), 'x': point['x'], 'y': point['y']})
    if boundary_declared(metadata):
        locations.extend({'id': f'{warehouse_id}:location:{name}', 'location_id': name,
            'warehouse_id': warehouse_id, 'kind': 'logical-boundary',
            'boundary_role': role, 'physical': False}
            for name, role in (('INBOUND', 'inbound'), ('OUTBOUND', 'outbound')))
    by_location = {row['location_id']: row for row in locations}
    if len(by_location) != len(locations):
        raise ValueError('duplicate resource location')
    plans = []
    boundary_registry = location_registry(scene) if boundary_declared(metadata) else None
    for task in batch.get('tasks', []):
        row = {key: task.get(key) for key in ('task_id', 'task_type', 'goods_id', 'source_id', 'target_id',
                'release_time', 'due_time', 'release_second', 'due_second')}
        row['source_id'], row['target_id'] = boundary_task(task, metadata, cutoff=batch.get('sim_time'),registry=boundary_registry)
        for key in ('task_id', 'goods_id', 'source_id', 'target_id'):
            ident(row[key], key)
        if row['source_id'] not in by_location or row['target_id'] not in by_location:
            raise ValueError('resource task has a foreign endpoint')
        if row['task_type'] not in ('INBOUND', 'OUTBOUND'):
            raise ValueError('unsupported resource task type')
        plans.append(row)
    plans.sort(key=lambda row: row['task_id'])
    if len({row['task_id'] for row in plans}) != len(plans):
        raise ValueError('duplicate resource tasks')
    initial = sorted(({'goods_id': e['goods_id'], 'location_id': e['location_id']}
        for e in batch.get('events', []) if e.get('kind') == 'initial_stock'),
        key=lambda row: row['goods_id'])
    if len({row['goods_id'] for row in initial}) != len(initial) or len({row['location_id'] for row in initial}) != len(initial):
        raise ValueError('duplicate initial resource goods or slots')
    if any(row['location_id'] not in expected_slots for row in initial):
        raise ValueError('initial resource stock is outside storage')
    # Journal intentionally omits tasks whose release time has not arrived.
    # The full immutable input is identified by declared file hashes, never
    # by the changing subset of published tasks materialized in this batch.
    input_identity = source
    dataset_id = 'dataset:sha256:' + digest(input_identity)
    dataset = {'id': dataset_id, **input_identity, 'initial_published_tasks': len(plans),
               'initial_inventory': len(initial), 'warehouse_id': warehouse_id,
               'task_materialization': 'published source facts only; full plan identity is tasks_sha256'}
    goods_ids = sorted({row['goods_id'] for row in plans} | {row['goods_id'] for row in initial})
    goods = [{'id': f'{dataset_id}:goods:{goods}', 'goods_id': goods, 'dataset_id': dataset_id} for goods in goods_ids]
    inventory = [{**row, 'goods_node_id': f"{dataset_id}:goods:{row['goods_id']}",
                  'location_node_id': by_location[row['location_id']]['id']} for row in initial]
    tasks = [{**row, 'id': f"{dataset_id}:task:{row['task_id']}", 'dataset_id': dataset_id,
        'goods_node_id': f"{dataset_id}:goods:{row['goods_id']}",
        'source_node_id': by_location[row['source_id']]['id'],
        'target_node_id': by_location[row['target_id']]['id']} for row in plans]
    return {'version': VERSION, 'run_id': run, 'warehouse': warehouse, 'dataset': dataset,
            'racks': racks, 'locations': locations, 'goods': goods, 'inventory': inventory, 'tasks': tasks,
            'descriptor_sha256': digest(source)}


def materialize_task_records(batch, warehouse_id, dataset_id):
    """Incrementally project only changed/published source task facts."""
    scene = batch['metadata']['scene']; spec = scene['spec']
    rack_ids = {rack['id'] for rack in scene['racks']}
    stations = {p['id'] for p in scene.get('stations', [])}
    if boundary_declared(batch['metadata']):
        stations |= {'INBOUND', 'OUTBOUND'}

    def endpoint(value):
        ident(value, 'task endpoint')
        if value in stations:
            return
        # Business rack IDs may themselves contain '-'; parse the suffix only.
        import re
        match = re.fullmatch(r'(.+)-L(\d+)-C(\d+)-F(\d+)', value)
        if not match or match[1] not in rack_ids:
            raise ValueError('resource task has a foreign endpoint')
        if not (1 <= int(match[2]) <= spec['levels'] and 1 <= int(match[3]) <= spec['baysPerFace']
                and 1 <= int(match[4]) <= spec['faces']):
            raise ValueError('resource task has an invalid storage endpoint')

    tasks, goods, seen = [], {}, set()
    boundary_registry = location_registry(scene) if boundary_declared(batch['metadata']) else None
    for task in batch.get('tasks', []):
        row = {key: task.get(key) for key in ('task_id', 'task_type', 'goods_id', 'source_id', 'target_id',
                'release_time', 'due_time', 'release_second', 'due_second')}
        row['source_id'], row['target_id'] = boundary_task(task, batch['metadata'], cutoff=batch.get('sim_time'),registry=boundary_registry)
        ident(row['task_id'], 'task'); ident(row['goods_id'], 'goods')
        if row['task_id'] in seen or row['task_type'] not in ('INBOUND', 'OUTBOUND'):
            raise ValueError('duplicate/invalid published resource task')
        seen.add(row['task_id']); endpoint(row['source_id']); endpoint(row['target_id'])
        goods_id = f"{dataset_id}:goods:{row['goods_id']}"
        goods[row['goods_id']] = {'id': goods_id, 'goods_id': row['goods_id'], 'dataset_id': dataset_id}
        tasks.append({**row, 'id': f"{dataset_id}:task:{row['task_id']}", 'dataset_id': dataset_id,
            'goods_node_id': goods_id, 'source_node_id': f"{warehouse_id}:location:{row['source_id']}",
            'target_node_id': f"{warehouse_id}:location:{row['target_id']}"})
    return {'tasks': tasks, 'goods': list(goods.values())}


def validate_forward_batches(batches):
    """Call before the parent write; return prepared bootstrap for reuse."""
    if not batches:
        return None
    run = ident(batches[0].get('run_id'), 'run')
    previous = integer(batches[0].get('seq'), 'seq', 1) - 1
    source = descriptor(batches[0].get('metadata', {}))
    for batch in batches:
        if batch.get('run_id') != run or integer(batch.get('seq'), 'seq', 1) != previous + 1:
            raise ValueError('mixed run or discontinuous resource batches')
        if descriptor(batch.get('metadata', {})) != source:
            raise ValueError('resource layout/input identity changed within run')
        if batch['seq'] != 1:
            materialize_task_records(batch,
                'warehouse:sha256:' + source['scene_content_sha256'],
                'dataset:sha256:' + digest(source))
        previous += 1
    return build_package(batches[0]) if batches[0]['seq'] == 1 else None
