"""Create a new provenance-tracked seven-column logical-boundary dataset."""
import argparse
from copy import deepcopy
import csv
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from backend.day1.data import FIELDS, sha
from backend.wide.data import validate_dataset, read_dataset
from backend.wide.free_ports import DECLARATION, VERSION


def convert(scene, source, output, *, inbound=True, outbound=True):
    source, output = Path(source).resolve(), Path(output).resolve()
    if not inbound and not outbound:
        raise ValueError('select at least one logical boundary role')
    if output.exists():
        raise FileExistsError('choose a new output; previous inputs remain immutable')
    if output.is_relative_to(source):
        raise ValueError('new dataset must not be inside the immutable source')
    validate_dataset(scene, source)
    manifest, tasks, _ = read_dataset(scene, source)
    changed = 0
    rows = []
    for task in tasks:
        row = {key: task[key] for key in FIELDS}
        field, marker = ('source_id', 'INBOUND') if task['task_type'] == 'INBOUND' else ('target_id', 'OUTBOUND')
        if (task['task_type'] == 'INBOUND' and inbound or task['task_type'] == 'OUTBOUND' and outbound):
            changed += row[field] != marker
            row[field] = marker
        rows.append(row)
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source/'initial_inventory.csv', output/'initial_inventory.csv')
    with (output/'tasks.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader(); writer.writerows(rows)
    result = deepcopy(manifest)
    result.update(version=VERSION, boundary_routing=dict(DECLARATION),
        files={name: sha(output/name) for name in ('tasks.csv', 'initial_inventory.csv')},
        boundary_conversion={'source_manifest_sha256':sha(source/'manifest.json'),
            'source_files':dict(manifest['files']), 'physical_station_capacity_changed':False,
            'converted_endpoints':changed, 'inbound':inbound, 'outbound':outbound,
            'preserved_fields':['task_id','task_type','goods_id','release_time','due_time','rack_endpoint'],
            'comparison_scope':'new routing scenario; not byte-identical fixed-port input'})
    (output/'manifest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n',encoding='utf-8')
    report = validate_dataset(scene, output)
    (output/'validation.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n',encoding='utf-8')
    return result, report


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scene',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--role',choices=('both','inbound','outbound'),default='both')
    args=parser.parse_args(argv)
    manifest,report=convert(args.scene,args.source,args.output,
        inbound=args.role!='outbound',outbound=args.role!='inbound')
    print(json.dumps({'manifest':manifest,'validation':report},ensure_ascii=False,indent=2))


if __name__=='__main__':main()
