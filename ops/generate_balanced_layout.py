"""Register one conservative layout in this process, without editing core.

100x84m is rejected by the existing circulation guard. Ten rack rows occupy
60m, so 88m is the mathematical minimum depth with two 14m margins. The final
100x90m candidate leaves one extra metre per side; no guard is lowered and no
floating-point boundary adjustment is used.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from backend.wide import layout_candidates as factory

NOMINAL_DEPTH = 90.
SHAPE = {'width': 100., 'depth': NOMINAL_DEPTH, 'columns': 4, 'rows': 10, 'rack_count': 40}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@contextmanager
def registered(shape):
    previous = factory.CANDIDATES.get('balanced')
    existed = 'balanced' in factory.CANDIDATES
    factory.CANDIDATES['balanced'] = dict(shape)
    try:
        yield
    finally:
        if existed:
            factory.CANDIDATES['balanced'] = previous
        else:
            factory.CANDIDATES.pop('balanced', None)


def boundary_evidence():
    source = json.loads(factory.BASE.read_text(encoding='utf-8'))
    _, span_x = factory._axis_positions(4, source['spec']['rackWidth'])
    _, span_y = factory._axis_positions(10, source['spec']['rackDepth'])
    records = []
    for depth in (84., 88., NOMINAL_DEPTH):
        with registered({**SHAPE, 'depth': depth}):
            try:
                factory.build_candidate('balanced')
                accepted, error = True, None
            except ValueError as exc:
                accepted, error = False, str(exc)
        records.append({'requested_depth_m': depth, 'rack_span_y_m': span_y,
                        'actual_factory_top_margin_m': (depth-span_y)/2,
                        'accepted_by_existing_factory': accepted, 'error': error})
    return {'nominal_dimensions_m': [100., NOMINAL_DEPTH], 'rows': 10, 'columns': 4,
            'rack_span_exact_formula': '10*1.2 + 8*5 + 1*8 = 60m',
            'minimum_nominal_depth_formula': '60 + 2*14 = 88m',
            'factory_accumulated_span_x_m': span_x, 'factory_accumulated_span_y_m': span_y,
            'minimum_top_margin_unchanged_m': 14., 'configuration_depth_m': NOMINAL_DEPTH,
            'explicit_extra_total_depth_over_mathematical_minimum_m': 2.,
            'extra_margin_per_side_over_mathematical_minimum_m': 1.,
            'floating_representation_adjustment_used': False,
            'core_thresholds_changed': False, 'attempts': records,
            'interpretation': '84m violates this generator circulation contract; this is not a proof every possible 84m design is physically impossible'}


def generate(output):
    output = Path(output)
    core_file = Path(factory.__file__)
    before = {'core_factory_sha256': sha(core_file), 'base_scene_sha256': sha(factory.BASE)}
    evidence = boundary_evidence()
    with registered(SHAPE):
        manifest = factory.write_candidate('balanced', output, audit=True)
    after = {'core_factory_sha256': sha(core_file), 'base_scene_sha256': sha(factory.BASE)}
    if before != after:
        raise AssertionError('frozen source identity changed')
    evidence.update(source_identity=before, source_files_unchanged=True,
                    scene_sha256=manifest['scene_sha256'],
                    geometric_audit_passed=manifest['geometric_audit_passed'],
                    throughput_accepted=False, default_layout_changed=False)
    (output/'boundary-preflight.json').write_text(json.dumps(evidence, indent=2)+'\n', encoding='utf-8')
    return {'manifest': manifest, 'boundary_preflight': evidence}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = generate(args.output)
    print(json.dumps(result, indent=2))
    return 0 if result['manifest']['geometric_audit_passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
