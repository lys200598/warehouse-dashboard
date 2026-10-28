"""Deterministic dataset contract and independent result oracles."""
import hashlib
import json
import math
import re

VERSION = 'warehouse-scale-v2'
DIMENSIONS = 32
VECTOR_INDEX = 'whscale_archive_vector_v2'
STAGES = ('unknown','idle','mission_queue','to_source','picking','to_target','placing',
          'returning','charge_queue','to_charge','charging','to_handoff','handoff',
          'handoff_receive','handoff_give','yielding','recovery_wait','faulted')


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,100}', value):
        raise ValueError('run identity must be 1..101 ASCII identifier characters')
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def source_sequence(seq, steps, available):
    """Evenly resample observed commits, retaining explicit source sequence.

    Repeated source states are deliberate controlled scale augmentation.  The
    archive sequence is NOT presented as additional real simulation commits.
    """
    if (type(seq) is not int or type(steps) is not int or not 1 <= seq <= steps or steps < 2
            or not available or any(type(s) is not int or s<1 for s in available)
            or available!=sorted(set(available))):
        raise ValueError('invalid archive sequence contract')
    return available[(seq - 1) * (len(available) - 1) // (steps - 1)]


def feature_vector(payload, seq, robot):
    """32-dimensional *structured* feature, not a text/image semantic embedding.

    First 8 dimensions are bounded physical/categorical features.  Remaining
    dimensions are deterministic low-amplitude identity/time features to expose
    approximate-index self-retrieval quality even for identical idle snapshots.
    """
    state = json.loads(payload) if isinstance(payload, str) else payload
    stage = str(state.get('stage', 'unknown'))
    base = [float(state.get('x', 0)) / 100, float(state.get('y', 0)) / 100,
            float(state.get('soc', state.get('battery', 0))) / 100,
            float(bool(state.get('carrying', False))), robot / 100,
            STAGES.index(stage) / (len(STAGES)-1) if stage in STAGES else 0,
            math.sin(seq / 101), math.cos(seq / 101)]
    salt = hashlib.sha256(f'{seq}:{robot}'.encode()).digest()
    base.extend(((salt[i] / 255) * 2 - 1) * .2 for i in range(24))
    norm = math.sqrt(sum(x * x for x in base))
    return [v / norm for v in base]


def cosine(left, right):
    norm = math.sqrt(sum(x*x for x in left) * sum(x*x for x in right))
    return sum(a*b for a, b in zip(left, right)) / norm


def archive_state(run, seq, robot, source_run, source_seq, source_payload):
    state = json.loads(source_payload)
    if state.get('robot_id') != robot:
        raise ValueError('source robot identity mismatch')
    return {'key':f'{run}:{seq}:{robot}', 'run_id':run, 'seq':seq, 'tick':seq,
            'robot_id':robot, 'robot_key':f'{run}:{robot}', 'step_key':f'{run}:{seq}',
            'source_key':f'{source_run}:{source_seq}:{robot}', 'source_seq':source_seq,
            'source_run':source_run, 'payload':source_payload, 'source_payload_sha256':fingerprint(state),
            'embedding':feature_vector(source_payload, seq, robot),
            'scale_kind':'synthetic_resampled_history; not additional execution'}


def validate_history(rows, run, robot, seq, source_run, available, steps, templates):
    expected = list(range(seq, max(0, seq-5), -1))
    if [r['seq'] for r in rows] != expected:
        raise ValueError('history cutoff/order/completeness mismatch')
    for row in rows:
        source = source_sequence(row['seq'], steps, available)
        original = json.loads(templates[(source, robot)])
        if (row['key'] != f'{run}:{row["seq"]}:{robot}' or row['robot_id'] != robot
                or row['source_key'] != f'{source_run}:{source}:{robot}'
                or json.loads(row['payload']) != original
                or row['source_payload_sha256'] != fingerprint(original)):
            raise ValueError('history source provenance/payload mismatch')
    return True


def validate_vectors(rows, vector, run, seq, source_run=None, available=None, steps=None, templates=None):
    if not rows:
        raise ValueError('time-filtered approximate vector query returned no candidates')
    for row in rows:
        if (row['run_id'] != run or row['seq'] > seq or row['seq']<1
                or len(row['embedding']) != DIMENSIONS
                or not math.isfinite(float(row['score']))
                or not all(math.isfinite(v) for v in row['embedding'])):
            raise ValueError('wrong-run/future/invalid vector result')
        if templates is not None:
            robot=row['robot_id']
            if type(robot) is not int or not 1<=robot<=100 or row['key']!=f'{run}:{row["seq"]}:{robot}':
                raise ValueError('vector candidate identity mismatch')
            source=source_sequence(row['seq'],steps,available)
            expected=feature_vector(templates[(source,robot)],row['seq'],robot)
            if any(abs(a-b)>1e-10 for a,b in zip(expected,row['embedding'])):
                raise ValueError('vector candidate differs from independent source oracle')
        # Neo4j cosine score = (1 + cosine_similarity) / 2.
        expected_score = (1 + cosine(vector, row['embedding'])) / 2
        if abs(float(row['score']) - expected_score) > 2e-4:
            raise ValueError('vector score disagrees with independent cosine oracle')
    return True


def validate_business(result, run, seq, expected_robot, expected_task):
    if (result['run_id']!=run or result['seq']!=seq or result['robot']!=expected_robot
            or result['task']!=expected_task):
        raise ValueError('real business robot/task differs from fixed source oracle')
    return True
