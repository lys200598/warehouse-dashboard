"""Legacy A1/R2/X3 access capabilities for the opt-in 2-D mixed fleet.

The legacy ``extension`` is an EXTRA horizontal extension parameter, not an
arm's total length. Both rack faces already have their own approach endpoint.
Unless a slot/rack explicitly declares ``required_extension_m``, additional
horizontal reach is zero; this module makes no IK or contact-physics claim.
"""
from copy import deepcopy
import math

from ..domain import ROBOT_TYPES, robot_type_key


VERSION = 'warehouse-three-type-access-v1'
TYPES = ('A1', 'R2', 'X3')
LEGACY_COUNTS = {'A1': 40, 'R2': 40, 'X3': 20}
_PROFILES = deepcopy(ROBOT_TYPES)


def fleet_assignment(mode='all-x3', counts=None, robot_count=100):
    if mode not in ('all-x3', 'mixed'):
        raise ValueError('fleet_mode must be all-x3 or mixed')
    if type(robot_count) is not int or robot_count != 100:
        raise ValueError('the warehouse fleet requires 100 robots')
    if mode == 'all-x3':
        if counts is not None:
            raise ValueError('fleet_counts requires explicit mixed mode')
        return {i: 'X3' for i in range(1, 101)}
    counts = dict(LEGACY_COUNTS) if counts is None else counts
    if (not isinstance(counts, dict) or not counts or set(counts) - set(TYPES)
            or any(type(v) is not int or v < 0 for v in counts.values())
            or sum(counts.values()) != robot_count):
        raise ValueError('fleet_counts must contain known nonnegative integer counts summing to 100')
    weights = {key: counts.get(key, 0) for key in TYPES}
    if weights == LEGACY_COUNTS:
        return {i: robot_type_key(i) for i in range(1, 101)}
    # Smooth weighted round-robin spreads every type over both parking edges;
    # custom mixes do not inadvertently put all long-arm units in one corner.
    current, result = {key: 0 for key in TYPES}, {}
    for i in range(1, 101):
        for key in TYPES:
            current[key] += weights[key]
        selected = max(TYPES, key=lambda key: current[key])
        result[i] = selected
        current[selected] -= robot_count
    return result


def profile(key):
    if key not in TYPES:
        raise ValueError('unknown robot capability profile')
    return dict(_PROFILES[key])


def slot_requirement(slot, scene):
    level = slot.get('level')
    spec = scene['spec']
    levels, rack_height = spec.get('levels'), spec.get('rackHeight')
    if (type(levels) is not int or levels < 1 or isinstance(rack_height, bool)
            or not isinstance(rack_height, (int, float)) or not math.isfinite(rack_height) or rack_height <= 0):
        raise ValueError('finite rack height and positive integer levels required')
    if type(level) is not int or not 1 <= level <= levels:
        raise ValueError('slot capability level invalid')
    # Same shelf/box geometry as the original dashboard: 7 cm base plus half
    # the 21 cm box; rack layer spacing follows the authoritative scene.
    height = .07 + (level - 1) * rack_height / levels + .105
    rack = next((r for r in scene['racks'] if r['id'] == slot.get('rack_id')), {})
    extension = slot.get('required_extension_m', rack.get('required_extension_m', 0.))
    if isinstance(extension, bool) or not isinstance(extension, (int, float)) or not math.isfinite(extension) or extension < 0:
        raise ValueError('required extra extension must be finite and nonnegative')
    return {'level': level, 'height_m': height, 'extra_extension_m': extension}


def access_reason(robot, requirements):
    key = robot.get('robot_type_key')
    if key not in TYPES:
        raise ValueError('unknown robot capability profile')
    p = _PROFILES[key]
    for requirement in requirements:
        if requirement['level'] > p['maxLevel']:
            return 'capability_level'
        if requirement['height_m'] > p['maxHeight'] + 1e-9:
            return 'capability_height'
        if requirement['extra_extension_m'] > p['extension'] + 1e-9:
            return 'capability_extension'
    return None


def spare_cost(robot, requirements, seconds):
    """Soft preference only; never changes cardinality or hard feasibility."""
    if not requirements:
        return 0.
    return max(0, _PROFILES[robot['robot_type_key']]['maxLevel'] - max(r['level'] for r in requirements)) * seconds
