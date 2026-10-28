"""Real committed warehouse history: no archive resampling or invented frames."""
import math

from ..knowledge import digest
from ..scale_benchmark.model import feature_vector

VERSION = 'warehouse-real-scale-v1'
DIMENSIONS = 32
VECTOR_INDEX = 'whreal_state_vector_v1'
VECTOR_PROPERTY = 'real_embedding'
SOURCE_KIND = 'actual_runtime_committed_state'
VECTOR_KIND = '32d_structured_physical_time_identity_features_not_semantic'


def positive_integer(value, name, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or maximum is not None and value > maximum:
        raise ValueError('invalid ' + name)
    return value


def validate_state(state, robot=None):
    if not isinstance(state, dict):
        raise ValueError('robot state must be an object')
    owner = positive_integer(state.get('robot_id'), 'robot ID', 100)
    if robot is not None and owner != robot:
        raise ValueError('robot source identity mismatch')
    for name in ('x', 'y', 'battery'):
        value = state.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError('finite physical state required')
    if not 0 <= state['battery'] <= 100:
        raise ValueError('battery outside [0,100]')
    if type(state.get('carrying')) is not bool or not isinstance(state.get('stage'), str) or not state['stage']:
        raise ValueError('typed carrying/stage state required')
    for name in ('task_id', 'cargo_id'):
        value = state.get(name)
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError('typed task/cargo identity required')
    if state['carrying'] and not state.get('cargo_id'):
        raise ValueError('carrying state requires its real cargo ID')
    digest(state)  # All nested fields must also have valid finite JSON values.
    return owner


def real_vector(state, seq, robot=None):
    positive_integer(seq, 'sequence')
    owner = validate_state(state, robot)
    value = feature_vector(state, seq, owner)
    if len(value) != DIMENSIONS or not all(math.isfinite(v) for v in value):
        raise ValueError('invalid structured embedding')
    return value
