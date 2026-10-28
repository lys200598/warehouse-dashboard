"""Validated all-or-nothing replacement of continuous-time fleet reservations.

The coordinator must serialize calls (as it does simulation steps). Validation
uses a private table; failed proposals do not erase old escapes or change even
the diagnostic counters of the live table. This is not a physical emergency
stop implementation and does not make unmodelled robots disappear.
"""
from copy import deepcopy
from dataclasses import dataclass, field
import math


EPSILON = 1e-6


@dataclass(frozen=True)
class TransactionResult:
    ok: bool
    reason: str
    owners: tuple = ()
    conflicts: tuple = ()
    details: dict = field(default_factory=dict)

    def __bool__(self):
        return self.ok


def _ordered(values):
    return tuple(sorted(values, key=lambda value: (type(value).__name__, repr(value))))


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _point(point):
    if not isinstance(point, dict) or not all(_finite(point.get(axis)) for axis in ('x', 'y')):
        raise ValueError('invalid_point')
    return {'x': point['x'], 'y': point['y']}


def _same_point(left, right):
    return math.hypot(left['x'] - right['x'], left['y'] - right['y']) <= EPSILON


def stationary_guard(point, start, end, phase='safety_hold'):
    """A real stationary body for an explicit finite planning interval.

    Callers must renew or replace the guard before its end. An idle or failed
    body is not presumed absent after this modelling horizon.
    """
    point = _point(point)
    if not _finite(start) or not _finite(end) or end <= start:
        raise ValueError('invalid_guard_interval')
    return {'a': dict(point), 'b': dict(point), 'start': start, 'end': end, 'phase': phase}


def _normalise(parts, now, expected):
    if not isinstance(parts, (list, tuple)) or not parts:
        raise ValueError('empty_plan_would_remove_body')
    result = deepcopy(list(parts))
    for part in result:
        if not isinstance(part, dict):
            raise ValueError('invalid_part')
        part['a'], part['b'] = _point(part.get('a')), _point(part.get('b'))
        if not _finite(part.get('start')) or not _finite(part.get('end')) or part['end'] <= part['start']:
            raise ValueError('invalid_part_interval')
        part.setdefault('phase', 'moving')
    if now is not None and abs(result[0]['start'] - now) > EPSILON:
        raise ValueError('plan_does_not_start_now')
    if expected is not None and not _same_point(result[0]['a'], _point(expected)):
        raise ValueError('start_position_mismatch')
    for previous, current in zip(result, result[1:]):
        if abs(previous['end'] - current['start']) > EPSILON:
            raise ValueError('time_discontinuity')
        if not _same_point(previous['b'], current['a']):
            raise ValueError('position_discontinuity')
    return result


def try_replace_many(table, proposals, *, now=None, current_positions=None,
                     stationary_guards=None, validate_only=False):
    """Validate and commit absolute-time plans without partial replacement.

    ``proposals`` is ``owner -> [parts]``. Optional ``stationary_guards`` maps
    owners not in proposals to either ``{point,start,end}``, a single complete
    part, or a list of complete parts. ``current_positions``, when supplied,
    must contain every changed owner; each first point must match the actual
    body. Setting ``now`` requires every replacement to start at that time.

    Collision checks include every unchanged reservation and every candidate
    pair and use MotionTable's continuous relative-motion kernel, not samples.
    This checks temporal geometry; static obstacles/kinematics, complete safe
    terminal tails and the guard horizon are the caller's planning contract.
    """
    if not isinstance(proposals, dict) or not isinstance(stationary_guards or {}, dict):
        return TransactionResult(False, 'invalid_owner_mapping')
    stationary_guards = stationary_guards or {}
    owners = _ordered(set(proposals) | set(stationary_guards))
    if not owners:
        return TransactionResult(False, 'empty_transaction')
    if set(proposals) & set(stationary_guards):
        return TransactionResult(False, 'guard_and_plan_same_owner', owners)
    if any(owner is None for owner in owners):
        return TransactionResult(False, 'missing_owner', owners)
    if now is not None and not _finite(now):
        return TransactionResult(False, 'invalid_now', owners)
    if current_positions is not None and any(owner not in current_positions for owner in owners):
        return TransactionResult(False, 'missing_current_position', owners)
    prepared = {}
    for owner in owners:
        try:
            parts = proposals.get(owner)
            if owner in stationary_guards:
                guard = stationary_guards[owner]
                if isinstance(guard, dict) and 'point' in guard:
                    parts = [stationary_guard(guard['point'], guard['start'], guard['end'],
                                              guard.get('phase', 'safety_hold'))]
                else:
                    parts = [guard] if isinstance(guard, dict) else guard
            expected = current_positions[owner] if current_positions is not None else None
            prepared[owner] = _normalise(parts, now, expected)
            if owner in stationary_guards and any(not _same_point(p['a'], p['b']) for p in prepared[owner]):
                raise ValueError('guard_is_not_stationary')
        except (KeyError, TypeError, ValueError) as error:
            return TransactionResult(False, str(error), owners, details={'owner': owner})
    trial = deepcopy(table)
    for owner in owners:
        trial._erase(owner)
    for owner in owners:
        for index, part in enumerate(prepared[owner]):
            conflicts = set()
            if not trial.clear_motion(part['a'], part['b'], part['start'], part['end'], owner, conflicts):
                return TransactionResult(False, 'continuous_collision', owners, _ordered(conflicts),
                                         {'owner': owner, 'part_index': index})
        trial.reserve(prepared[owner], owner=owner)
    if not validate_only:
        # One state pointer replacement: planner references to the table object
        # remain valid, unlike replacing only a caller's local table variable.
        table.__dict__ = trial.__dict__
    return TransactionResult(True, 'validated' if validate_only else 'committed', owners)
