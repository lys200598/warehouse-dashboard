"""Explicit, configurable energy and custody contracts for the v3 simulator.

SOC values are percentage points, not fractions or battery capacity (Wh).
Profiles are modelling assumptions, not calibrated hardware performance.
Energy admission never substitutes for collision/queue-space validation.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import math
from typing import Any, Iterable, Mapping


MOVING_PHASES = frozenset({"moving", "to_source", "to_target", "to_charge", "returning", "yielding", "to_handoff"})
WAITING_PHASES = frozenset({"idle", "standby", "traffic_wait", "mission_queue", "charge_queue", "blocked", "fault_wait", "recovery_wait", "swapping"})
HANDLING_PHASES = frozenset({"picking", "placing", "handoff", "handoff_give", "handoff_receive", "docking", "undocking"})


def _finite(value: float, name: str, minimum: float = 0.0) -> float:
    if not isinstance(value, (float, int)) or isinstance(value, bool) or not math.isfinite(value) or value < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return float(value)


def _soc(value: float, name: str = "soc") -> float:
    value = _finite(value, name)
    if value > 100:
        raise ValueError(f"{name} exceeds 100")
    return value


@dataclass(frozen=True)
class EnergyProfile:
    """A net-SOC model. Charging is piecewise linear above taper_start_soc."""

    name: str = "transport-assumed-v1"
    empty_soc_per_second: float = 15.0 / 3600
    loaded_factor: float = 1.2
    handling_factor: float = 1.2
    idle_soc_per_second: float = 2.0 / 3600
    charge_soc_per_second: float = 60.0 / 3600
    taper_start_soc: float = 80.0
    taper_factor: float = 0.5
    hardware_calibrated: bool = False

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("profile name is required")
        for field in ("empty_soc_per_second", "loaded_factor", "handling_factor", "idle_soc_per_second", "charge_soc_per_second", "taper_factor"):
            _finite(getattr(self, field), field, 1e-15)
        _soc(self.taper_start_soc, "taper_start_soc")
        if self.taper_factor > 1:
            raise ValueError("taper_factor must be <= 1")

    @classmethod
    def legacy(cls):
        return cls(name="legacy-assumed-v2", loaded_factor=1.0, handling_factor=1.0,
                   idle_soc_per_second=.00025, taper_factor=1.0)

    @classmethod
    def conservative(cls):
        return cls(name="conservative-sensitivity-v1", empty_soc_per_second=30.0 / 3600,
                   loaded_factor=1.2, handling_factor=1.4, idle_soc_per_second=6.0 / 3600,
                   charge_soc_per_second=40.0 / 3600)

    def as_dict(self) -> dict:
        return asdict(self)

    def rate(self, phase: str, loaded: bool = False) -> float:
        """Nonnegative consumption rate; charging gain has a separate API."""
        if phase == "charging":
            return 0.0
        if phase in WAITING_PHASES:
            return self.idle_soc_per_second
        if phase in HANDLING_PHASES:
            return self.empty_soc_per_second * self.handling_factor
        if phase in MOVING_PHASES:
            return self.empty_soc_per_second * (self.loaded_factor if loaded else 1.0)
        raise ValueError(f"unrecognised energy phase: {phase!r}")

    def consume(self, parts: Iterable[Mapping[str, Any]], initial_loaded: bool = False) -> float:
        """Integrate actual span durations; charging never offsets required SOC.

        ``loaded`` on a part overrides the inferred load for that part. Marker
        transitions happen at the END of a part, matching the execution model.
        """
        loaded, total, previous_end = bool(initial_loaded), 0.0, None
        for part in parts:
            start, end = _finite(part["start"], "part start"), _finite(part["end"], "part end")
            if end <= start:
                raise ValueError("energy span duration must be positive")
            if previous_end is not None and abs(start - previous_end) > 1e-7:
                raise ValueError("energy spans must be time-contiguous; represent waits explicitly")
            previous_end = end
            is_loaded = bool(part.get("loaded", loaded))
            total += (end - start) * self.rate(part["phase"], is_loaded)
            marker = part.get("marker")
            if marker in {"picked", "received_handoff", "handoff_received"}:
                loaded = True
            elif marker in {"delivered", "given_handoff", "handoff_given"}:
                loaded = False
        return total

    drain = consume

    def charge_seconds(self, arrival_soc: float, target_soc: float = 80.0) -> int:
        arrival_soc, target_soc = _soc(arrival_soc, "arrival_soc"), _soc(target_soc, "target_soc")
        if target_soc <= arrival_soc:
            return 0
        normal = max(0.0, min(target_soc, self.taper_start_soc) - arrival_soc)
        tapered = max(0.0, target_soc - max(arrival_soc, self.taper_start_soc))
        seconds = normal / self.charge_soc_per_second + tapered / (self.charge_soc_per_second * self.taper_factor)
        return math.ceil(seconds - 1e-10)

    def charge_gain(self, soc: float, seconds: float) -> float:
        soc, seconds = _soc(soc), _finite(seconds, "seconds")
        initial = soc
        below = max(0.0, self.taper_start_soc - soc)
        normal_gain = min(below, seconds * self.charge_soc_per_second)
        soc += normal_gain
        seconds -= normal_gain / self.charge_soc_per_second
        soc = min(100.0, soc + max(0.0, seconds) * self.charge_soc_per_second * self.taper_factor)
        return soc - initial


@dataclass(frozen=True)
class AdmissionResult:
    admitted: bool
    required_soc: float
    task_soc: float
    to_charge_soc: float
    queue_soc: float
    reserve_soc: float
    remaining_soc: float
    dock_id: str | None
    reason: str

    def as_dict(self):
        # Strict JSON evidence cannot carry Infinity for unreachable stations.
        return {key: (None if isinstance(value, float) and not math.isfinite(value) else value)
                for key, value in asdict(self).items()}


def required_soc(task_parts, to_charge_parts, profile: EnergyProfile,
                 reserve_soc: float = 20.0, queue_seconds: float = 0.0,
                 initial_loaded: bool = False) -> float:
    """Mission + a validated route to charge + ADDITIONAL wait + hard reserve.

    Waits already present in either span list are counted by ``consume``.
    Caller supplies additional queue margin only once. Geometric safety and
    charger exclusivity must be established before using this energy result.
    """
    reserve_soc, queue_seconds = _soc(reserve_soc, "reserve_soc"), _finite(queue_seconds, "queue_seconds")
    return (profile.consume(task_parts, initial_loaded) + profile.consume(to_charge_parts, False) +
            queue_seconds * profile.idle_soc_per_second + reserve_soc)


def assess_admission(soc: float, task_parts, charge_options, profile: EnergyProfile,
                     reserve_soc: float = 20.0, max_queue_seconds: float = 7200.0,
                     initial_loaded: bool = False) -> AdmissionResult:
    """Choose an energy-feasible *already planned* charger alternative.

    Each option has ``dock_id``, ``parts`` (mission end -> dock), optional
    ``available_at`` and ``queue_seconds`` (extra contingency beyond the route).
    If a route already waits until available_at that wait is not counted twice.
    Options with ``reachable=False`` are excluded. This function does NOT create
    an actual queue reservation or certify a physical place for that wait.
    """
    soc, reserve_soc = _soc(soc), _soc(reserve_soc, "reserve_soc")
    max_queue_seconds = _finite(max_queue_seconds, "max_queue_seconds")
    task_parts = list(task_parts)
    task_soc = profile.consume(task_parts, initial_loaded)
    mission_end = float(task_parts[-1]["end"]) if task_parts else 0.0
    options = []
    reachable = False
    queue_rejected = False
    for option in charge_options:
        if option.get("reachable", True) is False:
            continue
        dock_id = option.get("dock_id")
        if not isinstance(dock_id, str) or not dock_id:
            raise ValueError("each charge option needs a nonempty dock_id")
        parts = list(option.get("parts", []))
        if parts and abs(float(parts[0]["start"]) - mission_end) > 1e-7:
            raise ValueError("charger route must start at mission end")
        to_charge_soc = profile.consume(parts, False)
        arrival = float(parts[-1]["end"]) if parts else mission_end
        available = _finite(option.get("available_at", arrival), "available_at")
        extra = _finite(option.get("queue_seconds", 0), "queue_seconds")
        queue = max(0.0, available - arrival) + extra
        route_wait = sum(float(p["end"]) - float(p["start"]) for p in parts if p["phase"] in WAITING_PHASES)
        reachable = True
        if queue + route_wait > max_queue_seconds + 1e-7:
            queue_rejected = True
            continue
        queue_soc = queue * profile.idle_soc_per_second
        required = task_soc + to_charge_soc + queue_soc + reserve_soc
        options.append(AdmissionResult(soc + 1e-9 >= required, required, task_soc,
            to_charge_soc, queue_soc, reserve_soc, soc - task_soc - to_charge_soc - queue_soc,
            dock_id, "accepted" if soc + 1e-9 >= required else "insufficient_energy"))
    if options:
        return min(options, key=lambda result: (result.required_soc, result.dock_id))
    reason = "queue_bound_exceeded" if reachable and queue_rejected else "no_reachable_charger"
    return AdmissionResult(False, math.inf, task_soc, math.inf, 0.0, reserve_soc,
                           soc - task_soc, None, reason)


class HandoffError(ValueError):
    pass


def validate_handoff(donor, receiver, task, goods, *, min_separation: float = 1.1,
                     max_reach: float = 1.6) -> None:
    """Validate custody preconditions at completion of an actual handoff hold.

    The simulator must separately ensure both robots remain at these legal
    handoff poses for the whole service interval. This is a state contract,
    not a kinematic or collision planner.
    """
    min_separation = _finite(min_separation, "min_separation", 1e-9)
    max_reach = _finite(max_reach, "max_reach", min_separation)
    if donor["robot_id"] == receiver["robot_id"]:
        raise HandoffError("donor and receiver must be different robots")
    cargo = task.get("cargo_id", task.get("goods_id"))
    if not cargo or cargo != task.get("goods_id", cargo):
        raise HandoffError("task cargo identity mismatch")
    if not donor.get("carrying") or donor.get("cargo_id") != cargo or goods.get(cargo) != f"ROBOT-{donor['robot_id']}":
        raise HandoffError("donor is not the unique recorded cargo holder")
    if task.get("status") != "carrying" or task.get("robot_id") != donor["robot_id"]:
        raise HandoffError("task is not carrying under donor responsibility")
    if donor.get("task_id") != task.get("task_id"):
        raise HandoffError("donor is executing a different task")
    if receiver.get("carrying") or receiver.get("cargo_id") is not None:
        raise HandoffError("receiver must be empty")
    if receiver.get("task_id") not in {None, task.get("task_id")}:
        raise HandoffError("receiver is committed to a different task")
    if any(holder == f"ROBOT-{donor['robot_id']}" and key != cargo for key, holder in goods.items()) or \
            any(holder == f"ROBOT-{receiver['robot_id']}" for holder in goods.values()):
        raise HandoffError("cargo ledger disagrees with single-parcel robot capacity")
    forbidden = {"charging", "failed", "fault", "disabled", "to_charge"}
    if donor.get("stage") in forbidden or receiver.get("stage") in forbidden:
        raise HandoffError("charging, unavailable or travelling-to-charge robot cannot hand off")
    points = []
    for robot in (donor, receiver):
        x, y = float(robot["x"]), float(robot["y"])
        if not math.isfinite(x) or not math.isfinite(y):
            raise HandoffError("handoff positions must be finite")
        points.append((x, y))
    distance = math.dist(*points)
    if not min_separation - 1e-7 <= distance <= max_reach + 1e-7:
        raise HandoffError("handoff poses violate body separation or reach")


def transfer_custody(donor, receiver, task, goods, *, now: float,
                     reason: str = "low_energy", min_separation: float = 1.1,
                     max_reach: float = 1.6):
    """Return copied donor, receiver, task, goods and one atomic transfer record.

    No caller object changes on success or failure. Repeating a committed
    transfer fails because the donor no longer holds the cargo.
    """
    now = _finite(now, "handoff completion time")
    if not isinstance(reason, str) or not reason:
        raise HandoffError("handoff reason is required")
    validate_handoff(donor, receiver, task, goods, min_separation=min_separation, max_reach=max_reach)
    d, r, t, g = deepcopy(donor), deepcopy(receiver), deepcopy(task), deepcopy(goods)
    cargo = t.get("cargo_id", t["goods_id"])
    old_owner, new_owner = d["robot_id"], r["robot_id"]
    d.update(carrying=False, cargo_id=None, task_id=None)
    r.update(carrying=True, cargo_id=cargo, task_id=t["task_id"])
    t.update(robot_id=new_owner, status="carrying", handoffs=int(t.get("handoffs", 0)) + 1,
             last_handoff_at=now)
    g[cargo] = f"ROBOT-{new_owner}"
    event = {"kind": "cargo_handoff", "sim_time": now, "task_id": t["task_id"],
             "goods_id": cargo, "donor_id": old_owner, "receiver_id": new_owner,
             "from_location": f"ROBOT-{old_owner}", "location_id": f"ROBOT-{new_owner}",
             "reason": reason}
    return d, r, t, g, event
