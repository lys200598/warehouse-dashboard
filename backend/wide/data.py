"""Wide-layout data contract and explicit immutable dataset migration.

The validator retains v3's proven integrity/capacity checks with this module's
loader, without a fixed calendar-duration cap. Old modules are untouched.
P and C names are compatible aliases of ONE capacity-one physical bay.
"""
from __future__ import annotations

from copy import deepcopy
from collections import Counter
import csv
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import re
import shutil

from ..day1.data import FIELDS, read_dataset, sha


def _number(value, name, *, minimum=0, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"invalid scene {name}")
    if value < minimum or positive and value <= minimum:
        raise ValueError(f"invalid scene {name}")
    return value


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"invalid scene {name}")
    return value


def _rectangle(item, spec):
    for key in ("x", "y", "w", "d"):
        _number(item.get(key), f"{item.get('id')} {key}", positive=key in {"w", "d"})
    if item["x"] + item["w"] > spec["width"] or item["y"] + item["d"] > spec["depth"]:
        raise ValueError("scene rectangle exceeds dimensions")


def _point(point, name, spec):
    if not isinstance(point, dict):
        raise ValueError(f"missing scene point: {name}")
    for key, bound in (("x", "width"), ("y", "depth")):
        if _number(point.get(key), name) > spec[bound]:
            raise ValueError(f"scene point out of bounds: {name}")


def load_scene(path):
    """Read a parameterized 100-bay map and return normalized scene and slots.

    The returned aliases are descriptive; the execution layer must still use
    canonical resource capacity and continuously retain every real robot body.
    An empty pad is not a static obstacle. No original JSON is rewritten.
    """
    try:
        scene = json.loads(Path(path).read_text(encoding="utf-8"))
        spec = scene["spec"]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError("invalid wide scene") from error
    if spec.get("unit") != "m":
        raise ValueError("wide scene units must be metres")
    for key in ("width", "depth", "gridStep", "slotPitch", "robotRadius"):
        _number(spec.get(key), key, positive=True)
    for key in ("levels", "baysPerFace", "faces", "robotCount", "chargerCount"):
        _positive_int(spec.get(key), key)
    if spec["faces"] != 2 or spec["robotCount"] != 100 or spec["chargerCount"] != 100:
        raise ValueError("wide pilot requires two rack faces and 100 robots/canonical bays")
    radius = spec["robotRadius"]
    for key in ("robotBodyWidth", "robotBodyDepth"):
        _number(spec.get(key), key, positive=True)
    if radius + 1e-7 < math.hypot(spec["robotBodyWidth"]/2, spec["robotBodyDepth"]/2):
        raise ValueError("planning circle does not cover loaded robot body")
    homes, docks = scene.get("homes", []), scene.get("docks", [])
    if len(homes) != 100 or len(docks) != 100:
        raise ValueError("100 home aliases and 100 canonical docks required")
    for item in homes + docks:
        ident = item.get("id")
        if not isinstance(ident,str) or not ident or ident.strip()!=ident:
            raise ValueError("invalid bay identifier")
    by_id = {dock.get("id"): dock for dock in docks}
    home_ids = {home.get("id") for home in homes}
    if None in by_id or len(by_id) != 100 or len(home_ids) != 100 or None in home_ids or home_ids & set(by_id):
        raise ValueError("duplicate/overlapping bay identifiers")
    aliases, used, physical = {}, set(), set()
    for dock in docks:
        _rectangle(dock, spec)
        _point(dock.get("point"), dock["id"], spec)
        _point(dock.get("gate"), dock["id"] + " gate", spec)
        if isinstance(dock.get("capacity"),bool) or dock.get("capacity") != 1 or dock.get("staticCollision") is not False:
            raise ValueError("canonical docks require capacity one and non-solid empty hardware")
        centre = (dock["point"]["x"], dock["point"]["y"])
        if centre in physical:
            raise ValueError("duplicate physical dock point")
        physical.add(centre)
        if not (dock["x"] + radius <= centre[0] <= dock["x"] + dock["w"] - radius and
                dock["y"] + radius <= centre[1] <= dock["y"] + dock["d"] - radius):
            raise ValueError("robot body does not fit its dock")
    for index, dock in enumerate(docks):
        for other in docks[index+1:]:
            if math.dist(tuple(dock["point"][key] for key in ("x", "y")),
                         tuple(other["point"][key] for key in ("x", "y"))) < 2 * radius - 1e-7:
                raise ValueError("initial docked robot bodies overlap")
    for home in homes:
        canonical = home.get("alias_of")
        if canonical not in by_id or canonical in used:
            raise ValueError("home aliases must bijectively reference canonical docks")
        dock = by_id[canonical]
        if home.get("point") != dock["point"] or any(home.get(k) != dock[k] for k in ("x", "y", "w", "d")):
            raise ValueError("home alias must have the exact canonical physical pose and rectangle")
        used.add(canonical)
        aliases[home["id"]] = canonical
        home["kind"] = "home"
        home["gate"] = dict(dock["gate"])
        home["approachY"] = dock["gate"]["y"]
        home["staticCollision"] = False
    slots, rack_ids = {}, set()
    for rack in scene.get("racks", []):
        ident = rack.get("id")
        if not isinstance(ident, str) or not ident or ident in rack_ids:
            raise ValueError("invalid or duplicate rack ID")
        rack_ids.add(ident)
        _rectangle(rack, spec)
        pitch = rack["w"] / spec["baysPerFace"]
        if abs(pitch - spec["slotPitch"]) > 1e-7:
            raise ValueError("rack width and slot pitch disagree")
        count = spec["levels"] * spec["baysPerFace"] * spec["faces"]
        if rack.get("capacity") != count:
            raise ValueError("rack capacity disagrees with levels/bays/faces")
        for level in range(1, spec["levels"]+1):
            for face in range(spec["faces"]):
                for bay in range(spec["baysPerFace"]):
                    key = f"{ident}-L{level}-C{bay+1}-F{face+1}"
                    slot = {"slot_id":key, "rack_id":ident, "level":level, "bay":bay+1, "face":face,
                            "x":rack["x"]+(bay+.5)*pitch,
                            "y":rack["y"]-.7 if face==0 else rack["y"]+rack["d"]+.7,
                            "occupied":False, "cargo_id":None, "reservedBy":None}
                    _point(slot, key, spec)
                    slots[key] = slot
    if len(slots) != scene.get("capacity") or len(slots) != 1728:
        raise ValueError("wide scene must preserve all 1728 physical storage slots")
    station_ids = set()
    for station in scene.get("stations", []):
        ident = station.get("id")
        if ident in station_ids or ident in slots or station.get("kind") not in {"inbound", "outbound"}:
            raise ValueError("invalid or duplicate boundary station")
        station_ids.add(ident)
        _point(station, ident, spec)
        if ident.startswith('IN-') and station['kind']!='inbound' or ident.startswith('OUT-') and station['kind']!='outbound':
            raise ValueError("boundary station role conflicts with its preserved ID")
    if station_ids != {f"{kind}-{i}" for kind in ("IN", "OUT") for i in range(1,9)}:
        raise ValueError("all eight IN and eight OUT IDs must be preserved")
    for obstacle in scene.get("buffers", []):
        _rectangle(obstacle, spec)
    scene["location_aliases"] = aliases
    scene["canonical_bay_ids"] = list(by_id)
    scene["physical_bay_count"] = len(by_id)
    scene["loader_contract"] = "wide-parameterized-100-canonical-bays-v1"
    return scene, slots


def migrate_dataset(old_scene, new_scene, source, output, *, deadline_policy="preserve"):
    """Create a fresh, provenance-tracked mapping of a valid input to a new map.

    preserve keeps BOTH CSV files byte-for-byte. explicit-reference changes
    only due_time using new-map Manhattan distance plus map-size deadhead,
    0.8 m/s, 22 s handling and the source's declared/default 900 s slack.
    Neither policy uses measured simulation completion times.
    """
    if deadline_policy not in {"preserve", "explicit-reference"}:
        raise ValueError("unknown deadline migration policy")
    source, output = Path(source), Path(output)
    if output.exists():
        raise FileExistsError("migration output exists; choose a new directory")
    if output.resolve().is_relative_to(source.resolve()):
        raise ValueError("migration output must not be inside the original immutable dataset")
    old_shape = json.loads(Path(old_scene).read_text(encoding="utf-8"))
    if len(old_shape.get("docks", [])) == 100:
        source_validation = validate_dataset(old_scene, source)
    else:
        from ..scheduling_v3.data import validate_dataset as old_validate
        source_validation = old_validate(old_scene, source)
    scene, slots = load_scene(new_scene)
    manifest, tasks, initial = read_dataset(old_scene, source)
    ports = {p["id"]: p for p in scene["stations"]}
    for task in tasks:
        if task["source_id"] not in slots and task["source_id"] not in ports or \
                task["target_id"] not in slots and task["target_id"] not in ports:
            raise ValueError("old task location is missing from new layout")
    old_manifest_sha = sha(source/"manifest.json")
    source_files = dict(manifest["files"])
    changes = 0
    slack = manifest.get("deadline_slack_seconds", 900)
    _number(slack, "deadline slack")
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source/"initial_inventory.csv", output/"initial_inventory.csv")
    if deadline_policy == "preserve":
        shutil.copyfile(source/"tasks.csv", output/"tasks.csv")
    else:
        reference_deadhead = scene["spec"]["width"] + scene["spec"]["depth"]
        for task in tasks:
            a, b = (slots.get(task[key]) or ports[task[key]] for key in ("source_id", "target_id"))
            seconds = math.ceil((abs(a["x"]-b["x"])+abs(a["y"]-b["y"])+reference_deadhead)/.8 + 22 + slack)
            due = (datetime.fromisoformat(task["release_time"])+timedelta(seconds=seconds)).isoformat()
            changes += due != task["due_time"]
            task["due_time"] = due
        with (output/"tasks.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS, extrasaction="ignore")
            writer.writeheader(); writer.writerows(tasks)
    migrated = deepcopy(manifest)
    migrated.update(version="warehouse-wide-migrated-v1", scene_sha256=sha(new_scene),
        scene_version=scene["spec"]["version"], capacity=len(slots),
        files={name:sha(output/name) for name in ("tasks.csv", "initial_inventory.csv")},
        migration={"source_manifest_sha256":old_manifest_sha, "source_scene_sha256":sha(old_scene),
                   "source_files":source_files, "deadline_policy":deadline_policy,
                   "due_time_changes":changes, "task_ids_preserved":True,
                   "goods_ids_preserved":True, "release_times_preserved":True,
                   "source_reference_verified":source_validation['valid'],
                   "uses_measured_completion_times":False,
                   "hotspot_location_labels":"source labels retained as provenance; reclassify spatial regions using new coordinates",
                   "reference_deadline":"new-map Manhattan + (width+depth) deadhead at .8m/s +22s handling + source/default900s slack" if deadline_policy!="preserve" else "original timestamps unchanged; runtime feasibility requires fresh validation"})
    if manifest.get('hotspot_centre') is not None:
        migrated['migration']['source_spatial_metadata'] = {
            'hotspot_centre':deepcopy(manifest['hotspot_centre']),
            'hotspot_slot_ids':deepcopy(manifest.get('hotspot_slot_ids',[]))}
        migrated['migration']['spatial_reclassification_required'] = True
        migrated['hotspot_centre'] = None
    (output/"manifest.json").write_text(json.dumps(migrated, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    report = validate_dataset(new_scene, output)
    report["migration"] = deepcopy(migrated["migration"])
    (output/"validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return migrated, report


# Strict contract snapshot follows; its only layout dependency is load_scene.

# Compatibility export: None means no artificial fixed-duration limit. Actual
# timestamps still need to be valid ISO8601 datetimes and tasks remain bounded
# by the caller's explicit resource budget, not a number of calendar days.
MAX_DURATION_SECONDS = None
MAX_DENSE_REPORT_BUCKETS = 4096
_INPUT_FILES = {"tasks.csv", "initial_inventory.csv"}


def _time_bucket_report(duration, bucket_seconds, counts):
    """Preserve old short lists, but never allocate a list proportional to years.

    Sparse nonzero buckets are relative to manifest start, just like the old
    dense lists. All zero buckets are implicit, including an empty workload.
    Integer ceil also works for large positive durations without float overflow.
    """
    buckets = (duration + bucket_seconds - 1) // bucket_seconds
    if buckets <= MAX_DENSE_REPORT_BUCKETS:
        return [counts.get(index, 0) for index in range(buckets)]
    return {"format": "sparse-v1", "bucket_seconds": bucket_seconds,
            "bucket_count": buckets, "zero_buckets_omitted": True,
            "nonzero": [{"bucket": index, "tasks": value}
                        for index, value in sorted(counts.items())]}


def _count(value, name, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (maximum is not None and value > maximum):
        raise ValueError(f"manifest {name} must be a nonnegative bounded integer")
    return value


def _timestamp(value, name, *, reference_offset=None):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO8601 timestamp")
    try:
        result = datetime.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid {name}") from error
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{name} must carry an explicit timezone offset")
    if result.microsecond:
        raise ValueError(f"{name} must use integer-second precision")
    if reference_offset is not None and result.utcoffset() != reference_offset:
        raise ValueError(f"{name} timezone offset must match manifest start")
    return result


def _identifier(value, name):
    if not isinstance(value, str) or not 1 <= len(value) <= 128 or value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"invalid {name}")
    return value


def _rows(path, fields):
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            if next(reader, None) != fields:
                raise ValueError(f"{path.name} columns changed")
            for number, values in enumerate(reader, 2):
                if not values:
                    continue
                if len(values) != len(fields):
                    raise ValueError(f"{path.name} row {number} has incorrect field count")
                yield dict(zip(fields, values))
    except (OSError, UnicodeError, csv.Error) as error:
        raise ValueError(f"cannot read {path.name}: {type(error).__name__}") from error


def validate_dataset(scene_path, directory, *, max_tasks=None):
    """Validate fixed seven-field tasks without a fixed calendar-duration cap.

    File hashes, scene identity, counts and physical ID references are checked
    before the inventory replay. A workload may pass this contract but exceed
    the fleet's service capacity: runtime must still enforce actual admission,
    inventory, per-port buffering, traffic safety and deadline measurements.
    ``max_tasks`` is an optional positive row budget, not a duration restriction.
    Fixed-file loading remains in memory; this is not a streaming runtime API.
    """
    directory = Path(directory)
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid or missing dataset manifest") from error
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    if max_tasks is not None:
        _positive_int(max_tasks, "max_tasks resource budget")
    duration = _count(manifest.get("duration_seconds"), "duration_seconds", MAX_DURATION_SECONDS)
    if duration < 1:
        raise ValueError("manifest duration_seconds must be positive")
    start = _timestamp(manifest.get("start"), "manifest start")
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != _INPUT_FILES:
        raise ValueError("manifest input files must be exactly tasks.csv and initial_inventory.csv")
    for filename, expected in files.items():
        if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            raise ValueError(f"invalid SHA-256 for {filename}")
        try:
            actual = sha(directory / filename)
        except OSError as error:
            raise ValueError(f"missing dataset input {filename}") from error
        if actual != expected:
            raise ValueError(f"dataset hash mismatch: {filename}")
    if manifest.get("scene_sha256") != sha(scene_path):
        raise ValueError("scene hash mismatch")
    scene, slots = load_scene(scene_path)
    capacity = _count(manifest.get("capacity"), "capacity")
    if capacity != len(slots):
        raise ValueError("manifest capacity does not match physical storage slots")
    expected_counts = {key: _count(manifest.get(key), key) for key in
                       ("tasks", "inbound", "outbound", "initial_inventory")}
    if expected_counts["inbound"] + expected_counts["outbound"] != expected_counts["tasks"]:
        raise ValueError("manifest inbound/outbound counts do not sum to tasks")
    if expected_counts["initial_inventory"] > capacity:
        raise ValueError("initial inventory exceeds physical capacity")
    if max_tasks is not None and expected_counts["tasks"] > max_tasks:
        raise ValueError("dataset exceeds configured max_tasks resource budget")

    # Validate raw ISO timestamps BEFORE read_dataset's integer conversion.
    raw_tasks = []
    for row in _rows(directory / "tasks.csv", FIELDS):
        if max_tasks is not None and len(raw_tasks) >= max_tasks:
            raise ValueError("dataset exceeds configured max_tasks resource budget")
        raw_tasks.append(row)
    initial = list(_rows(directory / "initial_inventory.csv", ["goods_id", "location_id"]))
    ids = set()
    for task in raw_tasks:
        task_id = _identifier(task["task_id"], "task ID")
        if task_id in ids:
            raise ValueError("duplicate task ID")
        ids.add(task_id)
        for key in ("goods_id", "source_id", "target_id"):
            _identifier(task[key], key)
        release = _timestamp(task["release_time"], f"{task_id} release_time", reference_offset=start.utcoffset())
        due = _timestamp(task["due_time"], f"{task_id} due_time", reference_offset=start.utcoffset())
        released = (release - start).total_seconds()
        if not 0 <= released < duration or due <= release:
            raise ValueError(f"invalid task time range: {task_id}")

    # Reuse the existing consumer contract after the stricter preflight.
    _, tasks, _ = read_dataset(scene_path, directory)
    actual_counts = {"tasks": len(tasks), "inbound": sum(t["task_type"] == "INBOUND" for t in tasks),
                     "outbound": sum(t["task_type"] == "OUTBOUND" for t in tasks), "initial_inventory": len(initial)}
    if actual_counts != expected_counts:
        raise ValueError(f"manifest counts mismatch: expected {expected_counts}, actual {actual_counts}")

    stock, where, ever_seen = {}, {}, set()
    for row in initial:
        goods = _identifier(row["goods_id"], "initial goods ID")
        slot = _identifier(row["location_id"], "initial location ID")
        if slot not in slots or slot in stock or goods in ever_seen:
            raise ValueError("invalid initial inventory: unknown/duplicate slot or duplicate goods")
        stock[slot], where[goods] = goods, slot
        ever_seen.add(goods)
    ports = {station["id"]: station["kind"] for station in scene["stations"]}
    low = high = len(stock)
    hourly, daily = Counter(), Counter()
    for task in sorted(tasks, key=lambda row: (row["release_second"], row["task_id"])):
        second = task["release_second"]
        hourly[second // 3600] += 1
        daily[second // 86400] += 1
        goods, kind = task["goods_id"], task["task_type"]
        if kind == "INBOUND":
            slot = task["target_id"]
            if slot not in slots or slot in stock or goods in ever_seen or ports.get(task["source_id"]) != "inbound":
                raise ValueError(f"invalid inbound reference dependency: {task['task_id']}")
            stock[slot], where[goods] = goods, slot
            ever_seen.add(goods)
        elif kind == "OUTBOUND":
            slot = task["source_id"]
            if stock.get(slot) != goods or where.get(goods) != slot or ports.get(task["target_id"]) != "outbound":
                raise ValueError(f"invalid outbound reference dependency: {task['task_id']}")
            del stock[slot]
            del where[goods]
        else:
            raise ValueError(f"unknown task type: {task['task_id']}")
        low, high = min(low, len(stock)), max(high, len(stock))
        if not 0 <= len(stock) <= capacity:
            raise ValueError("reference-plan capacity exceeded")
    return {
        "valid": True, **actual_counts, "initial": len(initial), "duration_seconds": duration,
        "capacity": capacity, "start": start.isoformat(), "timezone_offset_seconds": int(start.utcoffset().total_seconds()),
        "planned_min_inventory": low, "planned_max_inventory": high, "planned_final_inventory": len(stock),
        "hourly_tasks": _time_bucket_report(duration, 3600, hourly),
        "daily_tasks": _time_bucket_report(duration, 86400, daily),
        "reference_order": ["release_second", "task_id"],
        "hashes_verified": True, "runtime_feasibility_claim": False,
        "note": "Reference release-order inventory/dependency replay only; does not prove runtime timely completion, traffic safety or actual admission capacity.",
    }
