"""Strict bounded multi-day input contract; no changes to the day1 baseline.

Reference-plan inventory checks establish logical dependencies, not that the
fleet will finish tasks by their release timestamps or deadlines.
"""
from __future__ import annotations

import csv
from datetime import datetime
import json
import math
from pathlib import Path
import re

from ..day1.data import FIELDS, load_scene, read_dataset, sha


MAX_DURATION_SECONDS = 7 * 86400
_INPUT_FILES = {"tasks.csv", "initial_inventory.csv"}


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
    if not isinstance(value, str) or not value or value.strip() != value or any(ord(char) < 32 for char in value):
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


def validate_dataset(scene_path, directory):
    """Validate <=7 days of seven-field tasks and return a reference report.

    File hashes, scene identity, counts and physical ID references are checked
    before the inventory replay. A workload may pass this contract but exceed
    the fleet's service capacity: runtime must still enforce actual admission,
    inventory, per-port buffering, traffic safety and deadline measurements.
    """
    directory = Path(directory)
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid or missing dataset manifest") from error
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
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

    # Validate raw ISO timestamps BEFORE read_dataset's integer conversion.
    raw_tasks = list(_rows(directory / "tasks.csv", FIELDS))
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
    hourly = [0] * math.ceil(duration / 3600)
    daily = [0] * math.ceil(duration / 86400)
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
        "hourly_tasks": hourly, "daily_tasks": daily, "reference_order": ["release_second", "task_id"],
        "hashes_verified": True, "runtime_feasibility_claim": False,
        "note": "Reference release-order inventory/dependency replay only; does not prove runtime timely completion, traffic safety or actual admission capacity.",
    }
