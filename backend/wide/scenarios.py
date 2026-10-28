"""Reproducible bounded scenario inputs, reusing the day1 seven-field contract.

These are synthetic warehouse boundary tasks, not business orders or real LaDe
records. Cargo constraints are reference-plan checks; runtime still gates entry.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import random

from .data import FIELDS, load_scene, sha, MAX_DURATION_SECONDS, validate_dataset


SCENARIOS = ("uniform", "peak", "hotspot", "continuous")


def _integer(value, name, minimum=1, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f"invalid {name}")
    return value


def _peak_time(second, duration):
    """Monotone quantile compression into three delivery waves (not iid noise)."""
    u = min(1 - 1e-12, max(0, second / duration))
    wave = min(2, int(u * 3))
    local = u * 3 - wave
    starts = (.05, .40, .75)
    return int(duration * (starts[wave] + .15 * local))


def generate_scenario(scene_path, output, *, scenario="uniform", duration_seconds=3600,
                      seed=20261027, active_slots=None, slot_interval_seconds=21600,
                      deadline_slack_seconds=900, profile="transport-assumed-v1",
                      date="2026-10-27", hotspot_multiplier=4, max_tasks=250000):
    """Generate a fresh directory and return (manifest, reference_validation).

    ``active_slots`` restricts which storage slots receive tasks, NOT the actual
    1728-slot capacity or the unchanged 864-item initial stock. Turnover is a
    release-plan parameter, not a promise the fleet can sustain this demand.
    Duration has no fixed calendar cap. ``max_tasks`` is a configurable resource
    budget; actual release/due timestamps must fit ISO8601 datetime arithmetic.
    """
    if scenario not in SCENARIOS:
        raise ValueError("unknown warehouse scenario")
    _integer(seed, "seed", 0, 2 ** 64 - 1)
    _integer(duration_seconds, "duration_seconds", 1, MAX_DURATION_SECONDS)
    _integer(slot_interval_seconds, "slot_interval_seconds", 60)
    _integer(deadline_slack_seconds, "deadline_slack_seconds", 0)
    _integer(hotspot_multiplier, "hotspot_multiplier", 1, 20)
    _integer(max_tasks, "max_tasks", 1)
    if not isinstance(profile, str) or not profile:
        raise ValueError("profile metadata must be a nonempty string")
    scene, slots = load_scene(scene_path)
    if active_slots is not None:
        _integer(active_slots, "active_slots", 1, len(slots))
    output = Path(output)
    if output.exists():
        raise FileExistsError("scenario output exists; choose a new directory")
    rng = random.Random(seed)
    all_keys = list(slots)
    shuffled = list(all_keys)
    rng.shuffle(shuffled)
    initial_set = set(shuffled[:len(slots)//2])
    active_order = list(all_keys)
    rng.shuffle(active_order)
    active = set(active_order if active_slots is None else active_order[:active_slots])
    # Select a physical, local patch, not arbitrary logical IDs labelled 'hot'.
    centre_key = min(active, key=lambda k: (slots[k]["x"], slots[k]["y"], k))
    centre = slots[centre_key]
    by_distance = sorted(active, key=lambda k: ((slots[k]["x"] - centre["x"]) ** 2 +
                                               (slots[k]["y"] - centre["y"]) ** 2, k))
    hot_count = max(1, math.ceil(len(active) * .2)) if scenario == "hotspot" else 0
    hot_slots = set(by_distance[:hot_count])
    min_interval = max(1, slot_interval_seconds // hotspot_multiplier) if hot_slots else slot_interval_seconds
    if ((duration_seconds + min_interval - 1) // min_interval) * len(active) > max_tasks:
        raise ValueError("scenario exceeds configured bounded task budget")
    try:
        if not isinstance(date, str):
            raise ValueError("date must be an ISO8601 calendar date")
        start = datetime.fromisoformat(date + "T00:00:00+08:00")
    except (TypeError, ValueError) as error:
        raise ValueError("date must be an ISO8601 calendar date") from error
    stations = {station["id"]: station for station in scene["stations"]}
    initial, tasks, serial = [], [], 0
    for index, (key, slot) in enumerate(slots.items()):
        occupied = key in initial_set
        goods = f"INITIAL-{index + 1:05d}" if occupied else None
        if occupied:
            initial.append({"goods_id": goods, "location_id": key})
        if key not in active:
            continue
        interval = max(1, slot_interval_seconds // hotspot_multiplier) if key in hot_slots else slot_interval_seconds
        nominal = rng.randrange(0, interval)
        previous, incoming = -1, 0
        while nominal < duration_seconds:
            try:
                release = _peak_time(nominal, duration_seconds) if scenario == "peak" else nominal
            except (OverflowError, ValueError) as error:
                raise ValueError("generated task exceeds supported ISO8601 datetime range") from error
            release = max(previous + 1, release)
            if release >= duration_seconds:
                break
            previous = release
            serial += 1
            kind = "OUTBOUND" if occupied else "INBOUND"
            station = stations[f"{'OUT' if occupied else 'IN'}-{rng.randrange(1, 9)}"]
            if not occupied:
                incoming += 1
                goods = f"V3-G-{index + 1:04d}-{incoming:04d}"
            source, target = (key, station["id"]) if occupied else (station["id"], key)
            reference = (abs(slot["x"] - station["x"]) + abs(slot["y"] - station["y"]) + scene["spec"]["width"] + scene["spec"]["depth"]) / .8 + 22
            due = release + math.ceil(reference) + deadline_slack_seconds
            try:
                released_at = (start + timedelta(seconds=release)).isoformat()
                due_at = (start + timedelta(seconds=due)).isoformat()
            except (OverflowError, ValueError) as error:
                raise ValueError("generated task exceeds supported ISO8601 datetime range") from error
            tasks.append({"task_id": f"V3-T-{serial:07d}", "task_type": kind, "goods_id": goods,
                          "release_time": released_at,
                          "due_time": due_at,
                          "source_id": source, "target_id": target})
            occupied = not occupied
            if scenario == "continuous":
                nominal += max(1, round(rng.expovariate(1.0 / interval)))
            else:
                nominal += interval
            if serial > max_tasks:
                raise ValueError("scenario exceeds configured bounded task budget")
    tasks.sort(key=lambda task: (task["release_time"], task["task_id"]))
    output.mkdir(parents=True, exist_ok=False)
    for filename, rows, fields in [("tasks.csv", tasks, FIELDS),
                                  ("initial_inventory.csv", initial, ["goods_id", "location_id"])]:
        with (output / filename).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    manifest = {
        "version": "warehouse-wide-scenarios-v1", "scenario": scenario, "seed": seed,
        "start": start.isoformat(), "duration_seconds": duration_seconds,
        "profile": profile, "profile_role": "runtime-assumption metadata only; no charge decisions in CSV",
        "time_unit": "ISO8601 +08:00", "scene_sha256": sha(scene_path), "capacity": len(slots),
        "initial_inventory": len(initial), "tasks": len(tasks),
        "inbound": sum(t["task_type"] == "INBOUND" for t in tasks),
        "outbound": sum(t["task_type"] == "OUTBOUND" for t in tasks),
        "active_slots": len(active), "active_slot_ids": sorted(active),
        "slot_turnover_seconds": slot_interval_seconds, "hotspot_multiplier": hotspot_multiplier if hot_slots else 1,
        "hotspot_slot_ids": sorted(hot_slots), "hotspot_centre": {"x": centre["x"], "y": centre["y"]} if hot_slots else None,
        "deadline_slack_seconds": deadline_slack_seconds,
        "source": "synthetic warehouse boundary tasks; LaDe-like seven fields, NOT actual Jilin records",
        "assumptions": {
            "reference_plan": "Each slot strictly alternates OUT/IN with unique inbound goods; release order is a reference plan, not runtime completion.",
            "uniform": "Independent fixed phase per slot, periodic releases.",
            "peak": "Monotone compression of periodic release times into three waves within this duration.",
            "hotspot": "Nearest physical 20% of active slots have faster turnover; total demand is not matched to uniform.",
            "continuous": "Independent exponential interarrival per slot after random initial phase; minimum 1 second.",
            "admission": "Runtime separately enforces occupied/reserved target slots, total inside goods and six boxes per inbound port.",
            "deadline": "Manhattan reference plus (width+depth)m deadhead at 0.8m/s, 22s handling and configurable slack; not guaranteed fleet feasibility.",
            "duration": "No fixed calendar-duration cap; positive integer seconds with valid ISO8601 task timestamps. Fixed files remain materialized, not streamed.",
            "resource_budget": f"At most {max_tasks} generated task rows; caller-configurable max_tasks, independent of duration.",
        },
        "files": {filename: sha(output / filename) for filename in ["tasks.csv", "initial_inventory.csv"]},
    }
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = validate_dataset(scene_path, output, max_tasks=max_tasks)
    report.update(scenario=scenario, duration_seconds=duration_seconds, runtime_feasibility_claim=False)
    (output / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest, report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scene", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, default="uniform")
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--slot-interval-seconds", type=int, default=21600)
    parser.add_argument("--active-slots", type=int)
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--max-tasks", type=int, default=250000,
                        help="explicit generated-row resource budget, not a calendar-duration cap")
    args = parser.parse_args()
    print(json.dumps(generate_scenario(args.scene, args.output, scenario=args.scenario,
          duration_seconds=args.duration_seconds, slot_interval_seconds=args.slot_interval_seconds,
          active_slots=args.active_slots, seed=args.seed, max_tasks=args.max_tasks), ensure_ascii=False, indent=2))
