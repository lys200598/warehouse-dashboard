"""Generate design-compatible INBOUND/OUTBOUND warehouse task packages."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


TASK_FIELDS = ("task_id", "task_type", "goods_id", "release_time", "due_time", "source_id", "target_id")
INVENTORY_FIELDS = ("goods_id", "location_id")
TZ_UTC8 = timezone(timedelta(hours=8))
# Use the versioned V5 contract committed with this repository.  The former
# ``lys-warehouse-design-20261027`` directory is only a hand-off artifact and
# is intentionally not required on a fresh clone.
DEFAULT_LAYOUT = Path("data/layouts/warehouse100-wide-v2.json")
DEFAULT_LOCATIONS = Path("docs/dataset-contract/layout/task-locations.csv")
DEFAULT_OUTPUT = Path("data/generated/warehouse100-day1")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_locations(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    locations = {row["location_id"]: row for row in rows}
    if len(rows) != len(locations):
        raise ValueError("duplicate location_id")
    return locations


def validate(tasks: list[dict[str, str]], inventory: list[dict[str, str]], locations: dict[str, dict[str, str]], capacity: int) -> dict[str, Any]:
    slots = {key for key, value in locations.items() if value["kind"] == "slot"}
    inbound = {key for key, value in locations.items() if value["kind"] == "inbound"}
    outbound = {key for key, value in locations.items() if value["kind"] == "outbound"}
    stock: dict[str, str] = {}
    goods_at: dict[str, str] = {}
    for row in inventory:
        if row["location_id"] not in slots or row["location_id"] in stock or row["goods_id"] in goods_at:
            raise ValueError("invalid initial inventory")
        stock[row["location_id"]] = row["goods_id"]
        goods_at[row["goods_id"]] = row["location_id"]
    seen_tasks, seen_goods = set(), set(goods_at)
    minimum = maximum = len(stock)
    counts: Counter[str] = Counter()
    for row in sorted(tasks, key=lambda item: (item["release_time"], item["task_id"])):
        task_id, kind, goods = row["task_id"], row["task_type"], row["goods_id"]
        release, due = datetime.fromisoformat(row["release_time"]), datetime.fromisoformat(row["due_time"])
        if task_id in seen_tasks or release.tzinfo is None or due <= release:
            raise ValueError(f"invalid task: {task_id}")
        seen_tasks.add(task_id)
        if kind == "OUTBOUND":
            if row["target_id"] not in outbound or stock.get(row["source_id"]) != goods:
                raise ValueError(f"invalid outbound dependency: {task_id}")
            del stock[row["source_id"]]
            del goods_at[goods]
        elif kind == "INBOUND":
            if row["source_id"] not in inbound or row["target_id"] not in slots or row["target_id"] in stock or goods in seen_goods:
                raise ValueError(f"invalid inbound dependency: {task_id}")
            stock[row["target_id"]] = goods
            goods_at[goods] = row["target_id"]
            seen_goods.add(goods)
        else:
            raise ValueError(f"unsupported task type: {kind}")
        counts[kind] += 1
        minimum, maximum = min(minimum, len(stock)), max(maximum, len(stock))
        if len(stock) > capacity:
            raise ValueError("capacity exceeded")
    return {"valid": True, "tasks": len(tasks), "inbound": counts["INBOUND"], "outbound": counts["OUTBOUND"], "initial": len(inventory), "planned_min_inventory": minimum, "planned_max_inventory": maximum, "planned_final_inventory": len(stock)}


def generate(output_dir: Path, layout_path: Path, locations_path: Path, day: date, seed: int, cycles: int, overwrite: bool) -> dict[str, Any]:
    if cycles < 1:
        raise ValueError("cycles must be positive")
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise FileExistsError(f"output directory is not empty: {output_dir}; use --overwrite")
    layout = json.loads(layout_path.read_text(encoding="utf-8"))
    locations = load_locations(locations_path)
    slots = sorted(key for key, value in locations.items() if value["kind"] == "slot")
    in_ports = sorted(key for key, value in locations.items() if value["kind"] == "inbound")
    out_ports = sorted(key for key, value in locations.items() if value["kind"] == "outbound")
    capacity = int(layout["spec"]["slotsPerRack"]) * len(layout["racks"])
    if len(slots) != capacity or not in_ports or not out_ports:
        raise ValueError("layout and task locations are inconsistent")
    initial_slots = sorted(random.Random(seed).sample(slots, capacity // 2))
    inventory = [{"goods_id": f"INITIAL-{index:05d}", "location_id": slot} for index, slot in enumerate(initial_slots, 1)]
    start = datetime(day.year, day.month, day.day, tzinfo=TZ_UTC8)
    period, transfer_gap = 86_400 // cycles, 300
    if period <= transfer_gap:
        raise ValueError("too many cycles for one day")
    pending: list[dict[str, str]] = []
    generated_goods = 0
    for index, initial in enumerate(inventory):
        current_goods = initial["goods_id"]
        phase = index * (period - transfer_gap) // len(inventory)
        for cycle in range(cycles):
            outbound_at = cycle * period + phase
            pending.append({"task_id": "", "task_type": "OUTBOUND", "goods_id": current_goods, "release_time": (start + timedelta(seconds=outbound_at)).isoformat(timespec="seconds"), "due_time": (start + timedelta(seconds=outbound_at + 1800)).isoformat(timespec="seconds"), "source_id": initial["location_id"], "target_id": out_ports[(index + cycle) % len(out_ports)]})
            generated_goods += 1
            current_goods = f"GOODS-{day.strftime('%Y%m%d')}-{generated_goods:06d}"
            inbound_at = outbound_at + transfer_gap
            pending.append({"task_id": "", "task_type": "INBOUND", "goods_id": current_goods, "release_time": (start + timedelta(seconds=inbound_at)).isoformat(timespec="seconds"), "due_time": (start + timedelta(seconds=inbound_at + 1800)).isoformat(timespec="seconds"), "source_id": in_ports[(index + cycle) % len(in_ports)], "target_id": initial["location_id"]})
    pending.sort(key=lambda row: (row["release_time"], row["task_type"], row["goods_id"]))
    tasks = [{**row, "task_id": f"T-{index:06d}"} for index, row in enumerate(pending, 1)]
    report = validate(tasks, inventory, locations, capacity)
    output_dir.mkdir(parents=True, exist_ok=True)
    tasks_path, inventory_path = output_dir / "tasks.csv", output_dir / "initial_inventory.csv"
    for path, fields, rows in ((tasks_path, TASK_FIELDS, tasks), (inventory_path, INVENTORY_FIELDS, inventory)):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader(); writer.writerows(rows)
    manifest = {"version": "warehouse100-generated-day1-v1", "scenario": "balanced-turnover", "seed": seed, "start": start.isoformat(timespec="seconds"), "duration_seconds": 86_400, "time_unit": "ISO8601 +08:00", "scene_sha256": digest(layout_path), "tasks": len(tasks), "inbound": report["inbound"], "outbound": report["outbound"], "initial_inventory": len(inventory), "capacity": capacity, "source": "synthetic; warehouse100 seven-field task structure", "assumptions": {"cycles_per_initial_goods": cycles, "outbound_before_inbound_seconds": transfer_gap, "execution_assignment": "runtime decision; absent from tasks.csv"}, "files": {"tasks.csv": digest(tasks_path), "initial_inventory.csv": digest(inventory_path)}}
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"output_dir": str(output_dir), "manifest": manifest, "validation": report}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate warehouse100 graph input data.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--layout", type=Path, default=DEFAULT_LAYOUT)
    parser.add_argument("--locations", type=Path, default=DEFAULT_LOCATIONS)
    parser.add_argument("--day", type=date.fromisoformat, default=date(2026, 10, 27))
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--cycles", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    result = generate(args.output_dir, args.layout, args.locations, args.day, args.seed, args.cycles, args.overwrite)
    print(json.dumps({"output_dir": result["output_dir"], **result["validation"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
