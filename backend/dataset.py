"""CSV task-source support for the warehouse simulation."""

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = {
    "task_id", "robot_id", "rack_id", "slot_id", "cargo_id", "task_type", "priority",
    "release_time", "start_x", "start_y", "target_x", "target_y", "level", "bay", "depth_row",
}


def _positive_int(value: str, field: str, row_number: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: {field} must be an integer") from exc
    if result < 1:
        raise ValueError(f"row {row_number}: {field} must be positive")
    return result


def load_warehouse_tasks(path: str | Path) -> list[dict[str, Any]]:
    """Read and time-order a generator CSV without assigning simulation state."""
    source = Path(path)
    with source.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("dataset CSV has no header")
        if {"task_id", "task_type", "goods_id", "release_time", "due_time", "source_id", "target_id"}.issubset(reader.fieldnames):
            return _load_warehouse100_tasks(reader)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            raise ValueError("dataset CSV misses columns: " + ", ".join(sorted(missing)))
        records: list[dict[str, Any]] = []
        task_ids: set[str] = set()
        for row_number, row in enumerate(reader, start=2):
            if row["task_type"] != "putaway":
                raise ValueError(f"row {row_number}: only task_type=putaway is supported")
            task_id = row["task_id"]
            if not task_id or task_id in task_ids:
                raise ValueError(f"row {row_number}: task_id must be unique and non-empty")
            task_ids.add(task_id)
            try:
                release_at = datetime.fromisoformat(row["release_time"]).timestamp()
            except (TypeError, ValueError) as exc:
                raise ValueError(f"row {row_number}: invalid release_time") from exc
            records.append({
                "task_id": task_id,
                "robot_id": row["robot_id"],
                "rack_id": _positive_int(row["rack_id"], "rack_id", row_number),
                "slot_id": row["slot_id"],
                "cargo_id": row["cargo_id"],
                "priority": row["priority"] if row["priority"] in {"normal", "high", "urgent"} else "normal",
                "release_at": release_at,
                "level": _positive_int(row["level"], "level", row_number),
                "bay": _positive_int(row["bay"], "bay", row_number),
                "depth_row": _positive_int(row["depth_row"], "depth_row", row_number),
                "source": dict(row),
            })
    records.sort(key=lambda item: (item["release_at"], item["task_id"]))
    if records:
        origin = records[0]["release_at"]
        for record in records:
            record["release_offset"] = round(record["release_at"] - origin, 3)
    return records


def _load_warehouse100_tasks(reader: csv.DictReader) -> list[dict[str, Any]]:
    """Adapt the seven-field Warehouse100 demand format without losing its semantics."""
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        task_id, task_type = row["task_id"], row["task_type"]
        if not task_id or task_id in seen or task_type not in {"INBOUND", "OUTBOUND"}:
            raise ValueError(f"row {row_number}: invalid warehouse100 task")
        seen.add(task_id)
        try:
            release_at = datetime.fromisoformat(row["release_time"]).timestamp()
        except (TypeError, ValueError) as exc:
            raise ValueError(f"row {row_number}: invalid release_time") from exc
        slot_id = row["target_id"] if task_type == "INBOUND" else row["source_id"]
        match = re.search(r"R(\d+)-L(\d+)", slot_id)
        records.append({
            "task_id": task_id, "robot_id": "", "rack_id": int(match.group(1)) if match else 0,
            "slot_id": slot_id, "cargo_id": row["goods_id"], "priority": "normal", "release_at": release_at,
            "level": int(match.group(2)) if match else 1, "bay": 1, "depth_row": 1,
            "task_type": task_type, "source_id": row["source_id"], "target_id": row["target_id"], "source": dict(row),
        })
    records.sort(key=lambda item: (item["release_at"], item["task_id"]))
    if records:
        origin = records[0]["release_at"]
        for record in records:
            record["release_offset"] = round(record["release_at"] - origin, 3)
    return records


def load_initial_inventory(task_path: str | Path) -> list[dict[str, str]]:
    """Read the optional inventory beside a seven-field task CSV."""
    path = Path(task_path).with_name("initial_inventory.csv")
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if any(not row.get("goods_id") or not row.get("location_id") for row in rows):
        raise ValueError("initial_inventory.csv requires goods_id and location_id")
    return rows
