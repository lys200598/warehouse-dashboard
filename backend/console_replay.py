"""Record 4173 console snapshots as immutable replay runs.

The recorder writes the same three files the standalone replay service scans:
``latest.json``, ``frames.sqlite`` and ``checkpoint.json``.  Frames are adapted
from the console snapshot schema to the wide replay page schema; the scene is
synthesized from the console's own racks and stations so robot coordinates stay
in the console's coordinate system.
"""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import sqlite3
import threading
import zlib
from typing import Any

from .checkpoint import write_checkpoint
from .domain import outbound_point


VERSION = "console-replay-v1"


def console_scene(snapshot: dict[str, Any]) -> dict[str, Any]:
    racks = list(snapshot.get("racks") or [])
    robots = list(snapshot.get("robots") or [])
    console_scene_value = snapshot.get("scene") or {}
    stations = []
    for station in console_scene_value.get("stations") or []:
        if not isinstance(station, dict):
            continue
        stations.append({
            "id": f"IN-{station.get('id', len(stations) + 1)}",
            "x": float(station.get("x", 0)),
            "y": float(station.get("y", 0)),
            "kind": "inbound",
        })
    for robot_id in range(1, max(1, len(robots)) + 1):
        point = outbound_point(robot_id)
        stations.append({"id": f"OUT-{robot_id}", "x": point["x"], "y": point["y"], "kind": "outbound"})

    max_bays = 1
    max_faces = 1
    max_levels = 1
    rack_pitch = 0.4
    mapped = []
    for rack in racks:
        slots = list(rack.get("slots") or [])
        for slot in slots:
            max_bays = max(max_bays, int(slot.get("bay", 0)) + 1)
            max_faces = max(max_faces, int(slot.get("depthRow", 0)) + 1)
            max_levels = max(max_levels, int(slot.get("level", 1)))
        width = float(rack.get("width", 2.8))
        if slots:
            rack_pitch = width / max(1, max_bays)
        mapped.append({
            "id": rack.get("id"),
            "x": float(rack.get("x", 0)),
            "y": float(rack.get("y", 0)),
            "w": width,
            "d": float(rack.get("depth", 1.15)),
            "capacity": len(slots),
        })

    xs = [rack["x"] + rack["w"] for rack in mapped] + [station["x"] for station in stations]
    ys = [rack["y"] + rack["d"] for rack in mapped] + [station["y"] for station in stations]
    inbound = console_scene_value.get("inbound") or {}
    if isinstance(inbound, dict):
        xs.append(float(inbound.get("x", 0)) + float(inbound.get("width", 0)))
        ys.append(float(inbound.get("y", 0)) + float(inbound.get("depth", 0)))
    width = max(xs or [32.0]) + 1.5
    depth = max(ys or [22.0]) + 1.5
    queue = console_scene_value.get("queueArea")
    buffers = []
    if isinstance(queue, dict):
        buffers.append({
            "x": float(queue.get("x", 0)),
            "y": float(queue.get("y", 0)),
            "w": float(queue.get("width", 0)),
            "d": float(queue.get("depth", 0)),
        })
    return {
        "spec": {
            "version": VERSION,
            "unit": "m",
            "width": width,
            "depth": depth,
            "levels": max_levels,
            "baysPerFace": max_bays,
            "faces": max_faces,
            "slotsPerRack": max_bays * max_faces * max_levels,
            "slotPitch": rack_pitch,
            "rackHeight": 1.8,
            "robotRadius": 0.4,
            "robotCount": len(robots),
        },
        "racks": mapped,
        "stations": stations,
        "buffers": buffers,
        "docks": [],
        "capacity": sum(rack["capacity"] for rack in mapped),
    }


def console_frame(snapshot: dict[str, Any]) -> dict[str, Any]:
    robots = []
    for robot in snapshot.get("robots") or []:
        robots.append({
            "robot_id": int(robot["id"]),
            "x": float(robot.get("x", 0)),
            "y": float(robot.get("y", 0)),
            "stage": str(robot.get("stage", "idle")),
            "taskType": robot.get("taskType"),
            "battery": float(robot.get("battery", 0)),
            "cargo_id": robot.get("cargoId"),
            "task_id": robot.get("taskId"),
            "carrying": bool(robot.get("carrying", False)),
            "home_id": robot.get("homeId"),
            "dock_id": robot.get("dockId"),
        })
    stages = Counter(robot["stage"] for robot in robots)
    moving = sum(stages.get(name, 0) for name in ("toQueue", "toPick", "toRack", "returning"))
    handling = sum(stages.get(name, 0) for name in ("picking", "placing"))
    charging = stages.get("charging", 0)
    queued = sum(stages.get(name, 0) for name in ("queued", "warning"))
    standby = max(0, len(robots) - moving - handling - charging - queued)
    metrics = dict(snapshot.get("metrics") or {})
    metrics["completed"] = int(snapshot.get("completed", metrics.get("completed", 0)))
    return {
        "backend": "console-4173",
        "runId": snapshot.get("runId"),
        "tick": int(snapshot.get("tick", 0)),
        "elapsed": float(snapshot.get("elapsed", 0)),
        "running": bool(snapshot.get("running", False)),
        "robots": robots,
        "counts": {
            "moving": moving,
            "handling": handling,
            "charging": charging,
            "queued": queued,
            "standby": standby,
        },
        "inside": int(metrics.get("occupiedSlots", 0)),
        "metrics": metrics,
        "knowledge": snapshot.get("knowledge"),
        "totalTasks": int(metrics.get("inProgressTasks", 0)) + int(snapshot.get("completed", 0)),
        "pending": int(metrics.get("inProgressTasks", 0)),
        "scheduler": {"release": "console-4173", "policy": "live", "parameters": {}, "planner": "legacy"},
    }


def latest_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "runId": snapshot.get("runId"),
        "tick": int(snapshot.get("tick", 0)),
        "elapsed": float(snapshot.get("elapsed", 0)),
        "running": bool(snapshot.get("running", False)),
        "knowledge": snapshot.get("knowledge"),
        "backend": "console-4173",
    }


class ConsoleFrameRecorder:
    """Thread-safe writer for one console run."""

    def __init__(self, root: Path, run_id: str, interval_seconds: float = 5.0):
        self.root = Path(root)
        self.run_id = str(run_id)
        self.interval = max(0.25, float(interval_seconds))
        self.directory = self.root / f"console-{self.run_id}"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.db_path = self.directory / "frames.sqlite"
        self._lock = threading.Lock()
        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS frames (second INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        self._db.commit()
        self.last_elapsed = None
        self.scene_written = False
        self._last_latest = None
        self.closed = False

    def due(self, elapsed: float) -> bool:
        return self.last_elapsed is None or float(elapsed) - self.last_elapsed >= self.interval

    def record(self, snapshot: dict[str, Any]) -> bool:
        if self.closed:
            return False
        elapsed = float(snapshot.get("elapsed", 0))
        with self._lock:
            if self.last_elapsed is not None and elapsed - self.last_elapsed < self.interval:
                return False
            if not self.scene_written:
                write_checkpoint(self.directory / "checkpoint.json", {
                    "runId": self.run_id,
                    "run_metadata": {"scene": console_scene(snapshot)},
                })
                self.scene_written = True
            frame = console_frame(snapshot)
            raw = zlib.compress(json.dumps(frame, ensure_ascii=False, separators=(",", ":")).encode())
            self._db.execute("INSERT OR REPLACE INTO frames(second,payload) VALUES(?,?)", (int(elapsed), raw))
            self._db.commit()
            payload = latest_payload(snapshot)
            write_checkpoint(self.directory / "latest.json", payload)
            self._last_latest = payload
            self.last_elapsed = elapsed
            return True

    def close(self) -> None:
        if self.closed:
            return
        with self._lock:
            if self._last_latest is not None:
                stopped = dict(self._last_latest)
                stopped["running"] = False
                write_checkpoint(self.directory / "latest.json", stopped)
            self._db.commit()
            self._db.close()
            self.closed = True
