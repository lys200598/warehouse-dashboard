"""Read warehouse100 packages and write their static facts to Neo4j.

The module deliberately has no Neo4j import at module load time.  This keeps
the data package validation usable on machines that have not installed the
Neo4j Python driver yet, and makes ``--dry-run`` completely offline.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA_QUERIES = (
    "CREATE CONSTRAINT warehouse_graph_warehouse_id IF NOT EXISTS FOR (n:Warehouse) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_zone_id IF NOT EXISTS FOR (n:Zone) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_rack_id IF NOT EXISTS FOR (n:Rack) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_location_id IF NOT EXISTS FOR (n:Location) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_dataset_id IF NOT EXISTS FOR (n:Dataset) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_goods_id IF NOT EXISTS FOR (n:Goods) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_graph_task_id IF NOT EXISTS FOR (n:WarehouseTask) REQUIRE n.id IS UNIQUE",
    "CREATE INDEX warehouse_graph_task_dataset_task IF NOT EXISTS FOR (n:WarehouseTask) ON (n.dataset_id, n.task_id)",
)

WRITE_WAREHOUSE = """
MERGE (warehouse:Warehouse {id: $warehouse.id})
SET warehouse += $warehouse
WITH warehouse
UNWIND $zones AS row
MERGE (zone:Zone {id: row.id})
SET zone += row
MERGE (warehouse)-[:HAS_ZONE]->(zone)
"""

WRITE_RACKS = """
UNWIND $rows AS row
MATCH (warehouse:Warehouse {id: row.warehouse_id}), (zone:Zone {id: row.zone_id})
MERGE (rack:Rack {id: row.id})
SET rack += row
MERGE (warehouse)-[:HAS_RACK]->(rack)
MERGE (zone)-[:HAS_RACK]->(rack)
"""

WRITE_LOCATIONS = """
UNWIND $rows AS row
MATCH (warehouse:Warehouse {id: row.warehouse_id})
MERGE (location:Location {id: row.id})
SET location += row
MERGE (warehouse)-[:HAS_LOCATION]->(location)
WITH location, row
RETURN count(location) AS locations
"""

LINK_LOCATION_SLOTS = """
UNWIND $rows AS row
WITH row WHERE row.rack_node_id IS NOT NULL
MATCH (rack:Rack {id: row.rack_node_id}), (location:Location {id: row.id})
MERGE (rack)-[:HAS_SLOT]->(location)
"""

WRITE_DATASET = """
MERGE (dataset:Dataset {id: $dataset.id})
SET dataset += $dataset
WITH dataset
MATCH (warehouse:Warehouse {id: $warehouse_id})
MERGE (dataset)-[:USES_LAYOUT]->(warehouse)
"""

WRITE_INVENTORY = """
UNWIND $rows AS row
MATCH (dataset:Dataset {id: $dataset_id}), (location:Location {id: row.location_node_id})
MERGE (goods:Goods {id: row.id})
SET goods += row
MERGE (dataset)-[:INCLUDES_GOODS]->(goods)
MERGE (goods)-[:INITIAL_AT]->(location)
"""

WRITE_TASKS = """
UNWIND $rows AS row
MATCH (dataset:Dataset {id: $dataset_id}),
      (source:Location {id: row.source_node_id}),
      (target:Location {id: row.target_node_id})
MERGE (goods:Goods {id: row.goods_node_id})
SET goods.dataset_id = $dataset_id, goods.goods_id = row.goods_id
MERGE (task:WarehouseTask {id: row.id})
SET task += row
MERGE (dataset)-[:INCLUDES_TASK]->(task)
MERGE (task)-[:CONCERNS]->(goods)
MERGE (task)-[:FROM]->(source)
MERGE (task)-[:TO]->(target)
"""


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _logical_boundaries(manifest: dict[str, Any]) -> dict[str, str]:
    """Return declared logical endpoints without treating them as physical ports.

    The balanced-layout simulator may receive an inbound demand at ``INBOUND``
    or an outbound demand at ``OUTBOUND``.  A physical station is selected only
    by a particular run.  Static graph import therefore persists these two
    plan endpoints as explicit non-physical Location nodes instead of forcing
    a guessed IN-n/OUT-n edge into the shared Dataset graph.
    """
    routing = manifest.get("boundary_routing")
    if routing is None:
        return {}
    if not isinstance(routing, dict) or routing.get("version") != "warehouse-logical-boundaries-v1":
        raise ValueError("unsupported boundary_routing declaration")
    values = {
        "INBOUND": routing.get("logical_inbound"),
        "OUTBOUND": routing.get("logical_outbound"),
    }
    if values != {"INBOUND": "INBOUND", "OUTBOUND": "OUTBOUND"}:
        raise ValueError("invalid logical boundary declaration")
    return {value: "inbound" if value == "INBOUND" else "outbound" for value in values.values()}


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


@dataclass(frozen=True)
class WarehouseGraphPackage:
    """Normalized records whose IDs are scoped to a layout or data package."""

    warehouse: dict[str, Any]
    zones: list[dict[str, Any]]
    racks: list[dict[str, Any]]
    locations: list[dict[str, Any]]
    dataset: dict[str, Any]
    inventory: list[dict[str, Any]]
    tasks: list[dict[str, Any]]

    def summary(self) -> dict[str, int]:
        kinds = Counter(item["kind"] for item in self.locations)
        task_types = Counter(item["task_type"] for item in self.tasks)
        return {
            "zones": len(self.zones), "racks": len(self.racks), "locations": len(self.locations),
            "slots": kinds["slot"], "stations": len(self.locations) - kinds["slot"],
            "initial_goods": len(self.inventory), "tasks": len(self.tasks),
            "inbound_tasks": task_types["INBOUND"], "outbound_tasks": task_types["OUTBOUND"],
        }


def load_graph_package(package_dir: Path, layout_path: Path, locations_path: Path) -> WarehouseGraphPackage:
    """Load a generated day package and fail before any database write if invalid."""
    package_dir, layout_path, locations_path = Path(package_dir), Path(layout_path), Path(locations_path)
    manifest = json.loads((package_dir / "manifest.json").read_text(encoding="utf-8"))
    validation = json.loads((package_dir / "validation.json").read_text(encoding="utf-8"))
    if not validation.get("valid"):
        raise ValueError("package validation.json is not valid")
    layout = json.loads(layout_path.read_text(encoding="utf-8"))
    if manifest.get("scene_sha256") != _sha256(layout_path):
        raise ValueError("manifest scene_sha256 does not match layout.json")
    expected_files = manifest.get("files", {})
    for name in ("tasks.csv", "initial_inventory.csv"):
        if expected_files.get(name) != _sha256(package_dir / name):
            raise ValueError(f"manifest hash does not match {name}")
    layout_version = layout["spec"]["version"]
    warehouse_id = f"warehouse:{layout_version}"
    dataset_id = f"dataset:{manifest['version']}:{manifest['start'][:10]}"
    racks_by_id = {rack["id"]: rack for rack in layout["racks"]}
    raw_locations = _rows(locations_path)
    raw_inventory, raw_tasks = _rows(package_dir / "initial_inventory.csv"), _rows(package_dir / "tasks.csv")
    required_task_fields = {"task_id", "task_type", "goods_id", "release_time", "due_time", "source_id", "target_id"}
    if not raw_tasks or not required_task_fields.issubset(raw_tasks[0]):
        raise ValueError("tasks.csv does not use the warehouse100 seven-field schema")
    location_ids = {row["location_id"] for row in raw_locations}
    if len(location_ids) != len(raw_locations):
        raise ValueError("task-locations.csv contains duplicate location_id")
    logical_boundaries = _logical_boundaries(manifest)
    if any(row["location_id"] not in location_ids for row in raw_inventory):
        raise ValueError("initial_inventory.csv refers to an unknown location")
    for row in raw_tasks:
        source, target = row["source_id"], row["target_id"]
        source_ok = source in location_ids or (row["task_type"] == "INBOUND" and source in logical_boundaries)
        target_ok = target in location_ids or (row["task_type"] == "OUTBOUND" and target in logical_boundaries)
        if not source_ok or not target_ok:
            raise ValueError("tasks.csv refers to an unknown location")
        if source in logical_boundaries and source != "INBOUND":
            raise ValueError("logical outbound boundary cannot be an inbound source")
        if target in logical_boundaries and target != "OUTBOUND":
            raise ValueError("logical inbound boundary cannot be an outbound target")
    slots = {row["location_id"] for row in raw_locations if row["kind"] == "slot"}
    if len(slots) != int(layout["spec"]["slotsPerRack"]) * len(racks_by_id):
        raise ValueError("layout capacity does not match task-locations.csv")
    if len(raw_inventory) > len(slots) or len({row["goods_id"] for row in raw_inventory}) != len(raw_inventory):
        raise ValueError("initial_inventory.csv exceeds capacity or duplicates goods")
    if any(row["location_id"] not in slots for row in raw_inventory):
        raise ValueError("initial inventory must occupy slots")
    task_ids = {row["task_id"] for row in raw_tasks}
    if len(task_ids) != len(raw_tasks) or any(row["task_type"] not in {"INBOUND", "OUTBOUND"} for row in raw_tasks):
        raise ValueError("tasks.csv has duplicate task IDs or unsupported task types")

    warehouse = {"id": warehouse_id, "layout_version": layout_version, **layout["spec"]}
    zone_names = sorted({rack["zone"] for rack in racks_by_id.values()})
    zones = [{"id": f"{warehouse_id}:zone:{zone}", "warehouse_id": warehouse_id, "name": zone} for zone in zone_names]
    racks = [{"id": f"{warehouse_id}:rack:{rack['id']}", "warehouse_id": warehouse_id,
              "zone_id": f"{warehouse_id}:zone:{rack['zone']}", **rack} for rack in racks_by_id.values()]
    locations = []
    for row in raw_locations:
        rack_id = row.get("rack_id") or ""
        locations.append({
            "id": f"{warehouse_id}:location:{row['location_id']}", "warehouse_id": warehouse_id,
            "location_id": row["location_id"], "kind": row["kind"], "rack_id": rack_id or None,
            "rack_node_id": f"{warehouse_id}:rack:{rack_id}" if rack_id else None,
            "level": int(row["level"]) if row.get("level") else None,
            "face": int(row["face"]) if row.get("face") else None,
            "bay": int(row["bay"]) if row.get("bay") else None,
            "x": float(row["x"]), "y": float(row["y"]),
        })
    for location_id, direction in logical_boundaries.items():
        locations.append({
            "id": f"{warehouse_id}:location:{location_id}", "warehouse_id": warehouse_id,
            "location_id": location_id, "kind": "logical-boundary", "rack_id": None,
            "rack_node_id": None, "direction": direction, "physical": False,
        })
    # Neo4j node properties must be scalar values or lists, not nested maps.
    dataset = {
        "id": dataset_id,
        "warehouse_id": warehouse_id,
        "version": manifest["version"],
        "scenario": manifest["scenario"],
        "start": manifest["start"],
        "duration_seconds": manifest["duration_seconds"],
        "time_unit": manifest["time_unit"],
        "scene_sha256": manifest["scene_sha256"],
        "tasks": manifest["tasks"],
        "inbound": manifest["inbound"],
        "outbound": manifest["outbound"],
        "initial_inventory": manifest["initial_inventory"],
        "capacity": manifest["capacity"],
        "source": manifest["source"],
        "boundary_routing_json": json.dumps(manifest.get("boundary_routing", {}), ensure_ascii=False, sort_keys=True),
        "assumptions_json": json.dumps(manifest.get("assumptions", {}), ensure_ascii=False, sort_keys=True),
        "files_json": json.dumps(manifest.get("files", {}), ensure_ascii=False, sort_keys=True),
    }
    inventory = [{"id": f"{dataset_id}:goods:{row['goods_id']}", "dataset_id": dataset_id,
                  "goods_id": row["goods_id"], "location_id": row["location_id"],
                  "location_node_id": f"{warehouse_id}:location:{row['location_id']}"} for row in raw_inventory]
    tasks = [{"id": f"{dataset_id}:task:{row['task_id']}", "dataset_id": dataset_id,
              "task_id": row["task_id"], "task_type": row["task_type"], "goods_id": row["goods_id"],
              "goods_node_id": f"{dataset_id}:goods:{row['goods_id']}", "release_time": row["release_time"],
              "due_time": row["due_time"], "source_id": row["source_id"], "target_id": row["target_id"],
              "source_node_id": f"{warehouse_id}:location:{row['source_id']}",
              "target_node_id": f"{warehouse_id}:location:{row['target_id']}"} for row in raw_tasks]
    return WarehouseGraphPackage(warehouse, zones, racks, locations, dataset, inventory, tasks)


def validation_report(package: WarehouseGraphPackage) -> dict[str, Any]:
    """Counts exposed before import, suitable for CI and import audit logs."""
    summary = package.summary()
    return {
        "valid": True, "dataset_id": package.dataset["id"], "warehouse_id": package.warehouse["id"],
        **summary,
        "estimated_nodes": summary["zones"] + summary["racks"] + summary["locations"] + 2 + summary["initial_goods"] + summary["tasks"],
        "estimated_relationships": summary["zones"] + summary["racks"] * 2 + summary["locations"] * 2 + summary["slots"] + summary["initial_goods"] * 2 + summary["tasks"] * 4 + 1,
    }


def create_schema(session: Any) -> None:
    for query in SCHEMA_QUERIES:
        session.run(query).consume()


def import_graph(session: Any, package: WarehouseGraphPackage, batch_size: int = 1_000) -> dict[str, int]:
    """Write an idempotent static graph. Caller owns the Neo4j session."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    session.run(WRITE_WAREHOUSE, warehouse=package.warehouse, zones=package.zones).consume()
    for batch in _chunks(package.racks, batch_size):
        session.run(WRITE_RACKS, rows=batch).consume()
    for batch in _chunks(package.locations, batch_size):
        session.run(WRITE_LOCATIONS, rows=batch).consume()
        session.run(LINK_LOCATION_SLOTS, rows=batch).consume()
    session.run(WRITE_DATASET, dataset=package.dataset, warehouse_id=package.warehouse["id"]).consume()
    for batch in _chunks(package.inventory, batch_size):
        session.run(WRITE_INVENTORY, dataset_id=package.dataset["id"], rows=batch).consume()
    for batch in _chunks(package.tasks, batch_size):
        session.run(WRITE_TASKS, dataset_id=package.dataset["id"], rows=batch).consume()
    return package.summary()
